use super::{ActorError, Channel, Dependencies, Sender, SupervisorEvent};
use crate::{
    prelude::RegistryAccess,
    runtime::{Registry, RuntimeScope, SupervisedSystemScopedRuntime, SystemScopedRuntime},
};
use async_trait::async_trait;
use futures::{
    future::{AbortHandle, Abortable},
    FutureExt,
};
use std::{borrow::Cow, fmt::Debug, panic::AssertUnwindSafe, sync::Arc};
use tokio::sync::{oneshot, RwLock};

/// A system is effectively a shared actor which groups other systems and actors and obfuscates them.
/// Systems route incoming messages to their children and may provide a public API over their shared state.
#[async_trait]
pub trait System {
    /// Allows specifying a system's startup dependencies. Ex. (Sys<OtherSystem>, Res<MyResource>)
    type Dependencies: Dependencies + Send + Sync;
    /// The type of event this system will receive and forward to its children
    type ChildEvents: 'static + Send + Sync;
    /// The type of channel this system will use to receive events
    type Channel: Channel<Self::ChildEvents> + Send;
    /// An optional custom event type which can be sent to the supervisor
    type SupervisorEvent;

    /// The main function for the system
    async fn run<'a, Reg: RegistryAccess + Send + Sync>(
        this: Arc<RwLock<Self>>,
        rt: &mut SystemScopedRuntime<'a, Self, Reg>,
        deps: Self::Dependencies,
    ) -> Result<(), ActorError>
    where
        Self: Sized;

    /// Run this system with a supervisor
    /// Note: Redefine this method if your system requires a supervisor handle to function!
    async fn run_supervised<'a, Reg: RegistryAccess + Send + Sync, H, E>(
        this: Arc<RwLock<Self>>,
        rt: &mut SupervisedSystemScopedRuntime<'a, Self, Reg, H, E>,
        deps: Self::Dependencies,
    ) -> Result<(), ActorError>
    where
        Self: Send + Sync + Sized,
        H: 'static + Sender<E> + Clone + Send + Sync,
        E: 'static + SupervisorEvent<Arc<RwLock<Self>>> + Send + Sync + From<Self::SupervisorEvent>,
    {
        Self::run(this, &mut rt.scope, deps).await
    }

    /// Get this system's name
    fn name() -> Cow<'static, str> {
        std::any::type_name::<Self>().into()
    }

    /// Start with this system as the root scope
    async fn start_as_root<Reg: 'static + RegistryAccess + Send + Sync>(self) -> anyhow::Result<()>
    where
        Self: 'static + Sized + Send + Sync,
    {
        let mut scope = Reg::instantiate(Self::name()).await;
        let system = Arc::new(RwLock::new(self));
        scope.add_data(system.clone()).await;
        let (sender, receiver) = <Self::Channel as Channel<Self::ChildEvents>>::new();
        scope.add_data(sender.clone()).await;
        let (oneshot_send, oneshot_recv) = oneshot::channel::<()>();
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        scope.shutdown_handles_mut().push((Some(oneshot_send), abort_handle.clone()));
        let deps = Self::Dependencies::instantiate(&mut scope)
            .await
            .map_err(|e| anyhow::anyhow!("Cannot spawn system {}: {}", std::any::type_name::<Self>(), e))
            .unwrap();
        let res = {
            let mut system_rt = SystemScopedRuntime::unsupervised(&mut scope, receiver, oneshot_recv);
            Abortable::new(
                AssertUnwindSafe(Self::run(system, &mut system_rt, deps)).catch_unwind(),
                abort_registration,
            )
            .await
        };
        RuntimeScope::handle_res_unsupervised::<Self>(res, &mut scope).await
    }
}
