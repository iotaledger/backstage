use super::{ActorError, Channel, Dependencies, Sender, SupervisorEvent};
use crate::{
    prelude::RegistryAccess,
    runtime::{ActorScopedRuntime, RuntimeScope, SupervisedActorScopedRuntime},
};
use async_trait::async_trait;
use futures::{
    future::{AbortHandle, Abortable},
    FutureExt,
};
use std::{borrow::Cow, fmt::Debug, panic::AssertUnwindSafe};
use tokio::sync::oneshot;
/// The all-important Actor trait. This defines an Actor and what it do.
#[async_trait]
pub trait Actor {
    /// Allows specifying an actor's startup dependencies. Ex. (Act<OtherActor>, Res<MyResource>)
    type Dependencies: Dependencies + Send + Sync;
    /// The type of event this actor will receive
    type Event: 'static + Send + Sync;
    /// The type of channel this actor will use to receive events
    type Channel: Channel<Self::Event> + Send;
    /// An optional custom event type which can be sent to the supervisor
    type SupervisorEvent;

    /// The main function for the actor
    async fn run<'a, Reg: RegistryAccess + Send + Sync>(
        &mut self,
        rt: &mut ActorScopedRuntime<'a, Self, Reg>,
        deps: Self::Dependencies,
    ) -> Result<(), ActorError>
    where
        Self: Sized;

    /// Run this actor with a supervisor
    /// Note: Redefine this method if your actor requires a supervisor handle to function!
    async fn run_supervised<'a, Reg: RegistryAccess + Send + Sync, H, E>(
        &mut self,
        rt: &mut SupervisedActorScopedRuntime<'a, Self, Reg, H, E>,
        deps: Self::Dependencies,
    ) -> Result<(), ActorError>
    where
        Self: Sized,
        H: 'static + Sender<E> + Clone + Send + Sync,
        E: 'static + SupervisorEvent<Self> + Send + Sync + From<Self::SupervisorEvent>,
    {
        self.run(&mut rt.scope, deps).await
    }

    /// Get this actor's name
    fn name() -> Cow<'static, str> {
        std::any::type_name::<Self>().into()
    }

    /// Start with this actor as the root scope
    async fn start_as_root<Reg: 'static + RegistryAccess + Send + Sync>(mut self) -> anyhow::Result<()>
    where
        Self: Sized,
    {
        let mut scope = Reg::instantiate(Self::name()).await;
        let (sender, receiver) = <Self::Channel as Channel<Self::Event>>::new();
        scope.add_data(sender.clone()).await;
        let (oneshot_send, oneshot_recv) = oneshot::channel::<()>();
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        scope.shutdown_handles_mut().push((Some(oneshot_send), abort_handle.clone()));
        let deps = Self::Dependencies::instantiate(&mut scope)
            .await
            .map_err(|e| anyhow::anyhow!("Cannot spawn actor {}: {}", std::any::type_name::<Self>(), e))
            .unwrap();
        let res = {
            let mut actor_rt = ActorScopedRuntime::unsupervised(&mut scope, receiver, oneshot_recv);
            Abortable::new(AssertUnwindSafe(self.run(&mut actor_rt, deps)).catch_unwind(), abort_registration).await
        };
        RuntimeScope::handle_res_unsupervised::<Self>(res, &mut scope).await
    }
}
