use crate::{
    ActorError, Channel, Dependencies, Sender, SupervisedSystemScopedRuntime, SupervisorEvent, SystemRuntime, SystemScopedRuntime,
};
use async_trait::async_trait;
use std::{borrow::Cow, sync::Arc};
use tokio::sync::RwLock;

/// A system is effectively a shared actor which groups other systems and actors and obfuscates them.
/// Systems route incoming messages to their children and may provide a public API over their shared state.
#[async_trait]
pub trait System {
    /// Allows specifying a system's startup dependencies. Ex. (Sys<OtherSystem>, Res<MyResource>)
    type Dependencies: Dependencies<Self::Rt> + Send;
    /// The type of event this system will receive and forward to its children
    type ChildEvents: 'static + Send + Sync;
    /// The type of channel this system will use to receive events
    type Channel: Channel<Self::ChildEvents> + Send;
    /// The runtime this system requires to function
    type Rt: 'static + SystemRuntime;
    /// An optional custom event type which can be sent to the supervisor
    type SupervisorEvent;

    /// The main function for the system
    async fn run<'a>(this: Arc<RwLock<Self>>, rt: &mut SystemScopedRuntime<'a, Self>, deps: Self::Dependencies) -> Result<(), ActorError>
    where
        Self: Sized;

    /// Run this system with a supervisor
    /// Note: Redefine this method if your system requires a supervisor handle to function!
    async fn run_supervised<'a, H, E>(
        this: Arc<RwLock<Self>>,
        rt: &mut SupervisedSystemScopedRuntime<'a, Self, H, E>,
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
}
