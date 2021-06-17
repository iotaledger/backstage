use crate::{
    ActorError, Channel, Dependencies, Sender, Service, SupervisedSystemScopedRuntime, SupervisorEvent, SystemRuntime, SystemScopedRuntime,
};
use async_trait::async_trait;
use std::sync::Arc;
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
    async fn run<'a>(this: Arc<RwLock<Self>>, rt: SystemScopedRuntime<'a, Self>, deps: Self::Dependencies) -> Result<Service, ActorError>
    where
        Self: Sized;

    /// Run this system with a supervisor
    /// Note: Redefine this method if your system requires a supervisor handle to function!
    async fn run_supervised<'a, H, E>(
        this: Arc<RwLock<Self>>,
        rt: SupervisedSystemScopedRuntime<'a, Self, H, E>,
        deps: Self::Dependencies,
    ) -> Result<Service, ActorError>
    where
        Self: Send + Sync + Sized,
        H: 'static + Sender<E> + Clone + Send + Sync,
        E: 'static + SupervisorEvent + Send + Sync + From<Self::SupervisorEvent>,
    {
        Self::run(this, rt.scope, deps).await
    }

    /// Runs this system then reports back to the supervisor with the result
    async fn run_then_report<'a, H, E>(
        this: Arc<RwLock<Self>>,
        mut rt: SupervisedSystemScopedRuntime<'a, Self, H, E>,
        deps: Self::Dependencies,
    ) -> anyhow::Result<()>
    where
        Self: Send + Sync + Sized,
        H: 'static + Sender<E> + Clone + Send + Sync,
        E: 'static + SupervisorEvent + Send + Sync + From<Self::SupervisorEvent>,
    {
        let mut supervisor = rt.supervisor_handle().clone();
        let res = Self::run_supervised(this, rt, deps).await;
        supervisor.send(E::report(res)).await
    }
}
