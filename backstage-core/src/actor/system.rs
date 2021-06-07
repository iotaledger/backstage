use crate::{ActorError, Channel, Dependencies, Sender, Service, SupervisorEvent, SystemRuntime, SystemScopedRuntime};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::RwLock;

#[async_trait]
pub trait System<H: 'static + Sender<E> + Clone + Send + Sync, E: 'static + SupervisorEvent + Send + Sync> {
    type ChildEvents: 'static + Send + Sync;
    type Dependencies: Dependencies<Self::Rt> + Send;
    type Channel: Channel<Self::ChildEvents> + Send;
    type Rt: 'static + SystemRuntime;

    /// The main function for the system
    async fn run<'a>(
        this: Arc<RwLock<Self>>,
        rt: SystemScopedRuntime<'a, Self, H, E>,
        deps: Self::Dependencies,
    ) -> Result<Service, ActorError>
    where
        Self: Sized;

    async fn run_then_report<'a>(
        this: Arc<RwLock<Self>>,
        mut rt: SystemScopedRuntime<'a, Self, H, E>,
        deps: Self::Dependencies,
    ) -> anyhow::Result<()>
    where
        Self: Send + Sync + Sized,
    {
        let parent = rt.supervisor_handle().clone();
        let res = Self::run(this, rt, deps).await;
        if let Some(mut parent) = parent {
            parent.send(E::report(res)).await;
        }
        Ok(())
    }
}
