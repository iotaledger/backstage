use crate::{ActorError, ActorScopedRuntime, BaseRuntime, Channel, Dependencies, Sender, Service, SupervisorEvent};
use async_trait::async_trait;
/// The all-important Actor trait. This defines an Actor and what it do.
#[async_trait]
pub trait Actor<H: 'static + Sender<E> + Clone + Send + Sync, E: 'static + SupervisorEvent + Send + Sync> {
    type Dependencies: Dependencies<Self::Rt> + Send;
    type Event: 'static + Send + Sync;
    type Channel: Channel<Self::Event> + Send;
    type Rt: 'static + BaseRuntime;

    /// The main function for the actor
    async fn run<'a>(self, rt: ActorScopedRuntime<'a, Self, H, E>, deps: Self::Dependencies) -> Result<Service, ActorError>
    where
        Self: Sized;

    async fn run_then_report<'a>(self, mut rt: ActorScopedRuntime<'a, Self, H, E>, deps: Self::Dependencies) -> anyhow::Result<()>
    where
        Self: Sized,
    {
        let parent = rt.supervisor_handle().clone();
        let res = self.run(rt, deps).await;
        if let Some(mut parent) = parent {
            parent.send(E::report(res)).await;
        }
        Ok(())
    }
}
