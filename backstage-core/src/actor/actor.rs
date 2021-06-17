use crate::{
    ActorError, ActorScopedRuntime, BaseRuntime, Channel, Dependencies, Sender, Service, SupervisedActorScopedRuntime, SupervisorEvent,
};
use async_trait::async_trait;
/// The all-important Actor trait. This defines an Actor and what it do.
#[async_trait]
pub trait Actor {
    /// Allows specifying an actor's startup dependencies. Ex. (Act<OtherActor>, Res<MyResource>)
    type Dependencies: Dependencies<Self::Rt> + Send;
    /// The type of event this actor will receive
    type Event: 'static + Send + Sync;
    /// The type of channel this actor will use to receive events
    type Channel: Channel<Self::Event> + Send;
    /// The runtime this actor requires to function
    type Rt: 'static + BaseRuntime;
    /// An optional custom event type which can be sent to the supervisor
    type SupervisorEvent;

    /// The main function for the actor
    async fn run<'a>(self, rt: ActorScopedRuntime<'a, Self>, deps: Self::Dependencies) -> Result<Service, ActorError>
    where
        Self: Sized;

    /// Run this actor with a supervisor
    /// Note: Redefine this method if your actor requires a supervisor handle to function!
    async fn run_supervised<'a, H, E>(
        self,
        rt: SupervisedActorScopedRuntime<'a, Self, H, E>,
        deps: Self::Dependencies,
    ) -> Result<Service, ActorError>
    where
        Self: Sized,
        H: 'static + Sender<E> + Clone + Send + Sync,
        E: 'static + SupervisorEvent + Send + Sync + From<Self::SupervisorEvent>,
    {
        self.run(rt.scope, deps).await
    }

    /// Runs this actor then reports back to the supervisor with the result
    async fn run_then_report<'a, H, E>(
        self,
        mut rt: SupervisedActorScopedRuntime<'a, Self, H, E>,
        deps: Self::Dependencies,
    ) -> anyhow::Result<()>
    where
        Self: Sized,
        H: 'static + Sender<E> + Clone + Send + Sync,
        E: 'static + SupervisorEvent + Send + Sync + From<Self::SupervisorEvent>,
    {
        let mut supervisor = rt.supervisor_handle().clone();
        let res = self.run_supervised(rt, deps).await;
        supervisor.send(E::report(res)).await
    }
}
