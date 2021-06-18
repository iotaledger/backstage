use crate::{ActorError, ActorScopedRuntime, BaseRuntime, Channel, Dependencies, Sender, SupervisedActorScopedRuntime, SupervisorEvent};
use async_trait::async_trait;
use std::borrow::Cow;
/// The all-important Actor trait. This defines an Actor and what it do.
#[async_trait]
pub trait Actor {
    /// Allows specifying an actor's startup dependencies. Ex. (Act<OtherActor>, Res<MyResource>)
    type Dependencies: Send + Sync;
    /// The type of event this actor will receive
    type Event: 'static + Send + Sync;
    /// The type of channel this actor will use to receive events
    type Channel: Channel<Self::Event> + Send;
    /// The runtime this actor requires to function
    type Rt: 'static + BaseRuntime;
    /// An optional custom event type which can be sent to the supervisor
    type SupervisorEvent;

    /// The main function for the actor
    async fn run<'a>(&mut self, rt: &mut ActorScopedRuntime<'a, Self>, deps: Self::Dependencies) -> Result<(), ActorError>
    where
        Self: Sized;

    /// Run this actor with a supervisor
    /// Note: Redefine this method if your actor requires a supervisor handle to function!
    async fn run_supervised<'a, H, E>(
        &mut self,
        rt: &mut SupervisedActorScopedRuntime<'a, Self, H, E>,
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
}
