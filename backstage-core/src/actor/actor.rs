use super::result::*;
use crate::{ActorScopedRuntime, BaseRuntime, Channel, Dependencies};
use async_trait::async_trait;
/// The all-important Actor trait. This defines an Actor and what it do.
#[async_trait]
pub trait Actor<Rt: BaseRuntime> {
    type Dependencies: Dependencies<Rt> + Send;
    type Event: Send + Sync;
    type Channel: Channel<Self::Event> + Send;

    /// The main function for the actor
    async fn run<'a>(self, rt: ActorScopedRuntime<'a, Self, Rt>, deps: Self::Dependencies) -> Result<(), ActorError>
    where
        Self: Sized;
}
