use super::result::*;
use crate::{ActorRuntime, Channel, Dependencies};
use async_trait::async_trait;
/// The all-important Actor trait. This defines an Actor and what it do.
#[async_trait]
pub trait Actor {
    type Dependencies: Dependencies + Send;
    type Event: Clone + Send;
    type Channel: Channel<Self::Event> + Send;

    /// The main function for the actor
    async fn run<'a>(self, rt: ActorRuntime<'a, Self>, deps: Self::Dependencies) -> Result<(), ActorError>
    where
        Self: Sized;
}
