use super::result::*;
use crate::{ActorRuntime, Channel};
use async_trait::async_trait;
/// The all-important Actor trait. This defines an Actor and what it do.
#[async_trait]
pub trait Actor {
    type Dependencies;
    type Event: Send;
    type Channel: Channel<Self::Event> + Send;

    /// The main function for the actor
    async fn run(self, rt: ActorRuntime<Self>) -> Result<(), ActorError>
    where
        Self: Sized;
}
