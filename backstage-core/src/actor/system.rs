use std::sync::Arc;

use crate::{ActorError, BackstageRuntime, Channel};
use async_trait::async_trait;
use tokio::sync::RwLock;

#[async_trait]
pub trait System {
    type ChildEvents;
    type Dependencies;
    type Event: Send;
    type Channel: Channel<Self::Event> + Send;

    /// The main function for the system
    async fn run(this: Arc<RwLock<Self>>, rt: BackstageRuntime) -> Result<(), ActorError>;

    fn route(event: Self::ChildEvents);
}
