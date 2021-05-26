use crate::{ActorError, Channel, SystemRuntime};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::RwLock;

#[async_trait]
pub trait System {
    type ChildEvents;
    type Dependencies;
    type Event: Send;
    type Channel: Channel<Self::Event> + Send;

    /// The main function for the system
    async fn run(this: Arc<RwLock<Self>>, rt: SystemRuntime<Self>) -> Result<(), ActorError>
    where
        Self: Sized;

    fn route(event: Self::ChildEvents);
}
