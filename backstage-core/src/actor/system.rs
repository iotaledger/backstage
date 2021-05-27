use crate::{ActorError, BackstageRuntime, Channel, SystemRuntime};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::RwLock;

#[async_trait]
pub trait System {
    type ChildEvents: Send + Clone;
    type Dependencies;
    type Channel: Channel<Self::ChildEvents> + Send;

    /// The main function for the system
    async fn run(this: Arc<RwLock<Self>>, rt: SystemRuntime<Self>) -> Result<(), ActorError>
    where
        Self: Sized;

    async fn route(event: Self::ChildEvents, rt: &mut BackstageRuntime) -> anyhow::Result<()>
    where
        Self: Sized;
}
