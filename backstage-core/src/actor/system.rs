use crate::{ActorError, Channel, RuntimeScope, SystemRuntime};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::RwLock;

#[async_trait]
pub trait System {
    type ChildEvents: Send + Clone;
    type Dependencies;
    type Channel: Channel<Self::ChildEvents> + Send;

    /// The main function for the system
    async fn run<'a>(this: Arc<RwLock<Self>>, rt: SystemRuntime<'a, Self>) -> Result<(), ActorError>
    where
        Self: Sized;
}
