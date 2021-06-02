use crate::{ActorError, Channel, Dependencies, SystemRuntime};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::RwLock;

#[async_trait]
pub trait System {
    type ChildEvents: Send + Sync;
    type Dependencies: Dependencies + Send;
    type Channel: Channel<Self::ChildEvents> + Send;

    /// The main function for the system
    async fn run<'a>(this: Arc<RwLock<Self>>, rt: SystemRuntime<'a, Self>, deps: Self::Dependencies) -> Result<(), ActorError>
    where
        Self: Sized;
}
