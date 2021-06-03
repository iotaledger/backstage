use crate::{ActorError, Channel, Dependencies, SystemRuntime, SystemScopedRuntime};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::RwLock;

#[async_trait]
pub trait System<Rt: SystemRuntime> {
    type ChildEvents: Send + Sync;
    type Dependencies: Dependencies<Rt> + Send;
    type Channel: Channel<Self::ChildEvents> + Send;

    /// The main function for the system
    async fn run<'a>(this: Arc<RwLock<Self>>, rt: SystemScopedRuntime<'a, Self, Rt>, deps: Self::Dependencies) -> Result<(), ActorError>
    where
        Self: Sized;
}
