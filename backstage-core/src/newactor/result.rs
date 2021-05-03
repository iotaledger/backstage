use anyhow::anyhow;
use std::time::Duration;
use thiserror::Error;
#[derive(Error, Debug)]
pub enum ActorError {
    #[error("Invalid data was given to the actor: {0}!")]
    InvalidData(String),
    #[error("The actor experienced a runtime error!")]
    RuntimeError(ActorRequest),
    #[error("Actor Error: {source}")]
    Other { source: anyhow::Error, request: ActorRequest },
}

impl ActorError {
    pub fn request(&self) -> &ActorRequest {
        match self {
            ActorError::InvalidData(_) => &ActorRequest::Panic,
            ActorError::RuntimeError(r) => r,
            ActorError::Other { source: _, request: r } => r,
        }
    }
}

impl From<std::borrow::Cow<'static, str>> for ActorError {
    fn from(cow: std::borrow::Cow<'static, str>) -> Self {
        ActorError::Other {
            source: anyhow!(cow),
            request: ActorRequest::Panic,
        }
    }
}

/// Possible requests an actor can make to its supervisor
#[derive(Debug, Clone)]
pub enum ActorRequest {
    /// Actor wants to restart after this run
    Restart,
    /// Actor wants to schedule a restart sometime in the future
    Reschedule(Duration),
    /// Actor is done and can be removed from the service
    Finish,
    /// Something unrecoverable happened and we should actually
    /// shut down everything and notify the user
    Panic,
}
