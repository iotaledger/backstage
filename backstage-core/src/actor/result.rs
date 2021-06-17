use crate::Service;
use anyhow::anyhow;
use std::time::Duration;
use thiserror::Error;

/// An actor (or system)'s error result
#[derive(Debug)]
pub struct ActorError {
    /// The kind of error that occurred
    pub kind: ActorErrorKind,
    /// The actor (or system)'s service
    pub service: Service,
}

impl ActorError {
    /// Create a new actor error from an error kind
    pub fn new(kind: ActorErrorKind, service: Service) -> Self {
        Self { kind, service }
    }
}

/// Potential actor errors
#[derive(Error, Debug)]
pub enum ActorErrorKind {
    /// Indicates that the data provided to the actor was bad
    #[error("Invalid data was given to the actor: {0}!")]
    InvalidData(String),
    /// Indicates that a runtime error occurred
    #[error("The actor experienced a runtime error!")]
    RuntimeError(ActorRequest),
    /// Anything else that can go wrong
    #[error("Actor Error: {source}")]
    Other {
        /// The source of this error
        source: anyhow::Error,
        /// Indicates how the supervisor should handle this error
        request: ActorRequest,
    },
}

impl ActorErrorKind {
    /// Get the request from the error
    pub fn request(&self) -> &ActorRequest {
        match self {
            ActorErrorKind::InvalidData(_) => &ActorRequest::Panic,
            ActorErrorKind::RuntimeError(r) => r,
            ActorErrorKind::Other { source: _, request: r } => r,
        }
    }
}

impl From<std::borrow::Cow<'static, str>> for ActorErrorKind {
    fn from(cow: std::borrow::Cow<'static, str>) -> Self {
        ActorErrorKind::Other {
            source: anyhow!(cow),
            request: ActorRequest::Panic,
        }
    }
}

impl From<()> for ActorErrorKind {
    fn from(_: ()) -> Self {
        ActorErrorKind::Other {
            source: anyhow!("Error!"),
            request: ActorRequest::Finish,
        }
    }
}

impl From<anyhow::Error> for ActorErrorKind {
    fn from(e: anyhow::Error) -> Self {
        ActorErrorKind::Other {
            source: e,
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
