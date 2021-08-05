use super::Actor;
use anyhow::anyhow;
use std::{
    error::Error,
    fmt::{Debug, Display},
    ops::Deref,
    time::Duration,
};
use thiserror::Error;

/// Potential actor errors
#[derive(Error, Debug)]
pub enum ActorError {
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

impl ActorError {
    /// Get the request from the error
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

impl From<()> for ActorError {
    fn from(_: ()) -> Self {
        ActorError::Other {
            source: anyhow!("Error!"),
            request: ActorRequest::Finish,
        }
    }
}

impl From<anyhow::Error> for ActorError {
    fn from(e: anyhow::Error) -> Self {
        ActorError::Other {
            source: e,
            request: ActorRequest::Panic,
        }
    }
}

impl<S: Actor> From<InitError<S>> for ActorError {
    fn from(e: InitError<S>) -> Self {
        e.1.into()
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

/// Synchronous error from initializing an actor.
/// Contains the actor's state at the time of the error.
pub struct InitError<S>(pub S, pub anyhow::Error);

impl<S> InitError<S> {
    /// Access the actor's state
    pub fn state(&self) -> &S {
        &self.0
    }

    /// Access the error
    pub fn error(&self) -> &anyhow::Error {
        &self.1
    }
}

impl<S> Deref for InitError<S> {
    type Target = anyhow::Error;

    fn deref(&self) -> &Self::Target {
        self.error()
    }
}

impl<S: Actor> From<(S, anyhow::Error)> for InitError<S> {
    fn from((state, err): (S, anyhow::Error)) -> Self {
        Self(state, err)
    }
}

impl<S: Actor> Display for InitError<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.1, f)
    }
}

impl<S: Actor> Debug for InitError<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.1, f)
    }
}

impl<S: Actor> Error for InitError<S> {}
