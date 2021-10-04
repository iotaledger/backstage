// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::Actor;
use anyhow::anyhow;
use std::{
    error::Error,
    fmt::{Debug, Display},
    ops::Deref,
    time::Duration,
};
use thiserror::Error;

/// The returned result by the actor
pub type ActorResult<T> = std::result::Result<T, ActorError>;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// Actor shutdown error reason.
pub enum ActorRequest {
    /// Actor wants to restart after this run
    Restart(Option<Duration>),
    /// Actor is done and can be removed from the service
    Finish,
    /// Something unrecoverable happened and we should actually
    /// shut down everything and notify the user
    Panic,
}

/// An actor's error which contains an optional request for the supervisor
#[derive(Error, Debug)]
#[error("Source: {source}, request: {request:?}")]
pub struct ActorError {
    pub source: anyhow::Error,
    pub request: Option<ActorRequest>,
}

impl Default for ActorError {
    fn default() -> Self {
        Self {
            source: anyhow::anyhow!("An unknown error occurred!"),
            request: None,
        }
    }
}

pub trait ActorResultExt {
    type Error;
    fn err_request(self, request: ActorRequest) -> ActorResult<Self::Error>
    where
        Self: Sized;
}

impl<T> ActorResultExt for anyhow::Result<T> {
    type Error = T;
    fn err_request(self, request: ActorRequest) -> ActorResult<T>
    where
        Self: Sized,
    {
        match self {
            Err(source) => Err(ActorError {
                source,
                request: request.into(),
            }),
            Ok(t) => Ok(t),
        }
    }
}

impl From<anyhow::Error> for ActorError {
    fn from(source: anyhow::Error) -> Self {
        Self { source, request: None }
    }
}

impl Clone for ActorError {
    fn clone(&self) -> Self {
        Self {
            source: anyhow::anyhow!(self.source.to_string()),
            request: self.request.clone(),
        }
    }
}

impl From<std::borrow::Cow<'static, str>> for ActorError {
    fn from(cow: std::borrow::Cow<'static, str>) -> Self {
        ActorError {
            source: anyhow!(cow),
            request: ActorRequest::Panic.into(),
        }
    }
}

impl From<()> for ActorError {
    fn from(_: ()) -> Self {
        ActorError {
            source: anyhow!("Error!"),
            request: ActorRequest::Finish.into(),
        }
    }
}

impl<S: Actor> From<InitError<S>> for ActorError {
    fn from(e: InitError<S>) -> Self {
        e.1.into()
    }
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
