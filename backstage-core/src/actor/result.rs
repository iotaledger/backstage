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

#[derive(Error, Debug)]
#[error("Source: {source}, request: {request:?}")]
pub struct ActorError {
    pub source: anyhow::Error,
    pub request: Option<ActorRequest>,
}

impl ActorError {
    /// Get the request from the error
    pub fn request(&self) -> &Option<ActorRequest> {
        &self.request
    }
}

impl From<std::borrow::Cow<'static, str>> for ActorError {
    fn from(cow: std::borrow::Cow<'static, str>) -> Self {
        ActorError {
            source: anyhow!(cow),
            request: None,
        }
    }
}

impl From<()> for ActorError {
    fn from(_: ()) -> Self {
        ActorError {
            source: anyhow!("Error!"),
            request: None,
        }
    }
}

impl From<anyhow::Error> for ActorError {
    fn from(e: anyhow::Error) -> Self {
        ActorError {
            source: e,
            request: None,
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
    /// Actor wants to restart after this run, optionally after a delay
    Restart(Option<Duration>),
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
