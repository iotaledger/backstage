// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use std::{
    error::Error,
    fmt,
    time::Duration,
};
/// The returned result by the actor
pub type ActorResult<T> = std::result::Result<T, ActorError>;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// Actor shutdown error reason.
pub enum ActorRequest {
    /// The actor got Aborted while working on something critical.
    Aborted,
    /// The actor exit, as it cannot do anything further.
    // example maybe the disk is full or a bind address is in-use
    Exit,
    /// The actor is asking for restart, if possible.
    Restart(Option<Duration>),
}

#[derive(Debug)]
/// The Actor Error
pub struct ActorError {
    reason: anyhow::Error,
    request: ActorRequest,
}

impl Clone for ActorError {
    fn clone(&self) -> Self {
        let reason = self.reason.to_string();
        let request = self.request.clone();
        Self {
            reason: anyhow::Error::msg(reason),
            request,
        }
    }
}

impl fmt::Display for ActorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "actor errored: {} with request {:?}", &self.reason, &self.request)
    }
}

impl Error for ActorError {}

impl ActorError {
    /// Create exit error from anyhow error
    pub fn exit<E: Into<anyhow::Error>>(error: E) -> Self {
        Self {
            reason: error.into(),
            request: ActorRequest::Exit,
        }
    }
    /// Create exit error from message
    pub fn exit_msg<E>(msg: E) -> Self
    where
        E: fmt::Display + fmt::Debug + Send + Sync + 'static,
    {
        Self {
            reason: anyhow::Error::msg(msg),
            request: ActorRequest::Exit,
        }
    }
    /// Create Aborted error from anyhow::error, note: this soft error
    pub fn aborted<E: Into<anyhow::Error>>(error: E) -> Self {
        Self {
            reason: error.into(),
            request: ActorRequest::Aborted,
        }
    }
    /// Create Aborted error from message, note: this soft error
    pub fn aborted_msg<E>(msg: E) -> Self
    where
        E: fmt::Display + fmt::Debug + Send + Sync + 'static,
    {
        Self {
            reason: anyhow::Error::msg(msg),
            request: ActorRequest::Aborted,
        }
    }
    /// Create restart error, it means the actor is asking the supervisor for restart/reschedule if possible
    pub fn restart<E: Into<anyhow::Error>, D: Into<Option<Duration>>>(error: E, after: D) -> Self {
        Self {
            reason: error.into(),
            request: ActorRequest::Restart(after.into()),
        }
    }
    /// Create restart error, it means the actor is asking the supervisor for restart/reschedule if possible
    pub fn restart_msg<E, D: Into<Option<Duration>>>(msg: E, after: D) -> Self
    where
        E: fmt::Display + fmt::Debug + Send + Sync + 'static,
    {
        Self {
            reason: anyhow::Error::msg(msg),
            request: ActorRequest::Restart(after.into()),
        }
    }
}
