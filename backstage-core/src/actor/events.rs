// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::{ActorError, Service};
use std::{borrow::Cow, marker::PhantomData};

/// A report that an actor finished running successfully
#[derive(Clone, Debug)]
pub struct SuccessReport<T> {
    /// The actor's state when it finished running
    pub state: T,
    /// The actor's service when it finished running
    pub service: Service,
}

impl<T> SuccessReport<T> {
    pub(crate) fn new(state: T, service: Service) -> Self {
        Self { state, service }
    }
}

/// A report that an actor finished running with an error
#[derive(Debug)]
pub struct ErrorReport<T> {
    /// The actor's state when it finished running
    pub state: T,
    /// The actor's service when it finished running
    pub service: Service,
    /// The error that occurred
    pub error: ActorError,
}

impl<T> ErrorReport<T> {
    pub(crate) fn new(state: T, service: Service, error: ActorError) -> Self {
        Self { state, service, error }
    }
}

/// A status change notification
#[derive(Clone, Debug)]
pub struct StatusChange<T> {
    /// The actor's previous state
    pub prev_status: Cow<'static, str>,
    /// The actor's service
    pub service: Service,
    /// The actor type enum
    pub actor_type: PhantomData<fn(T) -> T>,
}

impl<T> StatusChange<T> {
    pub(crate) fn new(prev_status: Cow<'static, str>, service: Service) -> Self {
        Self {
            prev_status,
            service,
            actor_type: PhantomData,
        }
    }
}
