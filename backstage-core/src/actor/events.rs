// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::{Actor, ActorError, EventDriven, Service};
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
#[derive(Clone, Debug)]
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
    pub actor_type: T,
}

impl<T> StatusChange<T> {
    pub(crate) fn new(actor_type: T, prev_status: Cow<'static, str>, service: Service) -> Self {
        Self {
            prev_status,
            service,
            actor_type,
        }
    }
}

/// Specifies the types that children of this supervisor will be converted to
/// upon reporting an exit or a status change.
pub trait SupervisorEvent {
    /// The enumerated states of this supervisor's children
    type ChildStates: 'static + Send + Sync;
    /// The enumerated child types of this supervisor
    type Children: 'static + Send + Sync;

    /// Report a child's exit
    fn report(res: Result<SuccessReport<Self::ChildStates>, ErrorReport<Self::ChildStates>>) -> Self
    where
        Self: Sized;

    /// Report a child's successful exit
    fn report_ok(success: SuccessReport<Self::ChildStates>) -> Self
    where
        Self: Sized,
    {
        Self::report(Ok(success))
    }

    /// Report a child's exit with an error
    fn report_err(err: ErrorReport<Self::ChildStates>) -> Self
    where
        Self: Sized,
    {
        Self::report(Err(err))
    }

    /// Report a child's status change
    fn status_change(status_change: StatusChange<Self::Children>) -> Self
    where
        Self: Sized;
}

impl EventDriven for () {
    type Event = ();

    type Channel = ();
}

#[allow(missing_docs)]
pub struct NullChildStates;

#[allow(missing_docs)]
pub struct NullChildren;

impl SupervisorEvent for () {
    type ChildStates = NullChildStates;

    type Children = NullChildren;

    fn report(_res: Result<SuccessReport<Self::ChildStates>, ErrorReport<Self::ChildStates>>) -> Self
    where
        Self: Sized,
    {
    }

    fn status_change(_status_change: StatusChange<Self::Children>) -> Self
    where
        Self: Sized,
    {
    }
}

impl<T: Actor> From<T> for NullChildStates {
    fn from(_: T) -> Self {
        NullChildStates
    }
}

impl<T> From<PhantomData<T>> for NullChildren {
    fn from(_: PhantomData<T>) -> Self {
        NullChildren
    }
}
