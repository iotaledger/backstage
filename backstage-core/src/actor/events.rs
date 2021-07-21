use super::{Actor, ActorError, EventDriven, Service};
use std::{borrow::Cow, marker::PhantomData};
#[derive(Debug)]
pub struct SuccessReport<T> {
    pub state: T,
    pub service: Service,
}

impl<T> SuccessReport<T> {
    pub fn new(state: T, service: Service) -> Self {
        Self { state, service }
    }
}

#[derive(Debug)]
pub struct ErrorReport<T> {
    pub state: T,
    pub service: Service,
    pub error: ActorError,
}

impl<T> ErrorReport<T> {
    pub fn new(state: T, service: Service, error: ActorError) -> Self {
        Self { state, service, error }
    }
}
#[derive(Debug)]
pub struct StatusChange<T> {
    pub prev_status: Cow<'static, str>,
    pub service: Service,
    pub actor_type: T,
}

impl<T> StatusChange<T> {
    pub fn new(actor_type: T, prev_status: Cow<'static, str>, service: Service) -> Self {
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
    type ChildStates: 'static + Send + Sync;
    type Children: 'static + Send + Sync;

    fn report(res: Result<SuccessReport<Self::ChildStates>, ErrorReport<Self::ChildStates>>) -> Self
    where
        Self: Sized;

    fn report_ok(success: SuccessReport<Self::ChildStates>) -> Self
    where
        Self: Sized,
    {
        Self::report(Ok(success))
    }

    fn report_err(err: ErrorReport<Self::ChildStates>) -> Self
    where
        Self: Sized,
    {
        Self::report(Err(err))
    }

    fn status_change(status_change: StatusChange<Self::Children>) -> Self
    where
        Self: Sized;
}

impl EventDriven for () {
    type Event = ();

    type Channel = ();
}

pub struct NullChildStates;
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
