use crate::{ActorError, Service};

/// Defines an event's ability to be sent to a supervisor as either a report or status update
pub trait SupervisorEvent<T> {
    /// Create a supervisor event from an actor's result
    fn report(res: Result<SuccessReport<T>, ErrorReport<T>>) -> anyhow::Result<Self>
    where
        Self: Sized;

    fn report_ok(success: SuccessReport<T>) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Self::report(Ok(success))
    }

    fn report_err(err: ErrorReport<T>) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Self::report(Err(err))
    }

    /// Create a supervisor event from an actor's service
    fn status(service: Service) -> Self;
}

impl<T> SupervisorEvent<T> for () {
    fn report(_res: Result<SuccessReport<T>, ErrorReport<T>>) -> anyhow::Result<Self> {
        Ok(())
    }

    fn status(_service: Service) -> Self {
        ()
    }
}

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
