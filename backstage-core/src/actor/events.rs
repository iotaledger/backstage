use crate::{ActorError, Service};

/// Defines an event's ability to be sent to a supervisor as either a report or status update
pub trait SupervisorEvent {
    /// Create a supervisor event from an actor's result
    fn report(res: Result<Service, ActorError>) -> Self;

    /// Create a supervisor event from an actor's service
    fn status(service: Service) -> Self;
}

impl SupervisorEvent for () {
    fn report(_res: Result<Service, ActorError>) -> Self {
        ()
    }

    fn status(_service: Service) -> Self {
        ()
    }
}
