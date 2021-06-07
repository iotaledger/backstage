use crate::{ActorError, Service};

pub trait SupervisorEvent {
    fn report(res: Result<Service, ActorError>) -> Self;

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
