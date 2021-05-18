use super::{actor::Actor, service::Service};
use crate::EventHandle;

/// An actor builder, which defines how you take some
/// input data and create an `Actor` with it.
pub trait ActorBuilder<A> {
    /// Create the `Actor` with the builder data and a service
    /// provided by the supervisor.
    fn build<E, S>(self, service: Service) -> A
    where
        S: 'static + Send + EventHandle<E>,
        A: Actor<E, S>;
}
