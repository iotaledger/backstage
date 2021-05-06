use super::{actor::Actor, service::Service};
use crate::EventHandle;

/// An actor builder, which defines how you take some
/// input data and create an `Actor` with it.
pub trait ActorBuilder<A, E, S>
where
    A: Actor<E, S>,
    S: 'static + Send + EventHandle<E>,
{
    /// Create the `Actor` with the builder data and a service
    /// provided by the supervisor.
    fn build(self, service: Service) -> A;
}
