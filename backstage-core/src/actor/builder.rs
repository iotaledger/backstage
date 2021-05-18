use super::{actor::Actor, service::Service};
use crate::EventHandle;

/// An actor builder, which defines how you take some
/// input data and create an `Actor` with it.
pub trait ActorBuilder {
    /// The type of actor this builder will create
    type BuiltActor;

    /// Create the `Actor` with the builder data and a service
    /// provided by the supervisor.
    fn build<E, S>(self, service: Service) -> Self::BuiltActor
    where
        S: 'static + Send + EventHandle<E>,
        Self::BuiltActor: Actor<E, S>;
}
