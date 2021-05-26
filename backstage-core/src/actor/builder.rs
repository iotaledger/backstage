use super::{actor::Actor, service::Service};

/// An actor builder, which defines how you take some
/// input data and create an `Actor` with it.
pub trait ActorBuilder {
    /// The type of actor this builder will create
    type BuiltActor: Actor;

    /// Create the `Actor` with the builder data and a service
    /// provided by the supervisor.
    fn build(self, service: Service) -> Self::BuiltActor;
}
