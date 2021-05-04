use super::{actor::Actor, service::Service};

/// An actor builder, which defines how you take some
/// input data and create an `Actor` with it.
pub trait ActorBuilder<A>: Clone
where
    A: Actor,
{
    /// Create the `Actor` with the builder data and a service
    /// provided by the supervisor.
    fn build(self, service: Service) -> A;
}
