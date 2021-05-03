use super::{actor::Actor, service::Service};

pub trait ActorBuilder<A>
where
    A: Actor,
{
    fn build(self, service: Service) -> A;
}
