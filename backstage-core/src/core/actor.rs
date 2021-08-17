use super::{rt::Rt, ActorResult, Channel, Supervise};
use async_trait::async_trait;
use std::borrow::Cow;

/// The all-important Actor trait. This defines an Actor and what it do.
#[async_trait]
pub trait Actor: Sized + Send + Sync + 'static {
    /// Allows specifying an actor's startup dependencies. Ex. (Act<OtherActor>, Res<MyResource>)
    type Deps: Clone + Send + Sync + 'static = ();
    type Context<S: super::Supervise<Self>>: Send + 'static + Sync = Rt<Self, S>;
    /// The type of channel this actor will use to receive events
    type Channel: Channel;
    /// Used to initialize the actor.
    async fn init<S>(&mut self, rt: &mut Self::Context<S>) -> Result<Self::Deps, super::Reason>
    where
        S: super::Supervise<Self>;
    /// The main function for the actor
    async fn run<S>(&mut self, rt: &mut Self::Context<S>, deps: Self::Deps) -> ActorResult
    where
        S: super::Supervise<Self>;
    /// Get this actor's name
    fn type_name() -> Cow<'static, str> {
        std::any::type_name::<Self>().into()
    }
}

/// Shutdown contract , should be implemented on the handle
#[async_trait::async_trait]
pub trait Shutdown: Send + 'static + Sync {
    async fn shutdown(&self);
    fn scope_id(&self) -> super::ScopeId;
}

pub trait ShutdownEvent: Send {
    fn shutdown_event() -> Self;
}

// Null supervisor
pub struct NullSupervisor;
#[async_trait::async_trait]
impl<T: Send> super::Supervise<T> for NullSupervisor {
    // End of life for Actor of type T, invoked on shutdown.
    async fn eol(self, _scope_id: super::ScopeId, _service: super::Service, _actor: T, r: super::ActorResult) -> Option<()>
    where
        T: super::Actor,
    {
        Some(())
    }
}

#[async_trait::async_trait]
impl<T: Send> super::Report<T, super::Service> for NullSupervisor {
    /// Report any status & service changes
    async fn report(&self, scope_id: super::ScopeId, _service: super::Service) -> Option<()> {
        Some(())
    }
}

// test
#[cfg(test)]
mod tests {
    use crate::core::{Actor, ActorResult, IntervalChannel, Reason};
    use futures::stream::StreamExt;

    struct PrintHelloEveryFewMs;
    #[async_trait::async_trait]
    impl Actor for PrintHelloEveryFewMs {
        type Channel = IntervalChannel<100>;
        async fn init<S>(&mut self, rt: &mut Self::Context<S>) -> Result<Self::Deps, Reason>
        where
            S: super::Supervise<Self>,
        {
            Ok(())
        }
        async fn run<S>(&mut self, rt: &mut Self::Context<S>, deps: Self::Deps) -> ActorResult
        where
            S: super::Supervise<Self>,
        {
            while let Some(_) = rt.inbox_mut().next().await {
                println!("HelloWorld")
            }
            Ok(())
        }
    }
}
