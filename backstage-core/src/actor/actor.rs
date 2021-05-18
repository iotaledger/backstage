use super::{
    event_handle::EventHandle,
    result::*,
    service::{Service, ServiceStatus},
};
use async_trait::async_trait;
/// The all-important Actor trait. This defines an Actor and what it do.
#[async_trait]
pub trait Actor<E, S>
where
    S: 'static + Send + EventHandle<E>,
{
    /// Get the actor's service
    fn service(&mut self) -> &mut Service;

    /// Update the actor's status.
    /// This function assumes the service is stored locally in the actor's state.
    /// If this is not the case (for instance, it is stored globally in an `Arc`),
    /// this fn definition should be overridden so that it does not use the local fn.
    async fn update_status(&mut self, status: ServiceStatus, supervisor: &mut S) {
        let service = self.service();
        service.update_status(status);
        supervisor.update_status(service.clone()).ok();
    }

    /// Initialize the actor
    async fn init(&mut self, supervisor: &mut S) -> Result<(), ActorError>;

    /// The main function for the actor
    async fn run(&mut self, supervisor: &mut S) -> Result<(), ActorError>;

    /// Handle the actor shutting down
    async fn shutdown(&mut self, status: Result<(), ActorError>, supervisor: &mut S) -> Result<ActorRequest, ActorError>;

    /// Start the actor
    async fn start(mut self, mut supervisor: S) -> Result<ActorRequest, ActorError>
    where
        Self: Send + Sized,
    {
        self.update_status(ServiceStatus::Starting, &mut supervisor).await;
        let mut res = self.init(&mut supervisor).await;
        if res.is_ok() {
            self.update_status(ServiceStatus::Running, &mut supervisor).await;
            res = self.run(&mut supervisor).await;
            self.update_status(ServiceStatus::Stopping, &mut supervisor).await;
            let res = self.shutdown(res, &mut supervisor).await;
            self.update_status(ServiceStatus::Stopped, &mut supervisor).await;
            res
        } else {
            let res = self.shutdown(res, &mut supervisor).await;
            self.update_status(ServiceStatus::Stopped, &mut supervisor).await;
            res
        }
    }
}

/// A split-trait version of the `Actor` definition. Implementors of
/// this trait along with `Run`, `Init` and `Shutdown` will blanket impl `Actor`.
pub trait ActorTypes {
    /// Get the actor's service
    fn service(&mut self) -> &mut Service;
}

/// A split-trait version of the `Actor` init definition. Implementors of
/// this trait along with `Run` and `Shutdown` will blanket impl `Actor`.
#[async_trait]
pub trait Init<E, S>: ActorTypes
where
    S: 'static + Send + EventHandle<E>,
{
    /// Initialize the actor
    async fn init(&mut self, supervisor: &mut S) -> Result<(), ActorError>;
}

/// A split-trait version of the `Actor` run definition. Implementors of
/// this trait along with `Init` and `Shutdown` will blanket impl `Actor`.
#[async_trait]
pub trait Run<E, S>: ActorTypes
where
    S: 'static + Send + EventHandle<E>,
{
    /// The main function for the actor
    async fn run(&mut self, supervisor: &mut S) -> Result<(), ActorError>;
}

/// A split-trait version of the `Actor` run definition. Implementors of
/// this trait along with `Init` and `Run` will blanket impl `Actor`.
#[async_trait]
pub trait Shutdown<E, S>: ActorTypes
where
    S: 'static + Send + EventHandle<E>,
{
    /// Handle the actor shutting down
    async fn shutdown(&mut self, status: Result<(), ActorError>, supervisor: &mut S) -> Result<ActorRequest, ActorError>;
}

#[async_trait]
impl<T, E, S> Actor<E, S> for T
where
    T: SplitMarker + Init<E, S> + Run<E, S> + Shutdown<E, S> + Send,
    S: 'static + Send + EventHandle<E>,
{
    fn service(&mut self) -> &mut Service {
        <Self as ActorTypes>::service(self)
    }

    async fn init(&mut self, supervisor: &mut S) -> Result<(), ActorError>
    where
        S: 'static + Send + EventHandle<E>,
    {
        <Self as Init<E, S>>::init(&mut self, supervisor).await
    }

    async fn run(&mut self, supervisor: &mut S) -> Result<(), ActorError>
    where
        S: 'static + Send + EventHandle<E>,
    {
        <Self as Run<E, S>>::run(&mut self, supervisor).await
    }

    async fn shutdown(&mut self, status: Result<(), ActorError>, supervisor: &mut S) -> Result<ActorRequest, ActorError>
    where
        S: 'static + Send + EventHandle<E>,
    {
        <Self as Shutdown<E, S>>::shutdown(&mut self, status, supervisor).await
    }
}

trait SplitMarker {}

impl<T> SplitMarker for T where T: ActorTypes {}

pub trait EventActor<M, H, E, S>: Actor<E, S>
where
    S: 'static + Send + EventHandle<E>,
    H: EventHandle<M>,
{
    /// Get the actor's event handle
    fn handle(&self) -> H;
}
