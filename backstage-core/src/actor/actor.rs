use super::{
    event_handle::EventHandle,
    result::*,
    service::{Service, ServiceStatus},
    NullSupervisor,
};
use async_trait::async_trait;
/// The all-important Actor trait. This defines an Actor and what it do.
#[async_trait]
pub trait Actor {
    /// The actor's error type. Must be convertable to an `ActorError`.
    type Error: Send + Into<ActorError>;
    /// The actor's event type. Can be anything so long as you can find
    /// a way to send it between actors.
    type Event;
    /// The actor's event handle type
    type Handle: EventHandle<Self::Event> + Clone;

    /// Get the actor's event handle
    fn handle(&mut self) -> &mut Self::Handle;

    /// Get the actor's service
    fn service(&mut self) -> &mut Service;

    /// Update the actor's status.
    /// This function assumes the service is stored locally in the actor's state.
    /// If this is not the case (for instance, it is stored globally in an `Arc`),
    /// this fn definition should be overridden so that it does not use the local fn.
    async fn update_status<E, S>(&mut self, status: ServiceStatus, supervisor: &mut S)
    where
        S: 'static + Send + EventHandle<E>,
    {
        let service = self.service();
        service.update_status(status);
        supervisor.update_status(service.clone()).ok();
    }

    /// Initialize the actor
    async fn init<E, S>(&mut self, supervisor: &mut S) -> Result<(), Self::Error>
    where
        S: 'static + Send + EventHandle<E>;

    /// The main function for the actor
    async fn run<E, S>(&mut self, supervisor: &mut S) -> Result<(), Self::Error>
    where
        S: 'static + Send + EventHandle<E>;

    /// Handle the actor shutting down
    async fn shutdown<E, S>(&mut self, status: Result<(), Self::Error>, supervisor: &mut S) -> Result<ActorRequest, ActorError>
    where
        S: 'static + Send + EventHandle<E>;

    /// Start the actor
    async fn start<E, S>(mut self, mut supervisor: S) -> Result<ActorRequest, ActorError>
    where
        Self: Send + Sized,
        S: 'static + Send + EventHandle<E>,
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

    /// Start the actor unsupervised
    async fn start_unsupervised(mut self) -> Result<ActorRequest, ActorError>
    where
        Self: Send + Sized,
    {
        self.start(NullSupervisor).await
    }
}
