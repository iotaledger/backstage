use super::{event_handle::EventHandle, result::*, service::ServiceStatus, NullSupervisor};
use async_trait::async_trait;
#[async_trait]
pub trait Actor {
    type Error: Send + Into<ActorError>;
    type Event;
    type Handle: EventHandle<Self::Event>;

    /// Get the actor's event handle
    fn handle(&mut self) -> &mut Self::Handle;

    /// Update the actor's status
    fn update_status<E, S>(&mut self, status: ServiceStatus, supervisor: &mut S)
    where
        S: 'static + Send + EventHandle<E>;

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
        self.update_status(ServiceStatus::Starting, &mut supervisor);
        let mut res = self.init(&mut supervisor).await;
        if res.is_ok() {
            self.update_status(ServiceStatus::Running, &mut supervisor);
            res = self.run(&mut supervisor).await;
            self.update_status(ServiceStatus::Stopping, &mut supervisor);
            let res = self.shutdown(res, &mut supervisor).await;
            self.update_status(ServiceStatus::Stopped, &mut supervisor);
            res
        } else {
            let res = self.shutdown(res, &mut supervisor).await;
            self.update_status(ServiceStatus::Stopped, &mut supervisor);
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
