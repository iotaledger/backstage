use super::service::Service;

/// An event handle which defines some basic functionality
/// that can be used without implementation details.
pub trait EventHandle<M>
where
    M: ?Sized,
{
    /// Send a message of the appropriate type
    fn send(&mut self, message: M) -> anyhow::Result<()>;

    /// Send a message which indicates that the receiver should shut down
    fn shutdown(self) -> Option<Self>
    where
        Self: Sized;

    /// Send a message which indicates that the sender's status has changed
    fn update_status(&mut self, service: Service) -> anyhow::Result<()>
    where
        Self: Sized;
}
