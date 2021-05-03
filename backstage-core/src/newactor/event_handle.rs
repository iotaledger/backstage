use super::service::Service;
pub trait EventHandle<M>
where
    M: ?Sized,
{
    fn send(&mut self, message: M) -> anyhow::Result<()>;

    fn shutdown(self) -> Option<Self>
    where
        Self: Sized;

    fn update_status(&mut self, service: Service) -> anyhow::Result<()>
    where
        Self: Sized;
}
