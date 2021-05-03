pub use actor::*;
pub use builder::*;
pub use event_handle::*;
pub use result::*;
pub use service::*;

mod actor;
mod builder;
mod event_handle;
pub mod launcher;
mod result;
mod service;

pub struct NullSupervisor;

impl EventHandle<()> for NullSupervisor {
    fn send(&mut self, message: ()) -> anyhow::Result<()> {
        Ok(())
    }

    fn shutdown(self) -> Option<Self> {
        None
    }

    fn update_status(&mut self, _service: Service) -> anyhow::Result<()> {
        Ok(())
    }
}
