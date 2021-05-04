pub use actor::*;
pub use backstage_macros::build;
pub use builder::*;
pub use event_handle::*;
pub use result::*;
pub use service::*;

mod actor;
mod builder;
mod event_handle;
mod result;
mod service;

/// Defines the launcher actor and all associated types
#[cfg(feature = "launcher")]
pub mod launcher;

/// A null supervisor, to be used for unsupervised actors
pub struct NullSupervisor;

impl EventHandle<()> for NullSupervisor {
    fn send(&mut self, _message: ()) -> anyhow::Result<()> {
        Ok(())
    }

    fn shutdown(self) -> Option<Self> {
        None
    }

    fn update_status(&mut self, _service: Service) -> anyhow::Result<()> {
        Ok(())
    }
}
