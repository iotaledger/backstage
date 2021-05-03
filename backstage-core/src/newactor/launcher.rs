use super::{actor::Actor, builder::ActorBuilder, event_handle::EventHandle, result::*, service::Service};
use anyhow::anyhow;
use tokio::{sync::mpsc::UnboundedSender, task::JoinHandle};

#[derive(Debug)]
pub enum LauncherEvent {
    /// Start an application
    StartApp(String),
    /// Shutdown an application
    ShutdownApp(String),
    /// Request an application's status
    RequestService(String),
    /// Notify a status change
    StatusChange(Service),
    /// Passthrough event
    Passthrough { target: String, event: String },
    /// ExitProgram(using_ctrl_c: bool) using_ctrl_c will identify if the shutdown signal was initiated by ctrl_c
    ExitProgram {
        /// Did this exit program event happen because of a ctrl-c?
        using_ctrl_c: bool,
    },
}

#[derive(Debug, Clone)]
pub struct LauncherSender(pub UnboundedSender<LauncherEvent>);

impl EventHandle<LauncherEvent> for LauncherSender {
    fn send(&mut self, message: LauncherEvent) -> anyhow::Result<()> {
        self.0.send(message).map_err(|e| anyhow!(e))
    }

    fn shutdown(mut self) -> Option<Self> {
        if let Ok(()) = self.send(LauncherEvent::ExitProgram { using_ctrl_c: false }) {
            None
        } else {
            Some(self)
        }
    }

    fn update_status(&mut self, service: Service) -> anyhow::Result<()> {
        self.send(LauncherEvent::StatusChange(service))
    }
}

pub struct BuilderData<A: Actor, B: ActorBuilder<A>> {
    pub name: String,
    pub builder: B,
    pub event_handle: Option<A::Handle>,
    pub join_handle: Option<JoinHandle<Result<ActorRequest, ActorError>>>,
}

/// Useful function to exit program using ctrl_c signal
pub async fn ctrl_c(mut handle: LauncherSender) {
    // await on ctrl_c
    tokio::signal::ctrl_c().await.unwrap();
    // exit program using launcher
    let exit_program_event = LauncherEvent::ExitProgram { using_ctrl_c: true };
    handle.send(exit_program_event).ok();
}
