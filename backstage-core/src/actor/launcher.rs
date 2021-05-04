use super::{
    actor::Actor,
    builder::ActorBuilder,
    event_handle::EventHandle,
    result::*,
    service::{Service, SERVICE},
};
use anyhow::anyhow;
pub use backstage_macros::launcher;
use tokio::{sync::mpsc::UnboundedSender, task::JoinHandle};

/// Events used by the launcher to manage its child actors
#[derive(Debug)]
pub enum LauncherEvent {
    /// Start an actor with the provided name
    StartActor(String),
    /// Shutdown an actor with the provided name
    ShutdownActor(String),
    /// Request an actor's status using a provided name
    RequestService(String),
    /// Notify a status change
    StatusChange(Service),
    /// Passthrough event
    Passthrough { target: String, event: String },
    /// Exit the program
    ExitProgram {
        /// Did this exit program event happen because of a ctrl-c?
        using_ctrl_c: bool,
    },
}

/// The sender handle for the launcher actor. Can be used by other actors
/// to send status updates or other `LauncherEvent`s.
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

/// Defines resources needed to spawn an actor and manage it
pub struct BuilderData<A: Actor, B: ActorBuilder<A>> {
    /// The name of the actor
    pub name: String,
    /// The actor's builder, used to spawn new `Actor`s
    pub builder: B,
    /// The actor's event handle, stored after spawning
    pub event_handle: Option<A::Handle>,
    /// The actor's thread's join handle, used to await the completion of
    /// and actor which is terminating.
    pub join_handle: Option<JoinHandle<Result<ActorRequest, ActorError>>>,
}

impl<A: 'static + Actor + Send, B: ActorBuilder<A>> BuilderData<A, B> {
    /// Spawn and start an actor on a new thread, storing its handles
    pub async fn startup(&mut self, sender: LauncherSender) {
        let new_service = SERVICE.write().await.spawn(self.name.clone());
        let mut actor = self.builder.clone().build(new_service);
        self.event_handle.replace(actor.handle().clone());
        let join_handle = tokio::spawn(actor.start(sender));
        self.join_handle.replace(join_handle);
    }

    /// Shutdown the actor by sending a shutdown event.
    /// This does not confirm that the actor has successfully shutdown!
    pub fn shutdown(&mut self) {
        if let Some(mut handle) = self.event_handle.take() {
            let mut retries = 2;
            while let Some(h) = handle.shutdown() {
                if retries == 0 {
                    break;
                } else {
                    log::error!("Failed to shutdown {}! Retrying {} more time(s)...", self.name, retries);
                    handle = h;
                    retries -= 1;
                }
            }
        } else {
            log::error!("No handle found for {}!", self.name);
        }
    }

    /// Handle a terminating actor by awaiting its join handle, then performing
    /// the ActorRequest returned by the thread.
    pub fn handle_terminated(&mut self, mut sender: LauncherSender) {
        let name = self.name.clone();
        if let Some(handle) = self.join_handle.take() {
            tokio::spawn(async move {
                let join_res = handle.await;
                let request = match join_res {
                    Ok(res) => match res {
                        Ok(request) => request,
                        Err(e) => {
                            log::error!("{}", e.to_string());
                            e.request().clone()
                        }
                    },
                    Err(e) => {
                        log::error!("{}", e.to_string());
                        ActorRequest::Finish
                    }
                };

                match request {
                    ActorRequest::Restart => {
                        sender.send(LauncherEvent::StartActor(name)).ok();
                    }
                    ActorRequest::Reschedule(d) => {
                        log::info!("Rescheduling {} to be restarted after {} ms", name, d.as_millis());
                        tokio::time::sleep(d).await;
                        sender.send(LauncherEvent::StartActor(name)).ok();
                    }
                    ActorRequest::Finish => (),
                    ActorRequest::Panic => {
                        sender.send(LauncherEvent::ExitProgram { using_ctrl_c: false }).ok();
                    }
                }
            });
        } else {
            log::error!("No handle found for {}!", name);
        }
    }
}

/// Useful function to exit program using ctrl_c signal
pub async fn ctrl_c(mut sender: LauncherSender) {
    // await on ctrl_c
    tokio::signal::ctrl_c().await.unwrap();
    // exit program using launcher
    let exit_program_event = LauncherEvent::ExitProgram { using_ctrl_c: true };
    sender.send(exit_program_event).ok();
}
