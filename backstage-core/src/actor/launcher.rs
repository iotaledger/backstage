use super::{
    actor::{Actor, EventActor},
    builder::ActorBuilder,
    event_handle::EventHandle,
    result::*,
    service::{Service, SERVICE},
};
use anyhow::anyhow;
pub use backstage_macros::launcher;
use log::error;
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
pub struct BuilderData<A, B, E, S>
where
    A: EventActor<E, S>,
    B: ActorBuilder<A, E, S> + Clone,
    S: 'static + Send + EventHandle<E>,
{
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

impl<A, B, E, S> BuilderData<A, B, E, S>
where
    A: EventActor<E, S>,
    B: ActorBuilder<A, E, S> + Clone,
    S: 'static + Send + EventHandle<E>,
{
    /// Create a new builder data struct from a name and builder
    pub fn new(name: String, builder: B) -> Self {
        Self {
            name,
            builder,
            event_handle: None,
            join_handle: None,
        }
    }
}

impl<A, B> BuilderData<A, B, LauncherEvent, LauncherSender>
where
    A: 'static + EventActor<LauncherEvent, LauncherSender> + Send,
    B: ActorBuilder<A, LauncherEvent, LauncherSender> + Clone,
{
    /// Spawn and start an actor on a new thread, storing its handles
    pub async fn startup(&mut self, sender: LauncherSender) {
        let new_service = SERVICE.write().await.spawn(self.name.clone());
        let mut actor = self.builder.clone().build(new_service);
        self.event_handle.replace(actor.handle().clone());
        let join_handle = tokio::spawn(actor.start(sender));
        self.join_handle.replace(join_handle);
    }

    /// Shutdown the actor by sending a shutdown event. This fn will spawn a timeout thread
    /// which will wait for the actor's thread to terminate and send the result to the launcher.
    pub fn shutdown(&mut self, mut sender: LauncherSender) {
        if let Some(mut handle) = self.event_handle.take() {
            let mut retries = 2;
            while let Some(h) = handle.shutdown() {
                if retries == 0 {
                    break;
                } else {
                    error!("Failed to shutdown {}! Retrying {} more time(s)...", self.name, retries);
                    handle = h;
                    retries -= 1;
                }
            }
            let name = self.name.clone();
            if let Some(handle) = self.join_handle.take() {
                tokio::spawn(async move {
                    let timeout_res = tokio::time::timeout(A::SHUTDOWN_TIMEOUT, handle).await;
                    let request = match timeout_res {
                        Ok(join_res) => match join_res {
                            Ok(res) => match res {
                                Ok(request) => request,
                                Err(e) => {
                                    error!("{}", e.to_string());
                                    e.request().clone()
                                }
                            },
                            Err(e) => {
                                error!("{}", e.to_string());
                                ActorRequest::Finish
                            }
                        },
                        Err(_) => {
                            error!("Timeout shutting down {}!", name);
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
                error!("No join handle found for {}!", name);
            }
        } else {
            error!("No event handle found for {}!", self.name);
        }
    }

    /// Shutdown the actor by sending a shutdown event. This fn will block awaiting the completion.
    /// This is used by the launcher to terminate the application.
    pub async fn block_on_shutdown(&mut self) {
        if let Some(mut handle) = self.event_handle.take() {
            let mut retries = 2;
            while let Some(h) = handle.shutdown() {
                if retries == 0 {
                    break;
                } else {
                    error!("Failed to shutdown {}! Retrying {} more time(s)...", self.name, retries);
                    handle = h;
                    retries -= 1;
                }
            }
            let name = self.name.clone();
            if let Some(handle) = self.join_handle.take() {
                match tokio::time::timeout(A::SHUTDOWN_TIMEOUT, handle).await {
                    Ok(_) => (),
                    Err(_) => error!("Timeout shutting down {}!", name),
                }
            } else {
                error!("No join handle found for {}!", name);
            }
        } else {
            error!("No event handle found for {}!", self.name);
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
