use super::*;
use std::collections::HashMap;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
type BackstageContext = Backstage<Launcher, Box<dyn ActorHandle>>;
crate::builder!(LauncherBuilder {});

pub struct Launcher {
    recoverable_actors: HashMap<String, Box<dyn Recovery>>,
}

impl Launcher {
    /// Create new launcher actor struct
    pub fn new() -> Self {
        Self {
            recoverable_actors: HashMap::new(),
        }
    }
}

pub enum LauncherEvent {
    Boxed(Box<dyn DynLauncherEvent>),
    AknShutdown(Service, ActorResult),
    Service(Service),
    Recover(String, Box<dyn Recovery>),
    Shutdown,
}

#[derive(Clone)]
/// LauncherEvent handle used to access the launcher functionality
pub struct LauncherHandle(pub UnboundedSender<LauncherEvent>);
/// Inbox used by LauncherInbox actor to receive events
pub struct LauncherInbox(UnboundedReceiver<LauncherEvent>);
/// Channel implementation
impl Channel for Launcher {
    type Handle = LauncherHandle;
    type Inbox = LauncherInbox;
    fn channel(&mut self) -> Result<(Self::Handle, Self::Inbox), anyhow::Error> {
        let (tx, rx) = unbounded_channel();
        Ok((LauncherHandle(tx), LauncherInbox(rx)))
    }
}

impl ActorHandle for LauncherHandle {
    fn shutdown(self: Box<Self>) {
        self.0.send(LauncherEvent::Shutdown).ok();
    }
    fn aknshutdown(&self, service: Service, r: ActorResult) {
        self.0.send(LauncherEvent::AknShutdown(service, r)).ok();
    }
    fn service(&self, service: &Service) {
        self.0.send(LauncherEvent::Service(service.clone())).ok();
    }
    fn send(&self, event: Box<dyn std::any::Any>) -> Result<(), Box<dyn std::any::Any>> {
        let my_event: LauncherEvent = *event.downcast()?;
        self.0.send(my_event).ok();
        Ok(())
    }
}

/// The event type of registry
pub trait DynLauncherEvent: Send {
    /// implement how launcher supposed to handle the boxed event
    fn handle(self: Box<Self>, launhcer: &mut Launcher, backstage: &mut BackstageContext);
}

/// Launcher Actor implementation
#[async_trait::async_trait]
impl Actor<BackstageContext> for Launcher {
    async fn run(mut self, context: &mut BackstageContext) -> ActorResult {
        // spawn ctrl_c as child
        // let supervisor_handle = context.handle().clone().expect("Launcher Handle");
        // BackstageContext::spawn_task(ctrl_c(supervisor_handle));
        while let Some(event) = context.inbox().0.recv().await {
            match event {
                LauncherEvent::Boxed(launcher_event) => {
                    launcher_event.handle(&mut self, context);
                }
                LauncherEvent::AknShutdown(service, actor_result) => {
                    let name = service.name();
                    // check if we requested the service to shutdown
                    let requested_to_shutdown = context.requested_to_shutdown(name);
                    match actor_result {
                        Ok(()) => {
                            // this is graceful shutdown.
                            if requested_to_shutdown {
                                log::info!("{} got graceful shutdown by {}", name, context.service().name())
                            } else {
                                log::info!("{} gracefully shutdown itself", name)
                            }
                        }
                        Err(reason) => match reason {
                            crate::core::Reason::Exit => {
                                if requested_to_shutdown {
                                    log::warn!("{} got exit by {}", name, context.service().name())
                                } else {
                                    log::error!("{} exit itself", name)
                                }
                            }
                            crate::core::Reason::Aborted => {
                                if requested_to_shutdown {
                                    log::warn!("{} got aborted by {}", name, context.service().name())
                                } else {
                                    log::error!("{} aborted itself", name)
                                }
                            }
                            crate::core::Reason::Timeout => {
                                log::error!("{} timeout itself", name)
                            }
                            crate::core::Reason::Restart(optional_duration) => {
                                if requested_to_shutdown {
                                    log::error!("{} cannot get restarted, as we requested it to shutdown", name)
                                } else {
                                    if !context.service().is_stopping() {
                                        if let Some(recoverable_actor) = self.recoverable_actors.remove(name) {
                                            log::info!("{} is restarting after duration: {:?}", name, optional_duration);
                                            let handle = context.handle().clone().expect("launcher handle to exist");
                                            let name: String = name.to_string();
                                            let recover_fut = async move {
                                                if let Some(duration) = optional_duration {
                                                    tokio::time::sleep(duration).await;
                                                }
                                                handle.0.send(LauncherEvent::Recover(name, recoverable_actor)).ok();
                                            };
                                            tokio::spawn(recover_fut);
                                        } else {
                                            log::info!("{} cannot restart unrecoverable actor", name);
                                        }
                                    } else {
                                        log::error!("{} cannot get restarted, as {} is stopping", name, context.service().name())
                                    }
                                }
                            }
                        },
                    }
                    // update status change
                    context.status_change(service);
                    // check if all children are stopped (note: it will also return when there are no_children at all)
                    let all_children_stopped = context.service().microservices().iter().all(|(_, ms)| ms.is_stopped());
                    // check if the launcher is stopped in order to break the loop
                    if all_children_stopped && context.service().is_stopping() {
                        log::info!(
                            "{} is Stopping, and it aknowledged shutdown for all the children",
                            context.service().name()
                        );
                        break;
                    } else if all_children_stopped {
                        let status = context.service().service_status().clone();
                        log::info!(
                            "{} is {:?} and it aknowledged shutdown for all the children",
                            context.service().name(),
                            status
                        )
                    }
                }
                LauncherEvent::Recover(name, recoverable_actor) => {
                    if !context.service().is_stopping() {
                        if recoverable_actor.recover(&mut self, context) {
                            log::info!("{} recovered {}", context.service().name(), name);
                        } else {
                            log::error!("{} unable to recover {}", context.service().name(), name);
                        }
                    } else {
                        log::warn!("unable to recover {}, as {} is stopping", name, context.service().name());
                    }
                }
                LauncherEvent::Service(service) => {
                    context.status_change(service);
                }
                LauncherEvent::Shutdown => {
                    // if all microservices are stopped
                    if context.service().microservices().iter().all(|(_, ms)| ms.is_stopped()) {
                        // break the loop
                        break;
                    } else {
                        // init shutdown
                        context.shutdown();
                        // note the launcher will break the loop once it aknowledge shutdown for its children
                    };
                }
            }
        }
        Ok(())
    }
}

/// Add T actor by name
pub struct Add<T: Channel> {
    /// The actor name
    name: String,
    /// The actor type
    actor: T,
    /// The response_handle to return handle to the caller
    response_handle: tokio::sync::oneshot::Sender<Result<T::Handle, anyhow::Error>>,
}

impl<T: Channel + Clone> Add<T> {
    /// Create add actor event
    pub fn new(name: String, actor: T, response_handle: tokio::sync::oneshot::Sender<Result<T::Handle, anyhow::Error>>) -> Self {
        Self {
            name,
            actor,
            response_handle,
        }
    }
}

impl<T: Channel + Clone + 'static> DynLauncherEvent for Add<T>
where
    Backstage<Launcher, Box<dyn ActorHandle>>: Spawn<T, Box<dyn ActorHandle>>,
{
    fn handle(self: Box<Self>, launcher: &mut Launcher, backstage: &mut Backstage<Launcher, Box<dyn ActorHandle>>) {
        // ensure the launcher is not stopping
        if !backstage.service().is_stopping() {
            // create recoveralbe actor
            let recoverable = Recoverable::new(self.name.clone(), self.actor.clone());
            // add it to the launcher state
            launcher.recoverable_actors.insert(self.name.clone(), Box::new(recoverable));
            let supervisor = backstage.handle().clone().unwrap();
            let r = backstage.spawn(self.actor, Box::new(supervisor), Service::new(&self.name));
            self.response_handle.send(r).ok();
        } else {
            let msg = format!("{} cannot add {}", backstage.service().name(), self.name);
            self.response_handle.send(Err(anyhow::Error::msg(msg))).ok();
        }
    }
}

#[derive(Clone)]
struct Recoverable<T: Clone> {
    /// The actor name
    name: String,
    /// The actor type
    actor: T,
}

impl<T: Clone> Recoverable<T> {
    fn new(name: String, actor: T) -> Self {
        Self { name, actor }
    }
}

trait Recovery: Send {
    fn recover(self: Box<Self>, launcher: &mut Launcher, backstage: &mut Backstage<Launcher, Box<dyn ActorHandle>>) -> bool;
}

impl<T: Channel + Clone + 'static> Recovery for Recoverable<T>
where
    Backstage<Launcher, Box<dyn ActorHandle>>: Spawn<T, Box<dyn ActorHandle>>,
{
    fn recover(self: Box<Self>, launcher: &mut Launcher, backstage: &mut Backstage<Launcher, Box<dyn ActorHandle>>) -> bool {
        // create new recoveralbe actor
        let recoverable = Recoverable::new(self.name.clone(), self.actor.clone());
        // reinsert it to the launcher state
        launcher.recoverable_actors.insert(self.name.clone(), Box::new(recoverable));
        let supervisor = backstage.handle().clone().unwrap();
        let r = backstage.spawn(self.actor, Box::new(supervisor), Service::new(&self.name));
        if r.is_ok() {
            true
        } else {
            false
        }
    }
}
