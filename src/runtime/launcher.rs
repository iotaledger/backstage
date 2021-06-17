use super::*;
use std::collections::HashMap;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
// type BackstageContext<G> = Backstage<Launcher<G>, Box<dyn ActorHandle>, G>;
crate::builder!(LauncherBuilder {});

pub struct Launcher<G> {
    recoverable_actors: HashMap<String, Box<dyn Recovery<G>>>,
}

impl<G> Launcher<G> {
    /// Create new launcher actor struct
    pub fn new() -> Self {
        Self {
            recoverable_actors: HashMap::new(),
        }
    }
}

/// Launcher event type
pub enum LauncherEvent<G> {
    /// Launcher dynamic boxed variants
    Boxed(Box<dyn DynLauncherEvent<G>>),
    /// Children aknowledging shutdown
    AknShutdown(Service, ActorResult),
    /// Children service(s)
    Service(Service),
    /// Recover/Restart actor
    Recover(String, Box<dyn Recovery<G>>),
    /// Shutdown the launcher
    Shutdown,
}

#[derive(Clone)]
/// LauncherEvent handle used to access the launcher functionality
pub struct LauncherHandle<G: Clone + Send + 'static>(pub UnboundedSender<LauncherEvent<G>>);
/// Inbox used by LauncherInbox actor to receive events
pub struct LauncherInbox<G: Clone + Send + 'static>(UnboundedReceiver<LauncherEvent<G>>);
/// Channel implementation
impl<G: Clone + Send + 'static> Channel for Launcher<G> {
    type Handle = LauncherHandle<G>;
    type Inbox = LauncherInbox<G>;
    fn channel(&mut self) -> Result<(Self::Handle, Self::Inbox), anyhow::Error> {
        let (tx, rx) = unbounded_channel();
        Ok((LauncherHandle(tx), LauncherInbox(rx)))
    }
}

impl<G: Clone + Send + 'static> ActorHandle for LauncherHandle<G> {
    fn shutdown(self: Box<Self>) {
        self.0.send(LauncherEvent::<G>::Shutdown).ok();
    }
    fn aknshutdown(&self, service: Service, r: ActorResult) {
        self.0.send(LauncherEvent::<G>::AknShutdown(service, r)).ok();
    }
    fn service(&self, service: &Service) {
        self.0.send(LauncherEvent::<G>::Service(service.clone())).ok();
    }
    fn send(&self, event: Box<dyn std::any::Any>) -> Result<(), Box<dyn std::any::Any>> {
        let my_event: LauncherEvent<G> = *event.downcast()?;
        self.0.send(my_event).ok();
        Ok(())
    }
}

/// The event type of registry
pub trait DynLauncherEvent<G: Clone + Send + 'static>: Send {
    /// implement how launcher supposed to handle the boxed event
    fn handle(self: Box<Self>, launhcer: &mut Launcher<G>, backstage: &mut Backstage<Launcher<G>, Box<dyn ActorHandle>, G>);
}

/// Launcher Actor implementation
#[async_trait::async_trait]
impl<G: Send + Clone + 'static> Actor<Backstage<Self, Box<dyn ActorHandle>, G>> for Launcher<G> {
    async fn run(mut self, context: &mut Backstage<Self, Box<dyn ActorHandle>, G>) -> ActorResult {
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

impl<T: Channel + Clone + 'static, G: Send + Clone + 'static> DynLauncherEvent<G> for Add<T>
where
    Backstage<Launcher<G>, Box<dyn ActorHandle>, G>: Spawn<T, Box<dyn ActorHandle>>,
{
    fn handle(self: Box<Self>, launcher: &mut Launcher<G>, backstage: &mut Backstage<Launcher<G>, Box<dyn ActorHandle>, G>) {
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
            let msg = format!("{} is stopping, cannot add {}", backstage.service().name(), self.name);
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
/// Defines how to recover/restart actor
pub trait Recovery<G: Send + Clone + 'static>: Send {
    /// Restart the actor after shutdown
    fn recover(self: Box<Self>, launcher: &mut Launcher<G>, backstage: &mut Backstage<Launcher<G>, Box<dyn ActorHandle>, G>) -> bool;
}

impl<T: Channel + Clone + 'static, G> Recovery<G> for Recoverable<T>
where
    Backstage<Launcher<G>, Box<dyn ActorHandle>, G>: Spawn<T, Box<dyn ActorHandle>>,
    G: Send + Clone + 'static,
{
    fn recover(self: Box<Self>, launcher: &mut Launcher<G>, backstage: &mut Backstage<Launcher<G>, Box<dyn ActorHandle>, G>) -> bool {
        // create new recoveralbe actor
        let recoverable = Recoverable::new(self.name.clone(), self.actor.clone());
        // reinsert it to the launcher state
        launcher.recoverable_actors.insert(self.name.clone(), Box::new(recoverable));
        let s = crate::core::NullSupervisor;
        let supervisor: Box<dyn ActorHandle> = Box::new(backstage.handle().clone().unwrap());

        let r = backstage.spawn(self.actor, Box::new(supervisor), Service::new(&self.name));
        if r.is_ok() {
            true
        } else {
            false
        }
    }
}
