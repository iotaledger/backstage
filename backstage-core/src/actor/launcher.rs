use super::{
    actor::{Actor, EventActor},
    event_handle::EventHandle,
    result::*,
    service::{Service, SERVICE},
};
use crate::{NullSupervisor, ServiceStatus};
use anyhow::anyhow;
use async_trait::async_trait;
pub use backstage_macros::launcher;
use daggy::{Dag, Walker};
use log::{error, info};
use std::{
    collections::{hash_map::Entry, HashMap},
    marker::PhantomData,
    time::Duration,
};
use thiserror::Error;
use tokio::{
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
    task::JoinHandle,
};

pub trait Bridge<A, M, H>
where
    A: ActorHandle<M, H>,
    H: EventHandle<M>,
{
    fn shutdown_timeout(&self) -> Duration {
        Duration::from_secs(10)
    }
    fn bridge(self, service: Service) -> A;
}

#[async_trait]
pub trait ActorHandle<M, H>
where
    H: EventHandle<M>,
{
    fn handle(&self) -> H;
    async fn start(self, supervisor: LauncherSender) -> Result<ActorRequest, ActorError>;
}

#[derive(Debug, Error)]
pub enum LauncherError {
    #[error("No app found with name \"{0}\"")]
    NoApp(String),
    #[error("Duplicate app added with name \"{0}\"")]
    DuplicateApp(String),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl Into<ActorError> for LauncherError {
    fn into(self) -> ActorError {
        match self {
            LauncherError::NoApp(_) => ActorError::InvalidData(self.to_string()),
            LauncherError::DuplicateApp(_) => ActorError::InvalidData(self.to_string()),
            LauncherError::Other(source) => ActorError::Other {
                source,
                request: ActorRequest::Finish,
            },
        }
    }
}

pub type LauncherResult = Result<(), LauncherError>;

pub struct Launcher<B, A, M, H>
where
    B: Bridge<A, M, H>,
    A: ActorHandle<M, H>,
    H: EventHandle<M>,
{
    builders: HashMap<String, B>,
    event_handles: HashMap<String, H>,
    join_handles: HashMap<String, JoinHandle<Result<ActorRequest, ActorError>>>,
    dependencies: HashMap<String, Vec<String>>,
    inbox: UnboundedReceiver<LauncherEvent>,
    sender: LauncherSender,
    consumed_ctrl_c: bool,
    _phantom: PhantomData<(A, M)>,
}

impl<B, A, M, H> Default for Launcher<B, A, M, H>
where
    A: ActorHandle<M, H>,
    H: EventHandle<M>,
    B: Bridge<A, M, H>,
{
    fn default() -> Self {
        let (sender, inbox) = tokio::sync::mpsc::unbounded_channel::<LauncherEvent>();
        Self {
            builders: Default::default(),
            event_handles: Default::default(),
            join_handles: Default::default(),
            dependencies: Default::default(),
            inbox,
            sender: LauncherSender(sender),
            consumed_ctrl_c: false,
            _phantom: Default::default(),
        }
    }
}

impl<B, A, M, H> Launcher<B, A, M, H>
where
    A: 'static + ActorHandle<M, H> + Send,
    H: EventHandle<M>,
    B: Bridge<A, M, H> + Clone + Send + Sync,
    M: Send,
{
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add<I: Into<B>, S: Into<String> + Clone>(mut self, name: S, builder: I, dependencies: &[S]) -> Result<Self, LauncherError> {
        match self.builders.entry(name.into()) {
            Entry::Occupied(e) => {
                return Err(LauncherError::DuplicateApp(e.key().clone()));
            }
            Entry::Vacant(e) => {
                let name = e.key().clone();
                e.insert(builder.into());
                self.dependencies
                    .insert(name, dependencies.iter().cloned().map(Into::into).collect::<Vec<_>>());
            }
        }
        Ok(self)
    }

    pub fn execute<F: FnMut(&mut Self)>(mut self, mut f: F) -> Self {
        f(&mut self);
        self
    }

    pub async fn execute_async<F: FnOnce(Self) -> O, O: std::future::Future<Output = Self>>(self, f: F) -> Self {
        f(self).await
    }

    /// Spawn and start an actor on a new thread, storing its handles
    pub async fn startup_app(&mut self, name: &String) -> LauncherResult {
        let new_service = SERVICE.write().await.spawn(name.clone());
        if let Some(actor) = self.builders.get(name).cloned().map(|b| b.bridge(new_service)) {
            self.event_handles.insert(name.clone(), actor.handle());
            let join_handle = tokio::spawn(actor.start(self.sender.clone()));
            self.join_handles.insert(name.clone(), join_handle);
            Ok(())
        } else {
            Err(LauncherError::NoApp(name.clone()))
        }
    }

    /// Shutdown the actor by sending a shutdown event. This fn will spawn a timeout thread
    /// which will wait for the actor's thread to terminate and send the result to the launcher.
    pub fn shutdown_app(&mut self, name: &String) -> LauncherResult {
        if let Some(mut handle) = self.event_handles.remove(name) {
            let mut retries = 2;
            while let Err(_) = handle.shutdown() {
                if retries == 0 {
                    break;
                } else {
                    error!("Failed to shutdown {}! Retrying {} more time(s)...", name, retries);
                    retries -= 1;
                }
            }
            if let (Some(handle), Some(builder)) = (self.join_handles.remove(name), self.builders.get(name)) {
                let mut sender = self.sender.clone();
                let name = name.clone();
                let timeout = builder.shutdown_timeout();
                tokio::spawn(async move {
                    let timeout_res = tokio::time::timeout(timeout, handle).await;
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
                            sender.send(LauncherEvent::StartActor(name.clone())).ok();
                        }
                        ActorRequest::Reschedule(d) => {
                            log::info!("Rescheduling {} to be restarted after {} ms", name, d.as_millis());
                            tokio::time::sleep(d).await;
                            sender.send(LauncherEvent::StartActor(name.clone())).ok();
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
            Ok(())
        } else {
            Err(LauncherError::NoApp(name.clone()))
        }
    }

    pub async fn shutdown_all(&mut self) {
        let names = self.builders.keys().cloned().collect::<Vec<_>>();
        for name in names.iter() {
            self.block_on_shutdown(name).await;
        }
    }

    /// Shutdown the actor by sending a shutdown event. This fn will block awaiting the completion.
    /// This is used by the launcher to terminate the application.
    pub async fn block_on_shutdown(&mut self, name: &String) {
        if let Some(mut handle) = self.event_handles.remove(name) {
            let mut retries = 2;
            while let Err(_) = handle.shutdown() {
                if retries == 0 {
                    break;
                } else {
                    error!("Failed to shutdown {}! Retrying {} more time(s)...", name, retries);
                    retries -= 1;
                }
            }
            if let (Some(handle), Some(builder)) = (self.join_handles.remove(name), self.builders.get(name)) {
                match tokio::time::timeout(builder.shutdown_timeout(), handle).await {
                    Ok(_) => (),
                    Err(_) => error!("Timeout shutting down {}!", name),
                }
            } else {
                error!("No join handle found for {}!", name);
            }
        } else {
            error!("No event handle found for {}!", name);
        }
    }

    pub async fn launch(self) -> Result<ActorRequest, ActorError> {
        self.start(NullSupervisor).await
    }
}

#[async_trait]
impl<B, A, M, H> Actor<(), NullSupervisor> for Launcher<B, A, M, H>
where
    A: 'static + ActorHandle<M, H> + Send,
    H: EventHandle<M>,
    B: Bridge<A, M, H> + Clone + Send + Sync,
    M: Send,
{
    type Error = anyhow::Error;

    fn service(&mut self) -> &mut Service {
        panic!("Cannot access launcher service via a reference!");
    }

    async fn update_status(&mut self, status: ServiceStatus, _supervisor: &mut NullSupervisor)
    where
        Self: Sized,
    {
        SERVICE.write().await.update_status(status);
    }

    async fn init(&mut self, _supervisor: &mut NullSupervisor) -> Result<(), Self::Error> {
        info!("Initializing Launcher!");
        tokio::spawn(ctrl_c(self.sender.clone()));
        Ok(())
    }

    async fn run(&mut self, _supervisor: &mut NullSupervisor) -> Result<(), Self::Error> {
        info!("Running Launcher!");
        let mut dag = Dag::<(), (), u32>::new();
        let mut no_deps = Vec::new();
        let mut node_names = HashMap::new();
        let deps = self
            .dependencies
            .iter()
            .map(|(parent, deps)| {
                let node = dag.add_node(());
                node_names.insert(node, parent.clone());
                (parent.clone(), (node, deps.clone()))
            })
            .collect::<HashMap<_, _>>();
        for (parent, (parent_node, children)) in deps.iter() {
            if children.is_empty() {
                no_deps.push((parent.clone(), *parent_node));
            } else {
                for child in children.iter() {
                    self.builders
                        .get(parent)
                        .ok_or_else(|| ActorError::InvalidData(format!("{} is depended on by {}, but it does not exist", child, parent)))?;
                    let child_node = deps
                        .get(child)
                        .ok_or_else(|| ActorError::InvalidData(format!("{} is dependent on {}, but it does not exist", parent, child)))?
                        .0;
                    dag.add_edge(*parent_node, child_node, ()).map_err(|_| {
                        ActorError::InvalidData(format!("Cyclical dependencies defined involving {} and {}", child, parent))
                    })?;
                }
            }
        }
        for (mut parent_name, mut node) in no_deps {
            self.startup_app(&parent_name).await.map_err::<ActorError, _>(Into::into)?;
            loop {
                let parents = dag.parents(node);
                let mut iter = parents.iter(&dag).peekable();
                if iter.peek().is_none() {
                    break;
                }
                for (_, parent_node) in iter {
                    let child_name = node_names.get(&parent_node).unwrap();
                    info!("Starting actor {} as a dependent of {}", child_name, parent_name);
                    self.startup_app(child_name).await.map_err::<ActorError, _>(Into::into)?;
                    parent_name = child_name.clone();
                    node = parent_node;
                }
            }
        }
        while let Some(evt) = self.inbox.recv().await {
            match evt {
                LauncherEvent::StartActor(name) => {
                    info!("Starting actor: {}", name);
                    if let Err(e) = self.startup_app(&name).await {
                        error!("Failed to start actor with name \"{}\": {}", name, e);
                    }
                }
                LauncherEvent::ShutdownActor(name) => {
                    info!("Shutting down actor: {}", name);
                    if let Err(e) = self.shutdown_app(&name) {
                        error!("Failed to shutdown actor with name \"{}\": {}", name, e);
                    }
                }
                LauncherEvent::StatusChange(service) => {
                    SERVICE.write().await.insert_or_update_microservice(service);
                }
                LauncherEvent::ExitProgram { using_ctrl_c } => {
                    if using_ctrl_c && self.consumed_ctrl_c {
                        panic!("Exiting due to ctrl-c spam! Uh oh...");
                    }
                    if using_ctrl_c {
                        tokio::spawn(ctrl_c(self.sender.clone()));
                        self.consumed_ctrl_c = true;
                    }
                    if !SERVICE.read().await.is_stopping() {
                        info!("Shutting down the launcher and all sub-actors!");
                        SERVICE.write().await.update_status(ServiceStatus::Stopping);
                        info!("Waiting for all children to shut down...");
                        self.shutdown_all().await;
                        break;
                    }
                }
                LauncherEvent::RequestService(_) => {}
                LauncherEvent::Passthrough { target, event } => {}
            }
        }
        Ok(())
    }

    async fn shutdown(&mut self, status: Result<(), Self::Error>, _supervisor: &mut NullSupervisor) -> Result<ActorRequest, ActorError> {
        log::info!("Shutting down Launcher!");
        status?;
        Ok(ActorRequest::Finish)
    }

    async fn start(mut self, mut supervisor: NullSupervisor) -> Result<ActorRequest, ActorError>
    where
        Self: Send,
    {
        self.update_status(ServiceStatus::Starting, &mut supervisor).await;
        let mut res = self.init(&mut supervisor).await;
        if res.is_ok() {
            self.update_status(ServiceStatus::Running, &mut supervisor).await;
            res = self.run(&mut supervisor).await;
            self.update_status(ServiceStatus::Stopping, &mut supervisor).await;
            let res = self.shutdown(res, &mut supervisor).await;
            self.update_status(ServiceStatus::Stopped, &mut supervisor).await;
            res
        } else {
            let res = self.shutdown(res, &mut supervisor).await;
            self.update_status(ServiceStatus::Stopped, &mut supervisor).await;
            res
        }
    }
}

impl<B, A, M, H> EventActor<(), NullSupervisor> for Launcher<B, A, M, H>
where
    A: 'static + ActorHandle<M, H> + Send,
    H: EventHandle<M>,
    B: Bridge<A, M, H> + Clone + Send + Sync,
    M: Send,
{
    type Event = LauncherEvent;

    type Handle = LauncherSender;

    fn handle(&self) -> LauncherSender {
        self.sender.clone()
    }
}

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

    fn shutdown(&mut self) -> anyhow::Result<()> {
        self.send(LauncherEvent::ExitProgram { using_ctrl_c: false })
    }

    fn update_status(&mut self, service: Service) -> anyhow::Result<()> {
        self.send(LauncherEvent::StatusChange(service))
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

pub mod macros {
    #[macro_export]
    macro_rules! launcher {
        ($($builder:ident),+) => {
            {
                #[derive(Clone)]
                enum Builders {
                    $($builder($builder)),+
                }

                enum Actors {
                    $($builder(<$builder as ActorBuilder>::BuiltActor)),+
                }

                #[derive(Clone)]
                enum Handles {
                    $($builder(<<$builder as ActorBuilder>::BuiltActor as EventActor<LauncherEvent, LauncherSender>>::Handle)),+
                }

                enum Events {
                    $($builder(<<$builder as ActorBuilder>::BuiltActor as EventActor<LauncherEvent, LauncherSender>>::Event)),+
                }

                $(
                    impl From<$builder> for Builders {
                        fn from(b: $builder) -> Self {
                            Builders::$builder(b)
                        }
                    }
                )+

                #[async_trait]
                impl ActorHandle<Events, Handles> for Actors
                {
                    fn handle(&self) -> Handles {
                        match self {
                            $(Actors::$builder(actor) => Handles::$builder(EventActor::<_, LauncherSender>::handle(actor))),+
                        }
                    }

                    async fn start(self, supervisor: LauncherSender) -> Result<ActorRequest, ActorError> {
                        match self {
                            $(Actors::$builder(actor) => actor.start(supervisor).await),+
                        }
                    }
                }

                impl EventHandle<Events> for Handles {
                    fn send(&mut self, message: Events) -> anyhow::Result<()> {
                        match (self, message) {
                            $((Handles::$builder(handle), Events::$builder(message)) => handle.send(message),)+
                            _ => anyhow::bail!("Mismatching event and handle"),
                        }
                    }

                    fn shutdown(&mut self) -> anyhow::Result<()> {
                        match self {
                            $(Handles::$builder(handle) => handle.shutdown()),+
                        }
                    }

                    fn update_status(&mut self, service: Service) -> anyhow::Result<()> {
                        match self {
                            $(Handles::$builder(handle) => handle.update_status(service)),+
                        }
                    }
                }

                impl Bridge<Actors, Events, Handles> for Builders
                {
                    fn bridge(self, service: Service) -> Actors {
                        match self {
                            $(Builders::$builder(builder) => Actors::$builder(builder.build::<_, LauncherSender>(service))),+
                        }
                    }
                }
                Launcher::<Builders, Actors, Events, Handles>::new()
            }
        };
    }
}
