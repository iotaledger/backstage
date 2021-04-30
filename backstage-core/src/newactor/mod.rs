use anyhow::anyhow;
use async_trait::async_trait;
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};
use thiserror::Error;
use tokio::{
    sync::{mpsc::UnboundedSender, RwLock},
    task::JoinHandle,
};

lazy_static! {
    pub static ref SERVICE: Arc<RwLock<Service>> = Arc::new(RwLock::new(Service::new("Launcher")));
}

// Actor
// Known data/state (struct def)
// Init fn  - proc macro (default in trait)
// Run fn - proc macro (no default)
// Shutdown fn - proc macro (default in trait)

// Builder fn proc macro and backing trait
// defines dependencies
// build the Actor type from param struct

// Launcher Actor
// ::new()
// ::add_builder(name, impl Builder)
// ::start()

#[async_trait]
pub trait Actor {
    type Error: Send + Into<ActorError>;
    type Event;
    type Handle: EventHandle<Self::Event>;

    /// Get the actor's event handle
    fn handle(&mut self) -> &mut Self::Handle;

    /// Update the actor's status
    fn update_status<E, S>(&mut self, status: ServiceStatus, supervisor: &mut S)
    where
        S: 'static + Send + EventHandle<E>;

    /// Initialize the actor
    async fn init<E, S>(&mut self, supervisor: &mut S) -> Result<(), Self::Error>
    where
        S: 'static + Send + EventHandle<E>;

    /// The main function for the actor
    async fn run<E, S>(&mut self, supervisor: &mut S) -> Result<(), Self::Error>
    where
        S: 'static + Send + EventHandle<E>;

    /// Handle the actor shutting down
    async fn shutdown<E, S>(&mut self, status: Result<(), Self::Error>, supervisor: &mut S) -> Result<ActorRequest, ActorError>
    where
        S: 'static + Send + EventHandle<E>;

    /// Start the actor
    async fn start<E, S>(mut self, mut supervisor: S) -> Result<ActorRequest, ActorError>
    where
        Self: Send + Sized,
        S: 'static + Send + EventHandle<E>,
    {
        self.update_status(ServiceStatus::Starting, &mut supervisor);
        let mut res = self.init(&mut supervisor).await;
        if res.is_ok() {
            self.update_status(ServiceStatus::Running, &mut supervisor);
            res = self.run(&mut supervisor).await;
            self.update_status(ServiceStatus::Stopping, &mut supervisor);
            let res = self.shutdown(res, &mut supervisor).await;
            self.update_status(ServiceStatus::Stopped, &mut supervisor);
            res
        } else {
            let res = self.shutdown(res, &mut supervisor).await;
            self.update_status(ServiceStatus::Stopped, &mut supervisor);
            res
        }
    }

    /// Start the actor unsupervised
    async fn start_unsupervised(mut self) -> Result<ActorRequest, ActorError>
    where
        Self: Send + Sized,
    {
        self.start(NullSupervisor).await
    }
}

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

pub trait EventHandle<M> {
    fn send(&mut self, message: M) -> anyhow::Result<()>;

    fn shutdown(self) -> Option<Self>
    where
        Self: Sized;

    fn update_status(&mut self, service: Service) -> anyhow::Result<()>;
}

pub trait ActorBuilder<A>
where
    A: Actor,
{
    fn build(self, service: Service) -> A;
}

/// The possible statuses a service (application) can be
#[repr(u8)]
#[derive(Copy, Clone, PartialEq, Debug, Serialize, Deserialize)]
pub enum ServiceStatus {
    /// Early bootup
    Starting = 0,
    /// Late bootup
    Initializing = 1,
    /// The service is operational but one or more services failed(Degraded or Maintenance) or in process of being
    /// fully operational while startup.
    Degraded = 2,
    /// The service is fully operational.
    Running = 3,
    /// The service is shutting down, should be handled accordingly by active dependent services
    Stopping = 4,
    /// The service is maintenance mode, should be handled accordingly by active dependent services
    Maintenance = 5,
    /// The service is not running, should be handled accordingly by active dependent services
    Stopped = 6,
}

impl Default for ServiceStatus {
    fn default() -> Self {
        Self::Starting
    }
}

#[derive(Default, Clone, Debug, Serialize, Deserialize)]
pub struct IdPool {
    pool: Vec<u16>,
}

impl IdPool {
    pub fn get_id(&mut self) -> u16 {
        if self.pool.len() == 0 {
            self.pool.push(1);
            0
        } else if self.pool.len() == 1 {
            let id = self.pool[0];
            self.pool[0] += 1;
            id
        } else {
            self.pool.pop().unwrap()
        }
    }

    pub fn return_id(&mut self, id: u16) {
        self.pool.push(id);
    }
}

/// An application's metrics
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Service {
    /// The status of the service
    pub status: ServiceStatus,
    /// Service name (ie app name or microservice name)
    pub name: String,
    pub id: u16,
    /// Timestamp since the service is up
    pub up_since: SystemTime,
    /// Total milliseconds when the app service has been offline since up_since
    pub downtime_ms: u64,
    /// inner services similar
    pub microservices: std::collections::HashMap<u16, Service>,
    /// Optional log file path
    pub log_path: Option<String>,
    pub id_pool: IdPool,
}

impl Service {
    /// Create a new Service
    pub fn new<S: Into<String>>(name: S) -> Self {
        Self::default().with_name(name)
    }
    /// Spawn a new microservice and return a clone of it
    pub fn spawn<S: Into<String>>(&mut self, name: S) -> Self {
        let id = self.id_pool.get_id();
        self.microservices.insert(id, Self::default().with_name(name).with_id(id));
        self.microservices.get(&id).unwrap().clone()
    }
    /// Set the id
    pub fn with_id(mut self, id: u16) -> Self {
        self.id = id;
        self
    }
    /// Set the service status
    pub fn with_status(mut self, service_status: ServiceStatus) -> Self {
        self.status = service_status;
        self
    }
    /// Update the service status
    pub fn update_status(&mut self, service_status: ServiceStatus) {
        self.status = service_status;
    }
    /// Set the service (application) name
    pub fn with_name<S: Into<String>>(mut self, name: S) -> Self {
        self.name = name.into();
        self
    }
    /// Update the service (application) name
    pub fn update_name<S: Into<String>>(&mut self, name: S) {
        self.name = name.into();
    }
    /// Get the service (application) name
    pub fn get_name(&self) -> &String {
        &self.name
    }
    /// Set the service downtime in milliseconds
    pub fn with_downtime_ms(mut self, downtime_ms: u64) -> Self {
        self.downtime_ms = downtime_ms;
        self
    }
    /// Set the logging filepath
    pub fn with_log(mut self, log_path: String) -> Self {
        self.log_path = Some(log_path);
        self
    }
    /// Insert a new microservice
    pub fn update_microservice(&mut self, id: u16, microservice: Self) {
        self.microservices.insert(id, microservice);
    }
    /// Update the status of a microservice
    pub fn update_microservice_status(&mut self, id: &u16, status: ServiceStatus) {
        self.microservices.get_mut(id).map(|s| s.status = status);
    }
    /// Delete a microservice
    pub fn delete_microservice(&mut self, id: &u16) {
        self.microservices.remove(id);
    }
    /// Check if the service is stopping
    pub fn is_stopping(&self) -> bool {
        self.status == ServiceStatus::Stopping
    }
    /// Check if the service is stopped
    pub fn is_stopped(&self) -> bool {
        self.status == ServiceStatus::Stopped
    }
    /// Check if the service is running
    pub fn is_running(&self) -> bool {
        self.status == ServiceStatus::Running
    }
    /// Check if the service is starting
    pub fn is_starting(&self) -> bool {
        self.status == ServiceStatus::Starting
    }
    /// Check if the service is initializing
    pub fn is_initializing(&self) -> bool {
        self.status == ServiceStatus::Initializing
    }
    /// Check if the service is in maintenance
    pub fn is_maintenance(&self) -> bool {
        self.status == ServiceStatus::Maintenance
    }
    /// Check if the service is degraded
    pub fn is_degraded(&self) -> bool {
        self.status == ServiceStatus::Degraded
    }
    /// Get the service status
    pub fn service_status(&self) -> &ServiceStatus {
        &self.status
    }
}

impl Default for Service {
    fn default() -> Self {
        Self {
            status: Default::default(),
            name: Default::default(),
            id: Default::default(),
            up_since: SystemTime::now(),
            downtime_ms: Default::default(),
            microservices: Default::default(),
            log_path: Default::default(),
            id_pool: Default::default(),
        }
    }
}

#[derive(Error, Debug)]
pub enum ActorError {
    #[error("Invalid data was given to the actor: {0}!")]
    InvalidData(String),
    #[error("The actor experienced a runtime error!")]
    RuntimeError(ActorRequest),
    #[error("Actor Error: {source}")]
    Other { source: anyhow::Error, request: ActorRequest },
}

impl ActorError {
    pub fn request(&self) -> &ActorRequest {
        match self {
            ActorError::InvalidData(_) => &ActorRequest::Panic,
            ActorError::RuntimeError(r) => r,
            ActorError::Other { source: _, request: r } => r,
        }
    }
}

impl From<std::borrow::Cow<'static, str>> for ActorError {
    fn from(cow: std::borrow::Cow<'static, str>) -> Self {
        ActorError::Other {
            source: anyhow!(cow),
            request: ActorRequest::Panic,
        }
    }
}

/// Possible requests an actor can make to its supervisor
#[derive(Debug, Clone)]
pub enum ActorRequest {
    /// Actor wants to restart after this run
    Restart,
    /// Actor wants to schedule a restart sometime in the future
    Reschedule(Duration),
    /// Actor is done and can be removed from the service
    Finish,
    /// Something unrecoverable happened and we should actually
    /// shut down everything and notify the user
    Panic,
}

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
