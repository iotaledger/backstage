/// The returned result by the actor
pub type ActorResult = std::result::Result<(), Reason>;

#[derive(Clone)]
/// Actor shutdown error reason
pub enum Reason {
    /// The actor exit, as it cannot do anything further.
    Exit,
    /// The actor got aborted
    Aborted,
    /// The actor got aborted becasue it reached its timeout limit
    Timeout,
}

#[async_trait::async_trait]
/// Actor trait which implement the actor lifecycle
pub trait Actor<C: Essential<Actor = Self>>: Sized + Send + 'static + Channel {
    /// Actor lifecycle
    async fn run(self, context: &mut C) -> ActorResult;
}
/// Should be implemented for the actor type
pub trait Channel: Send {
    /// The actor handle type
    type Handle: Send + Clone + Sync + 'static;
    /// The actor inbox type
    type Inbox: Send;
    /// Initialize/create the actor channel
    fn channel(&mut self) -> Result<(Self::Handle, Self::Inbox), anyhow::Error>;
}

/// A useful trait forces runtime requirements
pub trait ActorHandle: 'static + Send + dyn_clone::DynClone {
    /// Child aknowledging shutdown using its supervisor handle
    fn aknshutdown(&self, service: Service, r: ActorResult);
    /// Asking the actor to shutdown
    fn shutdown(self: Box<Self>);
    /// Pushed services (mostly from children or supervisor/sibling, but it could be by anyone who has copy of the handle)
    fn service(&self, service: &Service);
    /// Send boxed events to the actor.
    fn send(&self, event: Box<dyn std::any::Any>) -> Result<(), Box<dyn std::any::Any>>;
}

/// Wrapper to Box<dyn ActorHandle>
#[derive(Clone)]
pub struct BoxedActorHandle(Box<dyn ActorHandle>);
dyn_clone::clone_trait_object!(ActorHandle);

impl ActorHandle for Box<dyn ActorHandle> {
    fn service(&self, service: &Service) {
        (**self).service(service)
    }
    fn shutdown(self: Box<Self>) {
        (*self).shutdown()
    }
    fn aknshutdown(&self, service: Service, r: ActorResult) {
        (**self).aknshutdown(service, r)
    }
    fn send(&self, event: Box<dyn std::any::Any>) -> Result<(), Box<dyn std::any::Any>> {
        (**self).send(event)
    }
}

impl ActorHandle for BoxedActorHandle {
    fn service(&self, service: &Service) {
        self.0.service(service)
    }
    fn shutdown(self: Box<Self>) {
        self.0.shutdown()
    }
    fn aknshutdown(&self, service: Service, r: ActorResult) {
        self.0.aknshutdown(service, r)
    }
    fn send(&self, event: Box<dyn std::any::Any>) -> Result<(), Box<dyn std::any::Any>> {
        self.0.send(event)
    }
}
/// Runtime Essential trait
#[async_trait::async_trait]
pub trait Essential: Send + Sized {
    /// The supervisor handle type
    type Supervisor: Send;
    /// The actor type which impl the Channel trait
    type Actor: Channel;
    /// Defines how to breakdown the context and it should aknowledge shutdown to its supervisor
    async fn breakdown(self, r: ActorResult);
    /// Get the service from the actor context
    fn service(&mut self) -> &mut Service;
    /// Get the supervisor handle from the actor context
    fn supervisor(&mut self) -> &mut Self::Supervisor;
    /// Get the actor's handle from the actor context
    fn handle(&mut self) -> &mut Option<<Self::Actor as Channel>::Handle>;
    /// Get the actor's inbox from the actor context
    fn inbox(&mut self) -> &mut <Self::Actor as Channel>::Inbox;
}
/// Runtime Spawn trait
pub trait Spawn<A: Channel, S>: Essential {
    /// Defines how to spawn Actor of Type A with supervisor of handle S
    fn spawn(&mut self, actor: A, supervisor: S, service: Service) -> Result<A::Handle, anyhow::Error>;
}

/// An application's metrics
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Service {
    /// The status of the service
    pub status: ServiceStatus,
    /// Service name (ie app name or microservice name)
    pub name: String,
    /// Timestamp since the service is up
    pub up_since: std::time::SystemTime,
    /// Total milliseconds when the app service has been offline since up_since
    pub downtime_ms: u64,
    /// inner services similar
    pub microservices: std::collections::HashMap<String, Service>,
}

impl Service {
    /// Create a new Service
    pub fn new() -> Self {
        Self::default()
    }
    /// Set the service status
    pub fn set_status(mut self, service_status: ServiceStatus) -> Self {
        self.status = service_status;
        self
    }
    /// Update the service status
    pub fn update_status(&mut self, service_status: ServiceStatus) {
        self.status = service_status;
    }
    /// Set the service (application) name
    pub fn set_name(mut self, name: String) -> Self {
        self.name = name;
        self
    }
    /// Update the service (application) name
    pub fn update_name(&mut self, name: String) {
        self.name = name;
    }
    /// Get the service (application) name
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Set the service downtime in milliseconds
    pub fn set_downtime_ms(mut self, downtime_ms: u64) -> Self {
        self.downtime_ms = downtime_ms;
        self
    }
    /// Insert a new microservice
    pub fn update_microservice(&mut self, service_name: String, microservice: Self) {
        self.microservices.insert(service_name, microservice);
    }
    /// Update the status of a microservice
    pub fn update_microservice_status(&mut self, service_name: &str, status: ServiceStatus) {
        self.microservices.get_mut(service_name).map(|s| s.status = status);
    }
    /// Delete a microservice
    pub fn delete_microservice(&mut self, service_name: &str) {
        self.microservices.remove(service_name);
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
            status: ServiceStatus::Starting,
            name: Default::default(),
            up_since: std::time::SystemTime::now(),
            downtime_ms: 0,
            microservices: Default::default(),
        }
    }
}

/// The possible statuses a service (application) can be
#[repr(u8)]
#[derive(Copy, Clone, PartialEq, Debug, serde::Serialize, serde::Deserialize)]
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

#[async_trait::async_trait]
/// Runtime trait which provides registry functionality
pub trait Registry: Essential {
    /// Register T value in registry to be accessible within all scopes
    async fn register<T: 'static + Sync + Send + Clone>(&mut self, name: String, handle: T) -> Result<(), anyhow::Error>;
    /// Clone T registered by name
    async fn lookup<T: 'static + Sync + Send + Clone>(&mut self, name: String) -> Result<Option<T>, anyhow::Error>;
    /// Remove T registered by name
    async fn remove<T: 'static + Sync + Send + Clone>(&mut self, name: String);
    /// Return all registered actor handles for a given type
    async fn lookup_all<T: 'static + Sync + Send + Clone>(&mut self)
        -> Result<Option<std::collections::HashMap<String, T>>, anyhow::Error>;
}

/// Registry mod implementation
pub mod registry;

/// Provides builder macro
pub mod builder;

pub use registry::GlobalRegistryEvent;
