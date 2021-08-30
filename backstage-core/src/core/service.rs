use super::{Actor, ScopeId};
use ptree::TreeItem;
use serde::{Deserialize, Serialize};
use std::{
    ops::{Deref, DerefMut},
    time::SystemTime,
};
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

impl std::fmt::Display for ServiceStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                ServiceStatus::Starting => "Starting",
                ServiceStatus::Initializing => "Initializing",
                ServiceStatus::Degraded => "Degraded",
                ServiceStatus::Running => "Running",
                ServiceStatus::Stopping => "Stopping",
                ServiceStatus::Maintenance => "Maintenance",
                ServiceStatus::Stopped => "Stopped",
            }
        )
    }
}

impl Default for ServiceStatus {
    fn default() -> Self {
        Self::Starting
    }
}

/// An actor's service metrics
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Service {
    // pub actor_type_id: std::any::TypeId,
    /// The actor type name, only for debuging or to provide context
    pub actor_type_name: &'static str,
    /// The status of the actor
    pub status: ServiceStatus,
    /// The directory name of the actor, must be unique within the same spawned level
    pub directory: Option<String>,
    /// The start timestamp, used to calculate uptime
    pub up_since: SystemTime,
    /// Accumulated downtime
    pub downtime_ms: u64,
    /// Microservices
    pub microservices: std::collections::HashMap<ScopeId, Self>,
}

impl Service {
    /// Create a new Service
    pub fn new<A: Actor>(directory: Option<String>) -> Self {
        Self {
        //    actor_type_id: std::any::TypeId::of::<A>(),
            actor_type_name: A::type_name(),
            directory: directory.into(),
            status: ServiceStatus::Starting,
            up_since: SystemTime::now(),
            downtime_ms: 0,
            microservices: std::collections::HashMap::new(),
        }
    }
    /// Return actor type name, note: only for debuging
    pub fn actor_type_name(&self) -> &'static str {
        self.actor_type_name
    }
    /// Set the service status
    pub fn with_status(mut self, service_status: ServiceStatus) -> Self {
        self.status = service_status;
        self
    }
    /// Update the service status
    pub fn update_status(&mut self, service_status: ServiceStatus) {
        // todo update the uptime/downtime if needed
        self.status = service_status;
    }
    pub fn directory(&self) -> &Option<String> {
        &self.directory
    }
    /// Set the service downtime in milliseconds
    pub fn with_downtime_ms(mut self, downtime_ms: u64) -> Self {
        self.downtime_ms = downtime_ms;
        self
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
    pub fn microservices(&self) -> &std::collections::HashMap<ScopeId, Service> {
        &self.microservices
    }
    pub fn microservices_mut(&mut self) -> &mut std::collections::HashMap<ScopeId, Service> {
        &mut self.microservices
    }
}

impl Default for Service {
    fn default() -> Self {
        Self {
            up_since: SystemTime::now(),
            ..Default::default()
        }
    }
}

impl TreeItem for Service {
    type Child = Service;

    fn write_self<W: std::io::Write>(&self, f: &mut W, _style: &ptree::Style) -> std::io::Result<()> {
        write!(
            f,
            "actor: {}, dir: {:?}, status: {}, uptime: {}",
            self.actor_type_name,
            self.directory,
            self.status,
            self.up_since.elapsed().expect("Expected elapsed to unwrap").as_millis()
        )
    }

    fn children(&self) -> std::borrow::Cow<[Self::Child]> {
        self.microservices
            .clone()
            .into_iter()
            .map(|(_, c)| c)
            .collect::<Vec<Self::Child>>()
            .into()
    }
}

impl std::fmt::Display for Service {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut buf = std::io::Cursor::new(Vec::<u8>::new());
        ptree::write_tree(self, &mut buf).ok();
        write!(f, "{}", String::from_utf8_lossy(&buf.into_inner()))
    }
}

#[async_trait::async_trait]
pub trait Supervise<T: Send>: Report<T, Service> + 'static + Send + Sized + Sync {
    /// Report End of life for a T actor
    /// return Some(()) if the report success
    async fn eol(self, scope_id: super::ScopeId, service: Service, actor: T, r: super::ActorResult) -> Option<()>
    where
        T: super::Actor;
}

#[async_trait::async_trait]
pub trait Report<T: Send, D: Send>: Send + Sized + 'static {
    /// Report any status & service changes
    /// return Some(()) if the report success
    async fn report(&self, scope_id: super::ScopeId, data: D) -> Option<()>;
}

/// Ideally it should be implemented using proc_macro on the event type
pub trait ReportEvent<T, D>: Send + 'static {
    fn report_event(scope: super::ScopeId, data: D) -> Self;
}

/// Ideally it should be implemented using proc_macro on the event type
pub trait EolEvent<T>: Send + 'static {
    fn eol_event(scope: super::ScopeId, service: Service, actor: T, r: super::ActorResult) -> Self;
}
