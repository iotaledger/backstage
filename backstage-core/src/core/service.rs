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
    /// version of the service, which should be icermented after every mutation
    pub version: u32,
    /// The status of the actor
    pub status: ServiceStatus,
    /// The name of the actor
    pub name: String,
    /// The start timestamp, used to calculate uptime
    pub up_since: SystemTime,
    /// Accumulated downtime
    pub downtime_ms: u64,
}

impl Service {
    /// Create a new Service
    pub fn new<S: Into<String>>(name: S) -> Self {
        Self {
            name: name.into(),
            ..Default::default()
        }
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
    pub fn name(&self) -> std::borrow::Cow<'static, str> {
        self.name.clone().into()
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
}

impl Default for Service {
    fn default() -> Self {
        Self {
            version: 0,
            status: Default::default(),
            name: Default::default(),
            up_since: SystemTime::now(),
            downtime_ms: Default::default(),
        }
    }
}

/// A tree of services
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct ServiceTree {
    /// The service at this level
    pub service: Service,
    /// The children of this level
    pub children: std::collections::HashMap<usize, ServiceTree>,
}

impl ServiceTree {
    /// Create new service tree
    pub fn new<T: Into<String>>(name: T) -> Self {
        ServiceTree {
            service: Service::new(name),
            ..Default::default()
        }
    }
}

impl Deref for ServiceTree {
    type Target = Service;

    fn deref(&self) -> &Self::Target {
        &self.service
    }
}

impl DerefMut for ServiceTree {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.service
    }
}

impl TreeItem for ServiceTree {
    type Child = ServiceTree;

    fn write_self<W: std::io::Write>(&self, f: &mut W, _style: &ptree::Style) -> std::io::Result<()> {
        write!(
            f,
            "{}: {}, uptime: {}",
            self.service.name,
            self.service.status,
            self.service.up_since.elapsed().expect("Expected elapsed to unwrap").as_millis()
        )
    }

    fn children(&self) -> std::borrow::Cow<[Self::Child]> {
        self.children
            .clone()
            .into_iter()
            .map(|(_, c)| c)
            .collect::<Vec<ServiceTree>>()
            .into()
    }
}

impl std::fmt::Display for ServiceTree {
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
