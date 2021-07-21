use num_traits::{FromPrimitive, NumAssignOps};
use ptree::TreeItem;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::convert::{TryFrom, TryInto};
use std::fmt::Display;
use std::ops::{Deref, DerefMut};
use std::time::SystemTime;

/// Defines anything which can be used as an actor's status
pub trait Status: TryFrom<&'static str> + Display + Clone {}
impl<T> Status for T where T: TryFrom<&'static str> + Display + Clone {}

#[derive(Clone)]
pub(crate) struct CustomStatus<T>(pub(crate) T);

/// The possible statuses a service can have
#[repr(u8)]
#[derive(Copy, Clone, PartialEq, Debug, Serialize, Deserialize)]
pub enum ServiceStatus {
    /// The actor exists, but is not yet running
    Starting = 0,
    /// The actor is initializing
    Initializing = 1,
    /// The actor is running
    Running = 2,
    /// The actor is stopping
    Stopping = 3,
    /// The actor has successfully stopped
    Stopped = 4,
}

impl Display for ServiceStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                ServiceStatus::Starting => "Starting",
                ServiceStatus::Initializing => "Initializing",
                ServiceStatus::Running => "Running",
                ServiceStatus::Stopping => "Stopping",
                ServiceStatus::Stopped => "Stopped",
            }
        )
    }
}

impl TryFrom<&str> for ServiceStatus {
    type Error = anyhow::Error;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        Ok(match s {
            "Starting" => ServiceStatus::Starting,
            "Initializing" => ServiceStatus::Initializing,
            "Running" => ServiceStatus::Running,
            "Stopping" => ServiceStatus::Stopping,
            "Stopped" => ServiceStatus::Stopped,
            _ => anyhow::bail!("Invalid Service Status!"),
        })
    }
}

impl<T: Status> Into<Cow<'static, str>> for CustomStatus<T> {
    fn into(self) -> Cow<'static, str> {
        self.0.to_string().into()
    }
}

impl Default for ServiceStatus {
    fn default() -> Self {
        Self::Starting
    }
}

/// A pool of unique IDs which can be assigned to services
#[derive(Default, Clone, Debug, Serialize, Deserialize)]
pub struct IdPool<T: NumAssignOps + FromPrimitive + Copy> {
    pool: Vec<T>,
}

impl<T: NumAssignOps + FromPrimitive + Copy> IdPool<T> {
    /// Get a new unique ID
    pub fn get_id(&mut self) -> T {
        if self.pool.len() == 0 {
            self.pool.push(T::from_isize(1).unwrap());
            T::from_isize(0).unwrap()
        } else if self.pool.len() == 1 {
            let id = self.pool[0];
            self.pool[0] += T::from_isize(1).unwrap();
            id
        } else {
            self.pool.pop().unwrap()
        }
    }

    /// Return an unused id
    pub fn return_id(&mut self, id: T) {
        self.pool.push(id);
    }
}

/// An actor's service metrics
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Service {
    /// The status of the actor
    pub status: Cow<'static, str>,
    /// The name of the actor
    pub name: Cow<'static, str>,
    /// The start timestamp, used to calculated uptime
    pub up_since: SystemTime,
}

impl Service {
    /// Create a new Service
    pub fn new<S: Into<Cow<'static, str>>>(name: S) -> Self {
        Self::default().with_name(name)
    }
    /// Set the service status
    pub fn with_status<S: Into<Cow<'static, str>>>(mut self, status: S) -> Self {
        self.status = status.into();
        self
    }
    /// Update the service status
    pub fn update_status<S: Into<Cow<'static, str>>>(&mut self, status: S) {
        self.status = status.into();
    }
    /// Set the service (application) name
    pub fn with_name<S: Into<Cow<'static, str>>>(mut self, name: S) -> Self {
        self.name = name.into();
        self
    }
    /// Update the service (application) name
    pub fn update_name<S: Into<Cow<'static, str>>>(&mut self, name: S) {
        self.name = name.into();
    }
    /// Check if the service is stopping
    pub fn is_stopping(&self) -> bool {
        match self.status.as_ref().try_into() {
            Ok(ServiceStatus::Stopping) => true,
            _ => false,
        }
    }
    /// Check if the service is stopped
    pub fn is_stopped(&self) -> bool {
        match self.status.as_ref().try_into() {
            Ok(ServiceStatus::Stopped) => true,
            _ => false,
        }
    }
    /// Check if the service is running
    pub fn is_running(&self) -> bool {
        match self.status.as_ref().try_into() {
            Ok(ServiceStatus::Running) => true,
            _ => false,
        }
    }
    /// Check if the service is starting
    pub fn is_starting(&self) -> bool {
        match self.status.as_ref().try_into() {
            Ok(ServiceStatus::Starting) => true,
            _ => false,
        }
    }
    /// Check if the service is initializing
    pub fn is_initializing(&self) -> bool {
        match self.status.as_ref().try_into() {
            Ok(ServiceStatus::Initializing) => true,
            _ => false,
        }
    }
    /// Get the status of this service
    pub fn status(&self) -> &Cow<'static, str> {
        &self.status
    }
    /// Get the service status
    pub fn service_status(&self) -> anyhow::Result<ServiceStatus> {
        ServiceStatus::try_from(self.status.as_ref())
    }
}

impl Default for Service {
    fn default() -> Self {
        Self {
            status: Default::default(),
            name: Default::default(),
            up_since: SystemTime::now(),
        }
    }
}

/// A tree of services
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServiceTree {
    /// The service at this level
    pub service: Service,
    /// The children of this level
    pub children: Vec<ServiceTree>,
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
            self.service.up_since.elapsed().unwrap().as_millis()
        )
    }

    fn children(&self) -> std::borrow::Cow<[Self::Child]> {
        self.children.clone().into()
    }
}

impl Display for ServiceTree {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut buf = std::io::Cursor::new(Vec::<u8>::new());
        ptree::write_tree(self, &mut buf).ok();
        write!(f, "{}", String::from_utf8_lossy(&buf.into_inner()))
    }
}
