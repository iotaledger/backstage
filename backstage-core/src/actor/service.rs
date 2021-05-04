use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::{
    collections::{hash_map::Entry, HashMap},
    sync::Arc,
    time::SystemTime,
};
use tokio::sync::RwLock;

lazy_static! {
    /// Global service used by launcher actors
    pub static ref SERVICE: Arc<RwLock<Service>> = Arc::new(RwLock::new(Service::new("Launcher")));
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

/// A pool of unique IDs which can be assigned to services
#[derive(Default, Clone, Debug, Serialize, Deserialize)]
pub struct IdPool {
    pool: Vec<u16>,
}

impl IdPool {
    /// Get a new unique ID
    pub fn get_id(&mut self) -> u16 {
        if self.pool.len() == 0 {
            self.pool.push(2);
            1
        } else if self.pool.len() == 1 {
            let id = self.pool[0];
            self.pool[0] += 1;
            id
        } else {
            self.pool.pop().unwrap()
        }
    }

    /// Return an unused id
    pub fn return_id(&mut self, id: u16) {
        self.pool.push(id);
    }
}

/// An actor's service metrics
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Service {
    /// The status of the actor
    pub status: ServiceStatus,
    /// The name of the actor
    pub name: String,
    /// The unique ID of the actor
    pub id: u16,
    /// The start timestamp, used to calculated uptime
    pub up_since: SystemTime,
    /// Accumulated downtime
    pub downtime_ms: u64,
    /// Child services by id
    pub microservices: HashMap<u16, Service>,
    /// Optional logging path
    pub log_path: Option<String>,
    /// A pool of unique ids
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
    /// Update the id
    pub fn update_id(&mut self, id: u16) {
        self.id = id;
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
    /// Insert a new microservice with a unique ID
    pub fn insert_microservice(&mut self, mut microservice: Self) {
        microservice.update_id(self.id_pool.get_id());
        self.microservices.insert(microservice.id, microservice);
    }
    /// Update a microservice, if it exists. Returns the old value, or an error.
    pub fn update_microservice(&mut self, microservice: Self) -> anyhow::Result<Self> {
        match self.microservices.entry(microservice.id) {
            Entry::Occupied(mut s) => Ok(s.insert(microservice)),
            Entry::Vacant(_) => {
                anyhow::bail!("No entry exists for id {}!", microservice.id)
            }
        }
    }
    /// Either Insert or Update a microservice, depending on whether it exists. Non-existing
    /// microservices will generate a new unique ID. Returns the old entry if there was one.
    pub fn insert_or_update_microservice(&mut self, microservice: Self) -> Option<Self> {
        match self.microservices.entry(microservice.id) {
            Entry::Occupied(mut s) => Some(s.insert(microservice)),
            Entry::Vacant(_) => {
                self.insert_microservice(microservice);
                None
            }
        }
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
