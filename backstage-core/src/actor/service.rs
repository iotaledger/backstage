// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use crate::prelude::ScopeId;
use ptree::TreeItem;
use serde::{Deserialize, Serialize};
use std::{
    borrow::Cow,
    convert::{TryFrom, TryInto},
    fmt::Display,
    ops::{Deref, DerefMut},
    sync::Arc,
    time::SystemTime,
};
use tokio::sync::RwLock;

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

impl Into<Cow<'static, str>> for ServiceStatus {
    fn into(self) -> Cow<'static, str> {
        self.to_string().into()
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

/// An actor's service metrics
#[derive(Clone, Debug)]
pub struct Service {
    /// Inner data
    inner: Arc<ServiceInner>,
}

/// Shared service data
#[derive(Debug)]
pub struct ServiceInner {
    /// The scope ID of this service
    scope_id: ScopeId,
    /// The status of the actor
    status: RwLock<Cow<'static, str>>,
    /// The name of the actor
    name: Cow<'static, str>,
    /// The start timestamp, used to calculated uptime
    up_since: SystemTime,
}

/// A snapshot of a service at a given time
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServiceView {
    /// The scope ID of this service
    scope_id: ScopeId,
    /// The status of the actor
    status: Cow<'static, str>,
    /// The name of the actor
    name: Cow<'static, str>,
    /// The start timestamp, used to calculated uptime
    up_since: SystemTime,
}

impl Service {
    /// Create a new Service
    pub fn new<S: Into<Cow<'static, str>>>(scope_id: ScopeId, name: S) -> Self {
        Self {
            inner: Arc::new(ServiceInner {
                scope_id,
                status: RwLock::new(ServiceStatus::Starting.to_string().into()),
                name: name.into(),
                up_since: SystemTime::now(),
            }),
        }
    }
}

impl Deref for Service {
    type Target = ServiceInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl ServiceInner {
    /// Update the service status
    pub async fn update_status<S: Into<Cow<'static, str>>>(&self, status: S) {
        *self.status.write().await = status.into();
    }
    /// Update the service (application) name
    pub fn update_name<S: Into<Cow<'static, str>>>(&mut self, name: S) {
        self.name = name.into();
    }

    /// Get the service scope id
    pub fn scope_id(&self) -> &ScopeId {
        &self.scope_id
    }
    /// Get the name of this service
    pub fn name(&self) -> &Cow<'static, str> {
        &self.name
    }
    /// Get the service uptime in milliseconds
    pub fn up_since(&self) -> &SystemTime {
        &self.up_since
    }

    /// Get a snapshot of the service
    pub async fn view(&self) -> ServiceView {
        ServiceView {
            scope_id: self.scope_id,
            status: self.status.read().await.clone(),
            name: self.name.clone(),
            up_since: self.up_since,
        }
    }
}

impl ServiceView {
    /// Update the service status
    pub(crate) fn update_status<S: Into<Cow<'static, str>>>(&mut self, status: S) {
        self.status = status.into();
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
    /// Get the service scope id
    pub fn scope_id(&self) -> &ScopeId {
        &self.scope_id
    }
    /// Get the status of this service
    pub fn status(&self) -> Cow<'static, str> {
        self.status.clone()
    }
    /// Get the name of this service
    pub fn name(&self) -> &Cow<'static, str> {
        &self.name
    }
    /// Get the service uptime in milliseconds
    pub fn up_since(&self) -> &SystemTime {
        &self.up_since
    }
    /// Get the service status
    pub fn service_status(&self) -> anyhow::Result<ServiceStatus> {
        ServiceStatus::try_from(self.status.as_ref())
    }
}

impl Display for ServiceView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} ({:x}) - {}, Uptime {} ms",
            self.name(),
            self.scope_id.as_fields().0,
            self.status(),
            self.up_since().elapsed().unwrap().as_millis(),
        )
    }
}

/// A tree of services
#[derive(Clone, Debug)]
pub struct ServiceTree {
    /// The service at this level
    pub service: ServiceView,
    /// The children of this level
    pub children: Vec<ServiceTree>,
}

impl Deref for ServiceTree {
    type Target = ServiceView;

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
            "{} ({:x}) - {}, Uptime {} ms",
            self.service.name(),
            self.scope_id.as_fields().0,
            self.service.status(),
            self.service.up_since().elapsed().unwrap().as_millis(),
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
