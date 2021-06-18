use super::*;

/// A provided implementation of all runtime traits: `BaseRuntime`, `SystemRuntime`, `ResourceRuntime`, and `PoolRuntime`
pub struct FullRuntime {
    pub(crate) join_handles: Vec<JoinHandle<anyhow::Result<()>>>,
    pub(crate) shutdown_handles: Vec<(Option<oneshot::Sender<()>>, AbortHandle)>,
    pub(crate) resources: Map<dyn CloneAny + Send + Sync>,
    pub(crate) senders: Map<dyn CloneAny + Send + Sync>,
    pub(crate) systems: Map<dyn CloneAny + Send + Sync>,
    pub(crate) pools: Map<dyn CloneAny + Send + Sync>,
    pub(crate) service: Service,
}

impl FullRuntime {
    /// Create a new, empty FullRuntime
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for FullRuntime {
    fn default() -> Self {
        Self {
            join_handles: Default::default(),
            shutdown_handles: Default::default(),
            resources: Map::new(),
            senders: Map::new(),
            systems: Map::new(),
            pools: Map::new(),
            service: Service::new("Runtime"),
        }
    }
}

#[async_trait]
impl BaseRuntime for FullRuntime {
    fn new(service: Service) -> Self {
        Self {
            service,
            ..Default::default()
        }
    }

    fn join_handles(&self) -> &Vec<JoinHandle<anyhow::Result<()>>> {
        &self.join_handles
    }
    fn join_handles_mut(&mut self) -> &mut Vec<JoinHandle<anyhow::Result<()>>> {
        &mut self.join_handles
    }

    fn shutdown_handles(&self) -> &Vec<(Option<oneshot::Sender<()>>, AbortHandle)> {
        &self.shutdown_handles
    }
    fn shutdown_handles_mut(&mut self) -> &mut Vec<(Option<oneshot::Sender<()>>, AbortHandle)> {
        &mut self.shutdown_handles
    }

    fn senders(&self) -> &Map<dyn CloneAny + Send + Sync> {
        &self.senders
    }
    fn senders_mut(&mut self) -> &mut Map<dyn CloneAny + Send + Sync> {
        &mut self.senders
    }

    fn service(&self) -> &Service {
        &self.service
    }

    fn service_mut(&mut self) -> &mut Service {
        &mut self.service
    }

    fn child<S: Into<String>>(&mut self, name: S) -> Self {
        Self {
            resources: self.resources.clone(),
            senders: self.senders.clone(),
            systems: self.systems.clone(),
            pools: self.pools.clone(),
            service: self.service.spawn(name),
            ..Default::default()
        }
    }
}

#[async_trait]
impl SystemRuntime for FullRuntime {
    fn systems(&self) -> &Map<dyn CloneAny + Send + Sync> {
        &self.systems
    }

    fn systems_mut(&mut self) -> &mut Map<dyn CloneAny + Send + Sync> {
        &mut self.systems
    }
}

impl ResourceRuntime for FullRuntime {
    fn resources(&self) -> &Map<dyn CloneAny + Send + Sync> {
        &self.resources
    }

    fn resources_mut(&mut self) -> &mut Map<dyn CloneAny + Send + Sync> {
        &mut self.resources
    }
}

impl PoolRuntime for FullRuntime {
    fn pools(&self) -> &Map<dyn CloneAny + Send + Sync> {
        &self.pools
    }

    fn pools_mut(&mut self) -> &mut Map<dyn CloneAny + Send + Sync> {
        &mut self.pools
    }
}

impl From<BasicRuntime> for FullRuntime {
    fn from(brt: BasicRuntime) -> Self {
        Self {
            join_handles: brt.join_handles,
            shutdown_handles: brt.shutdown_handles,
            senders: brt.senders,
            service: brt.service,
            ..Default::default()
        }
    }
}

impl From<SystemsRuntime> for FullRuntime {
    fn from(srt: SystemsRuntime) -> Self {
        Self {
            join_handles: srt.join_handles,
            shutdown_handles: srt.shutdown_handles,
            senders: srt.senders,
            systems: srt.systems,
            service: srt.service,
            ..Default::default()
        }
    }
}
