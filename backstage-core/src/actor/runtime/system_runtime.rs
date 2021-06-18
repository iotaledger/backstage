use super::*;
/// A provided implementation of the `SystemRuntime` trait
pub struct SystemsRuntime {
    pub(crate) join_handles: Vec<JoinHandle<anyhow::Result<()>>>,
    pub(crate) shutdown_handles: Vec<(Option<oneshot::Sender<()>>, AbortHandle)>,
    pub(crate) senders: Map<dyn CloneAny + Send + Sync>,
    pub(crate) systems: Map<dyn CloneAny + Send + Sync>,
    pub(crate) service: Service,
}

impl SystemsRuntime {
    /// Create a new, empty SystemsRuntime
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for SystemsRuntime {
    fn default() -> Self {
        Self {
            join_handles: Default::default(),
            shutdown_handles: Default::default(),
            senders: Map::new(),
            systems: Map::new(),
            service: Service::new("Runtime"),
        }
    }
}

#[async_trait]
impl BaseRuntime for SystemsRuntime {
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
            senders: self.senders.clone(),
            systems: self.systems.clone(),
            service: self.service.spawn(name),
            ..Default::default()
        }
    }
}

impl SystemRuntime for SystemsRuntime {
    fn systems(&self) -> &Map<dyn CloneAny + Send + Sync> {
        &self.systems
    }

    fn systems_mut(&mut self) -> &mut Map<dyn CloneAny + Send + Sync> {
        &mut self.systems
    }
}

impl From<BasicRuntime> for SystemsRuntime {
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

impl From<FullRuntime> for SystemsRuntime {
    fn from(frt: FullRuntime) -> Self {
        Self {
            join_handles: frt.join_handles,
            shutdown_handles: frt.shutdown_handles,
            senders: frt.senders,
            systems: frt.systems,
            service: frt.service,
            ..Default::default()
        }
    }
}
