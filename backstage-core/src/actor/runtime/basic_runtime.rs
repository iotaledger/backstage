use super::*;
/// A provided implementation of the `BaseRuntime` trait
pub struct BasicRuntime {
    pub(crate) join_handles: Vec<JoinHandle<anyhow::Result<()>>>,
    pub(crate) shutdown_handles: Vec<(Option<oneshot::Sender<()>>, AbortHandle)>,
    pub(crate) data: Map<dyn CloneAny + Send + Sync>,
    pub(crate) service: Service,
}

impl Default for BasicRuntime {
    fn default() -> Self {
        Self {
            join_handles: Default::default(),
            shutdown_handles: Default::default(),
            data: Map::new(),
            service: Service::new("Runtime"),
        }
    }
}

#[async_trait]
impl BaseRuntime for BasicRuntime {
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
        &self.data
    }

    fn senders_mut(&mut self) -> &mut Map<dyn CloneAny + Send + Sync> {
        &mut self.data
    }

    fn service(&self) -> &Service {
        &self.service
    }

    fn service_mut(&mut self) -> &mut Service {
        &mut self.service
    }

    fn dependency_channels(&self) -> &Map<dyn CloneAny + Send + Sync> {
        &self.data
    }

    fn dependency_channels_mut(&mut self) -> &mut Map<dyn CloneAny + Send + Sync> {
        &mut self.data
    }

    fn child<S: Into<String>>(&mut self, name: S) -> Self {
        Self {
            data: self.data.clone(),
            service: self.service.spawn(name),
            ..Default::default()
        }
    }
}

impl From<FullRuntime> for BasicRuntime {
    fn from(frt: FullRuntime) -> Self {
        Self {
            join_handles: frt.join_handles,
            shutdown_handles: frt.shutdown_handles,
            data: frt.data,
            service: frt.service,
            ..Default::default()
        }
    }
}

impl From<SystemsRuntime> for BasicRuntime {
    fn from(srt: SystemsRuntime) -> Self {
        Self {
            join_handles: srt.join_handles,
            shutdown_handles: srt.shutdown_handles,
            data: srt.data,
            service: srt.service,
            ..Default::default()
        }
    }
}
