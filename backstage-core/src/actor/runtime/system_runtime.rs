use super::*;
pub struct SystemsRuntime {
    pub(crate) join_handles: Vec<JoinHandle<Result<(), ActorError>>>,
    pub(crate) shutdown_handles: Vec<(Option<oneshot::Sender<()>>, AbortHandle)>,
    pub(crate) senders: Map<dyn CloneAny + Send + Sync>,
    pub(crate) systems: Map<dyn CloneAny + Send + Sync>,
}

impl SystemsRuntime {
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
        }
    }
}

#[async_trait]
impl BaseRuntime for SystemsRuntime {
    fn child(&self) -> Self {
        Self {
            senders: self.senders.clone(),
            systems: self.systems.clone(),
            ..Default::default()
        }
    }

    fn join_handles(&self) -> &Vec<JoinHandle<Result<(), ActorError>>> {
        &self.join_handles
    }

    fn join_handles_mut(&mut self) -> &mut Vec<JoinHandle<Result<(), ActorError>>> {
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
        }
    }
}
