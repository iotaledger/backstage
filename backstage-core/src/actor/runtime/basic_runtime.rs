use super::*;
pub struct BasicRuntime {
    pub(crate) join_handles: Vec<JoinHandle<Result<(), ActorError>>>,
    pub(crate) shutdown_handles: Vec<(Option<oneshot::Sender<()>>, AbortHandle)>,
    pub(crate) senders: Map<dyn CloneAny + Send + Sync>,
}

impl BasicRuntime {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for BasicRuntime {
    fn default() -> Self {
        Self {
            join_handles: Default::default(),
            shutdown_handles: Default::default(),
            senders: Map::new(),
        }
    }
}

#[async_trait]
impl BaseRuntime for BasicRuntime {
    fn child(&self) -> Self {
        Self {
            senders: self.senders.clone(),
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

impl From<FullRuntime> for BasicRuntime {
    fn from(frt: FullRuntime) -> Self {
        Self {
            join_handles: frt.join_handles,
            shutdown_handles: frt.shutdown_handles,
            senders: frt.senders,
        }
    }
}

impl From<SystemsRuntime> for BasicRuntime {
    fn from(frt: SystemsRuntime) -> Self {
        Self {
            join_handles: frt.join_handles,
            shutdown_handles: frt.shutdown_handles,
            senders: frt.senders,
        }
    }
}
