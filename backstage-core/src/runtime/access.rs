use super::*;
use crate::actor::{EventDriven, Service, ServiceStatus, ShutdownStream, TokioChannel, TokioSender};
use anymap::any::CloneAny;
use std::any::TypeId;

#[async_trait]
impl RegistryAccess for Registry {
    async fn instantiate<S: Into<String> + Send>(
        name: S,
        shutdown_handle: Option<ShutdownHandle>,
        abort_handle: Option<AbortHandle>,
    ) -> RuntimeScope<Self>
    where
        Self: Send + Sized,
    {
        RuntimeScope::new(
            Arc::new(RwLock::new(Registry::default())),
            None,
            name,
            shutdown_handle,
            abort_handle,
        )
        .await
    }

    async fn new_scope<P: Send + Into<Option<ScopeId>>, F: 'static + Send + Sync + FnOnce(ScopeId) -> String>(
        &mut self,
        parent: P,
        name_fn: F,
        shutdown_handle: Option<ShutdownHandle>,
        abort_handle: Option<AbortHandle>,
    ) -> usize {
        self.new_scope(parent, name_fn, shutdown_handle, abort_handle)
    }

    async fn drop_scope(&mut self, scope_id: &ScopeId) {
        self.drop_scope(scope_id)
    }

    async fn add_data<T: 'static + Send + Sync + Clone>(&mut self, scope_id: &ScopeId, data: T) -> anyhow::Result<()> {
        self.add_data(scope_id, data)
    }

    async fn depend_on<T: 'static + Send + Sync + Clone>(&mut self, scope_id: &ScopeId) -> anyhow::Result<()> {
        self.depend_on::<T>(scope_id)
    }

    async fn remove_data<T: 'static + Send + Sync + Clone>(&mut self, scope_id: &ScopeId) -> anyhow::Result<Option<T>> {
        self.remove_data(scope_id)
    }

    async fn get_data<T: 'static + Send + Sync + Clone>(&self, scope_id: &ScopeId) -> Option<T> {
        self.get_data(scope_id).cloned()
    }

    async fn get_service(&self, scope_id: &ScopeId) -> Option<Service> {
        self.get_service(scope_id)
    }

    async fn update_status(&mut self, scope_id: &ScopeId, status: ServiceStatus) -> anyhow::Result<()> {
        self.update_status(scope_id, status)
    }

    async fn abort(&mut self, scope_id: &ScopeId) {
        self.abort(scope_id)
    }

    async fn print(&self, scope_id: &ScopeId) {
        self.print(scope_id)
    }

    async fn service_tree(&self, scope_id: &ScopeId) -> Option<ServiceTree> {
        self.service_tree(scope_id)
    }
}

enum RequestType {
    NewScope {
        parent: Option<ScopeId>,
        name_fn: Box<dyn Send + Sync + FnOnce(ScopeId) -> String>,
        shutdown_handle: Option<ShutdownHandle>,
        abort_handle: Option<AbortHandle>,
    },
    DropScope(ScopeId),
    AddData {
        scope_id: ScopeId,
        data_type: TypeId,
        data: Box<dyn CloneAny + Send + Sync>,
    },
    DependOn {
        scope_id: ScopeId,
        data_type: TypeId,
    },
    RemoveData {
        scope_id: ScopeId,
        data_type: TypeId,
    },
    GetData {
        scope_id: ScopeId,
        data_type: TypeId,
    },
    GetService(ScopeId),
    UpdateStatus {
        scope_id: ScopeId,
        status: ServiceStatus,
    },
    Abort(ScopeId),
    Print(ScopeId),
    ServiceTree(ScopeId),
}

enum ResponseType {
    NewScope(usize),
    DropScope,
    AddData(anyhow::Result<()>),
    DependOn(anyhow::Result<()>),
    RemoveData(anyhow::Result<Option<Box<dyn CloneAny + Send + Sync>>>),
    GetData(Option<Box<dyn CloneAny + Send + Sync>>),
    GetService(Option<Service>),
    UpdateStatus(anyhow::Result<()>),
    Abort,
    Print,
    ServiceTree(Option<ServiceTree>),
}

struct RegistryActorRequest {
    req: RequestType,
    responder: tokio::sync::oneshot::Sender<ResponseType>,
}

impl RegistryActorRequest {
    pub fn new(req: RequestType) -> (Self, tokio::sync::oneshot::Receiver<ResponseType>) {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        (Self { req, responder: sender }, receiver)
    }
}

struct RegistryActor {
    registry: Registry,
}

#[async_trait]
impl Actor for RegistryActor
where
    Self: Send,
{
    type Dependencies = ();
    type Event = RegistryActorRequest;
    type Channel = TokioChannel<Self::Event>;

    async fn init<Reg: RegistryAccess + Send + Sync, Sup: EventDriven>(
        &mut self,
        _rt: &mut ActorScopedRuntime<Self, Reg, Sup>,
    ) -> Result<(), crate::actor::ActorError> {
        Ok(())
    }

    async fn run<Reg: RegistryAccess + Send + Sync, Sup: EventDriven>(
        &mut self,
        rt: &mut ActorScopedRuntime<Self, Reg, Sup>,
        _deps: Self::Dependencies,
    ) -> Result<(), crate::actor::ActorError>
    where
        Self: Sized,
    {
        while let Some(e) = rt.next_event().await {
            let res = match e.req {
                RequestType::NewScope {
                    parent,
                    name_fn,
                    shutdown_handle,
                    abort_handle,
                } => ResponseType::NewScope(self.registry.new_scope(parent, name_fn, shutdown_handle, abort_handle)),
                RequestType::DropScope(scope_id) => {
                    self.registry.drop_scope(&scope_id);
                    ResponseType::DropScope
                }
                RequestType::AddData { scope_id, data_type, data } => {
                    ResponseType::AddData(self.registry.add_data_raw(&scope_id, data_type, data))
                }
                RequestType::DependOn { scope_id, data_type } => ResponseType::DependOn(self.registry.depend_on_raw(&scope_id, data_type)),
                RequestType::RemoveData { scope_id, data_type } => {
                    ResponseType::RemoveData(self.registry.remove_data_raw(&scope_id, data_type))
                }
                RequestType::GetData { scope_id, data_type } => {
                    ResponseType::GetData(self.registry.get_data_raw(&scope_id, data_type).cloned())
                }
                RequestType::GetService(scope_id) => ResponseType::GetService(self.registry.get_service(&scope_id)),
                RequestType::UpdateStatus { scope_id, status } => {
                    ResponseType::UpdateStatus(self.registry.update_status(&scope_id, status))
                }
                RequestType::Abort(scope_id) => {
                    self.registry.abort(&scope_id);
                    ResponseType::Abort
                }
                RequestType::Print(scope_id) => {
                    self.registry.print(&scope_id);
                    ResponseType::Print
                }
                RequestType::ServiceTree(scope_id) => ResponseType::ServiceTree(self.registry.service_tree(&scope_id)),
            };
            e.responder.send(res);
        }
        log::debug!("Registry actor shutting down!");
        self.registry.print(&0);
        Ok(())
    }
}

/// A registry owned by an actor in the runtime and accessible via a tokio channel
pub struct ActorRegistry {
    handle: TokioSender<RegistryActorRequest>,
}

impl Clone for ActorRegistry {
    fn clone(&self) -> Self {
        Self {
            handle: self.handle.clone(),
        }
    }
}

#[async_trait]
impl RegistryAccess for ActorRegistry
where
    Self: Send + Sync,
{
    async fn instantiate<S: Into<String> + Send>(
        name: S,
        inner_shutdown_handle: Option<ShutdownHandle>,
        abort_handle: Option<AbortHandle>,
    ) -> RuntimeScope<Self>
    where
        Self: Send + Sized,
    {
        let mut registry = Registry::default();
        let scope_id = registry.new_scope(None, |_| "Registry".to_string(), None, None);
        let child_scope_id = registry.new_scope(scope_id, |_| name.into(), inner_shutdown_handle.clone(), abort_handle.clone());
        let mut actor = RegistryActor { registry };
        let (sender, receiver) = TokioChannel::new(&actor).await.unwrap();
        let (receiver, shutdown_handle) = ShutdownStream::new(receiver);
        let actor_registry = ActorRegistry { handle: sender.clone() };
        let mut actor_rt = ActorScopedRuntime::<_, _, ()> {
            scope: RuntimeScope {
                scope_id,
                parent_id: None,
                registry: Arc::new(RwLock::new(actor_registry.clone())),
                join_handles: Default::default(),
                shutdown_handle: Some(shutdown_handle.clone()),
                abort_handle: None,
            },
            handle: sender,
            receiver,
            shutdown_handle,
            supervisor_handle: None,
        };
        let child_scope = RuntimeScope {
            scope_id: child_scope_id,
            parent_id: Some(scope_id),
            registry: Arc::new(RwLock::new(actor_registry)),
            join_handles: Default::default(),
            shutdown_handle: inner_shutdown_handle,
            abort_handle,
        };
        tokio::spawn(async move {
            actor.run(&mut actor_rt, ()).await;
        });
        child_scope
    }

    async fn new_scope<P: Send + Into<Option<ScopeId>>, F: 'static + Send + Sync + FnOnce(ScopeId) -> String>(
        &mut self,
        parent: P,
        name_fn: F,
        shutdown_handle: Option<ShutdownHandle>,
        abort_handle: Option<AbortHandle>,
    ) -> usize {
        let (request, recv) = RegistryActorRequest::new(RequestType::NewScope {
            parent: parent.into(),
            name_fn: Box::new(name_fn),
            shutdown_handle,
            abort_handle,
        });
        self.handle.0.send(request);
        if let ResponseType::NewScope(r) = recv.await.unwrap() {
            r
        } else {
            panic!("Wrong response type!")
        }
    }

    async fn drop_scope(&mut self, scope_id: &ScopeId) {
        let (request, recv) = RegistryActorRequest::new(RequestType::DropScope(*scope_id));
        self.handle.0.send(request);
        if let ResponseType::DropScope = recv.await.unwrap() {
        } else {
            panic!("Wrong response type!")
        }
    }

    async fn add_data<T: 'static + Send + Sync + Clone>(&mut self, scope_id: &ScopeId, data: T) -> anyhow::Result<()> {
        let (request, recv) = RegistryActorRequest::new(RequestType::AddData {
            scope_id: *scope_id,
            data_type: TypeId::of::<T>(),
            data: Box::new(data),
        });
        self.handle.0.send(request);
        if let ResponseType::AddData(r) = recv.await.unwrap() {
            r
        } else {
            panic!("Wrong response type!")
        }
    }

    async fn depend_on<T: 'static + Send + Sync + Clone>(&mut self, scope_id: &ScopeId) -> anyhow::Result<()> {
        let (request, recv) = RegistryActorRequest::new(RequestType::DependOn {
            scope_id: *scope_id,
            data_type: TypeId::of::<T>(),
        });
        self.handle.0.send(request);
        if let ResponseType::DependOn(r) = recv.await.unwrap() {
            r
        } else {
            panic!("Wrong response type!")
        }
    }

    async fn remove_data<T: 'static + Send + Sync + Clone>(&mut self, scope_id: &ScopeId) -> anyhow::Result<Option<T>> {
        let (request, recv) = RegistryActorRequest::new(RequestType::RemoveData {
            scope_id: *scope_id,
            data_type: TypeId::of::<T>(),
        });
        self.handle.0.send(request);
        if let ResponseType::RemoveData(r) = recv.await.unwrap() {
            r.map(|o| o.map(|d| *unsafe { d.downcast_unchecked::<T>() }))
        } else {
            panic!("Wrong response type!")
        }
    }

    async fn get_data<T: 'static + Send + Sync + Clone>(&self, scope_id: &ScopeId) -> Option<T> {
        let (request, recv) = RegistryActorRequest::new(RequestType::GetData {
            scope_id: *scope_id,
            data_type: TypeId::of::<T>(),
        });
        self.handle.0.send(request);
        if let ResponseType::GetData(r) = recv.await.unwrap() {
            r.map(|d| *unsafe { d.downcast_unchecked::<T>() })
        } else {
            panic!("Wrong response type!")
        }
    }

    async fn get_service(&self, scope_id: &ScopeId) -> Option<Service> {
        let (request, recv) = RegistryActorRequest::new(RequestType::GetService(*scope_id));
        self.handle.0.send(request);
        if let ResponseType::GetService(r) = recv.await.unwrap() {
            r
        } else {
            panic!("Wrong response type!")
        }
    }

    async fn update_status(&mut self, scope_id: &ScopeId, status: ServiceStatus) -> anyhow::Result<()> {
        let (request, recv) = RegistryActorRequest::new(RequestType::UpdateStatus {
            scope_id: *scope_id,
            status,
        });
        self.handle.0.send(request);
        if let ResponseType::UpdateStatus(r) = recv.await.unwrap() {
            r
        } else {
            panic!("Wrong response type!")
        }
    }

    async fn abort(&mut self, scope_id: &ScopeId) {
        let (request, recv) = RegistryActorRequest::new(RequestType::Abort(*scope_id));
        self.handle.0.send(request);
        if let ResponseType::Abort = recv.await.unwrap() {
        } else {
            panic!("Wrong response type!")
        }
    }

    async fn print(&self, scope_id: &ScopeId) {
        let (request, recv) = RegistryActorRequest::new(RequestType::Print(*scope_id));
        self.handle.0.send(request);
        if let ResponseType::Print = recv.await.unwrap() {
        } else {
            panic!("Wrong response type!")
        }
    }

    async fn service_tree(&self, scope_id: &ScopeId) -> Option<ServiceTree> {
        let (request, recv) = RegistryActorRequest::new(RequestType::ServiceTree(*scope_id));
        self.handle.0.send(request);
        if let ResponseType::ServiceTree(r) = recv.await.unwrap() {
            r
        } else {
            panic!("Wrong response type!")
        }
    }
}
