use crate::actor::{EventDriven, Service, ServiceStatus, TokioChannel, TokioSender};
use anymap::any::CloneAny;
use std::any::TypeId;
use tokio::sync::Mutex;

use super::*;

/// A registry shared via an Arc and RwLock
#[derive(Clone)]
pub struct ArcedRegistry {
    registry: Arc<RwLock<Registry>>,
}

impl Deref for ArcedRegistry {
    type Target = Arc<RwLock<Registry>>;

    fn deref(&self) -> &Self::Target {
        &self.registry
    }
}

impl DerefMut for ArcedRegistry {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.registry
    }
}

#[async_trait]
impl RegistryAccess for ArcedRegistry {
    async fn instantiate<S: Into<String> + Send>(
        name: S,
        shutdown_handle: Option<oneshot::Sender<()>>,
        abort_handle: Option<AbortHandle>,
    ) -> RuntimeScope<Self>
    where
        Self: Send + Sized,
    {
        RuntimeScope::new(
            Self {
                registry: Arc::new(RwLock::new(Registry::default())),
            },
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
        shutdown_handle: Option<oneshot::Sender<()>>,
        abort_handle: Option<AbortHandle>,
    ) -> usize {
        self.registry
            .write()
            .await
            .new_scope(parent, name_fn, shutdown_handle, abort_handle)
    }

    async fn drop_scope(&mut self, scope_id: &ScopeId) {
        self.registry.write().await.drop_scope(scope_id)
    }

    async fn add_data<T: 'static + Send + Sync + Clone>(&mut self, scope_id: &ScopeId, data: T) -> anyhow::Result<()> {
        self.registry.write().await.add_data(scope_id, data)
    }

    async fn depend_on<T: 'static + Send + Sync + Clone>(&mut self, scope_id: &ScopeId) -> anyhow::Result<()> {
        self.registry.write().await.depend_on::<T>(scope_id)
    }

    async fn remove_data<T: 'static + Send + Sync + Clone>(&mut self, scope_id: &ScopeId) -> anyhow::Result<Option<T>> {
        self.registry.write().await.remove_data(scope_id)
    }

    async fn get_data<T: 'static + Send + Sync + Clone>(&mut self, scope_id: &ScopeId) -> Option<T> {
        self.registry.read().await.get_data(scope_id).cloned()
    }

    async fn get_service(&mut self, scope_id: &ScopeId) -> Option<Service> {
        self.registry.read().await.get_service(scope_id)
    }

    async fn update_status(&mut self, scope_id: &ScopeId, status: ServiceStatus) -> anyhow::Result<()> {
        self.registry.write().await.update_status(scope_id, status)
    }

    async fn abort(&mut self, scope_id: &ScopeId) {
        self.registry.write().await.abort(scope_id)
    }

    async fn print(&mut self, scope_id: &ScopeId) {
        self.registry.read().await.print(scope_id)
    }

    async fn service_tree(&mut self, scope_id: &ScopeId) -> Option<ServiceTree> {
        self.registry.read().await.service_tree(scope_id)
    }
}

enum RequestType {
    NewScope {
        parent: Option<ScopeId>,
        name_fn: Box<dyn Send + Sync + FnOnce(ScopeId) -> String>,
        shutdown_handle: Option<oneshot::Sender<()>>,
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

    async fn init<'a, Reg: RegistryAccess + Send + Sync, Sup: EventDriven>(
        &mut self,
        _rt: &mut ActorInitRuntime<'a, Self, Reg, Sup>,
    ) -> Result<(), crate::actor::ActorError> {
        Ok(())
    }

    async fn run<'a, Reg: RegistryAccess + Send + Sync, Sup: EventDriven>(
        &mut self,
        rt: &mut ActorScopedRuntime<'a, Self, Reg, Sup>,
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

struct RegistryShutdownHandle(oneshot::Sender<()>);

#[async_trait]
impl RegistryAccess for ActorRegistry
where
    Self: Send + Sync,
{
    async fn instantiate<S: Into<String> + Send>(
        name: S,
        shutdown_handle: Option<oneshot::Sender<()>>,
        abort_handle: Option<AbortHandle>,
    ) -> RuntimeScope<Self>
    where
        Self: Send + Sized,
    {
        let mut registry = Registry::default();
        let scope_id = registry.new_scope(None, |_| "Registry".to_string(), None, None);
        let child_scope_id = registry.new_scope(scope_id, |_| name.into(), shutdown_handle, abort_handle);
        let mut actor = RegistryActor { registry };
        let (sender, receiver) = TokioChannel::new(&actor).unwrap();
        let actor_registry = ActorRegistry { handle: sender };
        let mut scope = RuntimeScope {
            scope_id,
            parent_id: None,
            registry: actor_registry.clone(),
            join_handles: Default::default(),
        };
        let mut child_scope = RuntimeScope {
            scope_id: child_scope_id,
            parent_id: Some(scope_id),
            registry: actor_registry,
            join_handles: Default::default(),
        };
        let (oneshot_send, oneshot_recv) = oneshot::channel::<()>();
        tokio::spawn(async move {
            let mut actor_rt = ActorScopedRuntime::<'_, _, _, ()>::new(&mut scope, receiver, oneshot_recv, None);
            actor.run(&mut actor_rt, ()).await;
        });
        child_scope
            .add_resource(Arc::new(Mutex::new(RegistryShutdownHandle(oneshot_send))))
            .await;
        child_scope
    }

    async fn new_scope<P: Send + Into<Option<ScopeId>>, F: 'static + Send + Sync + FnOnce(ScopeId) -> String>(
        &mut self,
        parent: P,
        name_fn: F,
        shutdown_handle: Option<oneshot::Sender<()>>,
        abort_handle: Option<AbortHandle>,
    ) -> usize {
        let (request, recv) = RegistryActorRequest::new(RequestType::NewScope {
            parent: parent.into(),
            name_fn: Box::new(name_fn),
            shutdown_handle,
            abort_handle,
        });
        self.handle.send(request).await;
        if let ResponseType::NewScope(r) = recv.await.unwrap() {
            r
        } else {
            panic!("Wrong response type!")
        }
    }

    async fn drop_scope(&mut self, scope_id: &ScopeId) {
        let (request, recv) = RegistryActorRequest::new(RequestType::DropScope(*scope_id));
        self.handle.send(request).await;
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
        self.handle.send(request).await;
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
        self.handle.send(request).await;
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
        self.handle.send(request).await;
        if let ResponseType::RemoveData(r) = recv.await.unwrap() {
            r.map(|o| o.map(|d| *unsafe { d.downcast_unchecked::<T>() }))
        } else {
            panic!("Wrong response type!")
        }
    }

    async fn get_data<T: 'static + Send + Sync + Clone>(&mut self, scope_id: &ScopeId) -> Option<T> {
        let (request, recv) = RegistryActorRequest::new(RequestType::GetData {
            scope_id: *scope_id,
            data_type: TypeId::of::<T>(),
        });
        self.handle.send(request).await;
        if let ResponseType::GetData(r) = recv.await.unwrap() {
            r.map(|d| *unsafe { d.downcast_unchecked::<T>() })
        } else {
            panic!("Wrong response type!")
        }
    }

    async fn get_service(&mut self, scope_id: &ScopeId) -> Option<Service> {
        let (request, recv) = RegistryActorRequest::new(RequestType::GetService(*scope_id));
        self.handle.send(request).await;
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
        self.handle.send(request).await;
        if let ResponseType::UpdateStatus(r) = recv.await.unwrap() {
            r
        } else {
            panic!("Wrong response type!")
        }
    }

    async fn abort(&mut self, scope_id: &ScopeId) {
        let (request, recv) = RegistryActorRequest::new(RequestType::Abort(*scope_id));
        self.handle.send(request).await;
        if let ResponseType::Abort = recv.await.unwrap() {
        } else {
            panic!("Wrong response type!")
        }
    }

    async fn print(&mut self, scope_id: &ScopeId) {
        let (request, recv) = RegistryActorRequest::new(RequestType::Print(*scope_id));
        self.handle.send(request).await;
        if let ResponseType::Print = recv.await.unwrap() {
        } else {
            panic!("Wrong response type!")
        }
    }

    async fn service_tree(&mut self, scope_id: &ScopeId) -> Option<ServiceTree> {
        let (request, recv) = RegistryActorRequest::new(RequestType::ServiceTree(*scope_id));
        self.handle.send(request).await;
        if let ResponseType::ServiceTree(r) = recv.await.unwrap() {
            r
        } else {
            panic!("Wrong response type!")
        }
    }
}
