use crate::actor::{Service, TokioChannel, TokioSender};
use anymap::any::CloneAny;
use std::any::TypeId;
use tokio::sync::Mutex;

use super::*;

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
    async fn instantiate<S: Into<String> + Send>(name: S) -> RuntimeScope<Self>
    where
        Self: Send + Sized,
    {
        RuntimeScope::new(
            Self {
                registry: Arc::new(RwLock::new(Registry::default())),
            },
            None,
            name,
        )
        .await
    }

    async fn new_scope<P: Send + Into<Option<ScopeId>>>(&mut self, parent: P) -> usize {
        self.registry.write().await.new_scope(parent)
    }

    async fn drop_scope(&mut self, scope_id: &ScopeId) {
        self.registry.write().await.drop_scope(scope_id)
    }

    async fn add_data<T: 'static + Send + Sync + Clone>(&mut self, scope_id: &ScopeId, data: T) -> anyhow::Result<()> {
        self.registry.write().await.add_data(scope_id, data)
    }

    async fn remove_data<T: 'static + Send + Sync + Clone>(&mut self, scope_id: &ScopeId) -> anyhow::Result<Option<T>> {
        self.registry.write().await.remove_data(scope_id)
    }

    async fn get_data<T: 'static + Send + Sync + Clone>(&mut self, scope_id: &ScopeId) -> Option<T> {
        self.registry.read().await.get_data(scope_id).cloned()
    }
}

pub enum RequestType {
    NewScope(Option<ScopeId>),
    DropScope(ScopeId),
    AddData {
        scope_id: ScopeId,
        data_type: TypeId,
        data: Box<dyn CloneAny + Send + Sync>,
    },
    RemoveData {
        scope_id: ScopeId,
        data_type: TypeId,
    },
    GetData {
        scope_id: ScopeId,
        data_type: TypeId,
    },
}

pub enum ResponseType {
    NewScope(usize),
    DropScope,
    AddData(anyhow::Result<()>),
    RemoveData(anyhow::Result<Option<Box<dyn CloneAny + Send + Sync>>>),
    GetData(Option<Box<dyn CloneAny + Send + Sync>>),
}

pub struct RegistryActorRequest {
    req: RequestType,
    responder: tokio::sync::oneshot::Sender<ResponseType>,
}

impl RegistryActorRequest {
    pub fn new(req: RequestType) -> (Self, tokio::sync::oneshot::Receiver<ResponseType>) {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        (Self { req, responder: sender }, receiver)
    }
}

pub struct RegistryActor {
    registry: Registry,
}

#[async_trait]
impl Actor for RegistryActor
where
    Self: Send,
{
    type Dependencies = ();

    type Event = Box<RegistryActorRequest>;

    type Channel = TokioChannel<Self::Event>;

    type SupervisorEvent = ();

    async fn run<'a, Reg: RegistryAccess + Send + Sync>(
        &mut self,
        rt: &mut ActorScopedRuntime<'a, Self, Reg>,
        _deps: Self::Dependencies,
    ) -> Result<(), crate::actor::ActorError>
    where
        Self: Sized,
    {
        while let Some(e) = rt.next_event().await {
            let res = match e.req {
                RequestType::NewScope(parent) => ResponseType::NewScope(self.registry.new_scope(parent)),
                RequestType::DropScope(scope_id) => {
                    self.registry.drop_scope(&scope_id);
                    ResponseType::DropScope
                }
                RequestType::AddData { scope_id, data_type, data } => {
                    ResponseType::AddData(self.registry.add_data_raw(&scope_id, data_type, data))
                }
                RequestType::RemoveData { scope_id, data_type } => {
                    ResponseType::RemoveData(self.registry.remove_data_raw(&scope_id, data_type))
                }
                RequestType::GetData { scope_id, data_type } => {
                    ResponseType::GetData(self.registry.get_data_raw(&scope_id, data_type).cloned())
                }
            };
            e.responder.send(res);
        }
        Ok(())
    }
}

pub struct ActorRegistry {
    handle: TokioSender<Box<RegistryActorRequest>>,
}

impl Clone for ActorRegistry {
    fn clone(&self) -> Self {
        Self {
            handle: self.handle.clone(),
        }
    }
}

pub struct RegistryShutdownHandle(oneshot::Sender<()>);

#[async_trait]
impl RegistryAccess for ActorRegistry
where
    Self: Send + Sync,
{
    async fn instantiate<S: Into<String> + Send>(name: S) -> RuntimeScope<Self>
    where
        Self: Send + Sized,
    {
        let (sender, receiver) = TokioChannel::new();
        let mut registry = Registry::default();
        let scope_id = registry.new_scope(None);
        let mut actor = RegistryActor { registry };
        let mut scope = RuntimeScope {
            scope_id,
            registry: ActorRegistry { handle: sender },
            service: Service::new("Registry"),
            join_handles: Default::default(),
            shutdown_handles: Default::default(),
        };
        let mut child_scope = scope.child(name).await;
        let (oneshot_send, oneshot_recv) = oneshot::channel::<()>();
        // This doesn't work because we are trying to create a new scope (which involves calling the registry)
        // before the registry actor actually exists
        tokio::spawn(async move {
            let mut actor_rt = ActorScopedRuntime::unsupervised(&mut scope, receiver, oneshot_recv);
            actor.run(&mut actor_rt, ()).await;
        });
        child_scope
            .add_resource(Arc::new(Mutex::new(RegistryShutdownHandle(oneshot_send))))
            .await;
        child_scope
    }

    async fn new_scope<P: Send + Into<Option<ScopeId>>>(&mut self, parent: P) -> usize {
        log::debug!("Actor Registry spawning new scope");
        let (request, recv) = RegistryActorRequest::new(RequestType::NewScope(parent.into()));
        self.handle.send(Box::new(request)).await;
        if let ResponseType::NewScope(r) = recv.await.unwrap() {
            r
        } else {
            panic!("Wrong response type!")
        }
    }

    async fn drop_scope(&mut self, scope_id: &ScopeId) {
        log::debug!("Actor Registry dropping a scope");
        let (request, recv) = RegistryActorRequest::new(RequestType::DropScope(*scope_id));
        self.handle.send(Box::new(request)).await;
        if let ResponseType::DropScope = recv.await.unwrap() {
        } else {
            panic!("Wrong response type!")
        }
    }

    async fn add_data<T: 'static + Send + Sync + Clone>(&mut self, scope_id: &ScopeId, data: T) -> anyhow::Result<()> {
        log::debug!("Actor Registry adding data");
        let (request, recv) = RegistryActorRequest::new(RequestType::AddData {
            scope_id: *scope_id,
            data_type: TypeId::of::<T>(),
            data: Box::new(data),
        });
        self.handle.send(Box::new(request)).await;
        if let ResponseType::AddData(r) = recv.await.unwrap() {
            r
        } else {
            panic!("Wrong response type!")
        }
    }

    async fn remove_data<T: 'static + Send + Sync + Clone>(&mut self, scope_id: &ScopeId) -> anyhow::Result<Option<T>> {
        log::debug!("Actor Registry removing data");
        let (request, recv) = RegistryActorRequest::new(RequestType::RemoveData {
            scope_id: *scope_id,
            data_type: TypeId::of::<T>(),
        });
        self.handle.send(Box::new(request)).await;
        if let ResponseType::RemoveData(r) = recv.await.unwrap() {
            r.map(|o| o.map(|d| *unsafe { d.downcast_unchecked::<T>() }))
        } else {
            panic!("Wrong response type!")
        }
    }

    async fn get_data<T: 'static + Send + Sync + Clone>(&mut self, scope_id: &ScopeId) -> Option<T> {
        log::debug!("Actor Registry getting data");
        let (request, recv) = RegistryActorRequest::new(RequestType::GetData {
            scope_id: *scope_id,
            data_type: TypeId::of::<T>(),
        });
        self.handle.send(Box::new(request)).await;
        if let ResponseType::GetData(r) = recv.await.unwrap() {
            r.map(|d| *unsafe { d.downcast_unchecked::<T>() })
        } else {
            panic!("Wrong response type!")
        }
    }
}
