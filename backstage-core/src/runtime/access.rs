// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::actor::{
    CustomStatus, EventDriven, Service, ShutdownStream, SupervisorEvent, UnboundedTokioChannel, UnboundedTokioSender,
};
use anymap::any::CloneAny;
use std::any::TypeId;

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
        shutdown_handle: Option<ShutdownHandle>,
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
        &self,
        parent: P,
        name_fn: F,
        shutdown_handle: Option<ShutdownHandle>,
        abort_handle: Option<AbortHandle>,
    ) -> anyhow::Result<ScopeId> {
        Ok(self
            .registry
            .write()
            .await
            .new_scope(parent, name_fn, shutdown_handle, abort_handle)
            .await)
    }

    async fn drop_scope(&self, scope_id: &ScopeId) -> anyhow::Result<()> {
        self.registry.write().await.drop_scope(scope_id).await
    }

    async fn add_data<T: 'static + Send + Sync + Clone>(&self, scope_id: &ScopeId, data: T) -> anyhow::Result<()> {
        self.registry.read().await.add_data(scope_id, data).await
    }

    async fn depend_on<T: 'static + Send + Sync + Clone>(&self, scope_id: &ScopeId) -> anyhow::Result<DepStatus<T>> {
        self.registry.read().await.depend_on::<T>(scope_id).await
    }

    async fn remove_data<T: 'static + Send + Sync + Clone>(&self, scope_id: &ScopeId) -> anyhow::Result<Option<T>> {
        self.registry.read().await.remove_data(scope_id).await
    }

    async fn get_data<T: 'static + Send + Sync + Clone>(&self, scope_id: &ScopeId) -> anyhow::Result<DepStatus<T>> {
        self.registry.read().await.get_data(scope_id).await
    }

    async fn get_service(&self, scope_id: &ScopeId) -> anyhow::Result<Service> {
        self.registry.read().await.get_service(scope_id).await
    }

    async fn update_status(&self, scope_id: &ScopeId, status: Cow<'static, str>) -> anyhow::Result<()> {
        self.registry.read().await.update_status(scope_id, status).await
    }

    async fn abort(&self, scope_id: &ScopeId) -> anyhow::Result<()> {
        self.registry.read().await.abort(scope_id).await
    }

    async fn service_tree(&self, scope_id: &ScopeId) -> anyhow::Result<ServiceTree> {
        self.registry.read().await.service_tree(scope_id).await
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
        status: Cow<'static, str>,
    },
    Abort(ScopeId),
    ServiceTree(ScopeId),
}

enum ResponseType {
    NewScope(ScopeId),
    DropScope(anyhow::Result<()>),
    AddData(anyhow::Result<()>),
    DependOn(anyhow::Result<RawDepStatus>),
    RemoveData(anyhow::Result<Option<Box<dyn CloneAny + Send + Sync>>>),
    GetData(anyhow::Result<RawDepStatus>),
    GetService(anyhow::Result<Service>),
    UpdateStatus(anyhow::Result<()>),
    Abort(anyhow::Result<()>),
    ServiceTree(anyhow::Result<ServiceTree>),
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
    type Channel = UnboundedTokioChannel<Self::Event>;

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
        Sup::Event: SupervisorEvent,
        <Sup::Event as SupervisorEvent>::Children: From<PhantomData<Self>>,
    {
        self.registry
            .update_status(rt.id(), CustomStatus(ServiceStatus::Running))
            .await
            .ok();
        while let Some(e) = rt.next_event().await {
            let res = match e.req {
                RequestType::NewScope {
                    parent,
                    name_fn,
                    shutdown_handle,
                    abort_handle,
                } => ResponseType::NewScope(
                    self.registry
                        .new_scope(parent, name_fn, shutdown_handle, abort_handle)
                        .await,
                ),
                RequestType::DropScope(scope_id) => ResponseType::DropScope(self.registry.drop_scope(&scope_id).await),
                RequestType::AddData {
                    scope_id,
                    data_type,
                    data,
                } => ResponseType::AddData(self.registry.add_data_raw(&scope_id, data_type, data).await),
                RequestType::DependOn { scope_id, data_type } => {
                    ResponseType::DependOn(self.registry.depend_on_raw(&scope_id, data_type).await)
                }
                RequestType::RemoveData { scope_id, data_type } => {
                    ResponseType::RemoveData(self.registry.remove_data_raw(&scope_id, data_type).await)
                }
                RequestType::GetData { scope_id, data_type } => {
                    ResponseType::GetData(self.registry.get_data_raw(&scope_id, data_type).await)
                }
                RequestType::GetService(scope_id) => {
                    ResponseType::GetService(self.registry.get_service(&scope_id).await)
                }
                RequestType::UpdateStatus { scope_id, status } => {
                    ResponseType::UpdateStatus(self.registry.update_status(&scope_id, status).await)
                }
                RequestType::Abort(scope_id) => ResponseType::Abort(self.registry.abort(&scope_id).await),
                RequestType::ServiceTree(scope_id) => {
                    ResponseType::ServiceTree(self.registry.service_tree(&scope_id).await)
                }
            };
            e.responder.send(res).ok();
        }
        log::debug!("Registry actor shutting down!");
        Ok(())
    }
}

/// A registry owned by an actor in the runtime and accessible via a tokio channel
pub struct ActorRegistry {
    handle: UnboundedTokioSender<RegistryActorRequest>,
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
        inner_abort_handle: Option<AbortHandle>,
    ) -> RuntimeScope<Self>
    where
        Self: Send + Sized,
    {
        let mut registry = Registry::default();
        let scope_id = registry.new_scope(None, |_| "Registry".to_string(), None, None).await;
        let child_scope_id = registry
            .new_scope(scope_id, |_| name.into(), inner_shutdown_handle, inner_abort_handle)
            .await;
        let mut actor = RegistryActor { registry };
        let (sender, receiver) = UnboundedTokioChannel::new(&actor).await.unwrap();
        let (receiver, shutdown_handle) = ShutdownStream::new(receiver);
        let actor_registry = ActorRegistry { handle: sender.clone() };
        let (abort_handle, _) = AbortHandle::new_pair();
        let mut actor_rt = ActorScopedRuntime::<_, _, ()> {
            scope: RuntimeScope {
                scope_id,
                parent_id: None,
                registry: actor_registry.clone(),
                join_handles: Default::default(),
            },
            handle: sender,
            receiver,
            shutdown_handle,
            abort_handle,
            supervisor_handle: None,
        };
        let child_scope = RuntimeScope {
            scope_id: child_scope_id,
            parent_id: Some(scope_id),
            registry: actor_registry,
            join_handles: Default::default(),
        };
        tokio::spawn(async move {
            actor.run(&mut actor_rt, ()).await.ok();
        });
        child_scope
    }

    async fn new_scope<P: Send + Into<Option<ScopeId>>, F: 'static + Send + Sync + FnOnce(ScopeId) -> String>(
        &self,
        parent: P,
        name_fn: F,
        shutdown_handle: Option<ShutdownHandle>,
        abort_handle: Option<AbortHandle>,
    ) -> anyhow::Result<ScopeId> {
        let (request, recv) = RegistryActorRequest::new(RequestType::NewScope {
            parent: parent.into(),
            name_fn: Box::new(name_fn),
            shutdown_handle,
            abort_handle,
        });
        self.handle.0.send(request).map_err(|e| anyhow::anyhow!("{}", e))?;
        if let ResponseType::NewScope(r) = recv.await.unwrap() {
            Ok(r)
        } else {
            panic!("Wrong response type!")
        }
    }

    async fn drop_scope(&self, scope_id: &ScopeId) -> anyhow::Result<()> {
        let (request, recv) = RegistryActorRequest::new(RequestType::DropScope(*scope_id));
        self.handle.0.send(request).map_err(|e| anyhow::anyhow!("{}", e))?;
        if let ResponseType::DropScope(r) = recv.await.unwrap() {
            r
        } else {
            panic!("Wrong response type!")
        }
    }

    async fn add_data<T: 'static + Send + Sync + Clone>(&self, scope_id: &ScopeId, data: T) -> anyhow::Result<()> {
        let (request, recv) = RegistryActorRequest::new(RequestType::AddData {
            scope_id: *scope_id,
            data_type: TypeId::of::<T>(),
            data: Box::new(data),
        });
        self.handle.0.send(request).map_err(|e| anyhow::anyhow!("{}", e))?;
        if let ResponseType::AddData(r) = recv.await.unwrap() {
            r
        } else {
            panic!("Wrong response type!")
        }
    }

    async fn depend_on<T: 'static + Send + Sync + Clone>(&self, scope_id: &ScopeId) -> anyhow::Result<DepStatus<T>> {
        let (request, recv) = RegistryActorRequest::new(RequestType::DependOn {
            scope_id: *scope_id,
            data_type: TypeId::of::<T>(),
        });
        self.handle.0.send(request).map_err(|e| anyhow::anyhow!("{}", e))?;
        if let ResponseType::DependOn(r) = recv.await.unwrap() {
            r.map(|d| d.with_type())
        } else {
            panic!("Wrong response type!")
        }
    }

    async fn remove_data<T: 'static + Send + Sync + Clone>(&self, scope_id: &ScopeId) -> anyhow::Result<Option<T>> {
        let (request, recv) = RegistryActorRequest::new(RequestType::RemoveData {
            scope_id: *scope_id,
            data_type: TypeId::of::<T>(),
        });
        self.handle.0.send(request).map_err(|e| anyhow::anyhow!("{}", e))?;
        if let ResponseType::RemoveData(r) = recv.await.unwrap() {
            r.map(|o| o.map(|d| *unsafe { d.downcast_unchecked::<T>() }))
        } else {
            panic!("Wrong response type!")
        }
    }

    async fn get_data<T: 'static + Send + Sync + Clone>(&self, scope_id: &ScopeId) -> anyhow::Result<DepStatus<T>> {
        let (request, recv) = RegistryActorRequest::new(RequestType::GetData {
            scope_id: *scope_id,
            data_type: TypeId::of::<T>(),
        });
        self.handle.0.send(request).map_err(|e| anyhow::anyhow!("{}", e))?;
        if let ResponseType::GetData(r) = recv.await.unwrap() {
            r.map(RawDepStatus::with_type::<T>)
        } else {
            panic!("Wrong response type!")
        }
    }

    async fn get_service(&self, scope_id: &ScopeId) -> anyhow::Result<Service> {
        let (request, recv) = RegistryActorRequest::new(RequestType::GetService(*scope_id));
        self.handle.0.send(request).map_err(|e| anyhow::anyhow!("{}", e))?;
        if let ResponseType::GetService(r) = recv.await.unwrap() {
            r
        } else {
            panic!("Wrong response type!")
        }
    }

    async fn update_status(&self, scope_id: &ScopeId, status: Cow<'static, str>) -> anyhow::Result<()> {
        let (request, recv) = RegistryActorRequest::new(RequestType::UpdateStatus {
            scope_id: *scope_id,
            status,
        });
        self.handle.0.send(request).map_err(|e| anyhow::anyhow!("{}", e))?;
        if let ResponseType::UpdateStatus(r) = recv.await.unwrap() {
            r
        } else {
            panic!("Wrong response type!")
        }
    }

    async fn abort(&self, scope_id: &ScopeId) -> anyhow::Result<()> {
        let (request, recv) = RegistryActorRequest::new(RequestType::Abort(*scope_id));
        self.handle.0.send(request).map_err(|e| anyhow::anyhow!("{}", e))?;
        if let ResponseType::Abort(r) = recv.await.unwrap() {
            r
        } else {
            panic!("Wrong response type!")
        }
    }

    async fn service_tree(&self, scope_id: &ScopeId) -> anyhow::Result<ServiceTree> {
        let (request, recv) = RegistryActorRequest::new(RequestType::ServiceTree(*scope_id));
        self.handle.0.send(request).map_err(|e| anyhow::anyhow!("{}", e))?;
        if let ResponseType::ServiceTree(r) = recv.await.unwrap() {
            r
        } else {
            panic!("Wrong response type!")
        }
    }
}
