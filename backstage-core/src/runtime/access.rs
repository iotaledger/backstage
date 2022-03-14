// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::actor::{Envelope, HandleEvent, Service, ShutdownStream, UnboundedTokioChannel, UnboundedTokioSender};
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
    ) -> RuntimeScope
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

    async fn new_scope(
        &self,
        parent: Option<ScopeId>,
        name_fn: Box<dyn 'static + Send + Sync + FnOnce(ScopeId) -> String>,
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

    async fn add_data(
        &self,
        scope_id: &ScopeId,
        data_type: TypeId,
        data: Box<dyn CloneAny + Send + Sync>,
    ) -> anyhow::Result<()> {
        self.registry.read().await.add_data_raw(scope_id, data_type, data).await
    }

    async fn depend_on(&self, scope_id: &ScopeId, data_type: TypeId) -> anyhow::Result<RawDepStatus> {
        self.registry.read().await.depend_on_raw(scope_id, data_type).await
    }

    async fn remove_data(
        &self,
        scope_id: &ScopeId,
        data_type: TypeId,
    ) -> anyhow::Result<Box<dyn CloneAny + Send + Sync>> {
        self.registry.read().await.remove_data_raw(scope_id, data_type).await
    }

    async fn get_data(&self, scope_id: &ScopeId, data_type: TypeId) -> anyhow::Result<RawDepStatus> {
        self.registry.read().await.get_data_raw(scope_id, data_type).await
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

impl std::fmt::Debug for RequestType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NewScope {
                parent,
                name_fn: _,
                shutdown_handle,
                abort_handle,
            } => f
                .debug_struct("NewScope")
                .field("parent", parent)
                .field("shutdown_handle", shutdown_handle)
                .field("abort_handle", abort_handle)
                .finish(),
            Self::DropScope(arg0) => f.debug_tuple("DropScope").field(arg0).finish(),
            Self::AddData {
                scope_id,
                data_type,
                data,
            } => f
                .debug_struct("AddData")
                .field("scope_id", scope_id)
                .field("data_type", data_type)
                .field("data", data)
                .finish(),
            Self::DependOn { scope_id, data_type } => f
                .debug_struct("DependOn")
                .field("scope_id", scope_id)
                .field("data_type", data_type)
                .finish(),
            Self::RemoveData { scope_id, data_type } => f
                .debug_struct("RemoveData")
                .field("scope_id", scope_id)
                .field("data_type", data_type)
                .finish(),
            Self::GetData { scope_id, data_type } => f
                .debug_struct("GetData")
                .field("scope_id", scope_id)
                .field("data_type", data_type)
                .finish(),
            Self::GetService(arg0) => f.debug_tuple("GetService").field(arg0).finish(),
            Self::UpdateStatus { scope_id, status } => f
                .debug_struct("UpdateStatus")
                .field("scope_id", scope_id)
                .field("status", status)
                .finish(),
            Self::Abort(arg0) => f.debug_tuple("Abort").field(arg0).finish(),
            Self::ServiceTree(arg0) => f.debug_tuple("ServiceTree").field(arg0).finish(),
        }
    }
}

#[derive(Debug)]
enum ResponseType {
    NewScope(ScopeId),
    DropScope(anyhow::Result<()>),
    AddData(anyhow::Result<()>),
    DependOn(anyhow::Result<RawDepStatus>),
    RemoveData(anyhow::Result<Box<dyn CloneAny + Send + Sync>>),
    GetData(anyhow::Result<RawDepStatus>),
    GetService(anyhow::Result<Service>),
    UpdateStatus(anyhow::Result<()>),
    Abort(anyhow::Result<()>),
    ServiceTree(anyhow::Result<ServiceTree>),
}

#[derive(Debug)]
struct RegistryActorRequest {
    req: RequestType,
    responder: tokio::sync::oneshot::Sender<ResponseType>,
}

impl RegistryActorRequest {
    pub fn new(req: RequestType) -> (Box<Self>, tokio::sync::oneshot::Receiver<ResponseType>) {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        (Box::new(Self { req, responder: sender }), receiver)
    }
}

#[derive(Debug)]
struct RegistryActor {
    registry: Registry,
}

#[async_trait]
impl Actor for RegistryActor
where
    Self: Send,
{
    type Data = ();
    type Context = UnsupervisedContext<Self>;

    async fn init(&mut self, cx: &mut Self::Context) -> Result<Self::Data, crate::actor::ActorError>
    where
        Self: 'static + Sized + Send + Sync,
    {
        cx.update_status(ServiceStatus::Running).await?;
        Ok(())
    }
}

#[async_trait]
impl HandleEvent<RegistryActorRequest> for RegistryActor {
    async fn handle_event(
        &mut self,
        _cx: &mut Self::Context,
        RegistryActorRequest { req, responder }: RegistryActorRequest,
        _data: &mut Self::Data,
    ) -> Result<(), crate::actor::ActorError>
    where
        Self: 'static + Sized + Send + Sync,
    {
        let res = match req {
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
            RequestType::GetService(scope_id) => ResponseType::GetService(self.registry.get_service(&scope_id).await),
            RequestType::UpdateStatus { scope_id, status } => {
                ResponseType::UpdateStatus(self.registry.update_status(&scope_id, status).await)
            }
            RequestType::Abort(scope_id) => ResponseType::Abort(self.registry.abort(&scope_id).await),
            RequestType::ServiceTree(scope_id) => {
                ResponseType::ServiceTree(self.registry.service_tree(&scope_id).await)
            }
        };
        responder.send(res).ok();
        Ok(())
    }
}

/// A registry owned by an actor in the runtime and accessible via a tokio channel
pub struct ActorRegistry {
    handle: UnboundedTokioSender<Envelope<RegistryActor>>,
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
    async fn instantiate<S: 'static + Into<String> + Send + Sync>(
        name: S,
        inner_shutdown_handle: Option<ShutdownHandle>,
        inner_abort_handle: Option<AbortHandle>,
    ) -> RuntimeScope
    where
        Self: Send + Sized,
    {
        let mut registry = Registry::default();
        let scope_id = registry
            .new_scope(None, Box::new(|_| "Registry".to_string()), None, None)
            .await;
        let child_scope_id = registry
            .new_scope(
                scope_id.into(),
                Box::new(|_| name.into()),
                inner_shutdown_handle,
                inner_abort_handle,
            )
            .await;
        let mut actor = RegistryActor { registry };
        let (sender, receiver) = UnboundedTokioChannel::new(&actor).await.unwrap();
        let (receiver, shutdown_handle) = ShutdownStream::new(receiver);
        let actor_registry = ActorRegistry { handle: sender.clone() };
        let (abort_handle, _) = AbortHandle::new_pair();
        let mut cx = UnsupervisedContext {
            scope: RuntimeScope {
                scope_id,
                parent_id: None,
                registry: Box::new(actor_registry.clone()),
                join_handles: Default::default(),
            },
            handle: Act::new(scope_id, Box::new(sender), shutdown_handle, abort_handle),
            receiver: Box::new(receiver),
        };
        let child_scope = RuntimeScope {
            scope_id: child_scope_id,
            parent_id: Some(scope_id),
            registry: Box::new(actor_registry),
            join_handles: Default::default(),
        };
        // log::debug!("Spawning task for registry actor");
        tokio::spawn(async move {
            // Call handle events until shutdown
            actor.run(&mut cx, &mut ()).await
        });
        child_scope
    }

    async fn new_scope(
        &self,
        parent: Option<ScopeId>,
        name_fn: Box<dyn 'static + Send + Sync + FnOnce(ScopeId) -> String>,
        shutdown_handle: Option<ShutdownHandle>,
        abort_handle: Option<AbortHandle>,
    ) -> anyhow::Result<ScopeId> {
        let (request, recv) = RegistryActorRequest::new(RequestType::NewScope {
            parent: parent.into(),
            name_fn,
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

    async fn add_data(
        &self,
        scope_id: &ScopeId,
        data_type: TypeId,
        data: Box<dyn CloneAny + Send + Sync>,
    ) -> anyhow::Result<()> {
        let (request, recv) = RegistryActorRequest::new(RequestType::AddData {
            scope_id: *scope_id,
            data_type,
            data,
        });
        self.handle.0.send(request).map_err(|e| anyhow::anyhow!("{}", e))?;
        if let ResponseType::AddData(r) = recv.await.unwrap() {
            r
        } else {
            panic!("Wrong response type!")
        }
    }

    async fn depend_on(&self, scope_id: &ScopeId, data_type: TypeId) -> anyhow::Result<RawDepStatus> {
        let (request, recv) = RegistryActorRequest::new(RequestType::DependOn {
            scope_id: *scope_id,
            data_type,
        });
        self.handle.0.send(request).map_err(|e| anyhow::anyhow!("{}", e))?;
        if let ResponseType::DependOn(r) = recv.await.unwrap() {
            r
        } else {
            panic!("Wrong response type!")
        }
    }

    async fn remove_data(
        &self,
        scope_id: &ScopeId,
        data_type: TypeId,
    ) -> anyhow::Result<Box<dyn CloneAny + Send + Sync>> {
        let (request, recv) = RegistryActorRequest::new(RequestType::RemoveData {
            scope_id: *scope_id,
            data_type,
        });
        self.handle.0.send(request).map_err(|e| anyhow::anyhow!("{}", e))?;
        if let ResponseType::RemoveData(r) = recv.await.unwrap() {
            r
        } else {
            panic!("Wrong response type!")
        }
    }

    async fn get_data(&self, scope_id: &ScopeId, data_type: TypeId) -> anyhow::Result<RawDepStatus> {
        let (request, recv) = RegistryActorRequest::new(RequestType::GetData {
            scope_id: *scope_id,
            data_type,
        });
        self.handle.0.send(request).map_err(|e| anyhow::anyhow!("{}", e))?;
        if let ResponseType::GetData(r) = recv.await.unwrap() {
            r
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
