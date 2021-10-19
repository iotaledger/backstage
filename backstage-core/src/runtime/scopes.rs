// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::actor::{
    ActorError, ActorPool, BasicActorPool, CustomStatus, Dependencies, DependsOn, DynEvent, Envelope, ErrorReport,
    HandleEvent, InitError, KeyedActorPool, Receiver, Service, ServiceTree, ShutdownHandle, Status, StatusChange,
    SuccessReport, UnboundedTokioChannel,
};
use futures::{
    future::{AbortRegistration, Aborted},
    FutureExt, StreamExt,
};
use std::panic::AssertUnwindSafe;

/// A runtime which defines a particular scope and functionality to
/// create tasks within it.
pub struct RuntimeScope {
    pub(crate) scope_id: ScopeId,
    pub(crate) parent_id: Option<ScopeId>,
    pub(crate) registry: Box<dyn RegistryAccess + Send + Sync>,
    pub(crate) join_handles: Vec<JoinHandle<anyhow::Result<()>>>,
}

impl RuntimeScope {
    /// Get the scope id
    pub fn id(&self) -> &ScopeId {
        &self.scope_id
    }

    /// Get the parent's scope id, if one exists
    pub fn parent_id(&self) -> &Option<ScopeId> {
        &self.parent_id
    }

    /// Launch a new root runtime scope
    pub async fn launch<Reg, F, O>(f: F) -> anyhow::Result<O>
    where
        Reg: RegistryAccess + Send,
        O: Send + Sync,
        for<'b> F: Send + FnOnce(&'b mut RuntimeScope) -> BoxFuture<'b, anyhow::Result<O>>,
    {
        log::debug!("Spawning with registry {}", std::any::type_name::<Reg>());
        let mut scope = Reg::instantiate("Root", None, None).await;
        scope.update_status(ServiceStatus::Running).await.ok();
        let res = f(&mut scope).await;
        if res.is_err() {
            scope.abort().await;
        }
        scope.join().await;
        res
    }

    pub(crate) async fn new<Reg, P, S, O>(
        registry: Reg,
        parent_scope_id: P,
        name: O,
        shutdown_handle: Option<ShutdownHandle>,
        abort_handle: Option<AbortHandle>,
    ) -> Self
    where
        Reg: 'static + RegistryAccess + Send + Sync,
        P: Into<Option<ScopeId>> + Send,
        S: Into<String>,
        O: Into<Option<S>>,
    {
        let name_opt = name.into().map(Into::into);
        let parent_id = parent_scope_id.into();
        let scope_id = registry
            .new_scope(
                parent_id,
                Box::new(|id| name_opt.unwrap_or(format!("Scope {}", id))),
                shutdown_handle,
                abort_handle,
            )
            .await
            .expect("Registry is unavailable!");
        Self {
            scope_id,
            parent_id,
            registry: Box::new(registry),
            join_handles: Default::default(),
        }
    }

    pub(crate) async fn new_boxed<P, S, O>(
        registry: Box<dyn RegistryAccess + Send + Sync>,
        parent_scope_id: P,
        name: O,
        shutdown_handle: Option<ShutdownHandle>,
        abort_handle: Option<AbortHandle>,
    ) -> Self
    where
        P: Into<Option<ScopeId>> + Send,
        S: Into<String>,
        O: Into<Option<S>>,
    {
        let name_opt = name.into().map(Into::into);
        let parent_id = parent_scope_id.into();
        let scope_id = registry
            .new_scope(
                parent_id,
                Box::new(|id| name_opt.unwrap_or(format!("Scope {}", id))),
                shutdown_handle,
                abort_handle,
            )
            .await
            .expect("Registry is unavailable!");
        Self {
            scope_id,
            parent_id,
            registry,
            join_handles: Default::default(),
        }
    }

    pub(crate) async fn child<S: Into<String>, O: Into<Option<S>>>(
        &mut self,
        name: O,
        shutdown_handle: Option<ShutdownHandle>,
        abort_handle: Option<AbortHandle>,
    ) -> Self {
        Self::new_boxed(
            self.registry.clone(),
            Some(self.scope_id),
            name,
            shutdown_handle,
            abort_handle,
        )
        .await
    }

    pub(crate) async fn child_actor<A, H, S, O>(
        &mut self,
        actor: &A,
        name: O,
        supervisor_handle: H,
        abort_handle: AbortHandle,
    ) -> anyhow::Result<ActorContext<A, H>>
    where
        A: 'static + Actor + Send + Sync,
        S: Into<String>,
        O: Into<Option<S>>,
        H: 'static,
    {
        ActorContext::new(
            self.registry.clone(),
            actor,
            self.scope_id,
            name,
            supervisor_handle,
            abort_handle,
        )
        .await
    }

    pub(crate) async fn child_name_with<F: 'static + Send + Sync + FnOnce(ScopeId) -> String>(
        &mut self,
        name_fn: F,
        shutdown_handle: Option<ShutdownHandle>,
        abort_handle: Option<AbortHandle>,
    ) -> Self {
        let scope_id = self
            .registry
            .new_scope(Some(self.scope_id), Box::new(name_fn), shutdown_handle, abort_handle)
            .await
            .expect("Registry is unavailable!");
        Self {
            scope_id,
            parent_id: Some(self.scope_id),
            registry: self.registry.clone(),
            join_handles: Default::default(),
        }
    }

    /// Create a new scope within this one
    pub async fn scope<O, F>(&mut self, f: F) -> anyhow::Result<O>
    where
        O: Send + Sync,
        for<'b> F: Send + FnOnce(&'b mut RuntimeScope) -> BoxFuture<'b, anyhow::Result<O>>,
    {
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        let mut child_scope = self.child::<String, _>(None, None, Some(abort_handle)).await;
        child_scope.update_status(ServiceStatus::Running).await.ok();
        let res = Abortable::new(f(&mut child_scope), abort_registration).await;
        if let Ok(Err(_)) = res {
            child_scope.abort().await;
        }
        child_scope.join().await;
        res.map_err(|_| anyhow::anyhow!("Aborted scope!")).and_then(|res| res)
    }

    pub(crate) async fn add_data<T: 'static + Send + Sync + Clone>(&mut self, data: T) {
        log::debug!(
            "Adding {} to scope {:x}",
            std::any::type_name::<T>(),
            self.scope_id.as_fields().0
        );
        self.registry
            .add_data(&self.scope_id, std::any::TypeId::of::<T>(), Box::new(data))
            .await
            .expect(&format!("Scope {} is missing...", self.scope_id))
    }

    pub(crate) async fn depend_on<T: 'static + Send + Sync + Clone>(&mut self) -> DepStatus<T> {
        self.registry
            .depend_on(&self.scope_id, std::any::TypeId::of::<T>())
            .await
            .map(|res| res.with_type::<T>())
            .expect(&format!("Scope {} is missing...", self.scope_id))
    }

    pub(crate) async fn get_data<T: 'static + Send + Sync + Clone>(&self) -> DepStatus<T> {
        self.registry
            .get_data(&self.scope_id, std::any::TypeId::of::<T>())
            .await
            .map(|res| res.with_type::<T>())
            .expect(&format!("Scope {} is missing...", self.scope_id))
    }

    pub(crate) async fn get_data_opt<T: 'static + Send + Sync + Clone>(&self) -> Option<T> {
        self.get_data().await.into()
    }

    /// Query the registry for a dependency. This will return immediately whether or not it exists.
    pub async fn query_data<T: 'static + Clone + Send + Sync + Dependencies>(&mut self) -> anyhow::Result<T> {
        T::instantiate(self).await
    }

    /// Request a dependency and wait for it to be available.
    pub async fn request_data<T: 'static + Clone + Send + Sync + Dependencies>(&mut self) -> anyhow::Result<T> {
        T::request(self).await
    }

    /// Request a dependency and wait for it to be added, forming a link between this scope and
    /// the requested data. If the data is removed from this scope, it will be shut down.
    pub async fn link_data<T: 'static + Clone + Send + Sync + Dependencies>(&mut self) -> anyhow::Result<T> {
        T::link(self).await
    }

    pub(crate) async fn remove_data<T: 'static + Send + Sync + Clone>(&mut self) -> Option<T> {
        log::debug!(
            "Removing {} from scope {:x}",
            std::any::type_name::<T>(),
            self.scope_id.as_fields().0
        );
        self.registry
            .remove_data(&self.scope_id, std::any::TypeId::of::<T>())
            .await
            .ok()
            .flatten()
            .map(|res| unsafe { *res.downcast_unchecked() })
    }

    pub(crate) async fn remove_data_from_parent<T: 'static + Send + Sync + Clone>(&mut self) -> Option<T> {
        if let Some(parent_id) = self.parent_id.as_ref() {
            log::debug!(
                "Removing {} from scope {:x}",
                std::any::type_name::<T>(),
                parent_id.as_fields().0
            );
            self.registry
                .remove_data(parent_id, std::any::TypeId::of::<T>())
                .await
                .ok()
                .flatten()
                .map(|res| unsafe { *res.downcast_unchecked() })
        } else {
            None
        }
    }

    /// Get this scope's service
    pub async fn service(&mut self) -> Service {
        self.registry
            .get_service(&self.scope_id)
            .await
            .expect(&format!("Scope {} is missing...", self.scope_id))
    }

    /// Get a scope's service, if it exists
    pub async fn service_for_scope(&mut self, scope_id: &ScopeId) -> anyhow::Result<Service> {
        self.registry.get_service(&scope_id).await
    }

    /// Update this scope's service status
    pub async fn update_status<S: Status>(&mut self, status: S) -> anyhow::Result<()> {
        self.registry
            .update_status(&self.scope_id, CustomStatus(status).into())
            .await
    }

    /// Await the tasks in this runtime's scope
    pub(crate) async fn join(&mut self) {
        log::debug!("Joining scope {:x}", self.scope_id.as_fields().0);
        for handle in self.join_handles.drain(..) {
            handle.await.ok();
        }
        self.registry.drop_scope(&self.scope_id).await.ok();
    }

    /// Abort the tasks in this runtime's scope. This will shutdown tasks that have
    /// shutdown handles instead.
    async fn abort(&mut self)
    where
        Self: Sized,
    {
        self.registry.abort(&self.scope_id).await.ok();
    }

    /// Use your powers for ill and shut down some other scope.
    /// This will return an error if the scope does not exist.
    pub async fn shutdown_scope(&self, scope_id: &ScopeId) -> anyhow::Result<()> {
        self.registry.abort(scope_id).await
    }

    /// Get the service tree beginning with this scope
    pub async fn service_tree(&self) -> ServiceTree {
        self.registry
            .service_tree(&self.scope_id)
            .await
            .expect(&format!("Scope {} is missing...", self.scope_id))
    }

    /// Get the service tree beginning with a given scope
    pub async fn service_tree_for_scope(&self, scope_id: &ScopeId) -> anyhow::Result<ServiceTree> {
        self.registry.service_tree(scope_id).await
    }

    /// Get the entire service tree
    pub async fn root_service_tree(&self) -> anyhow::Result<ServiceTree> {
        self.registry.service_tree(&ROOT_SCOPE).await
    }

    /// Get an actor's event handle, if it exists in this scope.
    /// Note: This will only return a handle if the actor exists outside of a pool.
    pub async fn actor_event_handle<A>(&self) -> Option<Act<A>>
    where
        A: 'static + Actor,
    {
        self.get_data_opt::<Act<A>>()
            .await
            .and_then(|handle| (!handle.is_closed()).then(|| handle))
    }

    /// Send an event to a given actor, if it exists in this scope
    pub async fn send_actor_event<A, E>(&self, event: E) -> anyhow::Result<()>
    where
        A: 'static + Actor + HandleEvent<E> + Send + Sync,
        E: 'static + Send + Sync + DynEvent<A>,
    {
        let handle = self
            .get_data_opt::<Act<A>>()
            .await
            .and_then(|handle| (!handle.is_closed()).then(|| handle))
            .ok_or_else(|| anyhow::anyhow!("No channel for this actor!"))?;
        handle.send(Box::new(event))
    }

    /// Get a shared reference to a system if it exists in this runtime's scope
    pub async fn system<S>(&self) -> Option<Sys<S>>
    where
        S: 'static + System + Send + Sync,
    {
        if let Some(actor) = self.get_data_opt::<Act<S>>().await {
            if let Some(state) = self.get_data_opt::<Res<S::State>>().await {
                return Some(Sys { actor, state });
            }
        }
        None
    }

    /// Get a shared resource if it exists in this runtime's scope
    pub async fn resource<R: 'static + Send + Sync + Clone>(&self) -> Option<Res<R>> {
        self.get_data_opt::<Res<R>>().await
    }

    /// Add a shared resource and get a reference to it
    pub async fn add_resource<R: 'static + Send + Sync + Clone>(&mut self, resource: R) -> Res<R> {
        self.add_data(Res(resource.clone())).await;
        Res(resource)
    }

    /// Add a global, shared resource and get a reference to it
    pub async fn add_global_resource<R: 'static + Send + Sync + Clone>(&mut self, resource: R) -> Res<R> {
        log::debug!("Adding {} to root scope", std::any::type_name::<Res<R>>());
        self.registry
            .add_data(
                &ROOT_SCOPE,
                std::any::TypeId::of::<Res<R>>(),
                Box::new(Res(resource.clone())),
            )
            .await
            .expect(&format!("The root scope is missing..."));
        Res(resource)
    }

    /// Get the pool of a specified type if it exists in this runtime's scope
    pub async fn pool<P>(&mut self) -> Option<Pool<P>>
    where
        P: 'static + ActorPool + Send + Sync,
        Act<P::Actor>: Clone,
    {
        match self.get_data_opt::<Pool<P>>().await {
            Some(pool) => {
                if pool.verify().await {
                    Some(pool)
                } else {
                    self.remove_data::<Pool<P>>().await;
                    None
                }
            }
            None => None,
        }
    }

    /// Spawn a new, plain task
    pub async fn spawn_task<F>(&mut self, f: F) -> AbortHandle
    where
        for<'b> F: 'static + Send + FnOnce(&'b mut RuntimeScope) -> BoxFuture<'b, anyhow::Result<()>>,
    {
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        let mut child_scope = self
            .child_name_with(
                |scope_id| format!("Task {:x}", scope_id.as_fields().0),
                None,
                Some(abort_handle.clone()),
            )
            .await;
        let child_task = tokio::spawn(async move {
            let res = Abortable::new(AssertUnwindSafe(f(&mut child_scope)).catch_unwind(), abort_registration).await;
            child_scope.abort().await;
            child_scope.join().await;
            match res {
                Ok(res) => match res {
                    Ok(res) => match res {
                        Ok(_) => Ok(()),
                        Err(e) => anyhow::bail!(e),
                    },
                    Err(_) => {
                        anyhow::bail!("Panicked!")
                    }
                },
                Err(_) => {
                    anyhow::bail!("Aborted!")
                }
            }
        });
        self.join_handles.push(child_task);
        abort_handle
    }

    /// Synchronously initialize an actor and prepare to spawn it
    pub async fn init_actor_unsupervised<A>(&mut self, actor: A) -> Result<Initialized<Act<A>, A, (), ()>, InitError<A>>
    where
        A: 'static + Actor + Send + Sync,
    {
        if self.actor_event_handle::<A>().await.is_some() {
            let service = self.service().await;
            let name = actor.name();
            return Err((
                actor,
                anyhow::anyhow!(
                    "Attempted to add a duplicate actor ({}) to scope {} ({})",
                    name,
                    self.scope_id,
                    service.name()
                ),
            )
                .into());
        }
        self.common_init::<A, (), (), _>(actor, (), SpawnType::Actor).await
    }

    /// Synchronously initialize an actor and prepare to spawn it
    pub async fn init_actor<A, Sup, H>(
        &mut self,
        actor: A,
        supervisor_handle: H,
    ) -> Result<Initialized<Act<A>, A, Sup, H>, InitError<A>>
    where
        A: 'static + Actor + Send + Sync,
        H: 'static + Send,
        Sup: 'static + MaybeSupervisor<A, H>,
    {
        if let Some(handle) = self.actor_event_handle::<A>().await {
            if handle.scope_id == self.scope_id {
                let service = self.service().await;
                let name = actor.name();
                return Err((
                    actor,
                    anyhow::anyhow!(
                        "Attempted to add a duplicate actor ({}) to scope {} ({})",
                        name,
                        self.scope_id,
                        service.name()
                    ),
                )
                    .into());
            }
        }
        self.common_init::<A, Sup, H, Act<A>>(actor, supervisor_handle, SpawnType::Actor)
            .await
    }

    /// Spawn a new actor with a supervisor handle
    pub async fn spawn_actor<A, Sup, H>(&mut self, actor: A, supervisor_handle: H) -> Result<Act<A>, InitError<A>>
    where
        A: 'static + Actor + Send + Sync,
        H: 'static + Send,
        Sup: 'static + MaybeSupervisor<A, H>,
    {
        let init = self.init_actor::<A, Sup, H>(actor, supervisor_handle).await?;
        Ok(init.spawn(self).await)
    }

    /// Spawn a new actor with no supervisor
    pub async fn spawn_actor_unsupervised<A>(&mut self, actor: A) -> Result<Act<A>, InitError<A>>
    where
        A: 'static + Actor + Send + Sync,
    {
        let init = self.init_actor_unsupervised::<A>(actor).await?;
        Ok(init.spawn(self).await)
    }

    /// Synchronously initialize a system and prepare to spawn it
    pub async fn init_system_unsupervised<A>(
        &mut self,
        actor: A,
        state: A::State,
    ) -> Result<Initialized<Sys<A>, A, (), ()>, InitError<A>>
    where
        A: 'static + System + Send + Sync,
    {
        if let Some(handle) = self.actor_event_handle::<A>().await {
            if handle.scope_id == self.scope_id {
                let service = self.service().await;
                let name = actor.name();
                return Err((
                    actor,
                    anyhow::anyhow!(
                        "Attempted to add a duplicate actor ({}) to scope {} ({})",
                        name,
                        self.scope_id,
                        service.name()
                    ),
                )
                    .into());
            }
        }
        self.add_data(Res(state)).await;
        self.common_init::<A, (), (), Sys<A>>(actor, (), SpawnType::System)
            .await
    }

    /// Synchronously initialize a system and prepare to spawn it
    pub async fn init_system<A, Sup, H>(
        &mut self,
        actor: A,
        state: A::State,
        supervisor_handle: H,
    ) -> Result<Initialized<Sys<A>, A, Sup, H>, InitError<A>>
    where
        A: 'static + System + Send + Sync,
        H: 'static + Send,
        Sup: 'static + MaybeSupervisor<A, H>,
    {
        if self.actor_event_handle::<A>().await.is_some() {
            let service = self.service().await;
            let name = actor.name();
            return Err((
                actor,
                anyhow::anyhow!(
                    "Attempted to add a duplicate actor ({}) to scope {} ({})",
                    name,
                    self.scope_id,
                    service.name()
                ),
            )
                .into());
        }
        self.add_data(Res(state)).await;
        self.common_init::<A, Sup, H, Sys<A>>(actor, supervisor_handle, SpawnType::System)
            .await
    }

    /// Spawn a new system with a supervisor handle
    pub async fn spawn_system<A, Sup, H>(
        &mut self,
        actor: A,
        state: A::State,
        supervisor_handle: H,
    ) -> Result<Act<A>, InitError<A>>
    where
        A: 'static + System + Send + Sync,
        H: 'static + Send,
        Sup: 'static + MaybeSupervisor<A, H>,
    {
        if let Some(handle) = self.actor_event_handle::<A>().await {
            if handle.scope_id == self.scope_id {
                let service = self.service().await;
                let name = actor.name();
                return Err((
                    actor,
                    anyhow::anyhow!(
                        "Attempted to add a duplicate actor ({}) to scope {} ({})",
                        name,
                        self.scope_id,
                        service.name()
                    ),
                )
                    .into());
            }
        }
        self.add_data(Res(state)).await;
        self.common_init_and_spawn::<_, Sup, H, Sys<A>>(actor, supervisor_handle, SpawnType::System)
            .await
    }

    /// Spawn a new system with no supervisor
    pub async fn spawn_system_unsupervised<A>(&mut self, actor: A, state: A::State) -> Result<Act<A>, InitError<A>>
    where
        A: 'static + System + Send + Sync,
    {
        if self.actor_event_handle::<A>().await.is_some() {
            let service = self.service().await;
            let name = actor.name();
            return Err((
                actor,
                anyhow::anyhow!(
                    "Attempted to add a duplicate actor ({}) to scope {} ({})",
                    name,
                    self.scope_id,
                    service.name()
                ),
            )
                .into());
        }
        self.add_data(Res(state)).await;
        self.common_init_and_spawn::<_, (), (), Sys<A>>(actor, (), SpawnType::System)
            .await
    }

    async fn common_init<A, Sup, H, Deps>(
        &mut self,
        mut actor: A,
        supervisor_handle: H,
        spawn_type: SpawnType,
    ) -> Result<Initialized<Deps, A, Sup, H>, InitError<A>>
    where
        A: 'static + Actor + Send + Sync,
        Deps: 'static + Dependencies + Clone + Send + Sync,
        H: 'static + Send,
        Sup: 'static + MaybeSupervisor<A, H>,
    {
        log::debug!("Spawning {}", actor.name());
        let (abort_handle, abort_reg) = AbortHandle::new_pair();
        let mut actor_rt = match self
            .child_actor(&actor, actor.name(), supervisor_handle, abort_handle.clone())
            .await
        {
            Ok(rt) => rt,
            Err(e) => {
                return Err((actor, e).into());
            }
        };
        let handle = actor_rt.handle();
        match spawn_type {
            SpawnType::Actor | SpawnType::System => self.add_data(handle.clone()).await,
            SpawnType::Pool => actor_rt.add_data(handle.clone()).await,
        }
        let res = AssertUnwindSafe(actor.init(&mut actor_rt)).catch_unwind().await;
        if let Err(e) = Self::handle_init_res::<A, H>(res, &mut actor_rt).await {
            return Err((actor, e).into());
        }
        Ok(InitData {
            actor,
            actor_rt,
            abort_reg,
            spawn_type,
            _marker: PhantomData,
        }
        .into())
    }

    async fn common_spawn<A, Sup, H, Deps>(
        &mut self,
        InitData {
            mut actor,
            mut actor_rt,
            abort_reg: abort_registration,
            spawn_type,
            _marker,
        }: InitData<Deps, A, Sup, H>,
    ) -> Act<A>
    where
        A: 'static + Actor + Send + Sync,
        H: 'static + Send,
        Deps: 'static + Dependencies + Clone + Send + Sync,
        Sup: 'static + MaybeSupervisor<A, H>,
    {
        let handle = actor_rt.handle();
        let child_task = tokio::spawn(async move {
            let res = Abortable::new(
                AssertUnwindSafe(async {
                    let deps = Deps::link(&mut actor_rt.scope).await?;
                    // Call handle events until shutdown
                    while let Some(evt) = actor_rt.receiver.next().await {
                        // Handle the event
                        evt.handle((&mut actor_rt, &mut actor)).await?;
                        todo!("handle events")
                    }
                    actor.shutdown(&mut actor_rt).await
                })
                .catch_unwind(),
                abort_registration,
            )
            .await;
            Deps::cleanup(&mut actor_rt, spawn_type).await;
            Self::handle_run_res::<A, Sup, H>(res, &mut actor_rt, actor).await
        });
        self.join_handles.push(child_task);
        handle
    }

    async fn common_init_and_spawn<A, Sup, H, Deps>(
        &mut self,
        actor: A,
        supervisor_handle: H,
        spawn_type: SpawnType,
    ) -> Result<Act<A>, InitError<A>>
    where
        A: 'static + Actor + Send + Sync,
        H: 'static + Send,
        Sup: 'static + MaybeSupervisor<A, H>,
        Deps: 'static + Dependencies + Clone + Send + Sync,
    {
        let init = self
            .common_init::<_, Sup, H, Deps>(actor, supervisor_handle, spawn_type)
            .await?;
        Ok(init.spawn(self).await)
    }

    pub(crate) async fn handle_init_res<A, H>(
        res: std::thread::Result<Result<(), ActorError>>,
        child_scope: &mut ActorContext<A, H>,
    ) -> anyhow::Result<()>
    where
        A: Actor,
    {
        match res {
            Ok(res) => match res {
                Ok(_) => Ok(()),
                Err(e) => {
                    child_scope.abort().await;
                    child_scope.join().await;
                    anyhow::bail!(e)
                }
            },
            Err(e) => {
                child_scope.abort().await;
                child_scope.join().await;
                std::panic::resume_unwind(e);
            }
        }
    }

    pub(crate) async fn handle_run_res<A, Sup, H>(
        res: Result<std::thread::Result<Result<(), ActorError>>, Aborted>,
        child_scope: &mut ActorContext<A, H>,
        actor: A,
    ) -> anyhow::Result<()>
    where
        A: 'static + Actor + Send + Sync,
        Sup: MaybeSupervisor<A, H>,
    {
        let service = child_scope.service().await;
        child_scope.abort().await;
        child_scope.join().await;
        match res {
            Ok(res) => match res {
                Ok(res) => match res {
                    Ok(_) => Sup::send_report(
                        child_scope.supervisor_handle(),
                        Ok(SuccessReport::new(actor.into(), service)),
                    ),

                    Err(e) => {
                        log::error!("{} exited with error: {}", actor.name(), e);
                        Sup::send_report(
                            child_scope.supervisor_handle(),
                            Err(ErrorReport::new(actor.into(), service, e)),
                        )
                        .map_err(|e| anyhow::anyhow!(e))
                    }
                },
                Err(e) => {
                    Sup::send_report(
                        child_scope.supervisor_handle(),
                        Err(ErrorReport::new(
                            actor.into(),
                            service,
                            anyhow::anyhow!("Actor panicked!").into(),
                        )),
                    )
                    .ok();
                    std::panic::resume_unwind(e);
                }
            },
            Err(_) => Sup::send_report(
                child_scope.supervisor_handle(),
                Ok(SuccessReport::new(actor.into(), service)),
            )
            .map_err(|e| anyhow::anyhow!("Aborted!")),
        }
    }

    /// Spawn a new pool of actors of a given type with some metric
    pub async fn new_pool_with<P: ActorPool, Sup, H, F>(&mut self, supervisor_handle: H, f: F) -> anyhow::Result<()>
    where
        P: 'static + Send + Sync,
        P::Actor: Send + Sync,
        H: 'static + Clone + Send,
        Sup: 'static + MaybeSupervisor<P::Actor, H>,
        for<'b> F: 'static + Send + FnOnce(&'b mut ScopedActorPool<P, Sup, H>) -> BoxFuture<'b, anyhow::Result<()>>,
    {
        let pool = Pool::<P>::default();
        self.add_data(pool.clone()).await;
        let mut scoped_pool = ScopedActorPool {
            scope: self,
            pool,
            supervisor_handle,
            initialized: Default::default(),
        };
        f(&mut scoped_pool).await
    }

    /// Spawn a new pool of actors of a given type with some metric
    pub async fn spawn_pool_with<P: ActorPool, Sup, H, F>(&mut self, supervisor_handle: H, f: F) -> anyhow::Result<()>
    where
        P: 'static + Send + Sync,
        P::Actor: Send + Sync,
        H: 'static + Clone + Send,
        Sup: 'static + MaybeSupervisor<P::Actor, H>,
        for<'b> F: 'static + Send + FnOnce(&'b mut ScopedActorPool<P, Sup, H>) -> BoxFuture<'b, anyhow::Result<()>>,
    {
        f(&mut self.spawn_pool(supervisor_handle).await).await
    }

    /// Spawn a new pool of actors of a given type with some metric
    pub async fn spawn_pool<P: ActorPool, Sup, H>(&mut self, supervisor_handle: H) -> ScopedActorPool<'_, P, Sup, H>
    where
        P: 'static + Send + Sync,
        P::Actor: Send + Sync,
        H: 'static + Clone + Send,
        Sup: 'static + MaybeSupervisor<P::Actor, H>,
    {
        let pool = if let Some(pool) = self.pool::<P>().await {
            pool
        } else {
            let pool = Pool::<P>::default();
            self.add_data(pool.clone()).await;
            pool
        };
        let scoped_pool = ScopedActorPool {
            scope: self,
            pool,
            supervisor_handle,
            initialized: Default::default(),
        };
        scoped_pool
    }

    /// Initialize an actor into a pool
    pub async fn init_into_pool<P: ActorPool, Sup, H>(
        &mut self,
        supervisor_handle: H,
        actor: P::Actor,
    ) -> Result<Initialized<Act<P::Actor>, P::Actor, Sup, H>, InitError<P::Actor>>
    where
        P: 'static + BasicActorPool + Send + Sync,
        P::Actor: Send + Sync,
        H: 'static + Clone + Send,
        Sup: 'static + MaybeSupervisor<P::Actor, H>,
    {
        let pool = self.spawn_pool::<P, Sup, _>(supervisor_handle).await;
        let supervisor_handle = pool.supervisor_handle.clone();
        let init = pool
            .scope
            .common_init(actor, supervisor_handle, SpawnType::Pool)
            .await?;
        let handle = init.data.as_ref().unwrap().actor_rt.handle();
        pool.pool.push(handle).await;
        Ok(init)
    }

    /// Spawn an actor into a pool
    pub async fn spawn_into_pool<P: ActorPool, Sup, H>(
        &mut self,
        supervisor_handle: H,
        actor: P::Actor,
    ) -> Result<Act<P::Actor>, InitError<P::Actor>>
    where
        P: 'static + BasicActorPool + Send + Sync,
        P::Actor: Send + Sync,
        H: 'static + Clone + Send,
        Sup: 'static + MaybeSupervisor<P::Actor, H>,
    {
        let mut pool = self.spawn_pool::<P, Sup, _>(supervisor_handle).await;
        pool.spawn(actor).await
    }

    /// Initialize an actor into a pool
    pub async fn init_into_pool_keyed<P: ActorPool, Sup, H>(
        &mut self,
        supervisor_handle: H,
        key: P::Key,
        actor: P::Actor,
    ) -> Result<Initialized<Act<P::Actor>, P::Actor, Sup, H>, InitError<P::Actor>>
    where
        P: 'static + KeyedActorPool + Send + Sync,
        P::Actor: Send + Sync,
        H: 'static + Clone + Send,
        Sup: 'static + MaybeSupervisor<P::Actor, H>,
    {
        let pool = self.spawn_pool::<P, Sup, _>(supervisor_handle).await;
        let supervisor_handle = pool.supervisor_handle.clone();
        let init = pool
            .scope
            .common_init(actor, supervisor_handle, SpawnType::Pool)
            .await?;
        let handle = init.data.as_ref().unwrap().actor_rt.handle();
        pool.pool.push(key, handle).await;
        Ok(init)
    }

    /// Spawn an actor into a keyed pool
    pub async fn spawn_into_pool_keyed<P: ActorPool, Sup, H>(
        &mut self,
        supervisor_handle: H,
        key: P::Key,
        actor: P::Actor,
    ) -> Result<Act<P::Actor>, InitError<P::Actor>>
    where
        P: 'static + KeyedActorPool + Send + Sync,
        P::Actor: Send + Sync,
        H: 'static + Clone + Send,
        Sup: 'static + MaybeSupervisor<P::Actor, H>,
    {
        let mut pool = self.spawn_pool::<P, Sup, _>(supervisor_handle).await;
        pool.spawn_keyed(key, actor).await
    }
}

/// The possible spawn types for actors
#[allow(missing_docs)]
#[derive(Clone, Copy)]
pub enum SpawnType {
    Actor,
    System,
    Pool,
}

/// Data used to spawn an actor after initializing it
pub struct InitData<Deps, A, Sup, H>
where
    A: Actor,
{
    actor: A,
    actor_rt: ActorContext<A, H>,
    abort_reg: AbortRegistration,
    spawn_type: SpawnType,
    _marker: PhantomData<fn(Deps, Sup) -> (Deps, Sup)>,
}

/// A handle to an initialized actor. If unused, this will cleanup and abort the scope when dropped!
#[must_use = "An unused, initialized actor may cause unintended behavior!"]
pub struct Initialized<Deps, A, Sup, H>
where
    A: 'static + Actor + Send,
    H: 'static + Send,
    Deps: 'static + Dependencies + Clone + Send + Sync,
    Sup: 'static,
{
    data: Option<InitData<Deps, A, Sup, H>>,
}

impl<Deps, A, Sup, H> Initialized<Deps, A, Sup, H>
where
    A: 'static + Actor + Send + Sync,
    Deps: 'static + Dependencies + Clone + Send + Sync,
    H: 'static + Send,
    Sup: MaybeSupervisor<A, H>,
    Sup: 'static,
{
    /// Spawn the initialized actor with the given scope.
    /// This must be the same scope as the one used to init the actor!
    pub async fn spawn(mut self, rt: &mut RuntimeScope) -> Act<A> {
        rt.common_spawn(std::mem::take(&mut self.data).unwrap()).await
    }
}

impl<Deps, A, Sup, H> From<InitData<Deps, A, Sup, H>> for Initialized<Deps, A, Sup, H>
where
    A: 'static + Actor + Send,
    Deps: 'static + Dependencies + Clone + Send + Sync,
    Sup: 'static,
    H: 'static + Send,
{
    fn from(data: InitData<Deps, A, Sup, H>) -> Self {
        Self { data: Some(data) }
    }
}

impl<Deps, A, Sup, H> Drop for Initialized<Deps, A, Sup, H>
where
    A: 'static + Actor + Send,
    H: 'static + Send,
    Deps: 'static + Dependencies + Clone + Send + Sync,
    Sup: 'static,
{
    fn drop(&mut self) {
        if let Some(mut data) = std::mem::take(&mut self.data) {
            tokio::spawn(async move {
                Deps::cleanup(&mut data.actor_rt, data.spawn_type).await;
                data.actor_rt.abort().await;
            });
        }
    }
}

pub trait MaybeSupervisor<A, H> {
    fn send_status_change(handle: &H, status_change: StatusChange<A>) -> anyhow::Result<()> {
        Ok(())
    }
    fn send_report(handle: &H, report: Result<SuccessReport<A>, ErrorReport<A>>) -> anyhow::Result<()> {
        Ok(())
    }
}
impl<A, Sup, H> MaybeSupervisor<A, H> for Sup
where
    Sup: 'static + Actor + HandleEvent<StatusChange<A>> + HandleEvent<Result<SuccessReport<A>, ErrorReport<A>>>,
    H: Sender<Envelope<Sup>>,
    A: 'static + Send + Sync,
{
    fn send_status_change(handle: &H, status_change: StatusChange<A>) -> anyhow::Result<()> {
        handle.send(Box::new(status_change))
    }

    fn send_report(handle: &H, report: Result<SuccessReport<A>, ErrorReport<A>>) -> anyhow::Result<()> {
        handle.send(Box::new(report))
    }
}
impl<A> MaybeSupervisor<A, ()> for () {}

pub struct ActorContext<A, H>
where
    A: Actor,
{
    pub(crate) scope: RuntimeScope,
    pub(crate) handle: Act<A>,
    pub(crate) receiver: Box<dyn Receiver<Envelope<A>>>,
    pub(crate) supervisor_handle: H,
}

impl<A, H> ActorContext<A, H>
where
    A: 'static + Actor + Send + Sync,
{
    pub(crate) async fn new<P, S, O>(
        registry: Box<dyn RegistryAccess + Send + Sync>,
        actor: &A,
        parent_scope_id: P,
        name: O,
        supervisor_handle: H,
        abort_handle: AbortHandle,
    ) -> anyhow::Result<Self>
    where
        P: Into<Option<ScopeId>> + Send,
        S: Into<String>,
        O: Into<Option<S>>,
        H: 'static,
    {
        let (sender, receiver) = UnboundedTokioChannel::<Envelope<A>>::new(actor).await?;
        let shutdown_handle = ShutdownHandle::default();
        let scope = RuntimeScope::new_boxed(
            registry,
            parent_scope_id,
            name,
            Some(shutdown_handle.clone()),
            Some(abort_handle.clone()),
        )
        .await;
        Ok(Self {
            handle: Act::new(scope.scope_id, Box::new(sender), shutdown_handle, abort_handle),
            scope,
            receiver: Box::new(receiver),
            supervisor_handle,
        })
    }

    /// Initialize a new actor with a supervisor handle
    pub async fn init_actor<OtherA>(
        &mut self,
        actor: OtherA,
    ) -> Result<Initialized<Act<OtherA>, OtherA, A, Act<A>>, InitError<OtherA>>
    where
        OtherA: 'static + Actor + Send + Sync,
        A: MaybeSupervisor<OtherA, Act<A>>,
    {
        let handle = self.handle();
        self.scope.init_actor(actor, handle).await
    }

    /// Spawn a new actor with a supervisor handle
    pub async fn spawn_actor<OtherA>(&mut self, actor: OtherA) -> Result<Act<OtherA>, InitError<OtherA>>
    where
        OtherA: 'static + Actor + Send + Sync,
        A: MaybeSupervisor<OtherA, Act<A>>,
    {
        let handle = self.handle();
        self.scope.spawn_actor::<_, A, _>(actor, handle).await
    }

    /// Spawn a new system with a supervisor handle
    pub async fn init_system<OtherA>(
        &mut self,
        actor: OtherA,
        state: OtherA::State,
    ) -> Result<Initialized<Sys<OtherA>, OtherA, A, Act<A>>, InitError<OtherA>>
    where
        OtherA: 'static + System + Send + Sync,
        A: MaybeSupervisor<OtherA, Act<A>>,
    {
        let handle = self.handle();
        self.scope.init_system(actor, state, handle).await
    }

    /// Spawn a new system with a supervisor handle
    pub async fn spawn_system<OtherA>(
        &mut self,
        actor: OtherA,
        state: OtherA::State,
    ) -> Result<Act<OtherA>, InitError<OtherA>>
    where
        OtherA: 'static + System + Send + Sync,
        A: MaybeSupervisor<OtherA, Act<A>>,
    {
        let handle = self.handle();
        self.scope.spawn_system::<_, A, _>(actor, state, handle).await
    }

    /// Spawn a new pool of actors of a given type with some metric
    pub async fn new_pool_with<P: ActorPool, F>(&mut self, f: F) -> anyhow::Result<()>
    where
        P: 'static + Send + Sync,
        P::Actor: Send + Sync,
        A: 'static + MaybeSupervisor<P::Actor, Act<A>>,
        for<'b> F: 'static + Send + FnOnce(&'b mut ScopedActorPool<P, A, Act<A>>) -> BoxFuture<'b, anyhow::Result<()>>,
    {
        let handle = self.handle();
        self.scope.new_pool_with::<P, A, _, F>(handle, f).await
    }

    /// Spawn a new pool of actors of a given type with some metric
    pub async fn spawn_pool_with<P: ActorPool, F>(&mut self, f: F) -> anyhow::Result<()>
    where
        P: 'static + Send + Sync,
        P::Actor: Send + Sync,
        A: 'static + MaybeSupervisor<P::Actor, Act<A>>,

        for<'b> F: 'static + Send + FnOnce(&'b mut ScopedActorPool<P, A, Act<A>>) -> BoxFuture<'b, anyhow::Result<()>>,
    {
        let handle = self.handle();
        self.scope.spawn_pool_with::<P, A, _, F>(handle, f).await
    }

    /// Spawn a new pool of actors of a given type with some metric
    pub async fn spawn_pool<P: ActorPool>(&mut self) -> ScopedActorPool<'_, P, A, Act<A>>
    where
        A: 'static,
        P: 'static + Send + Sync,
        P::Actor: Send + Sync,
        A: MaybeSupervisor<P::Actor, Act<A>>,
    {
        let handle = self.handle();
        self.scope.spawn_pool::<P, A, _>(handle).await
    }

    /// Initialize an actor into a pool
    pub async fn init_into_pool<P: ActorPool>(
        &mut self,
        actor: P::Actor,
    ) -> Result<Initialized<Act<P::Actor>, P::Actor, A, Act<A>>, InitError<P::Actor>>
    where
        P: 'static + BasicActorPool + Send + Sync,
        P::Actor: Send + Sync,
        A: MaybeSupervisor<P::Actor, Act<A>>,
    {
        let pool = self.spawn_pool::<P>().await;
        let supervisor_handle = pool.supervisor_handle.clone();
        let init = pool
            .scope
            .common_init(actor, supervisor_handle, SpawnType::Pool)
            .await?;
        let handle = init.data.as_ref().unwrap().actor_rt.handle();
        pool.pool.push(handle).await;
        Ok(init)
    }

    /// Spawn an actor into a pool
    pub async fn spawn_into_pool<P: ActorPool>(&mut self, actor: P::Actor) -> Result<Act<P::Actor>, InitError<P::Actor>>
    where
        P: 'static + BasicActorPool + Send + Sync,
        P::Actor: Send + Sync,
        A: MaybeSupervisor<P::Actor, Act<A>>,
    {
        let handle = self.handle();
        self.scope.spawn_into_pool::<P, A, _>(handle, actor).await
    }

    /// Initialize an actor into a pool
    pub async fn init_into_pool_keyed<P: ActorPool>(
        &mut self,
        key: P::Key,
        actor: P::Actor,
    ) -> Result<Initialized<Act<P::Actor>, P::Actor, A, Act<A>>, InitError<P::Actor>>
    where
        P: 'static + KeyedActorPool + Send + Sync,
        P::Actor: Send + Sync,
        A: MaybeSupervisor<P::Actor, Act<A>>,
    {
        let pool = self.spawn_pool::<P>().await;
        let supervisor_handle = pool.supervisor_handle.clone();
        let init = pool
            .scope
            .common_init(actor, supervisor_handle, SpawnType::Pool)
            .await?;
        let handle = init.data.as_ref().unwrap().actor_rt.handle();
        pool.pool.push(key, handle).await;
        Ok(init)
    }

    /// Spawn an actor into a keyed pool
    pub async fn spawn_into_pool_keyed<P: ActorPool>(
        &mut self,
        key: P::Key,
        actor: P::Actor,
    ) -> Result<Act<P::Actor>, InitError<P::Actor>>
    where
        P: 'static + KeyedActorPool + Send + Sync,
        P::Actor: Send + Sync,
        A: MaybeSupervisor<P::Actor, Act<A>>,
    {
        let handle = self.handle();
        self.scope.spawn_into_pool_keyed::<P, A, _>(handle, key, actor).await
    }

    // /// Get the next event from the event receiver
    // pub async fn next_event(&mut self) -> Option<A::Event> {
    //     self.receiver.next().await
    // }

    /// Get this actors's handle
    pub fn handle(&self) -> Act<A>
    where
        Act<A>: Clone,
    {
        self.handle.clone()
    }

    fn dependencies(&self) -> &<Self as DependsOn>::Dependencies
    where
        Self: DependsOn,
    {
        todo!()
    }

    /// Get this actors's shutdown handle
    pub fn shutdown_handle(&self) -> ShutdownHandle {
        self.handle.shutdown_handle.clone()
    }

    /// Get the runtime's service
    pub async fn service(&mut self) -> Service {
        self.scope.service().await
    }

    /// Get this actor's supervisor handle
    pub fn supervisor_handle(&mut self) -> &mut H {
        &mut self.supervisor_handle
    }

    /// Update this scope's service status
    pub async fn update_status<S: Status, Sup>(&mut self, status: S) -> anyhow::Result<()>
    where
        H: Sender<Envelope<Sup>>,
        Sup: 'static + HandleEvent<StatusChange<A>>,
    {
        if !self.supervisor_handle.is_closed() {
            let mut service = self.service().await;
            let prev_status = service.status().clone();
            service.update_status(CustomStatus(status.clone()));
            self.supervisor_handle
                .send(Box::new(StatusChange::new(prev_status, service)))
                .ok();
        }
        self.scope.update_status(status).await
    }
}

impl<A, H> Deref for ActorContext<A, H>
where
    A: Actor,
{
    type Target = RuntimeScope;

    fn deref(&self) -> &Self::Target {
        &self.scope
    }
}

impl<A, H> DerefMut for ActorContext<A, H>
where
    A: Actor,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.scope
    }
}
/// A scope for an actor pool, which only allows spawning of the specified actor
pub struct ScopedActorPool<'a, P, Sup, H>
where
    P: ActorPool,
    P::Actor: 'static + Send + Sync,
    H: 'static + Clone + Send,
    Sup: 'static + MaybeSupervisor<P::Actor, H>,
{
    scope: &'a mut RuntimeScope,
    pool: Pool<P>,
    supervisor_handle: H,
    initialized: Vec<Initialized<Act<P::Actor>, P::Actor, Sup, H>>,
}

impl<'a, P, Sup, H> ScopedActorPool<'a, P, Sup, H>
where
    P: BasicActorPool,
    P::Actor: 'static + Send + Sync,
    H: 'static + Clone + Send,
    Sup: 'static + MaybeSupervisor<P::Actor, H>,
{
    /// Spawn a new actor into this pool
    pub async fn spawn(&mut self, actor: P::Actor) -> Result<Act<P::Actor>, InitError<P::Actor>>
    where
        P::Actor: 'static + Send + Sync,
    {
        let supervisor_handle = self.supervisor_handle.clone();
        let handle = self
            .scope
            .common_init_and_spawn::<_, Sup, H, Act<P::Actor>>(actor, supervisor_handle, SpawnType::Pool)
            .await?;
        self.pool.push(handle.clone()).await;
        Ok(handle)
    }

    /// Initialize actor for this pool
    pub async fn init(&mut self, actor: P::Actor) -> Result<(), InitError<P::Actor>> {
        let supervisor_handle = self.supervisor_handle.clone();
        let init = self
            .scope
            .common_init(actor, supervisor_handle, SpawnType::Pool)
            .await?;
        let handle = init.data.as_ref().unwrap().actor_rt.handle();
        self.pool.push(handle).await;
        self.initialized.push(init);
        Ok(())
    }
}

impl<'a, P, Sup, H> ScopedActorPool<'a, P, Sup, H>
where
    P: KeyedActorPool,
    P::Actor: 'static + Send + Sync,
    H: 'static + Clone + Send,
    Sup: 'static + MaybeSupervisor<P::Actor, H>,
{
    /// Spawn a new actor into this pool
    pub async fn spawn_keyed(&mut self, key: P::Key, actor: P::Actor) -> Result<Act<P::Actor>, InitError<P::Actor>>
    where
        P::Actor: 'static + Send + Sync,
    {
        if self.pool.get(&key).await.is_some() {
            let service = self.scope.service().await;
            return Err((
                actor,
                anyhow::anyhow!(
                    "Attempted to add a duplicate metric to pool {} in scope {} ({})",
                    std::any::type_name::<Pool<P>>(),
                    self.scope.scope_id,
                    service.name()
                ),
            )
                .into());
        }
        let supervisor_handle = self.supervisor_handle.clone();
        let handle = self
            .scope
            .common_init_and_spawn::<_, Sup, H, Act<P::Actor>>(actor, supervisor_handle, SpawnType::Pool)
            .await?;
        self.pool.push(key, handle.clone()).await;
        Ok(handle)
    }

    /// Initialize actor for this pool
    pub async fn init_keyed(&mut self, key: P::Key, actor: P::Actor) -> Result<(), InitError<P::Actor>> {
        if self.pool.get(&key).await.is_some() {
            let service = self.scope.service().await;
            return Err((
                actor,
                anyhow::anyhow!(
                    "Attempted to add a duplicate metric to pool {} in scope {} ({})",
                    std::any::type_name::<Pool<P>>(),
                    self.scope.scope_id,
                    service.name()
                ),
            )
                .into());
        }
        let supervisor_handle = self.supervisor_handle.clone();
        let init = self
            .scope
            .common_init(actor, supervisor_handle, SpawnType::Pool)
            .await?;
        let handle = init.data.as_ref().unwrap().actor_rt.handle();
        self.pool.push(key, handle).await;
        self.initialized.push(init);
        Ok(())
    }
}

impl<'a, P, Sup, H> ScopedActorPool<'a, P, Sup, H>
where
    P: ActorPool,
    P::Actor: 'static + Send + Sync,
    H: 'static + Clone + Send,
    Sup: 'static + MaybeSupervisor<P::Actor, H>,
{
    /// Finalize any initialized and unspawned actors in this pool by spawning them
    pub async fn spawn_all(&mut self) {
        for init in self.initialized.drain(..) {
            init.spawn(self.scope).await;
        }
    }
}
