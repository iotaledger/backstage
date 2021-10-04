// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::actor::{
    ActorError, ActorPool, BasicActorPool, CustomStatus, Dependencies, ErrorReport, EventDriven, InitError,
    KeyedActorPool, Service, ServiceTree, ShutdownHandle, ShutdownStream, Status, StatusChange, SuccessReport,
    SupervisorEvent,
};
use futures::{
    future::{AbortRegistration, Aborted},
    FutureExt, StreamExt,
};
use std::panic::AssertUnwindSafe;

/// A runtime which defines a particular scope and functionality to
/// create tasks within it.
pub struct RuntimeScope<Reg> {
    pub(crate) scope_id: ScopeId,
    pub(crate) parent_id: Option<ScopeId>,
    pub(crate) registry: Reg,
    pub(crate) join_handles: Vec<JoinHandle<anyhow::Result<()>>>,
}

impl<Reg: 'static + RegistryAccess + Send + Sync> RuntimeScope<Reg> {
    /// Get the scope id
    pub fn id(&self) -> &ScopeId {
        &self.scope_id
    }

    /// Get the parent's scope id, if one exists
    pub fn parent_id(&self) -> &Option<ScopeId> {
        &self.parent_id
    }

    /// Launch a new root runtime scope
    pub async fn launch<F, O>(f: F) -> anyhow::Result<O>
    where
        O: Send + Sync,
        for<'b> F: Send + FnOnce(&'b mut RuntimeScope<Reg>) -> BoxFuture<'b, anyhow::Result<O>>,
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

    pub(crate) async fn new<P: Into<Option<ScopeId>> + Send, S: Into<String>, O: Into<Option<S>>>(
        registry: Reg,
        parent_scope_id: P,
        name: O,
        shutdown_handle: Option<ShutdownHandle>,
        abort_handle: Option<AbortHandle>,
    ) -> Self {
        let name_opt = name.into().map(Into::into);
        let parent_id = parent_scope_id.into();
        let scope_id = registry
            .new_scope(
                parent_id.clone(),
                |id| name_opt.unwrap_or_else(|| format!("Scope {}", id)),
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
        Self::new(
            self.registry.clone(),
            self.scope_id,
            name,
            shutdown_handle,
            abort_handle,
        )
        .await
    }

    pub(crate) async fn child_actor<A, Sup, S, O>(
        &mut self,
        actor: &A,
        name: O,
        abort_handle: AbortHandle,
        supervisor_handle: Option<Act<Sup>>,
    ) -> anyhow::Result<ActorScopedRuntime<A, Reg, Sup>>
    where
        A: Actor,

        Sup: EventDriven,
        S: Into<String>,
        O: Into<Option<S>>,
    {
        ActorScopedRuntime::new(
            self.registry.clone(),
            actor,
            self.scope_id,
            name,
            abort_handle,
            supervisor_handle,
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
            .new_scope(self.scope_id, name_fn, shutdown_handle, abort_handle)
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
        for<'b> F: Send + FnOnce(&'b mut RuntimeScope<Reg>) -> BoxFuture<'b, anyhow::Result<O>>,
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
            .add_data(&self.scope_id, data)
            .await
            .expect(&format!("Scope {} is missing...", self.scope_id))
    }

    pub(crate) async fn depend_on<T: 'static + Send + Sync + Clone>(&mut self) -> DepStatus<T> {
        self.registry
            .depend_on::<T>(&self.scope_id)
            .await
            .expect(&format!("Scope {} is missing...", self.scope_id))
    }

    pub(crate) async fn get_data<T: 'static + Send + Sync + Clone>(&self) -> DepStatus<T> {
        self.registry
            .get_data(&self.scope_id)
            .await
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
        self.registry.remove_data(&self.scope_id).await.ok().flatten()
    }

    pub(crate) async fn remove_data_from_parent<T: 'static + Send + Sync + Clone>(&mut self) -> Option<T> {
        if let Some(parent_id) = self.parent_id.as_ref() {
            log::debug!(
                "Removing {} from scope {:x}",
                std::any::type_name::<T>(),
                parent_id.as_fields().0
            );
            self.registry.remove_data(parent_id).await.ok().flatten()
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
    pub async fn actor_event_handle<A: 'static + Actor>(&self) -> Option<Act<A>> {
        self.get_data_opt::<Act<A>>()
            .await
            .and_then(|handle| (!handle.is_closed()).then(|| handle))
    }

    /// Send an event to a given actor, if it exists in this scope
    pub async fn send_actor_event<A: 'static + Actor>(&self, event: A::Event) -> anyhow::Result<()> {
        let handle = self
            .get_data_opt::<Act<A>>()
            .await
            .and_then(|handle| (!handle.is_closed()).then(|| handle))
            .ok_or_else(|| anyhow::anyhow!("No channel for this actor!"))?;
        handle.send(event)
    }

    /// Get a shared reference to a system if it exists in this runtime's scope
    pub async fn system<S: 'static + System + Send + Sync>(&self) -> Option<Sys<S>> {
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
            .add_data(&ROOT_SCOPE, Res(resource.clone()))
            .await
            .expect(&format!("The root scope is missing..."));
        Res(resource)
    }

    /// Get the pool of a specified type if it exists in this runtime's scope
    pub async fn pool<P>(&mut self) -> Option<Pool<P>>
    where
        P: 'static + ActorPool + Send + Sync,
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
        for<'b> F: 'static + Send + FnOnce(&'b mut RuntimeScope<Reg>) -> BoxFuture<'b, anyhow::Result<()>>,
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
    pub async fn init_actor<A, Sup, I>(
        &mut self,
        actor: A,
        supervisor_handle: I,
    ) -> Result<Initialized<Act<A>, A, Reg, Sup>, InitError<A>>
    where
        A: 'static + Actor + Send + Sync + Into<<Sup::Event as SupervisorEvent>::ChildStates>,
        Sup: 'static + EventDriven,
        Sup::Event: SupervisorEvent,
        <Sup::Event as SupervisorEvent>::Children: From<PhantomData<A>>,
        I: Into<Option<Act<Sup>>>,
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
        let supervisor_handle = supervisor_handle.into();
        self.common_init(actor, supervisor_handle, SpawnType::Actor).await
    }

    /// Spawn a new actor with a supervisor handle
    pub async fn spawn_actor<A, Sup, I>(&mut self, actor: A, supervisor_handle: I) -> Result<Act<A>, InitError<A>>
    where
        A: 'static + Actor + Send + Sync + Into<<Sup::Event as SupervisorEvent>::ChildStates>,
        Sup: 'static + EventDriven,
        Sup::Event: SupervisorEvent,
        <Sup::Event as SupervisorEvent>::Children: From<PhantomData<A>>,
        I: Into<Option<Act<Sup>>>,
    {
        let init = self.init_actor(actor, supervisor_handle).await?;
        Ok(init.spawn(self).await)
    }

    /// Spawn a new actor with no supervisor
    pub async fn spawn_actor_unsupervised<A>(&mut self, actor: A) -> Result<Act<A>, InitError<A>>
    where
        A: 'static + Actor + Send + Sync,
    {
        self.spawn_actor::<A, (), _>(actor, None).await
    }

    /// Synchronously initialize a system and prepare to spawn it
    pub async fn init_system<A, Sup, I>(
        &mut self,
        actor: A,
        state: A::State,
        supervisor_handle: I,
    ) -> Result<Initialized<Sys<A>, A, Reg, Sup>, InitError<A>>
    where
        A: 'static + System + Send + Sync + Into<<Sup::Event as SupervisorEvent>::ChildStates>,
        Sup: 'static + EventDriven,
        Sup::Event: SupervisorEvent,
        <Sup::Event as SupervisorEvent>::Children: From<PhantomData<A>>,
        I: Into<Option<Act<Sup>>>,
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
        let supervisor_handle = supervisor_handle.into();
        self.add_data(Res(state)).await;
        self.common_init(actor, supervisor_handle, SpawnType::System).await
    }

    /// Spawn a new system with a supervisor handle
    pub async fn spawn_system<A, Sup, I>(
        &mut self,
        actor: A,
        state: A::State,
        supervisor_handle: I,
    ) -> Result<Act<A>, InitError<A>>
    where
        A: 'static + System + Send + Sync + Into<<Sup::Event as SupervisorEvent>::ChildStates>,
        Sup: 'static + EventDriven,
        Sup::Event: SupervisorEvent,
        <Sup::Event as SupervisorEvent>::Children: From<PhantomData<A>>,
        I: Into<Option<Act<Sup>>>,
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
        let supervisor_handle = supervisor_handle.into();
        self.add_data(Res(state)).await;
        self.common_init_and_spawn::<_, _, Sys<A>>(actor, supervisor_handle, SpawnType::System)
            .await
    }

    /// Spawn a new system with no supervisor
    pub async fn spawn_system_unsupervised<A>(&mut self, actor: A, state: A::State) -> Result<Act<A>, InitError<A>>
    where
        A: 'static + System + Send + Sync,
    {
        self.spawn_system::<A, (), _>(actor, state, None).await
    }

    async fn common_init<A, Sup, Deps>(
        &mut self,
        mut actor: A,
        supervisor_handle: Option<Act<Sup>>,
        spawn_type: SpawnType,
    ) -> Result<Initialized<Deps, A, Reg, Sup>, InitError<A>>
    where
        A: 'static + Actor + Send + Sync + Into<<Sup::Event as SupervisorEvent>::ChildStates>,
        Sup: 'static + EventDriven,
        Sup::Event: SupervisorEvent,
        <Sup::Event as SupervisorEvent>::Children: From<PhantomData<A>>,
        Deps: 'static + Dependencies + Clone + Send + Sync,
    {
        log::debug!("Spawning {}", actor.name());
        let (abort_handle, abort_reg) = AbortHandle::new_pair();
        let mut actor_rt = match self
            .child_actor(&actor, actor.name(), abort_handle.clone(), supervisor_handle.clone())
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
        if let Err(e) = Self::handle_init_res(res, &mut actor_rt).await {
            return Err((actor, e).into());
        }
        Ok(InitData {
            actor,
            actor_rt,
            abort_reg,
            supervisor_handle,
            spawn_type,
            _deps: PhantomData,
        }
        .into())
    }

    async fn common_spawn<A, Sup, Deps>(
        &mut self,
        InitData {
            mut actor,
            mut actor_rt,
            abort_reg: abort_registration,
            supervisor_handle,
            spawn_type,
            _deps,
        }: InitData<Deps, A, Reg, Sup>,
    ) -> Act<A>
    where
        A: 'static + Actor + Send + Sync + Into<<Sup::Event as SupervisorEvent>::ChildStates>,
        Sup: 'static + EventDriven,
        Sup::Event: SupervisorEvent,
        <Sup::Event as SupervisorEvent>::Children: From<PhantomData<A>>,
        Deps: 'static + Dependencies + Clone + Send + Sync,
    {
        let handle = actor_rt.handle();
        let child_task = tokio::spawn(async move {
            let res = Abortable::new(
                AssertUnwindSafe(async {
                    let deps = A::Dependencies::link(&mut actor_rt.scope).await?;
                    actor.run(&mut actor_rt, deps).await
                })
                .catch_unwind(),
                abort_registration,
            )
            .await;
            Deps::cleanup(&mut actor_rt, spawn_type).await;
            Self::handle_run_res(res, &mut actor_rt, supervisor_handle, actor).await
        });
        self.join_handles.push(child_task);
        handle
    }

    async fn common_init_and_spawn<A, Sup, Deps>(
        &mut self,
        actor: A,
        supervisor_handle: Option<Act<Sup>>,
        spawn_type: SpawnType,
    ) -> Result<Act<A>, InitError<A>>
    where
        A: 'static + Actor + Send + Sync + Into<<Sup::Event as SupervisorEvent>::ChildStates>,
        Sup: 'static + EventDriven,
        Sup::Event: SupervisorEvent,
        <Sup::Event as SupervisorEvent>::Children: From<PhantomData<A>>,
        Deps: 'static + Dependencies + Clone + Send + Sync,
    {
        let init = self
            .common_init::<_, _, Deps>(actor, supervisor_handle, spawn_type)
            .await?;
        Ok(init.spawn(self).await)
    }

    pub(crate) async fn handle_init_res<A, Sup>(
        res: std::thread::Result<Result<(), ActorError>>,
        child_scope: &mut ActorScopedRuntime<A, Reg, Sup>,
    ) -> anyhow::Result<()>
    where
        Sup: EventDriven,
        Sup::Event: SupervisorEvent,
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

    pub(crate) async fn handle_run_res<A, Sup>(
        res: Result<std::thread::Result<Result<(), ActorError>>, Aborted>,
        child_scope: &mut ActorScopedRuntime<A, Reg, Sup>,
        supervisor_handle: Option<Act<Sup>>,
        actor: A,
    ) -> anyhow::Result<()>
    where
        Sup: EventDriven,
        Sup::Event: SupervisorEvent,
        A: Actor + Into<<Sup::Event as SupervisorEvent>::ChildStates>,
    {
        let service = child_scope.service().await;
        child_scope.abort().await;
        child_scope.join().await;
        if let Some(supervisor) = supervisor_handle {
            match res {
                Ok(res) => match res {
                    Ok(res) => match res {
                        Ok(_) => supervisor.send(Sup::Event::report_ok(SuccessReport::new(actor.into(), service))),

                        Err(e) => {
                            log::error!("{} exited with error: {}", actor.name(), e);
                            supervisor.send(Sup::Event::report_err(ErrorReport::new(actor.into(), service, e)))
                        }
                    },
                    Err(e) => {
                        supervisor
                            .send(Sup::Event::report_err(ErrorReport::new(
                                actor.into(),
                                service,
                                anyhow::anyhow!("Actor exited with error!").into(),
                            )))
                            .ok();
                        std::panic::resume_unwind(e);
                    }
                },
                Err(_) => supervisor.send(Sup::Event::report_ok(SuccessReport::new(actor.into(), service))),
            }
        } else {
            match res {
                Ok(res) => match res {
                    Ok(res) => match res {
                        Ok(_) => Ok(()),
                        Err(e) => {
                            log::error!("{} exited with error: {}", actor.name(), e);
                            anyhow::bail!(e)
                        }
                    },
                    Err(e) => {
                        std::panic::resume_unwind(e);
                    }
                },
                Err(_) => {
                    anyhow::bail!("Aborted!")
                }
            }
        }
    }

    /// Spawn a new pool of actors of a given type with some metric
    pub async fn new_pool_with<Sup, I, F, P: ActorPool>(&mut self, supervisor_handle: I, f: F) -> anyhow::Result<()>
    where
        Sup: 'static + EventDriven,
        Sup::Event: SupervisorEvent,
        I: Into<Option<Act<Sup>>>,
        P: 'static + Send + Sync,

        for<'b> F: 'static + Send + FnOnce(&'b mut ScopedActorPool<Reg, Sup, P>) -> BoxFuture<'b, anyhow::Result<()>>,
    {
        f(&mut self.new_pool(supervisor_handle).await).await
    }

    /// Spawn a new pool of actors of a given type with some metric
    pub async fn spawn_pool_with<Sup, I, F, P: ActorPool>(&mut self, supervisor_handle: I, f: F) -> anyhow::Result<()>
    where
        Sup: 'static + EventDriven,
        Sup::Event: SupervisorEvent,
        I: Into<Option<Act<Sup>>>,
        P: 'static + Send + Sync,

        for<'b> F: 'static + Send + FnOnce(&'b mut ScopedActorPool<Reg, Sup, P>) -> BoxFuture<'b, anyhow::Result<()>>,
    {
        f(&mut self.spawn_pool(supervisor_handle).await).await
    }

    /// Spawn a new pool of actors of a given type with some metric
    pub async fn new_pool<Sup, I, P: ActorPool>(&mut self, supervisor_handle: I) -> ScopedActorPool<'_, Reg, Sup, P>
    where
        Sup: 'static + EventDriven,
        Sup::Event: SupervisorEvent,
        I: Into<Option<Act<Sup>>>,
        P: 'static + Send + Sync,
    {
        let pool = Pool::<P>::default();
        self.add_data(pool.clone()).await;

        let scoped_pool = ScopedActorPool {
            scope: self,
            pool,
            supervisor_handle: supervisor_handle.into(),
            initialized: Default::default(),
        };
        scoped_pool
    }

    /// Spawn a new pool (or use an existing one) of actors of a given type with some metric
    pub async fn spawn_pool<Sup, I, P: ActorPool>(&mut self, supervisor_handle: I) -> ScopedActorPool<'_, Reg, Sup, P>
    where
        Sup: 'static + EventDriven,
        Sup::Event: SupervisorEvent,
        I: Into<Option<Act<Sup>>>,
        P: 'static + Send + Sync,
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
            supervisor_handle: supervisor_handle.into(),
            initialized: Default::default(),
        };
        scoped_pool
    }

    /// Initialize an actor into a pool
    pub async fn init_into_pool<Sup, I, P: ActorPool>(
        &mut self,
        supervisor_handle: I,
        actor: P::Actor,
    ) -> Result<Initialized<Act<P::Actor>, P::Actor, Reg, Sup>, InitError<P::Actor>>
    where
        Sup: 'static + EventDriven,
        Sup::Event: SupervisorEvent,
        <Sup::Event as SupervisorEvent>::Children: From<PhantomData<P::Actor>>,
        I: Into<Option<Act<Sup>>>,
        P: 'static + BasicActorPool + Send + Sync,
        P::Actor: Actor + Into<<Sup::Event as SupervisorEvent>::ChildStates>,
    {
        let pool = self.spawn_pool::<_, _, P>(supervisor_handle).await;
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
    pub async fn spawn_into_pool<Sup, I, P: ActorPool>(
        &mut self,
        supervisor_handle: I,
        actor: P::Actor,
    ) -> Result<Act<P::Actor>, InitError<P::Actor>>
    where
        Sup: 'static + EventDriven,
        Sup::Event: SupervisorEvent,
        <Sup::Event as SupervisorEvent>::Children: From<PhantomData<P::Actor>>,
        I: Into<Option<Act<Sup>>>,
        P: 'static + BasicActorPool + Send + Sync,
        P::Actor: Actor + Into<<Sup::Event as SupervisorEvent>::ChildStates>,
    {
        let mut pool = self.spawn_pool::<_, _, P>(supervisor_handle).await;
        pool.spawn(actor).await
    }

    /// Initialize an actor into a pool
    pub async fn init_into_pool_keyed<Sup, I, P: ActorPool>(
        &mut self,
        supervisor_handle: I,
        key: P::Key,
        actor: P::Actor,
    ) -> Result<Initialized<Act<P::Actor>, P::Actor, Reg, Sup>, InitError<P::Actor>>
    where
        Sup: 'static + EventDriven,
        Sup::Event: SupervisorEvent,
        <Sup::Event as SupervisorEvent>::Children: From<PhantomData<P::Actor>>,
        I: Into<Option<Act<Sup>>>,
        P: 'static + KeyedActorPool + Send + Sync,
        P::Actor: Actor + Into<<Sup::Event as SupervisorEvent>::ChildStates>,
    {
        let pool = self.spawn_pool::<_, _, P>(supervisor_handle).await;
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
    pub async fn spawn_into_pool_keyed<Sup, I, P: ActorPool>(
        &mut self,
        supervisor_handle: I,
        key: P::Key,
        actor: P::Actor,
    ) -> Result<Act<P::Actor>, InitError<P::Actor>>
    where
        Sup: 'static + EventDriven,
        Sup::Event: SupervisorEvent,
        <Sup::Event as SupervisorEvent>::Children: From<PhantomData<P::Actor>>,
        I: Into<Option<Act<Sup>>>,
        P: 'static + KeyedActorPool + Send + Sync,
        P::Actor: Actor + Into<<Sup::Event as SupervisorEvent>::ChildStates>,
    {
        let mut pool = self.spawn_pool::<_, _, P>(supervisor_handle).await;
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
pub struct InitData<Deps, A, Reg, Sup>
where
    A: Actor,
    Sup: EventDriven,
    Reg: 'static,
{
    actor: A,
    actor_rt: ActorScopedRuntime<A, Reg, Sup>,
    abort_reg: AbortRegistration,
    supervisor_handle: Option<Act<Sup>>,
    spawn_type: SpawnType,
    _deps: PhantomData<fn(Deps) -> Deps>,
}

/// A handle to an initialized actor. If unused, this will cleanup and abort the scope when dropped!
#[must_use = "An unused, initialized actor may cause unintended behavior!"]
pub struct Initialized<Deps, A, Reg, Sup>
where
    Reg: 'static + RegistryAccess + Send + Sync,
    A: 'static + Actor,
    Sup: 'static + EventDriven,
    Reg: 'static,
    Deps: 'static + Dependencies + Clone + Send + Sync,
{
    data: Option<InitData<Deps, A, Reg, Sup>>,
}

impl<Deps, A, Reg, Sup> Initialized<Deps, A, Reg, Sup>
where
    Reg: 'static + RegistryAccess + Send + Sync,
    A: 'static + Actor + Send + Sync + Into<<Sup::Event as SupervisorEvent>::ChildStates>,
    Sup: 'static + EventDriven,
    Sup::Event: SupervisorEvent,
    <Sup::Event as SupervisorEvent>::Children: From<PhantomData<A>>,
    Deps: 'static + Dependencies + Clone + Send + Sync,
{
    /// Spawn the initialized actor with the given scope.
    /// This must be the same scope as the one used to init the actor!
    pub async fn spawn(mut self, rt: &mut RuntimeScope<Reg>) -> Act<A> {
        rt.common_spawn(std::mem::take(&mut self.data).unwrap()).await
    }
}

impl<Deps, A, Reg, Sup> From<InitData<Deps, A, Reg, Sup>> for Initialized<Deps, A, Reg, Sup>
where
    Reg: 'static + RegistryAccess + Send + Sync,
    A: 'static + Actor,
    Sup: 'static + EventDriven,
    Reg: 'static,
    Deps: 'static + Dependencies + Clone + Send + Sync,
{
    fn from(data: InitData<Deps, A, Reg, Sup>) -> Self {
        Self { data: Some(data) }
    }
}

impl<Deps, A, Reg, Sup> Drop for Initialized<Deps, A, Reg, Sup>
where
    Reg: 'static + RegistryAccess + Send + Sync,
    A: 'static + Actor,
    Sup: 'static + EventDriven,
    Reg: 'static,
    Deps: 'static + Dependencies + Clone + Send + Sync,
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

/// An actor's scope, which provides some helpful functions specific to an actor
pub struct ActorScopedRuntime<A, Reg, Sup>
where
    A: Actor,
    Sup: EventDriven,
    Reg: 'static,
{
    pub(crate) scope: RuntimeScope<Reg>,
    pub(crate) handle: <A::Channel as Channel<A, A::Event>>::Sender,
    pub(crate) receiver: ShutdownStream<<A::Channel as Channel<A, A::Event>>::Receiver>,
    pub(crate) shutdown_handle: ShutdownHandle,
    pub(crate) abort_handle: AbortHandle,
    pub(crate) supervisor_handle: Option<Act<Sup>>,
}

impl<A, Reg, Sup> ActorScopedRuntime<A, Reg, Sup>
where
    A: Actor,
    Sup: EventDriven,
    Reg: 'static + RegistryAccess + Send + Sync,
{
    pub(crate) async fn new<P: Into<Option<ScopeId>> + Send, S: Into<String>, O: Into<Option<S>>>(
        registry: Reg,
        actor: &A,
        parent_scope_id: P,
        name: O,
        abort_handle: AbortHandle,
        supervisor_handle: Option<Act<Sup>>,
    ) -> anyhow::Result<Self> {
        let (sender, receiver) = <A::Channel as Channel<A, A::Event>>::new(&actor).await?;
        let (receiver, shutdown_handle) = ShutdownStream::new(receiver);
        let scope = RuntimeScope::new(
            registry,
            parent_scope_id,
            name,
            Some(shutdown_handle.clone()),
            Some(abort_handle.clone()),
        )
        .await;
        Ok(Self {
            scope,
            handle: sender,
            receiver,
            shutdown_handle,
            supervisor_handle,
            abort_handle,
        })
    }

    /// Initialize a new actor with a supervisor handle
    pub async fn init_actor<OtherA>(
        &mut self,
        actor: OtherA,
    ) -> Result<Initialized<Act<OtherA>, OtherA, Reg, A>, InitError<OtherA>>
    where
        OtherA: 'static + Actor + Send + Sync + Into<<<A as EventDriven>::Event as SupervisorEvent>::ChildStates>,
        A: 'static + EventDriven,
        <A as EventDriven>::Event: SupervisorEvent,
        <<A as EventDriven>::Event as SupervisorEvent>::Children: From<PhantomData<OtherA>>,
    {
        let handle = self.handle();
        self.scope.init_actor(actor, handle).await
    }

    /// Spawn a new actor with a supervisor handle
    pub async fn spawn_actor<OtherA>(&mut self, actor: OtherA) -> Result<Act<OtherA>, InitError<OtherA>>
    where
        OtherA: 'static + Actor + Send + Sync + Into<<<A as EventDriven>::Event as SupervisorEvent>::ChildStates>,
        A: 'static + EventDriven,
        <A as EventDriven>::Event: SupervisorEvent,
        <<A as EventDriven>::Event as SupervisorEvent>::Children: From<PhantomData<OtherA>>,
    {
        let handle = self.handle();
        self.scope.spawn_actor(actor, handle).await
    }

    /// Spawn a new system with a supervisor handle
    pub async fn init_system<OtherA>(
        &mut self,
        actor: OtherA,
        state: OtherA::State,
    ) -> Result<Initialized<Sys<OtherA>, OtherA, Reg, A>, InitError<OtherA>>
    where
        OtherA: 'static + System + Send + Sync + Into<<<A as EventDriven>::Event as SupervisorEvent>::ChildStates>,
        A: 'static + EventDriven,
        <A as EventDriven>::Event: SupervisorEvent,
        <<A as EventDriven>::Event as SupervisorEvent>::Children: From<PhantomData<OtherA>>,
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
        OtherA: 'static + System + Send + Sync + Into<<<A as EventDriven>::Event as SupervisorEvent>::ChildStates>,
        A: 'static + EventDriven,
        <A as EventDriven>::Event: SupervisorEvent,
        <<A as EventDriven>::Event as SupervisorEvent>::Children: From<PhantomData<OtherA>>,
    {
        let handle = self.handle();
        self.scope.spawn_system(actor, state, handle).await
    }

    /// Spawn a new pool of actors of a given type with some metric
    pub async fn new_pool_with<P: ActorPool, F>(&mut self, f: F) -> anyhow::Result<()>
    where
        A: 'static + EventDriven,
        <A as EventDriven>::Event: SupervisorEvent,
        <<A as EventDriven>::Event as SupervisorEvent>::Children: From<PhantomData<P::Actor>>,
        P: 'static + Send + Sync,
        for<'b> F: 'static + Send + FnOnce(&'b mut ScopedActorPool<Reg, A, P>) -> BoxFuture<'b, anyhow::Result<()>>,
    {
        let handle = self.handle();
        self.scope.new_pool_with::<A, _, F, P>(handle, f).await
    }

    /// Spawn a new pool of actors of a given type with some metric
    pub async fn spawn_pool_with<P: ActorPool, F>(&mut self, f: F) -> anyhow::Result<()>
    where
        A: 'static + EventDriven,
        <A as EventDriven>::Event: SupervisorEvent,
        <<A as EventDriven>::Event as SupervisorEvent>::Children: From<PhantomData<P::Actor>>,
        P: 'static + Send + Sync,
        for<'b> F: 'static + Send + FnOnce(&'b mut ScopedActorPool<Reg, A, P>) -> BoxFuture<'b, anyhow::Result<()>>,
    {
        let handle = self.handle();
        self.scope.spawn_pool_with::<A, _, F, P>(handle, f).await
    }

    /// Spawn a new pool of actors of a given type with some metric
    pub async fn new_pool<P: ActorPool>(&mut self) -> ScopedActorPool<'_, Reg, A, P>
    where
        A: 'static + EventDriven,
        <A as EventDriven>::Event: SupervisorEvent,
        <<A as EventDriven>::Event as SupervisorEvent>::Children: From<PhantomData<P::Actor>>,
        P: 'static + Send + Sync,
    {
        let handle = self.handle();
        self.scope.new_pool::<A, _, P>(handle).await
    }

    /// Spawn a new pool of actors of a given type with some metric
    pub async fn spawn_pool<P: ActorPool>(&mut self) -> ScopedActorPool<'_, Reg, A, P>
    where
        A: 'static + EventDriven,
        <A as EventDriven>::Event: SupervisorEvent,
        <<A as EventDriven>::Event as SupervisorEvent>::Children: From<PhantomData<P::Actor>>,
        P: 'static + Send + Sync,
    {
        let handle = self.handle();
        self.scope.spawn_pool::<A, _, P>(handle).await
    }

    /// Initialize an actor into a pool
    pub async fn init_into_pool<P: ActorPool>(
        &mut self,
        actor: P::Actor,
    ) -> Result<Initialized<Act<P::Actor>, P::Actor, Reg, A>, InitError<P::Actor>>
    where
        A: 'static + EventDriven,
        <A as EventDriven>::Event: SupervisorEvent,
        <<A as EventDriven>::Event as SupervisorEvent>::Children: From<PhantomData<P::Actor>>,
        P: 'static + BasicActorPool + Send + Sync,
        P::Actor: Actor + Into<<<A as EventDriven>::Event as SupervisorEvent>::ChildStates>,
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
        A: 'static + EventDriven,
        <A as EventDriven>::Event: SupervisorEvent,
        <<A as EventDriven>::Event as SupervisorEvent>::Children: From<PhantomData<P::Actor>>,
        P: 'static + BasicActorPool + Send + Sync,
        P::Actor: Actor + Into<<<A as EventDriven>::Event as SupervisorEvent>::ChildStates>,
    {
        let handle = self.handle();
        self.scope.spawn_into_pool::<A, _, P>(handle, actor).await
    }

    /// Initialize an actor into a pool
    pub async fn init_into_pool_keyed<P: ActorPool>(
        &mut self,
        key: P::Key,
        actor: P::Actor,
    ) -> Result<Initialized<Act<P::Actor>, P::Actor, Reg, A>, InitError<P::Actor>>
    where
        A: 'static + EventDriven,
        <A as EventDriven>::Event: SupervisorEvent,
        <<A as EventDriven>::Event as SupervisorEvent>::Children: From<PhantomData<P::Actor>>,
        P: 'static + KeyedActorPool + Send + Sync,
        P::Actor: Actor + Into<<<A as EventDriven>::Event as SupervisorEvent>::ChildStates>,
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
        A: 'static + EventDriven,
        <A as EventDriven>::Event: SupervisorEvent,
        <<A as EventDriven>::Event as SupervisorEvent>::Children: From<PhantomData<P::Actor>>,
        P: 'static + KeyedActorPool + Send + Sync,
        P::Actor: Actor + Into<<<A as EventDriven>::Event as SupervisorEvent>::ChildStates>,
    {
        let handle = self.handle();
        self.scope.spawn_into_pool_keyed::<A, _, P>(handle, key, actor).await
    }

    /// Get the next event from the event receiver
    pub async fn next_event(&mut self) -> Option<A::Event> {
        self.receiver.next().await
    }

    /// Get this actors's handle
    pub fn handle(&self) -> Act<A> {
        Act::new(
            self.scope.scope_id,
            self.handle.clone(),
            self.shutdown_handle.clone(),
            self.abort_handle.clone(),
        )
    }

    /// Get this actors's shutdown handle
    pub fn shutdown_handle(&self) -> ShutdownHandle {
        self.shutdown_handle.clone()
    }

    /// Get the runtime's service
    pub async fn service(&mut self) -> Service {
        self.scope.service().await
    }

    /// Get this actor's supervisor handle
    pub fn supervisor_handle(&mut self) -> &mut Option<Act<Sup>> {
        &mut self.supervisor_handle
    }
}

impl<A, Reg, Sup> ActorScopedRuntime<A, Reg, Sup>
where
    A: Actor,
    Sup: EventDriven,
    Sup::Event: SupervisorEvent,
    Reg: 'static + RegistryAccess + Send + Sync,
{
    /// Update this scope's service status
    pub async fn update_status<S: Status>(&mut self, status: S) -> anyhow::Result<()>
    where
        <Sup::Event as SupervisorEvent>::Children: From<PhantomData<A>>,
    {
        if self.supervisor_handle.is_some() {
            let mut service = self.service().await;
            let prev_status = service.status().clone();
            service.update_status(CustomStatus(status.clone()));
            self.supervisor_handle
                .send(Sup::Event::status_change(StatusChange::new(
                    PhantomData::<A>.into(),
                    prev_status,
                    service,
                )))
                .ok();
        }
        self.scope.update_status(status).await
    }
}

impl<A, Reg, Sup> Deref for ActorScopedRuntime<A, Reg, Sup>
where
    A: Actor,
    Sup: EventDriven,
{
    type Target = RuntimeScope<Reg>;

    fn deref(&self) -> &Self::Target {
        &self.scope
    }
}

impl<A, Reg, Sup> DerefMut for ActorScopedRuntime<A, Reg, Sup>
where
    A: Actor,
    Sup: EventDriven,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.scope
    }
}
/// A scope for an actor pool, which only allows spawning of the specified actor
pub struct ScopedActorPool<'a, Reg, Sup, P>
where
    Reg: 'static + RegistryAccess + Send + Sync,
    Sup: 'static + EventDriven,
    Sup::Event: SupervisorEvent,
    P: ActorPool,
    P::Actor: 'static,
{
    scope: &'a mut RuntimeScope<Reg>,
    pool: Pool<P>,
    supervisor_handle: Option<Act<Sup>>,
    initialized: Vec<Initialized<Act<P::Actor>, P::Actor, Reg, Sup>>,
}

impl<'a, Reg, Sup, P> ScopedActorPool<'a, Reg, Sup, P>
where
    P::Actor: Actor + Into<<Sup::Event as SupervisorEvent>::ChildStates>,
    Reg: 'static + RegistryAccess + Send + Sync,
    Sup: 'static + EventDriven,
    Sup::Event: SupervisorEvent,
    <Sup::Event as SupervisorEvent>::Children: From<PhantomData<P::Actor>>,
    P: BasicActorPool,
{
    /// Spawn a new actor into this pool
    pub async fn spawn(&mut self, actor: P::Actor) -> Result<Act<P::Actor>, InitError<P::Actor>>
    where
        P::Actor: 'static + Send + Sync,
    {
        let supervisor_handle = self.supervisor_handle.clone();
        let handle = self
            .scope
            .common_init_and_spawn::<_, _, Act<P::Actor>>(actor, supervisor_handle, SpawnType::Pool)
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

impl<'a, Reg, Sup, P> ScopedActorPool<'a, Reg, Sup, P>
where
    P::Actor: Actor + Into<<Sup::Event as SupervisorEvent>::ChildStates>,
    Reg: 'static + RegistryAccess + Send + Sync,
    Sup: 'static + EventDriven,
    Sup::Event: SupervisorEvent,
    <Sup::Event as SupervisorEvent>::Children: From<PhantomData<P::Actor>>,
    P: KeyedActorPool,
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
            .common_init_and_spawn::<_, _, Act<P::Actor>>(actor, supervisor_handle, SpawnType::Pool)
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

impl<'a, Reg, Sup, P> ScopedActorPool<'a, Reg, Sup, P>
where
    P::Actor: Actor + Into<<Sup::Event as SupervisorEvent>::ChildStates>,
    Reg: 'static + RegistryAccess + Send + Sync,
    Sup: 'static + EventDriven,
    Sup::Event: SupervisorEvent,
    <Sup::Event as SupervisorEvent>::Children: From<PhantomData<P::Actor>>,
    P: ActorPool,
{
    /// Finalize any initialized and unspawned actors in this pool by spawning them
    pub async fn spawn_all(&mut self) {
        for init in self.initialized.drain(..) {
            init.spawn(self.scope).await;
        }
    }
}
