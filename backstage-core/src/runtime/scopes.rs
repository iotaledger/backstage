// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::actor::{
    Actor, ActorError, ActorPool, BasicActorPool, CustomStatus, Dependencies, DynEvent, Envelope, EnvelopeSender,
    ErrorReport, HandleEvent, InitError, KeyedActorPool, Receiver, Report, ServiceTree, ShutdownHandle, ShutdownStream,
    Status, StatusChange, SuccessReport, UnboundedTokioChannel,
};
use futures::{
    future::{AbortRegistration, Aborted},
    FutureExt,
};
use std::{
    convert::{TryFrom, TryInto},
    fmt::Debug,
    panic::AssertUnwindSafe,
    path::Path,
};

/// A runtime which defines a particular scope and functionality to
/// create tasks within it.
#[derive(Debug)]
pub struct RuntimeScope {
    pub(crate) scope: ScopeView,
    pub(crate) join_handles: Vec<JoinHandle<anyhow::Result<()>>>,
}

/// A view into a particular scope which provides the user-facing API
#[derive(Clone, Debug)]
#[repr(transparent)]
pub struct ScopeView(pub(crate) Scope);

impl Deref for RuntimeScope {
    type Target = ScopeView;

    fn deref(&self) -> &Self::Target {
        &self.scope
    }
}

impl ScopeView {
    /// Get the scope id
    pub fn id(&self) -> ScopeId {
        self.0.id
    }

    /// Get the parent scope, if one exists
    pub fn parent(&self) -> Option<ScopeView> {
        self.0.parent().cloned().map(ScopeView)
    }

    /// Get the child scopes
    pub async fn children(&self) -> Vec<ScopeView> {
        self.0
            .data
            .read()
            .await
            .children
            .values()
            .cloned()
            .map(ScopeView)
            .collect()
    }

    pub(crate) async fn add_data<T: 'static + Send + Sync + Clone>(&self, data: T) {
        log::debug!(
            "Adding {} to scope {:x}",
            std::any::type_name::<T>(),
            self.0.id.as_fields().0
        );
        self.0.add_data(std::any::TypeId::of::<T>(), Box::new(data)).await
    }

    pub(crate) async fn depend_on<T: 'static + Send + Sync + Clone>(&self) -> anyhow::Result<DepStatus<T>> {
        self.0
            .depend_on(std::any::TypeId::of::<T>())
            .await
            .map(|res| res.with_type::<T>())
    }

    pub(crate) async fn get_data<T: 'static + Send + Sync + Clone>(&self) -> anyhow::Result<DepStatus<T>> {
        self.0
            .get_data_promise(std::any::TypeId::of::<T>())
            .await
            .map(|res| res.with_type::<T>())
    }

    pub(crate) async fn get_data_opt<T: 'static + Send + Sync + Clone>(&self) -> Option<T> {
        self.0
            .get_data(std::any::TypeId::of::<T>())
            .await
            .map(|res| res.with_type::<T>())
    }

    /// Query the registry for a dependency. This will return immediately whether or not it exists.
    pub async fn query_data<T: 'static + Clone + Send + Sync + Dependencies>(&self) -> anyhow::Result<T> {
        T::request_opt(self).await
    }

    /// Request a dependency and wait for it to be available.
    pub async fn request_data<T: 'static + Clone + Send + Sync + Dependencies>(&self) -> anyhow::Result<T> {
        T::request(self).await
    }

    /// Request a dependency and wait for it to be added, forming a link between this scope and
    /// the requested data. If the data is removed from this scope, it will be shut down.
    pub async fn link_data<T: 'static + Clone + Send + Sync + Dependencies>(&self) -> anyhow::Result<T> {
        T::link(self).await
    }

    pub(crate) async fn remove_data<T: 'static + Send + Sync + Clone>(&self) -> Option<T> {
        log::debug!(
            "Removing {} from scope {:x}",
            std::any::type_name::<T>(),
            self.0.id.as_fields().0
        );
        self.0
            .remove_data(std::any::TypeId::of::<T>())
            .await
            .map(|res| res.with_type::<T>())
    }

    /// Get this scope's service
    pub async fn service(&self) -> ServiceView {
        self.0.get_service().await
    }

    /// Get the root scope
    pub fn root_scope(&self) -> ScopeView {
        self.find_scope(ROOT_SCOPE).unwrap()
    }

    /// Find a scope by id
    pub fn find_scope(&self, scope_id: ScopeId) -> Option<ScopeView> {
        self.0.find(scope_id).map(ScopeView)
    }

    /// Find a scope by its path. Relative paths will search from this scope while absolute paths will begin with the
    /// root.
    pub async fn find_scope_by_path(&self, path: &Path) -> Option<ScopeView> {
        self.0.find_by_path(path).await.map(ScopeView)
    }

    /// Update this scope's service status
    pub async fn update_status<S: Status>(&self, status: S) {
        self.0.update_status(CustomStatus(status)).await;
    }

    /// Shut down the scope
    pub async fn shutdown(&self) {
        self.0.shutdown().await;
    }

    /// Abort the tasks in this runtime's scope. This will shutdown tasks that have
    /// shutdown handles instead.
    pub(crate) async fn abort(&self) {
        self.0.abort().await;
    }

    /// Get the service tree beginning with this scope
    pub async fn service_tree(&self) -> ServiceTree {
        self.0.service_tree().await
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
        handle.send(event)
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
    pub async fn add_resource<R: 'static + Send + Sync + Clone>(&self, resource: R) -> Res<R> {
        self.add_data(Res(resource.clone())).await;
        Res(resource)
    }

    /// Add a global, shared resource and get a reference to it
    pub async fn add_global_resource<R: 'static + Send + Sync + Clone>(&self, resource: R) -> Res<R> {
        log::debug!("Adding {} to root scope", std::any::type_name::<Res<R>>());
        self.root_scope().add_data(Res(resource.clone())).await;
        Res(resource)
    }

    /// Get the pool of a specified type if it exists in this runtime's scope
    pub async fn pool<P>(&self) -> Option<Pool<P>>
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
}

impl RuntimeScope {
    fn root(abort_handle: AbortHandle) -> Self {
        let scope = ScopeView(Scope::root(abort_handle));
        Self {
            scope,
            join_handles: Default::default(),
        }
    }

    /// Launch a new root runtime scope
    pub async fn launch<F, O>(f: F) -> anyhow::Result<O>
    where
        O: Send + Sync,
        for<'b> F: Send + FnOnce(&'b mut RuntimeScope) -> BoxFuture<'b, anyhow::Result<O>>,
    {
        log::debug!("Spawning backstage runtime");
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        let mut scope = Self::root(abort_handle);
        scope.update_status(ServiceStatus::Running).await;
        let res = Abortable::new(f(&mut scope), abort_registration).await;
        if let Ok(Err(_)) = res {
            scope.abort().await;
        }
        scope.join().await;
        res.map_err(|_| anyhow::anyhow!("Aborted scope!")).and_then(|res| res)
    }

    async fn create_context<A, Sup, H>(
        &self,
        actor: &A,
        supervisor_handle: Option<H>,
        abort_handle: Option<AbortHandle>,
    ) -> anyhow::Result<A::Context>
    where
        H: 'static,
        A: 'static + Actor,
        Sup: 'static,
        A::Context: TryFrom<AnyContext<A, Sup, H>>,
        <A::Context as TryFrom<AnyContext<A, Sup, H>>>::Error: Into<anyhow::Error>,
    {
        let name = actor.name().into_owned();
        AnyContext::new(actor, |_| name, self, supervisor_handle, abort_handle)
            .await
            .and_then(|cx| {
                cx.try_into()
                    .map_err(|e: <A::Context as TryFrom<AnyContext<A, Sup, H>>>::Error| anyhow::anyhow!(e))
            })
    }

    pub(crate) async fn child<N: FnOnce(ScopeId) -> String>(
        &self,
        name: N,
        shutdown_handle: Option<ShutdownHandle>,
        abort_handle: Option<AbortHandle>,
        path: Option<&'static str>,
    ) -> Self {
        Self {
            scope: ScopeView(self.scope.0.child(name, shutdown_handle, abort_handle, path).await),
            join_handles: Default::default(),
        }
    }

    /// Create a new scope within this one
    pub async fn scope<O, F>(&self, f: F) -> anyhow::Result<O>
    where
        O: Send + Sync,
        for<'b> F: Send + FnOnce(&'b mut RuntimeScope) -> BoxFuture<'b, anyhow::Result<O>>,
    {
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        let mut child_scope = self
            .child(
                |id| format!("Scope {:x}", id.as_fields().0),
                None,
                Some(abort_handle),
                None,
            )
            .await;
        child_scope.update_status(ServiceStatus::Running).await;
        let res = Abortable::new(f(&mut child_scope), abort_registration).await;
        if let Ok(Err(_)) = res {
            child_scope.abort().await;
        }
        child_scope.join().await;
        res.map_err(|_| anyhow::anyhow!("Aborted scope!")).and_then(|res| res)
    }

    /// Await the tasks in this runtime's scope
    pub(crate) async fn join(&mut self) {
        log::debug!("Joining scope {:x}", self.0.id.as_fields().0);
        for handle in self.join_handles.drain(..) {
            handle.await.ok();
        }
        self.0.drop().await;
    }

    /// Spawn a new, plain task
    pub async fn spawn_task<F>(&mut self, f: F) -> AbortHandle
    where
        for<'b> F: 'static + Send + FnOnce(&'b mut RuntimeScope) -> BoxFuture<'b, anyhow::Result<()>>,
    {
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        let mut child_scope = self
            .child(
                |scope_id| format!("Task {:x}", scope_id.as_fields().0),
                None,
                Some(abort_handle.clone()),
                None,
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
    pub async fn init_actor_unsupervised<A>(&mut self, actor: A) -> Result<Initialized<A>, InitError<A>>
    where
        A: 'static + Actor + Send + Sync,
        A::Context: From<UnsupervisedContext<A>>,
    {
        if self.actor_event_handle::<A>().await.is_some() {
            let service = self.service().await;
            let name = actor.name();
            return Err((
                actor,
                anyhow::anyhow!(
                    "Attempted to add a duplicate actor ({}) to scope {:x} ({})",
                    name,
                    self.0.id.as_fields().0,
                    service.name()
                ),
            )
                .into());
        }
        let (abort_handle, abort_reg) = AbortHandle::new_pair();
        let name = actor.name().into_owned();
        let cx = match UnsupervisedContext::new(&actor, |_| name, self, Some(abort_handle)).await {
            Ok(cx) => cx,
            Err(e) => {
                return Err((actor, e).into());
            }
        };
        self.common_init(cx.into(), actor, abort_reg).await
    }

    /// Synchronously initialize an actor and prepare to spawn it
    pub async fn init_actor<A, Sup, H>(
        &mut self,
        actor: A,
        supervisor_handle: H,
    ) -> Result<Initialized<A>, InitError<A>>
    where
        A: 'static + Actor + Send + Sync,
        A::Context: TryFrom<AnyContext<A, Sup, H>>,
        <A::Context as TryFrom<AnyContext<A, Sup, H>>>::Error: Into<anyhow::Error>,
        H: 'static + EnvelopeSender<Sup>,
        Sup: 'static + Supervisor<A>,
    {
        if let Some(handle) = self.actor_event_handle::<A>().await {
            if handle.scope_id() == self.id() {
                let service = self.service().await;
                let name = actor.name();
                return Err((
                    actor,
                    anyhow::anyhow!(
                        "Attempted to add a duplicate actor ({}) to scope {:x} ({})",
                        name,
                        self.id().as_fields().0,
                        service.name()
                    ),
                )
                    .into());
            }
        }
        let (abort_handle, abort_reg) = AbortHandle::new_pair();
        let cx = match self
            .create_context(&actor, Some(supervisor_handle), Some(abort_handle))
            .await
        {
            Ok(cx) => cx,
            Err(e) => {
                return Err((actor, e).into());
            }
        };
        self.common_init(cx, actor, abort_reg).await
    }

    /// Spawn a new actor with a supervisor handle
    pub async fn spawn_actor<A, Sup, H>(&mut self, actor: A, supervisor_handle: H) -> Result<Act<A>, InitError<A>>
    where
        A: 'static + Actor + Send + Sync,
        A::Context: TryFrom<AnyContext<A, Sup, H>>,
        <A::Context as TryFrom<AnyContext<A, Sup, H>>>::Error: Into<anyhow::Error>,
        H: 'static + EnvelopeSender<Sup>,
        Sup: 'static + Supervisor<A>,
    {
        let init = self.init_actor(actor, supervisor_handle).await?;
        Ok(init.spawn(self).await)
    }

    /// Spawn a new actor with no supervisor
    pub async fn spawn_actor_unsupervised<A>(&mut self, actor: A) -> Result<Act<A>, InitError<A>>
    where
        A: 'static + Actor + Send + Sync,
        A::Context: From<UnsupervisedContext<A>>,
    {
        let init = self.init_actor_unsupervised(actor).await?;
        Ok(init.spawn(self).await)
    }

    /// Synchronously initialize a system and prepare to spawn it
    pub async fn init_system_unsupervised<A>(
        &mut self,
        actor: A,
        state: A::State,
    ) -> Result<Initialized<A>, InitError<A>>
    where
        A: 'static + System + Send + Sync,
        A::Context: From<UnsupervisedContext<A>>,
    {
        if let Some(handle) = self.actor_event_handle::<A>().await {
            if handle.scope_id() == self.id() {
                let service = self.service().await;
                let name = actor.name();
                return Err((
                    actor,
                    anyhow::anyhow!(
                        "Attempted to add a duplicate actor ({}) to scope {:x} ({})",
                        name,
                        self.id().as_fields().0,
                        service.name()
                    ),
                )
                    .into());
            }
        }
        self.add_data(Res(state)).await;
        let (abort_handle, abort_reg) = AbortHandle::new_pair();
        let name = actor.name().into_owned();
        let cx = match UnsupervisedContext::new(&actor, |_| name, self, Some(abort_handle)).await {
            Ok(cx) => cx,
            Err(e) => {
                return Err((actor, e).into());
            }
        };
        self.common_init(cx.into(), actor, abort_reg).await
    }

    /// Synchronously initialize a system and prepare to spawn it
    pub async fn init_system<A, Sup, H>(
        &mut self,
        actor: A,
        state: A::State,
        supervisor_handle: H,
    ) -> Result<Initialized<A>, InitError<A>>
    where
        A: 'static + System + Send + Sync,
        H: 'static + EnvelopeSender<Sup>,
        Sup: 'static + Supervisor<A>,
        A::Context: TryFrom<AnyContext<A, Sup, H>>,
        <A::Context as TryFrom<AnyContext<A, Sup, H>>>::Error: Into<anyhow::Error>,
    {
        if self.actor_event_handle::<A>().await.is_some() {
            let service = self.service().await;
            let name = actor.name();
            return Err((
                actor,
                anyhow::anyhow!(
                    "Attempted to add a duplicate actor ({}) to scope {:x} ({})",
                    name,
                    self.id(),
                    service.name()
                ),
            )
                .into());
        }
        self.add_data(Res(state)).await;
        let (abort_handle, abort_reg) = AbortHandle::new_pair();
        let cx = match self
            .create_context(&actor, Some(supervisor_handle), Some(abort_handle))
            .await
        {
            Ok(cx) => cx,
            Err(e) => {
                return Err((actor, e).into());
            }
        };
        self.common_init(cx, actor, abort_reg).await
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
        H: 'static + EnvelopeSender<Sup>,
        Sup: 'static + Supervisor<A>,
        A::Context: TryFrom<AnyContext<A, Sup, H>>,
        <A::Context as TryFrom<AnyContext<A, Sup, H>>>::Error: Into<anyhow::Error>,
    {
        if let Some(handle) = self.actor_event_handle::<A>().await {
            if handle.scope_id() == self.id() {
                let service = self.service().await;
                let name = actor.name();
                return Err((
                    actor,
                    anyhow::anyhow!(
                        "Attempted to add a duplicate actor ({}) to scope {:x} ({})",
                        name,
                        self.id().as_fields().0,
                        service.name()
                    ),
                )
                    .into());
            }
        }
        self.add_data(Res(state)).await;
        let (abort_handle, abort_reg) = AbortHandle::new_pair();
        let cx = match self
            .create_context(&actor, Some(supervisor_handle), Some(abort_handle))
            .await
        {
            Ok(cx) => cx,
            Err(e) => {
                return Err((actor, e).into());
            }
        };
        self.common_init_and_spawn(cx, actor, abort_reg).await
    }

    /// Spawn a new system with no supervisor
    pub async fn spawn_system_unsupervised<A>(&mut self, actor: A, state: A::State) -> Result<Act<A>, InitError<A>>
    where
        A: 'static + System + Send + Sync,
        A::Context: From<UnsupervisedContext<A>>,
    {
        if self.actor_event_handle::<A>().await.is_some() {
            let service = self.service().await;
            let name = actor.name();
            return Err((
                actor,
                anyhow::anyhow!(
                    "Attempted to add a duplicate actor ({}) to scope {:x} ({})",
                    name,
                    self.id().as_fields().0,
                    service.name()
                ),
            )
                .into());
        }
        self.add_data(Res(state)).await;
        let (abort_handle, abort_reg) = AbortHandle::new_pair();
        let name = actor.name().into_owned();
        let cx = match UnsupervisedContext::new(&actor, |_| name, self, Some(abort_handle)).await {
            Ok(cx) => cx,
            Err(e) => {
                return Err((actor, e).into());
            }
        };
        self.common_init_and_spawn(cx.into(), actor, abort_reg).await
    }

    async fn common_init<A>(
        &mut self,
        mut cx: A::Context,
        mut actor: A,
        abort_reg: AbortRegistration,
    ) -> Result<Initialized<A>, InitError<A>>
    where
        A: 'static + Actor + Send + Sync,
    {
        log::debug!("Initializing {}", actor.name());
        let handle = cx.handle();
        self.add_data(handle.clone()).await;
        let res = AssertUnwindSafe(actor.init(&mut cx)).catch_unwind().await;
        match Self::handle_init_res::<A>(res, &mut cx).await {
            Ok(data) => Ok(InitData {
                actor,
                data,
                cx,
                abort_reg,
            }
            .into()),
            Err(e) => Err((actor, e).into()),
        }
    }

    async fn common_spawn<A>(
        &mut self,
        InitData {
            mut actor,
            mut cx,
            mut data,
            abort_reg: abort_registration,
        }: InitData<A>,
    ) -> Act<A>
    where
        A: 'static + Actor + Send + Sync,
    {
        let handle = cx.handle().clone();
        let child_task = tokio::spawn(async move {
            let res = Abortable::new(
                AssertUnwindSafe(async {
                    // Call handle events until shutdown
                    let mut res = actor.run(&mut cx, &mut data).await;
                    if let Err(e) = actor.shutdown(&mut cx, &mut data).await {
                        res = Err(e);
                    }
                    res
                })
                .catch_unwind(),
                abort_registration,
            )
            .await;
            Self::handle_run_res(res, &mut cx, actor).await
        });
        self.join_handles.push(child_task);
        handle
    }

    async fn common_init_and_spawn<A>(
        &mut self,
        cx: A::Context,
        actor: A,
        abort_reg: AbortRegistration,
    ) -> Result<Act<A>, InitError<A>>
    where
        A: 'static + Actor + Send + Sync,
    {
        let init = self.common_init(cx, actor, abort_reg).await?;
        Ok(init.spawn(self).await)
    }

    pub(crate) async fn handle_init_res<A>(
        res: std::thread::Result<Result<A::Data, ActorError>>,
        cx: &mut dyn ActorContext<A>,
    ) -> anyhow::Result<A::Data>
    where
        A: Actor,
    {
        match res {
            Ok(res) => match res {
                Ok(d) => Ok(d),
                Err(e) => {
                    cx.abort().await;
                    cx.join().await;
                    anyhow::bail!(e)
                }
            },
            Err(e) => {
                cx.abort().await;
                cx.join().await;
                std::panic::resume_unwind(e);
            }
        }
    }

    pub(crate) async fn handle_run_res<A>(
        res: Result<std::thread::Result<Result<(), ActorError>>, Aborted>,
        cx: &mut dyn ActorContext<A>,
        actor: A,
    ) -> anyhow::Result<()>
    where
        A: 'static + Actor + Send + Sync,
    {
        let service = cx.service().await;
        cx.abort().await;
        cx.join().await;
        match res {
            Ok(res) => match res {
                Ok(res) => match res {
                    Ok(_) => cx.report_exit(SuccessReport::new(actor, service)).await,
                    Err(e) => {
                        log::error!("{} exited with error: {}", actor.name(), e);
                        cx.report_exit(ErrorReport::new(actor, service, e)).await
                    }
                },
                Err(e) => {
                    cx.report_exit(ErrorReport::new(
                        actor,
                        service,
                        anyhow::anyhow!("Actor panicked!").into(),
                    ))
                    .await
                    .ok();
                    std::panic::resume_unwind(e);
                }
            },
            Err(_) => cx
                .report_exit(SuccessReport::new(actor, service))
                .await
                .map_err(|_| anyhow::anyhow!("Aborted!")),
        }
    }

    /// Spawn a new pool of actors of a given type with some metric
    pub async fn new_pool_with<P: ActorPool, Sup, H, F>(&mut self, supervisor_handle: H, f: F) -> anyhow::Result<()>
    where
        P: 'static + Send + Sync,
        P::Actor: Actor + Send + Sync,
        H: 'static + Clone + EnvelopeSender<Sup>,
        Sup: 'static + Supervisor<P::Actor>,
        for<'b> F: 'static + Send + FnOnce(&'b mut ScopedActorPool<P, Sup, H>) -> BoxFuture<'b, anyhow::Result<()>>,
    {
        f(&mut self.new_pool(supervisor_handle).await).await
    }

    /// Spawn a new pool of actors of a given type with some metric
    pub async fn spawn_pool_with<P: ActorPool, Sup, H, F>(&mut self, supervisor_handle: H, f: F) -> anyhow::Result<()>
    where
        P: 'static + Send + Sync,
        P::Actor: Actor + Send + Sync,
        H: 'static + Clone + EnvelopeSender<Sup>,
        Sup: 'static + Supervisor<P::Actor>,
        for<'b> F: 'static + Send + FnOnce(&'b mut ScopedActorPool<P, Sup, H>) -> BoxFuture<'b, anyhow::Result<()>>,
    {
        f(&mut self.spawn_pool(supervisor_handle).await).await
    }

    /// Spawn a new pool of actors of a given type with some metric
    pub async fn new_pool<P: ActorPool, Sup, H>(&mut self, supervisor_handle: H) -> ScopedActorPool<'_, P, Sup, H>
    where
        P: 'static + Send + Sync,
        P::Actor: Actor + Send + Sync,
        H: 'static + Clone + EnvelopeSender<Sup>,
        Sup: 'static + Supervisor<P::Actor>,
    {
        let pool = Pool::<P>::default();
        self.add_data(pool.clone()).await;

        let scoped_pool = ScopedActorPool {
            scope: self,
            pool,
            supervisor_handle,
            initialized: Default::default(),
            _sup: PhantomData,
        };
        scoped_pool
    }

    /// Spawn a new pool of actors of a given type with some metric
    pub async fn spawn_pool<P: ActorPool, Sup, H>(&mut self, supervisor_handle: H) -> ScopedActorPool<'_, P, Sup, H>
    where
        P: 'static + Send + Sync,
        P::Actor: Actor + Send + Sync,
        H: 'static + Clone + EnvelopeSender<Sup>,
        Sup: 'static + Supervisor<P::Actor>,
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
            _sup: PhantomData,
        };
        scoped_pool
    }

    /// Initialize an actor into a pool
    pub async fn init_into_pool<P: ActorPool, Sup, H>(
        &mut self,
        supervisor_handle: H,
        actor: P::Actor,
    ) -> Result<Initialized<P::Actor>, InitError<P::Actor>>
    where
        P: 'static + BasicActorPool + Send + Sync,
        P::Actor: Actor + Send + Sync,
        <P::Actor as Actor>::Context: TryFrom<AnyContext<P::Actor, Sup, H>>,
        <<P::Actor as Actor>::Context as TryFrom<AnyContext<P::Actor, Sup, H>>>::Error: Into<anyhow::Error>,
        H: 'static + Clone + EnvelopeSender<Sup>,
        Sup: 'static + Supervisor<P::Actor>,
    {
        let (abort_handle, abort_reg) = AbortHandle::new_pair();
        let cx = match self
            .create_context(&actor, Some(supervisor_handle.clone()), Some(abort_handle))
            .await
        {
            Ok(cx) => cx,
            Err(e) => {
                return Err((actor, e).into());
            }
        };
        let pool = self.spawn_pool::<P, Sup, _>(supervisor_handle.clone()).await;
        let init = pool.scope.common_init(cx, actor, abort_reg).await?;
        let handle = init.data.as_ref().unwrap().cx.handle().clone();
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
        P::Actor: Actor + Send + Sync,
        <P::Actor as Actor>::Context: TryFrom<AnyContext<P::Actor, Sup, H>>,
        <<P::Actor as Actor>::Context as TryFrom<AnyContext<P::Actor, Sup, H>>>::Error: Into<anyhow::Error>,
        H: 'static + Clone + EnvelopeSender<Sup>,
        Sup: 'static + Supervisor<P::Actor>,
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
    ) -> Result<Initialized<P::Actor>, InitError<P::Actor>>
    where
        P: 'static + KeyedActorPool + Send + Sync,
        P::Actor: Actor + Send + Sync,
        <P::Actor as Actor>::Context: TryFrom<AnyContext<P::Actor, Sup, H>>,
        <<P::Actor as Actor>::Context as TryFrom<AnyContext<P::Actor, Sup, H>>>::Error: Into<anyhow::Error>,
        H: 'static + Clone + EnvelopeSender<Sup>,
        Sup: 'static + Supervisor<P::Actor>,
    {
        let (abort_handle, abort_reg) = AbortHandle::new_pair();
        let cx = match self
            .create_context(&actor, Some(supervisor_handle.clone()), Some(abort_handle))
            .await
        {
            Ok(cx) => cx,
            Err(e) => {
                return Err((actor, e).into());
            }
        };
        let pool = self.spawn_pool::<P, Sup, _>(supervisor_handle).await;
        let init = pool.scope.common_init(cx, actor, abort_reg).await?;
        let handle = init.data.as_ref().unwrap().cx.handle().clone();
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
        P::Actor: Actor + Send + Sync,
        <P::Actor as Actor>::Context: TryFrom<AnyContext<P::Actor, Sup, H>>,
        <<P::Actor as Actor>::Context as TryFrom<AnyContext<P::Actor, Sup, H>>>::Error: Into<anyhow::Error>,
        H: 'static + Clone + EnvelopeSender<Sup>,
        Sup: 'static + Supervisor<P::Actor>,
    {
        let mut pool = self.spawn_pool::<P, Sup, _>(supervisor_handle).await;
        pool.spawn_keyed(key, actor).await
    }
}

/// Data used to spawn an actor after initializing it
#[derive(Debug)]
pub struct InitData<A>
where
    A: Actor,
{
    actor: A,
    data: A::Data,
    cx: A::Context,
    abort_reg: AbortRegistration,
}

/// A handle to an initialized actor. If unused, this will cleanup and abort the scope when dropped!
#[must_use = "An unused, initialized actor may cause unintended behavior!"]
#[derive(Debug)]
pub struct Initialized<A>
where
    A: 'static + Actor + Send,
{
    data: Option<InitData<A>>,
}

impl<A> Initialized<A>
where
    A: 'static + Actor + Send + Sync,
{
    /// Spawn the initialized actor with the given scope.
    /// This must be the same scope as the one used to init the actor!
    pub async fn spawn(mut self, rt: &mut RuntimeScope) -> Act<A> {
        rt.common_spawn(std::mem::take(&mut self.data).unwrap()).await
    }
}

impl<A> From<InitData<A>> for Initialized<A>
where
    A: 'static + Actor + Send,
{
    fn from(data: InitData<A>) -> Self {
        Self { data: Some(data) }
    }
}

impl<A> Drop for Initialized<A>
where
    A: 'static + Actor + Send,
{
    fn drop(&mut self) {
        if let Some(data) = std::mem::take(&mut self.data) {
            tokio::spawn(async move {
                data.cx.abort().await;
            });
        }
    }
}

/// An Actor's context
#[async_trait]
pub trait ActorContext<A: Actor>: std::fmt::Debug + Send
where
    Self: DerefMut<Target = RuntimeScope>,
{
    /// Initialize a new actor with a supervisor handle
    async fn init_actor<OtherA>(&mut self, actor: OtherA) -> Result<Initialized<OtherA>, InitError<OtherA>>
    where
        OtherA: 'static + Actor + Send + Sync,
        OtherA::Context: TryFrom<AnyContext<OtherA, A, Act<A>>>,
        <OtherA::Context as TryFrom<AnyContext<OtherA, A, Act<A>>>>::Error: Into<anyhow::Error>,
        A: 'static + Supervisor<OtherA> + Send,
        Self: Sized,
    {
        let handle = self.handle().clone();
        self.deref_mut().init_actor(actor, handle).await
    }

    /// Spawn a new actor with a supervisor handle
    async fn spawn_actor<OtherA>(&mut self, actor: OtherA) -> Result<Act<OtherA>, InitError<OtherA>>
    where
        OtherA: 'static + Actor + Send + Sync,
        OtherA::Context: TryFrom<AnyContext<OtherA, A, Act<A>>>,
        <OtherA::Context as TryFrom<AnyContext<OtherA, A, Act<A>>>>::Error: Into<anyhow::Error>,
        Self: Sized,
        A: 'static + Supervisor<OtherA> + Send,
    {
        let handle = self.handle().clone();
        self.deref_mut().spawn_actor::<_, A, _>(actor, handle).await
    }

    /// Spawn a new system with a supervisor handle
    async fn init_system<OtherA>(
        &mut self,
        actor: OtherA,
        state: OtherA::State,
    ) -> Result<Initialized<OtherA>, InitError<OtherA>>
    where
        OtherA: 'static + System + Send + Sync,
        OtherA::Context: TryFrom<AnyContext<OtherA, A, Act<A>>>,
        <OtherA::Context as TryFrom<AnyContext<OtherA, A, Act<A>>>>::Error: Into<anyhow::Error>,
        A: 'static + Supervisor<OtherA> + Send,
        Self: Sized,
    {
        let handle = self.handle().clone();
        self.deref_mut().init_system(actor, state, handle).await
    }

    /// Spawn a new system with a supervisor handle
    async fn spawn_system<OtherA>(
        &mut self,
        actor: OtherA,
        state: OtherA::State,
    ) -> Result<Act<OtherA>, InitError<OtherA>>
    where
        OtherA: 'static + System + Send + Sync,
        OtherA::Context: TryFrom<AnyContext<OtherA, A, Act<A>>>,
        <OtherA::Context as TryFrom<AnyContext<OtherA, A, Act<A>>>>::Error: Into<anyhow::Error>,
        Self: Sized,
        A: 'static + Supervisor<OtherA>,
    {
        let handle = self.handle().clone();
        self.deref_mut().spawn_system::<_, A, _>(actor, state, handle).await
    }

    /// Spawn a new pool of actors of a given type with some metric
    async fn new_pool_with<P: ActorPool, F>(&mut self, f: F) -> anyhow::Result<()>
    where
        P: 'static + Send + Sync,
        P::Actor: Actor + Send + Sync,
        A: 'static + Supervisor<P::Actor> + Send,
        for<'b> F: 'static + Send + FnOnce(&'b mut ScopedActorPool<P, A, Act<A>>) -> BoxFuture<'b, anyhow::Result<()>>,
        Self: Sized,
    {
        let handle = self.handle().clone();
        self.deref_mut().new_pool_with::<P, A, _, F>(handle, f).await
    }

    /// Spawn a new pool of actors of a given type with some metric
    async fn spawn_pool_with<P: ActorPool, F>(&mut self, f: F) -> anyhow::Result<()>
    where
        P: 'static + Send + Sync,
        P::Actor: Actor + Send + Sync,
        A: 'static + Supervisor<P::Actor> + Send,
        for<'b> F: 'static + Send + FnOnce(&'b mut ScopedActorPool<P, A, Act<A>>) -> BoxFuture<'b, anyhow::Result<()>>,
        Self: Sized,
    {
        let handle = self.handle().clone();
        self.deref_mut().spawn_pool_with::<P, A, _, F>(handle, f).await
    }

    /// Spawn a new pool of actors of a given type with some metric
    async fn new_pool<P: ActorPool>(&mut self) -> ScopedActorPool<'_, P, A, Act<A>>
    where
        A: 'static + Supervisor<P::Actor> + Send,
        P: 'static + Send + Sync,
        P::Actor: Actor + Send + Sync,
        Self: Sized,
        A: Supervisor<P::Actor>,
    {
        let handle = self.handle().clone();
        self.deref_mut().new_pool::<P, A, _>(handle).await
    }

    /// Spawn a new pool of actors of a given type with some metric
    async fn spawn_pool<P: ActorPool>(&mut self) -> ScopedActorPool<'_, P, A, Act<A>>
    where
        A: 'static + Supervisor<P::Actor> + Send,
        P: 'static + Send + Sync,
        P::Actor: Actor + Send + Sync,
        Self: Sized,
        A: Supervisor<P::Actor>,
    {
        let handle = self.handle().clone();
        self.deref_mut().spawn_pool::<P, A, _>(handle).await
    }

    /// Initialize an actor into a pool
    async fn init_into_pool<P: ActorPool>(
        &mut self,
        actor: P::Actor,
    ) -> Result<Initialized<P::Actor>, InitError<P::Actor>>
    where
        P: 'static + BasicActorPool + Send + Sync,
        P::Actor: Actor + Send + Sync,
        <P::Actor as Actor>::Context: TryFrom<AnyContext<P::Actor, A, Act<A>>>,
        <<P::Actor as Actor>::Context as TryFrom<AnyContext<P::Actor, A, Act<A>>>>::Error: Into<anyhow::Error>,
        Self: Sized,
        A: 'static + Supervisor<P::Actor> + Send,
    {
        let supervisor_handle = self.handle().clone();
        let (abort_handle, abort_reg) = AbortHandle::new_pair();
        let cx = match self
            .create_context(&actor, Some(supervisor_handle.clone()), Some(abort_handle))
            .await
        {
            Ok(cx) => cx,
            Err(e) => {
                return Err((actor, e).into());
            }
        };
        let pool = self.spawn_pool::<P>().await;
        let init = pool.scope.common_init(cx, actor, abort_reg).await?;
        let handle = init.data.as_ref().unwrap().cx.handle().clone();
        pool.pool.push(handle).await;
        Ok(init)
    }

    /// Spawn an actor into a pool
    async fn spawn_into_pool<P: ActorPool>(&mut self, actor: P::Actor) -> Result<Act<P::Actor>, InitError<P::Actor>>
    where
        P: 'static + BasicActorPool + Send + Sync,
        P::Actor: Actor + Send + Sync,
        <P::Actor as Actor>::Context: TryFrom<AnyContext<P::Actor, A, Act<A>>>,
        <<P::Actor as Actor>::Context as TryFrom<AnyContext<P::Actor, A, Act<A>>>>::Error: Into<anyhow::Error>,
        Self: Sized,
        A: 'static + Supervisor<P::Actor>,
    {
        let handle = self.handle().clone();
        self.deref_mut().spawn_into_pool::<P, A, _>(handle, actor).await
    }

    /// Initialize an actor into a pool
    async fn init_into_pool_keyed<P: ActorPool>(
        &mut self,
        key: P::Key,
        actor: P::Actor,
    ) -> Result<Initialized<P::Actor>, InitError<P::Actor>>
    where
        P: 'static + KeyedActorPool + Send + Sync,
        P::Actor: Actor + Send + Sync,
        <P::Actor as Actor>::Context: TryFrom<AnyContext<P::Actor, A, Act<A>>>,
        <<P::Actor as Actor>::Context as TryFrom<AnyContext<P::Actor, A, Act<A>>>>::Error: Into<anyhow::Error>,
        Self: Sized,
        A: 'static + Supervisor<P::Actor> + Send,
    {
        let supervisor_handle = self.handle().clone();
        let (abort_handle, abort_reg) = AbortHandle::new_pair();
        let cx = match self
            .create_context(&actor, Some(supervisor_handle.clone()), Some(abort_handle))
            .await
        {
            Ok(cx) => cx,
            Err(e) => {
                return Err((actor, e).into());
            }
        };
        let pool = self.spawn_pool::<P>().await;
        let init = pool.scope.common_init(cx, actor, abort_reg).await?;
        let handle = init.data.as_ref().unwrap().cx.handle().clone();
        pool.pool.push(key, handle).await;
        Ok(init)
    }

    /// Spawn an actor into a keyed pool
    async fn spawn_into_pool_keyed<P: ActorPool>(
        &mut self,
        key: P::Key,
        actor: P::Actor,
    ) -> Result<Act<P::Actor>, InitError<P::Actor>>
    where
        P: 'static + KeyedActorPool + Send + Sync,
        P::Actor: Actor + Send + Sync,
        <P::Actor as Actor>::Context: TryFrom<AnyContext<P::Actor, A, Act<A>>>,
        <<P::Actor as Actor>::Context as TryFrom<AnyContext<P::Actor, A, Act<A>>>>::Error: Into<anyhow::Error>,
        Self: Sized,
        A: 'static + Supervisor<P::Actor>,
    {
        let handle = self.handle().clone();
        self.deref_mut()
            .spawn_into_pool_keyed::<P, A, _>(handle, key, actor)
            .await
    }

    /// Get this actors's handle
    fn handle(&self) -> &Act<A>
    where
        Act<A>: Clone;

    /// Get the inbox
    fn inbox(&mut self) -> &mut dyn Receiver<Envelope<A>>;

    /// Shutdown the actor
    async fn shutdown(&self)
    where
        A: 'static,
        Self: Sized,
    {
        self.handle().shutdown().await;
    }

    /// Get the runtime's service
    async fn service(&mut self) -> ServiceView;

    /// Update this scope's service status
    async fn update_status<S: Status + Send>(&mut self, status: S)
    where
        Self: Sized;

    /// Update this scope's service status
    async fn report_exit(&mut self, report: Report<A>) -> anyhow::Result<()>;
}

/// A supervisor which is defined as anything which can handle the relevant events, status changes and exit reports
pub trait Supervisor<A: Debug + Send + Sync>: HandleEvent<StatusChange<A>> + HandleEvent<Report<A>> {}
impl<Sup, A: Debug + Send + Sync> Supervisor<A> for Sup where Sup: HandleEvent<StatusChange<A>> + HandleEvent<Report<A>> {}

/// A supervised context
#[derive(Debug)]
pub struct SupervisedContext<A, Sup, H>
where
    A: 'static + Actor,
{
    pub(crate) scope: RuntimeScope,
    pub(crate) handle: Act<A>,
    pub(crate) receiver: Box<dyn Receiver<Envelope<A>>>,
    pub(crate) supervisor_handle: H,
    _sup: PhantomData<fn(Sup) -> Sup>,
}

impl<A, Sup, H> SupervisedContext<A, Sup, H>
where
    A: 'static + Actor,
{
    pub(crate) async fn new<N: FnOnce(ScopeId) -> String>(
        actor: &A,
        name: N,
        parent: &RuntimeScope,
        supervisor_handle: H,
        abort_handle: Option<AbortHandle>,
    ) -> anyhow::Result<Self> {
        let (sender, receiver) = UnboundedTokioChannel::<Envelope<A>>::new(actor).await?;
        let (receiver, shutdown_handle) = ShutdownStream::new(receiver);
        let scope = parent
            .child(name, Some(shutdown_handle), abort_handle, Some(A::PATH))
            .await;
        Ok(Self {
            handle: Act::new(scope.clone(), Box::new(sender)),
            scope,
            receiver: Box::new(receiver),
            supervisor_handle,
            _sup: PhantomData,
        })
    }
}

impl<A, Sup, H> SupervisedContext<A, Sup, H>
where
    A: 'static + Actor + Send + Sync,
{
    /// Get this actor's supervisor handle
    pub fn supervisor_handle(&self) -> &H {
        &self.supervisor_handle
    }
}

impl<A, Sup, H> TryFrom<AnyContext<A, Sup, H>> for SupervisedContext<A, Sup, H>
where
    A: 'static + Actor,
{
    type Error = anyhow::Error;

    fn try_from(value: AnyContext<A, Sup, H>) -> Result<Self, Self::Error> {
        match value {
            AnyContext::Supervised(s) => Ok(s),
            AnyContext::Unsupervised(_) => anyhow::bail!("Cannot convert Unsupervised type to Supervised!"),
        }
    }
}

#[async_trait]
impl<A, Sup, H: Debug> ActorContext<A> for SupervisedContext<A, Sup, H>
where
    A: 'static + Actor + Send + Sync,
    H: 'static + EnvelopeSender<Sup>,
    Sup: 'static + Supervisor<A>,
{
    /// Get this actors's handle
    fn handle(&self) -> &Act<A>
    where
        Act<A>: Clone,
    {
        &self.handle
    }

    /// Get the runtime's service
    async fn service(&mut self) -> ServiceView {
        self.scope.service().await
    }

    /// Update this scope's service status
    async fn update_status<S: Status + Send>(&mut self, status: S) {
        if !self.supervisor_handle.is_closed() {
            let mut service = self.service().await;
            let prev_status = service.status().clone();
            service.update_status(CustomStatus(status.clone()));
            self.supervisor_handle
                .send(StatusChange::<A>::new(prev_status, service))
                .ok();
        }
        self.scope.update_status(status).await
    }

    fn inbox(&mut self) -> &mut dyn Receiver<Envelope<A>> {
        &mut self.receiver
    }

    async fn report_exit(&mut self, report: Report<A>) -> anyhow::Result<()> {
        self.supervisor_handle.send(report).ok();
        Ok(())
    }
}

impl<A, Sup, H> Deref for SupervisedContext<A, Sup, H>
where
    A: Actor,
{
    type Target = RuntimeScope;

    fn deref(&self) -> &Self::Target {
        &self.scope
    }
}

impl<A, Sup, H> DerefMut for SupervisedContext<A, Sup, H>
where
    A: Actor,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.scope
    }
}

/// An unsupervised actor context
#[derive(Debug)]
pub struct UnsupervisedContext<A>
where
    A: 'static + Actor,
{
    pub(crate) scope: RuntimeScope,
    pub(crate) handle: Act<A>,
    pub(crate) receiver: Box<dyn Receiver<Envelope<A>>>,
}

impl<A> UnsupervisedContext<A>
where
    A: 'static + Actor + Send + Sync,
{
    pub(crate) async fn new<N: FnOnce(ScopeId) -> String>(
        actor: &A,
        name: N,
        parent: &RuntimeScope,
        abort_handle: Option<AbortHandle>,
    ) -> anyhow::Result<Self> {
        let (sender, receiver) = UnboundedTokioChannel::<Envelope<A>>::new(actor).await?;
        let (receiver, shutdown_handle) = ShutdownStream::new(receiver);
        let scope = parent
            .child(name, Some(shutdown_handle), abort_handle, Some(A::PATH))
            .await;
        Ok(Self {
            handle: Act::new(scope.clone(), Box::new(sender)),
            scope,
            receiver: Box::new(receiver),
        })
    }
}

impl<A, Sup, H> From<AnyContext<A, Sup, H>> for UnsupervisedContext<A>
where
    A: 'static + Actor + Send + Sync,
{
    fn from(value: AnyContext<A, Sup, H>) -> Self {
        match value {
            AnyContext::Supervised(s) => s.into(),
            AnyContext::Unsupervised(u) => u,
        }
    }
}

impl<A, Sup, H> From<SupervisedContext<A, Sup, H>> for UnsupervisedContext<A>
where
    A: 'static + Actor + Send + Sync,
{
    fn from(s: SupervisedContext<A, Sup, H>) -> Self {
        UnsupervisedContext {
            scope: s.scope,
            handle: s.handle,
            receiver: s.receiver,
        }
    }
}

#[async_trait]
impl<A> ActorContext<A> for UnsupervisedContext<A>
where
    A: 'static + Actor + Send + Sync,
{
    /// Get this actors's handle
    fn handle(&self) -> &Act<A>
    where
        Act<A>: Clone,
    {
        &self.handle
    }

    /// Get the runtime's service
    async fn service(&mut self) -> ServiceView {
        self.scope.service().await
    }

    /// Update this scope's service status
    async fn update_status<S: Status + Send>(&mut self, status: S) {
        self.scope.update_status(status).await
    }

    fn inbox(&mut self) -> &mut dyn Receiver<Envelope<A>> {
        &mut self.receiver
    }

    async fn report_exit(&mut self, _report: Report<A>) -> anyhow::Result<()> {
        Ok(())
    }
}

impl<A> Deref for UnsupervisedContext<A>
where
    A: Actor,
{
    type Target = RuntimeScope;

    fn deref(&self) -> &Self::Target {
        &self.scope
    }
}

impl<A> DerefMut for UnsupervisedContext<A>
where
    A: Actor,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.scope
    }
}

#[allow(missing_docs)]
#[derive(Debug)]
pub enum AnyContext<A, Sup, H>
where
    A: 'static + Actor,
{
    Supervised(SupervisedContext<A, Sup, H>),
    Unsupervised(UnsupervisedContext<A>),
}

impl<A, Sup, H> AnyContext<A, Sup, H>
where
    A: 'static + Actor + Send + Sync,
    H: 'static,
    Sup: 'static,
{
    pub(crate) async fn new<N: FnOnce(ScopeId) -> String>(
        actor: &A,
        name: N,
        parent: &RuntimeScope,
        supervisor_handle: Option<H>,
        abort_handle: Option<AbortHandle>,
    ) -> anyhow::Result<Self> {
        Ok(match supervisor_handle {
            Some(supervisor_handle) => AnyContext::Supervised(
                SupervisedContext::new(actor, name, parent, supervisor_handle, abort_handle).await?,
            ),
            None => AnyContext::Unsupervised(UnsupervisedContext::new(actor, name, parent, abort_handle).await?),
        })
    }

    /// Get this actor's supervisor handle
    pub fn supervisor_handle(&self) -> Option<&H> {
        match self {
            AnyContext::Supervised(s) => Some(s.supervisor_handle()),
            AnyContext::Unsupervised(_) => None,
        }
    }
}

#[async_trait]
impl<A, Sup, H: Debug> ActorContext<A> for AnyContext<A, Sup, H>
where
    A: 'static + Actor + Send + Sync,
    H: 'static + EnvelopeSender<Sup>,
    Sup: 'static + Supervisor<A>,
{
    /// Get this actors's handle
    fn handle(&self) -> &Act<A>
    where
        Act<A>: Clone,
    {
        match self {
            AnyContext::Supervised(s) => s.handle(),
            AnyContext::Unsupervised(u) => u.handle(),
        }
    }

    /// Get the runtime's service
    async fn service(&mut self) -> ServiceView {
        match self {
            AnyContext::Supervised(s) => s.service(),
            AnyContext::Unsupervised(u) => u.service(),
        }
        .await
    }

    /// Update this scope's service status
    async fn update_status<S: Status + Send>(&mut self, status: S) {
        match self {
            AnyContext::Supervised(s) => s.update_status(status).await,
            AnyContext::Unsupervised(u) => u.scope.update_status(status).await,
        }
    }

    fn inbox(&mut self) -> &mut dyn Receiver<Envelope<A>> {
        match self {
            AnyContext::Supervised(s) => s.inbox(),
            AnyContext::Unsupervised(u) => u.inbox(),
        }
    }

    async fn report_exit(&mut self, report: Report<A>) -> anyhow::Result<()> {
        match self {
            AnyContext::Supervised(s) => s.report_exit(report),
            AnyContext::Unsupervised(u) => u.report_exit(report),
        }
        .await
    }
}

impl<A, Sup, H> From<SupervisedContext<A, Sup, H>> for AnyContext<A, Sup, H>
where
    A: 'static + Actor + Send + Sync,
{
    fn from(s: SupervisedContext<A, Sup, H>) -> Self {
        AnyContext::Supervised(s)
    }
}

impl<A, Sup, H> From<UnsupervisedContext<A>> for AnyContext<A, Sup, H>
where
    A: 'static + Actor + Send + Sync,
{
    fn from(u: UnsupervisedContext<A>) -> Self {
        AnyContext::Unsupervised(u)
    }
}

impl<A, Sup, H> Deref for AnyContext<A, Sup, H>
where
    A: Actor,
{
    type Target = RuntimeScope;

    fn deref(&self) -> &Self::Target {
        match self {
            AnyContext::Supervised(s) => s.deref(),
            AnyContext::Unsupervised(u) => u.deref(),
        }
    }
}

impl<A, Sup, H> DerefMut for AnyContext<A, Sup, H>
where
    A: Actor,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            AnyContext::Supervised(s) => s.deref_mut(),
            AnyContext::Unsupervised(u) => u.deref_mut(),
        }
    }
}
/// A scope for an actor pool, which only allows spawning of the specified actor
#[derive(Debug)]
pub struct ScopedActorPool<'a, P, Sup, H>
where
    P: ActorPool,
    P::Actor: 'static + Actor + Send + Sync,
    H: 'static + EnvelopeSender<Sup> + Clone,
    Sup: 'static + Supervisor<P::Actor>,
{
    scope: &'a mut RuntimeScope,
    pool: Pool<P>,
    supervisor_handle: H,
    initialized: Vec<Initialized<P::Actor>>,
    _sup: PhantomData<fn(Sup) -> Sup>,
}

impl<'a, P, Sup, H> ScopedActorPool<'a, P, Sup, H>
where
    P: BasicActorPool,
    P::Actor: Actor + Send + Sync,
    <P::Actor as Actor>::Context: TryFrom<AnyContext<P::Actor, Sup, H>>,
    <<P::Actor as Actor>::Context as TryFrom<AnyContext<P::Actor, Sup, H>>>::Error: Into<anyhow::Error>,
    H: 'static + EnvelopeSender<Sup> + Clone,
    Sup: 'static + Supervisor<P::Actor>,
{
    /// Spawn a new actor into this pool
    pub async fn spawn(&mut self, actor: P::Actor) -> Result<Act<P::Actor>, InitError<P::Actor>>
    where
        P::Actor: 'static + Send + Sync,
    {
        let supervisor_handle = self.supervisor_handle.clone();
        let (abort_handle, abort_reg) = AbortHandle::new_pair();
        let cx = match self
            .scope
            .create_context(&actor, Some(supervisor_handle), Some(abort_handle))
            .await
        {
            Ok(cx) => cx,
            Err(e) => {
                return Err((actor, e).into());
            }
        };
        let handle = self.scope.common_init_and_spawn(cx, actor, abort_reg).await?;
        self.pool.push(handle.clone()).await;
        Ok(handle)
    }

    /// Initialize actor for this pool
    pub async fn init(&mut self, actor: P::Actor) -> Result<(), InitError<P::Actor>> {
        let supervisor_handle = self.supervisor_handle.clone();
        let (abort_handle, abort_reg) = AbortHandle::new_pair();
        let cx = match self
            .scope
            .create_context(&actor, Some(supervisor_handle), Some(abort_handle))
            .await
        {
            Ok(cx) => cx,
            Err(e) => {
                return Err((actor, e).into());
            }
        };
        let init = self.scope.common_init(cx, actor, abort_reg).await?;
        let handle = init.data.as_ref().unwrap().cx.handle();
        self.pool.push(handle.clone()).await;
        self.initialized.push(init);
        Ok(())
    }
}

impl<'a, P, Sup, H> ScopedActorPool<'a, P, Sup, H>
where
    P: KeyedActorPool,
    P::Actor: Actor + Send + Sync,
    <P::Actor as Actor>::Context: TryFrom<AnyContext<P::Actor, Sup, H>>,
    <<P::Actor as Actor>::Context as TryFrom<AnyContext<P::Actor, Sup, H>>>::Error: Into<anyhow::Error>,
    H: 'static + EnvelopeSender<Sup> + Clone,
    Sup: 'static + Supervisor<P::Actor>,
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
                    "Attempted to add a duplicate metric to pool {} in scope {:x} ({})",
                    std::any::type_name::<Pool<P>>(),
                    self.scope.id().as_fields().0,
                    service.name()
                ),
            )
                .into());
        }
        let supervisor_handle = self.supervisor_handle.clone();
        let (abort_handle, abort_reg) = AbortHandle::new_pair();
        let cx = match self
            .scope
            .create_context(&actor, Some(supervisor_handle), Some(abort_handle))
            .await
        {
            Ok(cx) => cx,
            Err(e) => {
                return Err((actor, e).into());
            }
        };
        let handle = self.scope.common_init_and_spawn(cx, actor, abort_reg).await?;
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
                    "Attempted to add a duplicate metric to pool {} in scope {:x} ({})",
                    std::any::type_name::<Pool<P>>(),
                    self.scope.id().as_fields().0,
                    service.name()
                ),
            )
                .into());
        }
        let supervisor_handle = self.supervisor_handle.clone();
        let (abort_handle, abort_reg) = AbortHandle::new_pair();
        let cx = match self
            .scope
            .create_context(&actor, Some(supervisor_handle), Some(abort_handle))
            .await
        {
            Ok(cx) => cx,
            Err(e) => {
                return Err((actor, e).into());
            }
        };
        let init = self.scope.common_init(cx, actor, abort_reg).await?;
        let handle = init.data.as_ref().unwrap().cx.handle();
        self.pool.push(key, handle.clone()).await;
        self.initialized.push(init);
        Ok(())
    }
}

impl<'a, P, Sup, H> ScopedActorPool<'a, P, Sup, H>
where
    P: ActorPool,
    P::Actor: 'static + Actor + Send + Sync,
    H: 'static + EnvelopeSender<Sup> + Clone,
    Sup: 'static + Supervisor<P::Actor>,
{
    /// Finalize any initialized and unspawned actors in this pool by spawning them
    pub async fn spawn_all(&mut self) {
        for init in self.initialized.drain(..) {
            init.spawn(self.scope).await;
        }
    }
}
