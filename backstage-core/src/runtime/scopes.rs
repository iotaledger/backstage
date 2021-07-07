use super::*;
use crate::actor::{
    ActorError, ActorRequest, DepStatus, Dependencies, ErrorReport, EventDriven, Service, ServiceTree, ShutdownStream, StatusChange,
    SuccessReport, SupervisorEvent,
};
use futures::{future::Aborted, FutureExt};
use std::{hash::Hash, panic::AssertUnwindSafe};
use tokio::sync::broadcast;

/// A runtime which defines a particular scope and functionality to
/// create tasks within it.
pub struct RuntimeScope<Reg: RegistryAccess> {
    pub(crate) scope_id: ScopeId,
    pub(crate) parent_id: Option<ScopeId>,
    pub(crate) registry: Reg,
    pub(crate) join_handles: Vec<JoinHandle<anyhow::Result<()>>>,
}

impl<Reg: 'static + RegistryAccess + Send + Sync> RuntimeScope<Reg> {
    /// Get the scope id
    pub fn id(&self) -> ScopeId {
        self.scope_id
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
        scope.update_status(ServiceStatus::Stopping).await.ok();
        scope.join().await;
        res
    }

    pub(crate) async fn new<P: Into<Option<usize>> + Send, S: Into<String>, O: Into<Option<S>>>(
        mut registry: Reg,
        parent_scope_id: P,
        name: O,
        shutdown_handle: Option<oneshot::Sender<()>>,
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
            .await;
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
        shutdown_handle: Option<oneshot::Sender<()>>,
        abort_handle: Option<AbortHandle>,
    ) -> Self {
        // Self::new(self.registry.clone(), self.scope_id, name).await
        let name_opt = name.into().map(Into::into);
        let scope_id = self
            .registry
            .new_scope(
                self.scope_id,
                |id| name_opt.unwrap_or_else(|| format!("Scope {}", id)),
                shutdown_handle,
                abort_handle,
            )
            .await;
        Self {
            scope_id,
            parent_id: Some(self.scope_id),
            registry: self.registry.clone(),
            join_handles: Default::default(),
        }
    }

    pub(crate) async fn child_name_with<F: 'static + Send + Sync + FnOnce(ScopeId) -> String>(
        &mut self,
        name_fn: F,
        shutdown_handle: Option<oneshot::Sender<()>>,
        abort_handle: Option<AbortHandle>,
    ) -> Self {
        let scope_id = self.registry.new_scope(self.scope_id, name_fn, shutdown_handle, abort_handle).await;
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
        for<'b> F: Send + FnOnce(&'b mut RuntimeScope<Reg>) -> BoxFuture<'b, O>,
    {
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        let mut child_scope = self.child::<String, _>(None, None, Some(abort_handle)).await;
        child_scope.update_status(ServiceStatus::Running).await.ok();
        let res = Abortable::new(f(&mut child_scope), abort_registration).await;
        child_scope.update_status(ServiceStatus::Stopping).await.ok();
        child_scope.join().await;
        res.map_err(|_| anyhow::anyhow!("Aborted scope!"))
    }

    pub(crate) async fn add_data<T: 'static + Send + Sync + Clone>(&mut self, data: T) {
        log::debug!("Adding {} to scope {}", std::any::type_name::<T>(), self.scope_id);
        self.registry
            .add_data(&self.scope_id, data)
            .await
            .expect(&format!("Scope {} is missing...", self.scope_id))
    }

    pub(crate) async fn add_data_to_parent<T: 'static + Send + Sync + Clone>(&mut self, data: T) {
        if let Some(parent_id) = self.parent_id {
            log::debug!("Adding {} to parent scope {}", std::any::type_name::<T>(), parent_id);
            self.registry
                .add_data(&parent_id, data)
                .await
                .expect(&format!("Scope {} is missing...", parent_id))
        }
    }

    pub(crate) async fn depend_on<T: 'static + Send + Sync + Clone>(&mut self) {
        self.registry
            .depend_on::<T>(&self.scope_id)
            .await
            .expect(&format!("Scope {} is missing...", self.scope_id))
    }

    pub(crate) async fn get_data<T: 'static + Send + Sync + Clone>(&mut self) -> Option<T> {
        self.registry.get_data(&self.scope_id).await
    }

    /// Request a dependency and wait for it to be added, forming a link between this scope and
    /// the requested data. If the data is removed from this scope, it will be shut down.
    pub async fn link_data<T: 'static + Send + Sync + Dependencies>(&mut self) -> anyhow::Result<T> {
        let status = T::request(self).await;
        T::link(self).await;
        match status {
            DepStatus::Ready(t) => Ok(t),
            DepStatus::Waiting(mut recv) => {
                if let Err(_) = recv.recv().await {
                    anyhow::bail!("Failed to acquire dependencies for {}", std::any::type_name::<T>());
                }
                T::instantiate(self)
                    .await
                    .map_err(|e| anyhow::anyhow!("Cannot spawn {}: {}", std::any::type_name::<T>(), e))
            }
        }
    }

    pub(crate) async fn remove_data<T: 'static + Send + Sync + Clone>(&mut self) -> Option<T> {
        log::debug!("Removing {} from scope {}", std::any::type_name::<T>(), self.scope_id);
        self.registry.remove_data(&self.scope_id).await.ok().flatten()
    }

    pub(crate) async fn service(&mut self) -> Service {
        self.registry
            .get_service(&self.scope_id)
            .await
            .expect(&format!("Scope {} is missing...", self.scope_id))
    }

    /// Update this scope's service status
    pub async fn update_status(&mut self, status: ServiceStatus) -> anyhow::Result<()> {
        self.registry.update_status(&self.scope_id, status).await
    }

    /// Await the tasks in this runtime's scope
    pub(crate) async fn join(&mut self) {
        log::debug!("Joining scope {}", self.scope_id);
        for handle in self.join_handles.drain(..) {
            handle.await.ok();
        }
        self.registry.drop_scope(&self.scope_id).await;
    }

    /// Abort the tasks in this runtime's scope. This will shutdown tasks that have
    /// shutdown handles instead.
    async fn abort(&mut self)
    where
        Self: Sized,
    {
        self.registry.abort(&self.scope_id).await;
    }

    pub async fn print(&mut self) {
        self.registry.print(&self.scope_id).await;
    }

    pub async fn print_root(&mut self) {
        self.registry.print(&0).await;
    }

    pub async fn service_tree(&mut self) -> ServiceTree {
        self.registry
            .service_tree(&self.scope_id)
            .await
            .expect(&format!("Scope {} is missing...", self.scope_id))
    }

    /// Get the join handles of this runtime's scoped tasks
    pub(crate) fn join_handles(&self) -> &Vec<JoinHandle<anyhow::Result<()>>> {
        &self.join_handles
    }

    /// Mutably get the join handles of this runtime's scoped tasks
    pub(crate) fn join_handles_mut(&mut self) -> &mut Vec<JoinHandle<anyhow::Result<()>>> {
        &mut self.join_handles
    }

    /// Get an actor's event handle, if it exists in this scope.
    /// Note: This will only return a handle if the actor exists outside of a pool.
    pub async fn actor_event_handle<A: Actor>(&mut self) -> Option<Act<A>> {
        self.get_data::<<A::Channel as Channel<A::Event>>::Sender>()
            .await
            .and_then(|handle| (!handle.is_closed()).then(|| Act(handle)))
    }

    /// Send an event to a given actor, if it exists in this scope
    pub async fn send_actor_event<A: Actor>(&mut self, event: A::Event) -> anyhow::Result<()> {
        let mut handle = self
            .get_data::<<A::Channel as Channel<A::Event>>::Sender>()
            .await
            .and_then(|handle| (!handle.is_closed()).then(|| handle))
            .ok_or_else(|| anyhow::anyhow!("No channel for this actor!"))?;
        Sender::<A::Event>::send(&mut handle, event).await
    }

    /// Get a shared reference to a system if it exists in this runtime's scope
    pub async fn system<S: 'static + System + Send + Sync>(&mut self) -> Option<Sys<S>> {
        if let Some(actor) = self.get_data::<<S::Channel as Channel<S::Event>>::Sender>().await {
            if let Some(state) = self.get_data::<S::State>().await {
                return Some(Sys {
                    actor: Act(actor),
                    state: Res(state),
                });
            }
        }
        None
    }

    /// Get a shared resource if it exists in this runtime's scope
    pub async fn resource<R: 'static + Send + Sync + Clone>(&mut self) -> Option<Res<R>> {
        self.get_data::<R>().await.map(|res| Res(res))
    }

    /// Add a shared resource and get a reference to it
    pub async fn add_resource<R: 'static + Send + Sync + Clone>(&mut self, resource: R) -> Res<R> {
        self.add_data(resource.clone()).await;
        if let Some(broadcaster) = self
            .registry
            .get_data::<broadcast::Sender<PhantomData<Res<R>>>>(&self.scope_id)
            .await
        {
            log::debug!("Broadcasting creation of Res<{}>", std::any::type_name::<R>());
            broadcaster.send(PhantomData).ok();
        }
        Res(resource)
    }

    /// Get the pool of a specified actor if it exists in this runtime's scope
    pub async fn pool_with_metric<A, M>(&mut self) -> Option<Pool<A, M>>
    where
        Self: 'static + Sized,
        A: 'static + Actor + Send + Sync,
        M: 'static + Hash + Eq + Clone + Send + Sync,
    {
        match self.get_data::<Arc<RwLock<ActorPool<A, M>>>>().await {
            Some(arc) => {
                if arc.write().await.verify() {
                    Some(Pool(arc))
                } else {
                    self.remove_data::<Arc<RwLock<ActorPool<A, M>>>>().await;
                    None
                }
            }
            None => None,
        }
    }

    /// Get the pool of a specified actor if it exists in this runtime's scope
    pub async fn pool<A>(&mut self) -> Option<Pool<A, ()>>
    where
        Self: 'static + Sized,
        A: 'static + Actor + Send + Sync,
    {
        match self.get_data::<Arc<RwLock<ActorPool<A, ()>>>>().await {
            Some(arc) => {
                if arc.write().await.verify() {
                    Some(Pool(arc))
                } else {
                    self.remove_data::<Arc<RwLock<ActorPool<A, ()>>>>().await;
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
            .child_name_with(|scope_id| format!("Task {}", scope_id), None, Some(abort_handle.clone()))
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
        self.join_handles_mut().push(child_task);
        abort_handle
    }

    /// Spawn a new actor with a supervisor handle
    pub async fn spawn_actor<A, Sup, I>(&mut self, actor: A, supervisor_handle: I) -> anyhow::Result<(AbortHandle, Act<A>)>
    where
        A: 'static + Actor + Send + Sync + Into<<Sup::Event as SupervisorEvent>::ChildStates>,
        Sup: 'static + EventDriven,
        Sup::Event: SupervisorEvent,
        <Sup::Event as SupervisorEvent>::Children: From<PhantomData<A>>,
        I: Into<Option<Act<Sup>>>,
    {
        if self.actor_event_handle::<A>().await.is_some() {
            let service = self.service().await;
            panic!(
                "Attempted to add a duplicate actor ({}) to scope {} ({})",
                std::any::type_name::<A>(),
                self.scope_id,
                service.name
            );
        }
        let supervisor_handle = supervisor_handle.into();

        self.common_spawn::<_, _, Act<A>, _>(actor, supervisor_handle, true, |_| async {}.boxed())
            .await
    }

    /// Spawn a new actor with no supervisor
    pub async fn spawn_actor_unsupervised<A>(&mut self, actor: A) -> anyhow::Result<(AbortHandle, Act<A>)>
    where
        A: 'static + Actor + Send + Sync,
    {
        self.spawn_actor::<A, (), _>(actor, None).await
    }

    /// Spawn a new system with a supervisor handle
    pub async fn spawn_system<A, Sup, I>(
        &mut self,
        actor: A,
        state: A::State,
        supervisor_handle: I,
    ) -> anyhow::Result<(AbortHandle, Act<A>)>
    where
        A: 'static + System + Send + Sync + Into<<Sup::Event as SupervisorEvent>::ChildStates>,
        Sup: 'static + EventDriven,
        Sup::Event: SupervisorEvent,
        <Sup::Event as SupervisorEvent>::Children: From<PhantomData<A>>,
        I: Into<Option<Act<Sup>>>,
    {
        if self.actor_event_handle::<A>().await.is_some() {
            let service = self.service().await;
            panic!(
                "Attempted to add a duplicate actor ({}) to scope {} ({})",
                std::any::type_name::<A>(),
                self.scope_id,
                service.name
            );
        }
        let supervisor_handle = supervisor_handle.into();
        self.add_data(state).await;
        self.common_spawn::<_, _, Sys<A>, _>(actor, supervisor_handle, true, |scope| {
            async move {
                scope.remove_data::<A::State>().await;
            }
            .boxed()
        })
        .await
    }

    /// Spawn a new system with no supervisor
    pub async fn spawn_system_unsupervised<A>(&mut self, actor: A, state: A::State) -> anyhow::Result<(AbortHandle, Act<A>)>
    where
        A: 'static + System + Send + Sync,
    {
        self.spawn_system::<A, (), _>(actor, state, None).await
    }

    async fn common_spawn<A, Sup, B, F>(
        &mut self,
        mut actor: A,
        supervisor_handle: Option<Act<Sup>>,
        sender_in_parent: bool,
        cleanup_fn: F,
    ) -> anyhow::Result<(AbortHandle, Act<A>)>
    where
        A: 'static + Actor + Send + Sync + Into<<Sup::Event as SupervisorEvent>::ChildStates>,
        Sup: 'static + EventDriven,
        Sup::Event: SupervisorEvent,
        <Sup::Event as SupervisorEvent>::Children: From<PhantomData<A>>,
        B: 'static + Send + Sync,
        for<'b> F: 'static + Send + Sync + FnOnce(&'b mut RuntimeScope<Reg>) -> BoxFuture<'b, ()>,
    {
        log::debug!("Spawning {}", std::any::type_name::<A>());
        let (sender, receiver) = <A::Channel as Channel<A::Event>>::new();
        let (oneshot_send, oneshot_recv) = oneshot::channel::<()>();
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        let mut child_scope = self.child(A::name(), Some(oneshot_send), Some(abort_handle.clone())).await;
        if sender_in_parent {
            self.add_data(sender.clone()).await;
        } else {
            child_scope.add_data(sender.clone()).await;
        }
        let res = {
            let mut actor_rt = ActorInitRuntime::new(&mut child_scope, supervisor_handle.clone());
            AssertUnwindSafe(actor.init(&mut actor_rt)).catch_unwind().await
        };
        let mut actor = Self::handle_init_res(res, &mut child_scope, supervisor_handle.clone(), actor).await?;
        let dep_status = A::Dependencies::request(&mut child_scope).await;
        let child_task = tokio::spawn(async move {
            let deps = match dep_status {
                DepStatus::Ready(deps) => deps,
                DepStatus::Waiting(mut recv) => {
                    log::debug!(
                        "{} waiting for dependencies {}",
                        std::any::type_name::<A>(),
                        std::any::type_name::<A::Dependencies>()
                    );
                    if let Err(_) = recv.recv().await {
                        panic!("Failed to acquire dependencies for {}", std::any::type_name::<A>());
                    }
                    log::debug!("{} acquired dependencies!", std::any::type_name::<A>());
                    A::Dependencies::instantiate(&mut child_scope)
                        .await
                        .map_err(|e| anyhow::anyhow!("Cannot spawn {}: {}", std::any::type_name::<A>(), e))
                        .unwrap()
                }
            };
            A::Dependencies::link(&mut child_scope).await;
            if let Some(broadcaster) = child_scope
                .registry
                .get_data::<broadcast::Sender<PhantomData<B>>>(&child_scope.scope_id)
                .await
            {
                log::debug!("Broadcasting creation of {}", std::any::type_name::<B>());
                broadcaster.send(PhantomData).ok();
            }

            let res = {
                let mut actor_rt = ActorScopedRuntime::new(&mut child_scope, receiver, oneshot_recv, supervisor_handle.clone());
                Abortable::new(AssertUnwindSafe(actor.run(&mut actor_rt, deps)).catch_unwind(), abort_registration).await
            };
            cleanup_fn(&mut child_scope).await;
            child_scope.remove_data::<<A::Channel as Channel<A::Event>>::Sender>().await;
            Self::handle_run_res(res, &mut child_scope, supervisor_handle, actor).await
        });
        self.join_handles_mut().push(child_task);
        Ok((abort_handle, Act(sender)))
    }

    pub(crate) async fn handle_init_res<T, Sup>(
        res: std::thread::Result<Result<(), ActorError>>,
        child_scope: &mut RuntimeScope<Reg>,
        supervisor_handle: Option<Act<Sup>>,
        state: T,
    ) -> anyhow::Result<T>
    where
        Sup: EventDriven,
        Sup::Event: SupervisorEvent,
        T: Into<<Sup::Event as SupervisorEvent>::ChildStates>,
    {
        let service = child_scope.service().await;
        if let Some(mut supervisor) = supervisor_handle {
            match res.ok() {
                Some(res) => match res {
                    Ok(_) => Ok(state),
                    Err(e) => {
                        supervisor
                            .send(Sup::Event::report_err(ErrorReport::new(state.into(), service, e)))
                            .await
                            .ok();
                        child_scope.abort().await;
                        child_scope.join().await;
                        anyhow::bail!("Actor Error!");
                    }
                },
                None => {
                    supervisor
                        .send(Sup::Event::report_err(ErrorReport::new(
                            state.into(),
                            service,
                            ActorError::RuntimeError(ActorRequest::Restart),
                        )))
                        .await
                        .ok();
                    child_scope.abort().await;
                    child_scope.join().await;
                    anyhow::bail!("Panicked!");
                }
            }
        } else {
            match res {
                Ok(res) => match res {
                    Ok(_) => Ok(state),
                    Err(e) => {
                        child_scope.abort().await;
                        child_scope.join().await;
                        anyhow::bail!(e)
                    }
                },
                Err(_) => {
                    child_scope.abort().await;
                    child_scope.join().await;
                    anyhow::bail!("Panicked!")
                }
            }
        }
    }

    pub(crate) async fn handle_run_res<T, Sup>(
        res: Result<std::thread::Result<Result<(), ActorError>>, Aborted>,
        child_scope: &mut RuntimeScope<Reg>,
        supervisor_handle: Option<Act<Sup>>,
        state: T,
    ) -> anyhow::Result<()>
    where
        Sup: EventDriven,
        Sup::Event: SupervisorEvent,
        T: Into<<Sup::Event as SupervisorEvent>::ChildStates>,
    {
        let service = child_scope.service().await;
        child_scope.abort().await;
        child_scope.join().await;
        if let Some(mut supervisor) = supervisor_handle {
            match res.ok().map(Result::ok) {
                Some(res) => match res {
                    Some(res) => match res {
                        Ok(_) => {
                            supervisor
                                .send(Sup::Event::report_ok(SuccessReport::new(state.into(), service)))
                                .await
                        }

                        Err(e) => {
                            supervisor
                                .send(Sup::Event::report_err(ErrorReport::new(state.into(), service, e)))
                                .await
                        }
                    },
                    None => {
                        supervisor
                            .send(Sup::Event::report_err(ErrorReport::new(
                                state.into(),
                                service,
                                ActorError::RuntimeError(ActorRequest::Restart),
                            )))
                            .await
                    }
                },
                None => {
                    supervisor
                        .send(Sup::Event::report_ok(SuccessReport::new(state.into(), service)))
                        .await
                }
            }
        } else {
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
        }
    }

    /// Spawn a new pool of actors of a given type with some metric
    pub async fn spawn_pool<A, M, Sup, I, F>(&mut self, supervisor_handle: I, f: F) -> anyhow::Result<()>
    where
        A: 'static + Actor + Send + Sync,
        Sup: EventDriven,
        Sup::Event: SupervisorEvent,
        M: 'static + Hash + Clone + Eq + Send + Sync,
        I: Into<Option<Act<Sup>>>,
        for<'b> F: 'static + Send + FnOnce(&'b mut ScopedActorPool<A, M, Reg, Sup>) -> BoxFuture<'b, anyhow::Result<()>>,
    {
        if self.pool_with_metric::<A, M>().await.is_some() {
            let service = self.service().await;
            panic!(
                "Attempted to add a duplicate pool ({}) to scope {} ({})",
                std::any::type_name::<Pool<A, M>>(),
                self.scope_id,
                service.name
            );
        }
        let mut pool = ActorPool::default();
        let mut scoped_pool = ScopedActorPool {
            scope: self,
            pool: &mut pool,
            supervisor_handle: supervisor_handle.into(),
        };
        f(&mut scoped_pool).await?;
        self.add_data(Arc::new(RwLock::new(pool))).await;
        Ok(())
    }

    /// Spawn a new pool of actors of a given type with no metrics
    pub async fn spawn_basic_pool<A, Sup, I, F>(&mut self, supervisor_handle: I, f: F) -> anyhow::Result<()>
    where
        A: 'static + Actor + Send + Sync,
        Sup: EventDriven,
        Sup::Event: SupervisorEvent,
        I: Into<Option<Act<Sup>>>,
        for<'b> F: 'static + Send + FnOnce(&'b mut ScopedActorPool<A, (), Reg, Sup>) -> BoxFuture<'b, anyhow::Result<()>>,
    {
        self.spawn_pool(supervisor_handle, f).await
    }

    /// Spawn a new actor into a pool, creating a pool if needed
    pub async fn spawn_into_pool_with_metric<A, M, Sup, I>(
        &mut self,
        actor: A,
        metric: M,
        supervisor_handle: I,
    ) -> anyhow::Result<(AbortHandle, Act<A>)>
    where
        A: 'static + Actor + Send + Sync + Into<<Sup::Event as SupervisorEvent>::ChildStates>,
        Sup: 'static + EventDriven,
        Sup::Event: SupervisorEvent,
        <Sup::Event as SupervisorEvent>::Children: From<PhantomData<A>>,
        M: 'static + Hash + Eq + Clone + Send + Sync,
        I: Into<Option<Act<Sup>>>,
    {
        let pool = match self.pool_with_metric::<A, M>().await {
            Some(res) => res,
            None => {
                let pool = Arc::new(RwLock::new(ActorPool::<A, M>::default()));
                self.add_data(pool.clone()).await;
                Pool(pool)
            }
        };
        if pool.write().await.get_by_metric(&metric).is_some() {
            let service = self.service().await;
            panic!(
                "Attempted to add a duplicate metric to pool {} in scope {} ({})",
                std::any::type_name::<Pool<A, M>>(),
                self.scope_id,
                service.name
            );
        }
        let supervisor_handle = supervisor_handle.into();
        let (abort_handle, sender) = self
            .common_spawn::<_, _, Act<A>, _>(actor, supervisor_handle, false, |_| async move {}.boxed())
            .await?;
        pool.write().await.push(sender.clone(), metric);
        Ok((abort_handle, sender))
    }

    /// Spawn a new actor into a pool with a default metric, creating a pool if needed
    pub async fn spawn_into_pool_with_default_metric<A, M, Sup, I>(
        &mut self,
        actor: A,
        supervisor_handle: I,
    ) -> anyhow::Result<(AbortHandle, Act<A>)>
    where
        A: 'static + Actor + Send + Sync + Into<<Sup::Event as SupervisorEvent>::ChildStates>,
        Sup: 'static + EventDriven,
        Sup::Event: SupervisorEvent,
        <Sup::Event as SupervisorEvent>::Children: From<PhantomData<A>>,
        M: 'static + Hash + Eq + Clone + Default + Send + Sync,
        I: Into<Option<Act<Sup>>>,
    {
        self.spawn_into_pool_with_metric(actor, M::default(), supervisor_handle).await
    }

    /// Spawn a new actor into a pool with no metrics, creating a pool if needed
    pub async fn spawn_into_pool<A, Sup, I>(&mut self, actor: A, supervisor_handle: I) -> anyhow::Result<(AbortHandle, Act<A>)>
    where
        A: 'static + Actor + Send + Sync + Into<<Sup::Event as SupervisorEvent>::ChildStates>,
        Sup: 'static + EventDriven,
        Sup::Event: SupervisorEvent,
        <Sup::Event as SupervisorEvent>::Children: From<PhantomData<A>>,
        I: Into<Option<Act<Sup>>>,
    {
        let supervisor_handle = supervisor_handle.into();
        let (abort_handle, sender) = self
            .common_spawn::<_, _, Act<A>, _>(actor, supervisor_handle, false, |_| async move {}.boxed())
            .await?;
        match self.pool::<A>().await {
            Some(res) => {
                res.write().await.push_no_metric(sender.clone());
            }
            None => {
                let mut pool = ActorPool::<A, ()>::default();
                pool.push_no_metric(sender.clone());
                self.add_data(Arc::new(RwLock::new(pool))).await;
            }
        };
        Ok((abort_handle, sender))
    }
}

/// A runtime for an actor to use when initializing
pub struct ActorInitRuntime<'a, A, Reg, Sup>
where
    A: Actor,
    Sup: EventDriven,
    Reg: 'static + RegistryAccess + Send + Sync,
{
    scope: &'a mut RuntimeScope<Reg>,
    supervisor_handle: Option<Act<Sup>>,
    _actor: PhantomData<A>,
}

impl<'a, A, Reg, Sup> ActorInitRuntime<'a, A, Reg, Sup>
where
    A: Actor,
    Sup: EventDriven,
    Reg: 'static + RegistryAccess + Send + Sync,
{
    pub(crate) fn new(scope: &'a mut RuntimeScope<Reg>, supervisor_handle: Option<Act<Sup>>) -> Self {
        Self {
            scope,
            supervisor_handle,
            _actor: PhantomData,
        }
    }

    /// Get this actors's handle
    pub async fn my_handle(&mut self) -> Act<A> {
        self.scope.actor_event_handle::<A>().await.unwrap()
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

impl<'a, A, Reg, Sup> ActorInitRuntime<'a, A, Reg, Sup>
where
    A: Actor,
    Sup: EventDriven,
    Sup::Event: SupervisorEvent,
    Reg: 'static + RegistryAccess + Send + Sync,
{
    /// Update this scope's service status
    pub async fn update_status(&mut self, status: ServiceStatus) -> anyhow::Result<()>
    where
        <Sup::Event as SupervisorEvent>::Children: From<PhantomData<A>>,
    {
        if self.supervisor_handle.is_some() {
            let mut service = self.service().await;
            let prev_status = service.status;
            service.update_status(status);
            self.supervisor_handle
                .send(Sup::Event::status_change(StatusChange::new(
                    PhantomData::<A>.into(),
                    prev_status,
                    service,
                )))
                .await
                .ok();
        }
        self.scope.update_status(status).await
    }
}

impl<'a, A, Reg, Sup> Deref for ActorInitRuntime<'a, A, Reg, Sup>
where
    A: Actor,
    Sup: EventDriven,
    Sup::Event: SupervisorEvent,
    Reg: 'static + RegistryAccess + Send + Sync,
{
    type Target = RuntimeScope<Reg>;

    fn deref(&self) -> &Self::Target {
        &self.scope
    }
}

impl<'a, A, Reg, Sup> DerefMut for ActorInitRuntime<'a, A, Reg, Sup>
where
    A: Actor,
    Sup: EventDriven,
    Sup::Event: SupervisorEvent,
    Reg: 'static + RegistryAccess + Send + Sync,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.scope
    }
}

/// An actor's scope, which provides some helpful functions specific to an actor
pub struct ActorScopedRuntime<'a, A, Reg, Sup>
where
    A: Actor,
    Sup: EventDriven,
    Reg: 'static + RegistryAccess + Send + Sync,
{
    scope: &'a mut RuntimeScope<Reg>,
    receiver: ShutdownStream<<A::Channel as Channel<A::Event>>::Receiver>,
    supervisor_handle: Option<Act<Sup>>,
}

impl<'a, A, Reg, Sup> ActorScopedRuntime<'a, A, Reg, Sup>
where
    A: Actor,
    Sup: EventDriven,
    Reg: 'static + RegistryAccess + Send + Sync,
{
    pub(crate) fn new(
        scope: &'a mut RuntimeScope<Reg>,
        receiver: <A::Channel as Channel<A::Event>>::Receiver,
        shutdown: oneshot::Receiver<()>,
        supervisor_handle: Option<Act<Sup>>,
    ) -> Self {
        Self {
            scope,
            receiver: ShutdownStream::new(shutdown, receiver),
            supervisor_handle,
        }
    }

    /// Get the next event from the event receiver
    pub async fn next_event(&mut self) -> Option<A::Event> {
        self.receiver.next().await
    }

    /// Get this actors's handle
    pub async fn my_handle(&mut self) -> Act<A> {
        self.scope.actor_event_handle::<A>().await.unwrap()
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

impl<'a, A, Reg, Sup> ActorScopedRuntime<'a, A, Reg, Sup>
where
    A: Actor,
    Sup: EventDriven,
    Sup::Event: SupervisorEvent,
    Reg: 'static + RegistryAccess + Send + Sync,
{
    /// Update this scope's service status
    pub async fn update_status(&mut self, status: ServiceStatus) -> anyhow::Result<()>
    where
        <Sup::Event as SupervisorEvent>::Children: From<PhantomData<A>>,
    {
        if self.supervisor_handle.is_some() {
            let mut service = self.service().await;
            let prev_status = service.status;
            service.update_status(status);
            self.supervisor_handle
                .send(Sup::Event::status_change(StatusChange::new(
                    PhantomData::<A>.into(),
                    prev_status,
                    service,
                )))
                .await
                .ok();
        }
        self.scope.update_status(status).await
    }
}

impl<'a, A, Reg, Sup> Deref for ActorScopedRuntime<'a, A, Reg, Sup>
where
    A: Actor,
    Sup: EventDriven,
    Sup::Event: SupervisorEvent,
    Reg: 'static + RegistryAccess + Send + Sync,
{
    type Target = RuntimeScope<Reg>;

    fn deref(&self) -> &Self::Target {
        &self.scope
    }
}

impl<'a, A, Reg, Sup> DerefMut for ActorScopedRuntime<'a, A, Reg, Sup>
where
    A: Actor,
    Sup: EventDriven,
    Sup::Event: SupervisorEvent,
    Reg: 'static + RegistryAccess + Send + Sync,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.scope
    }
}
/// A scope for an actor pool, which only allows spawning of the specified actor
pub struct ScopedActorPool<'a, A, M, Reg, Sup>
where
    A: Actor,
    M: Hash + Clone,
    Reg: 'static + RegistryAccess + Send + Sync,
    Sup: EventDriven,
    Sup::Event: SupervisorEvent,
{
    scope: &'a mut RuntimeScope<Reg>,
    pool: &'a mut ActorPool<A, M>,
    supervisor_handle: Option<Act<Sup>>,
}

impl<'a, A, M, Reg, Sup> ScopedActorPool<'a, A, M, Reg, Sup>
where
    A: Actor + Into<<Sup::Event as SupervisorEvent>::ChildStates>,
    M: Hash + Eq + Clone,
    Reg: 'static + RegistryAccess + Send + Sync,
    Sup: 'static + EventDriven,
    Sup::Event: SupervisorEvent,
    <Sup::Event as SupervisorEvent>::Children: From<PhantomData<A>>,
{
    /// Spawn a new actor into this pool
    pub async fn spawn_with_metric(&mut self, actor: A, metric: M) -> anyhow::Result<(AbortHandle, Act<A>)>
    where
        A: 'static + Send + Sync,
    {
        if self.pool.get_by_metric(&metric).is_some() {
            let service = self.scope.service().await;
            panic!(
                "Attempted to add a duplicate metric to pool {} in scope {} ({})",
                std::any::type_name::<Pool<A, M>>(),
                self.scope.scope_id,
                service.name
            );
        }
        let supervisor_handle = self.supervisor_handle.clone();
        let (abort_handle, sender) = self
            .scope
            .common_spawn::<_, _, Act<A>, _>(actor, supervisor_handle, false, |_| async move {}.boxed())
            .await?;
        self.pool.push(sender.clone(), metric);
        Ok((abort_handle, sender))
    }
}

impl<'a, A, M, Reg, Sup> ScopedActorPool<'a, A, M, Reg, Sup>
where
    A: Actor + Into<<Sup::Event as SupervisorEvent>::ChildStates>,
    M: Hash + Eq + Clone + Default,
    Reg: 'static + RegistryAccess + Send + Sync,
    Sup: 'static + EventDriven,
    Sup::Event: SupervisorEvent,
    <Sup::Event as SupervisorEvent>::Children: From<PhantomData<A>>,
{
    /// Spawn a new actor into this pool
    pub async fn spawn_default_metric(&mut self, actor: A) -> anyhow::Result<(AbortHandle, Act<A>)>
    where
        A: 'static + Send + Sync,
    {
        self.spawn_with_metric(actor, M::default()).await
    }
}

impl<'a, A, Reg, Sup> ScopedActorPool<'a, A, (), Reg, Sup>
where
    A: Actor + Into<<Sup::Event as SupervisorEvent>::ChildStates>,
    Reg: 'static + RegistryAccess + Send + Sync,
    Sup: 'static + EventDriven,
    Sup::Event: SupervisorEvent,
    <Sup::Event as SupervisorEvent>::Children: From<PhantomData<A>>,
{
    /// Spawn a new actor into this pool
    pub async fn spawn(&mut self, actor: A) -> anyhow::Result<(AbortHandle, Act<A>)>
    where
        A: 'static + Send + Sync,
    {
        let supervisor_handle = self.supervisor_handle.clone();
        let (abort_handle, sender) = self
            .scope
            .common_spawn::<_, _, Act<A>, _>(actor, supervisor_handle, false, |_| async move {}.boxed())
            .await?;
        self.pool.push_no_metric(sender.clone());
        Ok((abort_handle, sender))
    }
}
