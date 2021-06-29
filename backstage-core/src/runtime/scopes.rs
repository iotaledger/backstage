use super::*;
use crate::actor::{
    ActorError, ActorRequest, DepStatus, Dependencies, ErrorReport, Service, ShutdownStream, SuccessReport, SupervisorEvent,
};
use futures::{
    future::{AbortRegistration, Aborted},
    Future, FutureExt,
};
use std::{fmt::Debug, panic::AssertUnwindSafe};
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
        for<'b> F: Send + FnOnce(&'b mut RuntimeScope<Reg>) -> BoxFuture<'b, O>,
    {
        log::debug!("Spawning with registry {}", std::any::type_name::<Reg>());
        let mut scope = Reg::instantiate("Root", None, None).await;
        let res = f(&mut scope).await;
        scope.join().await;
        Ok(res)
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
        let res = Abortable::new(f(&mut child_scope), abort_registration).await;
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
    pub async fn update_status(&mut self, status: ServiceStatus) {
        self.registry
            .update_status(&self.scope_id, status)
            .await
            .expect(&format!("Scope {} is missing...", self.scope_id))
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
    pub async fn pool<A>(&mut self) -> Option<Pool<A>>
    where
        Self: 'static + Sized,
        A: 'static + Actor + Send + Sync,
    {
        match self.get_data::<Arc<RwLock<ActorPool<A>>>>().await {
            Some(arc) => {
                if arc.write().await.verify() {
                    Some(Pool(arc))
                } else {
                    self.remove_data::<Arc<RwLock<ActorPool<A>>>>().await;
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
    pub async fn spawn_actor<A, H, E>(&mut self, mut actor: A, supervisor_handle: H) -> (AbortHandle, Act<A>)
    where
        A: 'static + Actor + Send + Sync,
        H: 'static + Sender<E> + Clone + Send + Sync,
        E: 'static + SupervisorEvent<A> + Send + Sync,
    {
        let (sender, receiver) = <A::Channel as Channel<A::Event>>::new();
        self.add_data(sender.clone()).await;
        let (oneshot_send, oneshot_recv) = oneshot::channel::<()>();
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        let child_scope = self.child(A::name(), Some(oneshot_send), Some(abort_handle.clone())).await;
        self.common_spawn::<A, _, Act<A>, _, _>(
            child_scope,
            oneshot_recv,
            abort_registration,
            |mut child_scope, deps, oneshot_recv, abort_registration| async move {
                let res = {
                    let mut actor_rt = ActorScopedRuntime::supervised(&mut child_scope, receiver, oneshot_recv, supervisor_handle.clone());
                    Abortable::new(
                        AssertUnwindSafe(actor.run_supervised(&mut actor_rt, deps)).catch_unwind(),
                        abort_registration,
                    )
                    .await
                };
                child_scope.remove_data::<<A::Channel as Channel<A::Event>>::Sender>().await;
                Self::handle_res(res, &mut child_scope, supervisor_handle, actor).await
            },
        )
        .await;
        (abort_handle, Act(sender))
    }

    /// Spawn a new actor with no supervisor
    pub async fn spawn_actor_unsupervised<A>(&mut self, mut actor: A) -> (AbortHandle, Act<A>)
    where
        A: 'static + Actor + Send + Sync,
    {
        let (sender, receiver) = <A::Channel as Channel<A::Event>>::new();
        self.add_data(sender.clone()).await;
        let (oneshot_send, oneshot_recv) = oneshot::channel::<()>();
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        let child_scope = self.child(A::name(), Some(oneshot_send), Some(abort_handle.clone())).await;
        self.common_spawn::<A, _, Act<A>, _, _>(
            child_scope,
            oneshot_recv,
            abort_registration,
            |mut child_scope, deps, oneshot_recv, abort_registration| async move {
                let res = {
                    let mut actor_rt = ActorScopedRuntime::unsupervised(&mut child_scope, receiver, oneshot_recv);
                    Abortable::new(AssertUnwindSafe(actor.run(&mut actor_rt, deps)).catch_unwind(), abort_registration).await
                };
                child_scope.remove_data::<<A::Channel as Channel<A::Event>>::Sender>().await;
                Self::handle_res_unsupervised(res, &mut child_scope).await
            },
        )
        .await;
        (abort_handle, Act(sender))
    }

    /// Spawn a new system with a supervisor handle
    pub async fn spawn_system<A, R, H, E>(&mut self, mut actor: A, state: A::State, supervisor_handle: H) -> (AbortHandle, Act<A>)
    where
        A: 'static + System + Send + Sync,
        H: 'static + Sender<E> + Clone + Send + Sync,
        E: 'static + SupervisorEvent<A> + Send + Sync,
    {
        let (sender, receiver) = <A::Channel as Channel<A::Event>>::new();
        self.add_data(sender.clone()).await;
        self.add_data(state).await;
        let (oneshot_send, oneshot_recv) = oneshot::channel::<()>();
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        let child_scope = self.child(A::name(), Some(oneshot_send), Some(abort_handle.clone())).await;
        self.common_spawn::<A, _, Sys<A>, _, _>(
            child_scope,
            oneshot_recv,
            abort_registration,
            |mut child_scope, deps, oneshot_recv, abort_registration| async move {
                let res = {
                    let mut actor_rt = ActorScopedRuntime::supervised(&mut child_scope, receiver, oneshot_recv, supervisor_handle.clone());
                    Abortable::new(
                        AssertUnwindSafe(actor.run_supervised(&mut actor_rt, deps)).catch_unwind(),
                        abort_registration,
                    )
                    .await
                };
                child_scope.remove_data::<<A::Channel as Channel<A::Event>>::Sender>().await;
                child_scope.remove_data::<A::State>().await;
                Self::handle_res(res, &mut child_scope, supervisor_handle, actor).await
            },
        )
        .await;
        (abort_handle, Act(sender))
    }

    /// Spawn a new system with no supervisor
    pub async fn spawn_system_unsupervised<A>(&mut self, mut actor: A, state: A::State) -> (AbortHandle, Act<A>)
    where
        A: 'static + System + Send + Sync,
    {
        let (sender, receiver) = <A::Channel as Channel<A::Event>>::new();
        self.add_data(sender.clone()).await;
        self.add_data(state).await;
        let (oneshot_send, oneshot_recv) = oneshot::channel::<()>();
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        let child_scope = self.child(A::name(), Some(oneshot_send), Some(abort_handle.clone())).await;
        self.common_spawn::<A, _, Sys<A>, _, _>(
            child_scope,
            oneshot_recv,
            abort_registration,
            |mut child_scope, deps, oneshot_recv, abort_registration| async move {
                let res = {
                    let mut actor_rt = ActorScopedRuntime::unsupervised(&mut child_scope, receiver, oneshot_recv);
                    Abortable::new(AssertUnwindSafe(actor.run(&mut actor_rt, deps)).catch_unwind(), abort_registration).await
                };
                child_scope.remove_data::<<A::Channel as Channel<A::Event>>::Sender>().await;
                child_scope.remove_data::<A::State>().await;
                Self::handle_res_unsupervised(res, &mut child_scope).await
            },
        )
        .await;
        (abort_handle, Act(sender))
    }

    async fn common_spawn<
        T,
        Deps: 'static + Dependencies + Send + Sync,
        B: 'static + Send + Sync,
        F: 'static + Send + Sync + FnOnce(RuntimeScope<Reg>, Deps, oneshot::Receiver<()>, AbortRegistration) -> O,
        O: Send + Future<Output = anyhow::Result<()>>,
    >(
        &mut self,
        mut child_scope: RuntimeScope<Reg>,
        oneshot_recv: oneshot::Receiver<()>,
        abort_registration: AbortRegistration,
        run_fn: F,
    ) {
        log::debug!("Spawning {}", std::any::type_name::<T>());
        let dep_status = Deps::request(&mut child_scope).await;
        let child_task = tokio::spawn(async move {
            let deps = match dep_status {
                DepStatus::Ready(deps) => deps,
                DepStatus::Waiting(mut recv) => {
                    log::info!(
                        "{} waiting for dependencies {}",
                        std::any::type_name::<T>(),
                        std::any::type_name::<Deps>()
                    );
                    if let Err(_) = recv.recv().await {
                        panic!("Failed to acquire dependencies for {}", std::any::type_name::<T>());
                    }
                    log::info!("{} acquired dependencies!", std::any::type_name::<T>());
                    Deps::instantiate(&mut child_scope)
                        .await
                        .map_err(|e| anyhow::anyhow!("Cannot spawn {}: {}", std::any::type_name::<T>(), e))
                        .unwrap()
                }
            };
            Deps::link(&mut child_scope).await;
            if let Some(broadcaster) = child_scope
                .registry
                .get_data::<broadcast::Sender<PhantomData<B>>>(&child_scope.scope_id)
                .await
            {
                log::debug!("Broadcasting creation of {}", std::any::type_name::<B>());
                broadcaster.send(PhantomData).ok();
            }

            run_fn(child_scope, deps, oneshot_recv, abort_registration).await
        });
        self.join_handles_mut().push(child_task);
    }

    pub(crate) async fn handle_res<T, H, E>(
        res: Result<std::thread::Result<Result<(), ActorError>>, Aborted>,
        child_scope: &mut RuntimeScope<Reg>,
        mut supervisor_handle: H,
        state: T,
    ) -> anyhow::Result<()>
    where
        H: 'static + Sender<E> + Clone + Send + Sync,
        E: 'static + SupervisorEvent<T> + Send + Sync,
    {
        let service = child_scope.service().await;
        child_scope.abort().await;
        child_scope.join().await;
        match res.ok().map(Result::ok) {
            Some(res) => match res {
                Some(res) => match res {
                    Ok(_) => match E::report_ok(SuccessReport::new(state, service)) {
                        Ok(evt) => supervisor_handle.send(evt).await,
                        Err(e) => {
                            log::error!("{}", e);
                            anyhow::bail!(e)
                        }
                    },
                    Err(e) => match E::report_err(ErrorReport::new(state, service, e)) {
                        Ok(evt) => supervisor_handle.send(evt).await,
                        Err(e) => {
                            log::error!("{}", e);
                            anyhow::bail!(e)
                        }
                    },
                },
                // TODO: Maybe abort the children here?
                // or alternatively allow for restarting in the same scope?
                None => match E::report_err(ErrorReport::new(state, service, ActorError::RuntimeError(ActorRequest::Restart))) {
                    Ok(evt) => supervisor_handle.send(evt).await,
                    Err(e) => {
                        log::error!("{}", e);
                        anyhow::bail!(e)
                    }
                },
            },
            None => match E::report_ok(SuccessReport::new(state, service)) {
                Ok(evt) => supervisor_handle.send(evt).await,
                Err(e) => {
                    log::error!("{}", e);
                    anyhow::bail!(e)
                }
            },
        }
    }

    pub(crate) async fn handle_res_unsupervised(
        res: Result<std::thread::Result<Result<(), ActorError>>, Aborted>,
        child_scope: &mut RuntimeScope<Reg>,
    ) -> anyhow::Result<()> {
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
    }

    /// Spawn a new pool of actors of a given type
    pub async fn spawn_pool<A, H, E, I, F>(&mut self, supervisor_handle: I, f: F) -> anyhow::Result<()>
    where
        A: 'static + Actor + Send + Sync,
        H: 'static + Sender<E> + Clone + Send + Sync,
        E: 'static + SupervisorEvent<A> + Send + Sync,
        I: Into<Option<H>>,
        for<'b> F: 'static + Send + FnOnce(&'b mut ScopedActorPool<A, Reg, H, E>) -> BoxFuture<'b, anyhow::Result<()>>,
    {
        let mut pool = ActorPool::default();
        let mut scoped_pool = ScopedActorPool {
            scope: self,
            pool: &mut pool,
            supervisor_handle: supervisor_handle.into(),
            _evt: PhantomData,
        };
        f(&mut scoped_pool).await?;
        self.add_data(Arc::new(RwLock::new(pool))).await;
        Ok(())
    }

    /// Spawn a new actor into a pool, creating a pool if needed
    pub async fn spawn_into_pool<A: 'static + Actor, H, E, I: Into<Option<H>>>(
        &mut self,
        mut actor: A,
        supervisor_handle: I,
    ) -> (AbortHandle, Act<A>)
    where
        A: 'static + Actor + Send + Sync,
        H: 'static + Sender<E> + Clone + Send + Sync,
        E: 'static + SupervisorEvent<A> + Send + Sync + Debug,
        I: Into<Option<H>>,
    {
        let supervisor_handle = supervisor_handle.into();
        let (sender, receiver) = <A::Channel as Channel<A::Event>>::new();
        let (oneshot_send, oneshot_recv) = oneshot::channel::<()>();
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        let mut child_scope = self.child(A::name(), Some(oneshot_send), Some(abort_handle.clone())).await;
        child_scope.add_data(sender.clone()).await;
        self.common_spawn::<A, _, Act<A>, _, _>(
            child_scope,
            oneshot_recv,
            abort_registration,
            |mut child_scope, deps, oneshot_recv, abort_registration| async move {
                let res = {
                    let mut actor_rt = ActorScopedRuntime::supervised(&mut child_scope, receiver, oneshot_recv, supervisor_handle.clone());
                    Abortable::new(
                        AssertUnwindSafe(actor.run_supervised(&mut actor_rt, deps)).catch_unwind(),
                        abort_registration,
                    )
                    .await
                };
                Self::handle_res(res, &mut child_scope, supervisor_handle, actor).await
            },
        )
        .await;
        match self.pool::<A>().await {
            Some(res) => {
                res.write().await.push(sender.clone());
            }
            None => {
                let mut pool = ActorPool::<A>::default();
                pool.push(sender.clone());
                self.add_data(Arc::new(RwLock::new(pool))).await;
            }
        };
        (abort_handle, Act(sender))
    }
}

/// An actor's scope, which provides some helpful functions specific to an actor
pub struct ActorScopedRuntime<'a, A: Actor, Reg: 'static + RegistryAccess + Send + Sync>
where
    A::Event: 'static,
{
    scope: &'a mut RuntimeScope<Reg>,
    receiver: ShutdownStream<<A::Channel as Channel<A::Event>>::Receiver>,
}

/// A supervised actor's scope. The actor can request its supervisor's handle from here.
pub struct SupervisedActorScopedRuntime<'a, A: Actor, Reg: 'static + RegistryAccess + Send + Sync, H, E>
where
    A::Event: 'static,
    H: 'static + Sender<E> + Clone + Send + Sync,
    E: 'static + SupervisorEvent<A> + Send + Sync,
{
    pub(crate) scope: ActorScopedRuntime<'a, A, Reg>,
    supervisor_handle: H,
    _event: PhantomData<E>,
}

impl<'a, A: Actor, Reg: 'static + RegistryAccess + Send + Sync> ActorScopedRuntime<'a, A, Reg> {
    pub(crate) fn unsupervised(
        scope: &'a mut RuntimeScope<Reg>,
        receiver: <A::Channel as Channel<A::Event>>::Receiver,
        shutdown: oneshot::Receiver<()>,
    ) -> Self {
        Self {
            scope,
            receiver: ShutdownStream::new(shutdown, receiver),
        }
    }

    pub(crate) fn supervised<H, E>(
        scope: &'a mut RuntimeScope<Reg>,
        receiver: <A::Channel as Channel<A::Event>>::Receiver,
        shutdown: oneshot::Receiver<()>,
        supervisor_handle: H,
    ) -> SupervisedActorScopedRuntime<A, Reg, H, E>
    where
        H: 'static + Sender<E> + Clone + Send + Sync,
        E: 'static + SupervisorEvent<A> + Send + Sync,
    {
        SupervisedActorScopedRuntime {
            scope: Self::unsupervised(scope, receiver, shutdown),
            supervisor_handle,
            _event: PhantomData,
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
}

impl<'a, A: Actor, Reg: 'static + RegistryAccess + Send + Sync, H, E> SupervisedActorScopedRuntime<'a, A, Reg, H, E>
where
    H: 'static + Sender<E> + Clone + Send + Sync,
    E: 'static + SupervisorEvent<A> + Send + Sync,
{
    /// Get this actor's supervisor handle
    pub fn supervisor_handle(&mut self) -> &mut H {
        &mut self.supervisor_handle
    }
}

impl<'a, A: Actor, Reg: 'static + RegistryAccess + Send + Sync> Deref for ActorScopedRuntime<'a, A, Reg> {
    type Target = RuntimeScope<Reg>;

    fn deref(&self) -> &Self::Target {
        &self.scope
    }
}

impl<'a, A: Actor, Reg: 'static + RegistryAccess + Send + Sync> DerefMut for ActorScopedRuntime<'a, A, Reg> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.scope
    }
}

impl<'a, A: Actor, Reg: 'static + RegistryAccess + Send + Sync, H, E> Deref for SupervisedActorScopedRuntime<'a, A, Reg, H, E>
where
    H: 'static + Sender<E> + Clone + Send + Sync,
    E: 'static + SupervisorEvent<A> + Send + Sync,
{
    type Target = ActorScopedRuntime<'a, A, Reg>;

    fn deref(&self) -> &Self::Target {
        &self.scope
    }
}

impl<'a, A: Actor, Reg: 'static + RegistryAccess + Send + Sync, H, E> DerefMut for SupervisedActorScopedRuntime<'a, A, Reg, H, E>
where
    H: 'static + Sender<E> + Clone + Send + Sync,
    E: 'static + SupervisorEvent<A> + Send + Sync,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.scope
    }
}

/// A scope for an actor pool, which only allows spawning of the specified actor
pub struct ScopedActorPool<'a, A: Actor, Reg: 'static + RegistryAccess + Send + Sync, H, E>
where
    H: 'static + Sender<E> + Clone + Send + Sync,
    E: 'static + SupervisorEvent<A> + Send + Sync,
{
    scope: &'a mut RuntimeScope<Reg>,
    pool: &'a mut ActorPool<A>,
    supervisor_handle: Option<H>,
    _evt: PhantomData<E>,
}

impl<'a, A, Reg: 'static + RegistryAccess + Send + Sync, H, E> ScopedActorPool<'a, A, Reg, H, E>
where
    H: 'static + Sender<E> + Clone + Send + Sync,
    E: 'static + SupervisorEvent<A> + Send + Sync + std::fmt::Debug,
    A: Actor,
{
    /// Spawn a new actor into this pool
    pub async fn spawn(&mut self, mut actor: A) -> (AbortHandle, Act<A>)
    where
        A: 'static + Send + Sync,
    {
        let (sender, receiver) = <A::Channel as Channel<A::Event>>::new();
        self.pool.push(sender.clone());
        let (oneshot_send, oneshot_recv) = oneshot::channel::<()>();
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        let mut child_scope = self.scope.child(A::name(), Some(oneshot_send), Some(abort_handle.clone())).await;
        child_scope.add_data(sender.clone()).await;
        let supervisor_handle = self.supervisor_handle.clone();
        self.scope
            .common_spawn::<A, _, Act<A>, _, _>(
                child_scope,
                oneshot_recv,
                abort_registration,
                |mut child_scope, deps, oneshot_recv, abort_registration| async move {
                    let res = {
                        let mut actor_rt =
                            ActorScopedRuntime::supervised(&mut child_scope, receiver, oneshot_recv, supervisor_handle.clone());
                        Abortable::new(
                            AssertUnwindSafe(actor.run_supervised(&mut actor_rt, deps)).catch_unwind(),
                            abort_registration,
                        )
                        .await
                    };
                    RuntimeScope::handle_res(res, &mut child_scope, supervisor_handle, actor).await
                },
            )
            .await;
        (abort_handle, Act(sender))
    }
}
