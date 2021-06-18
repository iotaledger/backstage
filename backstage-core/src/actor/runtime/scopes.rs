use std::{fmt::Debug, panic::AssertUnwindSafe};

use futures::FutureExt;

use crate::{ActorError, ActorRequest, ErrorReport, Service, SuccessReport};

use super::*;

/// A runtime which defines a particular scope and functionality to
/// create tasks within it.
pub struct RuntimeScope<'a, Rt> {
    /// The underlying runtime. Most functionality is forwarded to the scope,
    /// but this may still be useful in certain situations.
    pub rt: &'a mut Rt,
}

impl<'a, Rt: 'static> RuntimeScope<'a, Rt> {
    pub(crate) fn new(rt: &'a mut Rt) -> Self {
        Self { rt }
    }

    /// Spawn a new, plain task with the same runtime as this scope
    pub fn spawn_task<F>(&mut self, f: F) -> AbortHandle
    where
        Rt: BaseRuntime,
        for<'b> F: 'static + Send + FnOnce(RuntimeScope<'b, Rt>) -> BoxFuture<'b, anyhow::Result<()>>,
    {
        let child_rt = self.rt.child("Task");
        self.common_spawn_task(child_rt, f)
    }

    /// Spawn a new, plain task with a specified runtime
    pub fn spawn_task_with_runtime<TaskRt, F>(&mut self, f: F) -> AbortHandle
    where
        Rt: BaseRuntime + Into<TaskRt>,
        TaskRt: 'static + BaseRuntime,
        for<'b> F: 'static + Send + FnOnce(RuntimeScope<'b, TaskRt>) -> BoxFuture<'b, anyhow::Result<()>>,
    {
        let child_rt: TaskRt = self.rt.child("Task").into();
        self.common_spawn_task(child_rt, f)
    }

    fn common_spawn_task<ChildRt, F>(&mut self, mut child_rt: ChildRt, f: F) -> AbortHandle
    where
        for<'b> F: 'static + Send + FnOnce(RuntimeScope<'b, ChildRt>) -> BoxFuture<'b, anyhow::Result<()>>,
        Rt: BaseRuntime,
        ChildRt: 'static + BaseRuntime,
    {
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        let child_task = tokio::spawn(async move {
            let scope = RuntimeScope::new(&mut child_rt);
            let res = Abortable::new(f(scope), abort_registration).await;
            match res {
                Ok(res) => {
                    child_rt.join().await;
                    res
                }
                Err(_) => {
                    log::debug!("Aborting children of task!");
                    child_rt.abort();
                    anyhow::bail!("Aborted!")
                }
            }
        });
        self.rt.join_handles_mut().push(child_task);
        self.rt.shutdown_handles_mut().push((None, abort_handle.clone()));
        abort_handle
    }

    /// Spawn a new actor with a supervisor handle
    pub fn spawn_actor<A, H, E>(
        &mut self,
        mut actor: A,
        mut supervisor_handle: H,
    ) -> (AbortHandle, <A::Channel as Channel<A::Event>>::Sender)
    where
        Rt: BaseRuntime + Into<A::Rt>,
        A: 'static + Actor + Send + Sync,
        H: 'static + Sender<E> + Clone + Send + Sync,
        E: 'static + SupervisorEvent<A> + Send + Sync + From<A::SupervisorEvent>,
    {
        let (sender, receiver) = <A::Channel as Channel<A::Event>>::new();
        self.rt.senders_mut().insert(sender.clone());
        let mut child_rt = self.rt.child(A::name()).into();
        let deps = A::Dependencies::instantiate(&mut child_rt)
            .map_err(|e| anyhow::anyhow!("Cannot spawn actor {}: {}", std::any::type_name::<A>(), e))
            .unwrap();
        let (oneshot_send, oneshot_recv) = oneshot::channel::<()>();
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        self.rt.shutdown_handles_mut().push((Some(oneshot_send), abort_handle.clone()));
        let child_task = tokio::spawn(async move {
            let res = {
                let mut actor_rt =
                    ActorScopedRuntime::supervised(RuntimeScope::new(&mut child_rt), receiver, oneshot_recv, supervisor_handle.clone());
                Abortable::new(
                    AssertUnwindSafe(actor.run_supervised(&mut actor_rt, deps)).catch_unwind(),
                    abort_registration,
                )
                .await
                .ok()
                .map(Result::ok)
            };
            match res {
                Some(res) => match res {
                    Some(res) => {
                        child_rt.join().await;
                        match res {
                            Ok(_) => match E::report_ok(SuccessReport::new(actor, child_rt.service().clone())) {
                                Ok(evt) => supervisor_handle.send(evt).await,
                                Err(e) => {
                                    log::error!("{}", e);
                                    anyhow::bail!(e)
                                }
                            },
                            Err(e) => match E::report_err(ErrorReport::new(actor, child_rt.service().clone(), e)) {
                                Ok(evt) => supervisor_handle.send(evt).await,
                                Err(e) => {
                                    log::error!("{}", e);
                                    anyhow::bail!(e)
                                }
                            },
                        }
                    }
                    None => match E::report_err(ErrorReport::new(
                        actor,
                        child_rt.service().clone(),
                        ActorError::RuntimeError(ActorRequest::Restart),
                    )) {
                        Ok(evt) => supervisor_handle.send(evt).await,
                        Err(e) => {
                            log::error!("{}", e);
                            anyhow::bail!(e)
                        }
                    },
                },
                None => {
                    log::debug!("Aborting children of actor!");
                    child_rt.abort();
                    match E::report_ok(SuccessReport::new(actor, child_rt.service().clone())) {
                        Ok(evt) => supervisor_handle.send(evt).await,
                        Err(e) => {
                            log::error!("{}", e);
                            anyhow::bail!(e)
                        }
                    }
                }
            }
        });
        self.rt.join_handles_mut().push(child_task);
        (abort_handle, sender)
    }

    /// Spawn a new actor with no supervisor
    pub fn spawn_actor_unsupervised<A>(&mut self, mut actor: A) -> (AbortHandle, <A::Channel as Channel<A::Event>>::Sender)
    where
        Rt: BaseRuntime + Into<A::Rt>,
        A: 'static + Actor + Send + Sync,
    {
        let (sender, receiver) = <A::Channel as Channel<A::Event>>::new();
        self.rt.senders_mut().insert(sender.clone());
        let mut child_rt = self.rt.child(A::name()).into();
        let deps = A::Dependencies::instantiate(&mut child_rt)
            .map_err(|e| anyhow::anyhow!("Cannot spawn actor {}: {}", std::any::type_name::<A>(), e))
            .unwrap();
        let (oneshot_send, oneshot_recv) = oneshot::channel::<()>();
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        self.rt.shutdown_handles_mut().push((Some(oneshot_send), abort_handle.clone()));
        let child_task = tokio::spawn(async move {
            let res = {
                let mut actor_rt = ActorScopedRuntime::unsupervised(RuntimeScope::new(&mut child_rt), receiver, oneshot_recv);
                Abortable::new(actor.run(&mut actor_rt, deps), abort_registration).await
            };
            match res {
                Ok(res) => {
                    child_rt.join().await;
                    match res {
                        Ok(_) => Ok(()),
                        Err(e) => anyhow::bail!(e),
                    }
                }
                Err(_) => {
                    log::debug!("Aborting children of actor!");
                    child_rt.abort();
                    anyhow::bail!("Aborted!")
                }
            }
        });
        self.rt.join_handles_mut().push(child_task);
        (abort_handle, sender)
    }

    /// Spawn a new system with a supervisor handle
    pub fn spawn_system<S, H, E>(
        &mut self,
        system: S,
        mut supervisor_handle: H,
    ) -> (AbortHandle, <S::Channel as Channel<S::ChildEvents>>::Sender)
    where
        Rt: SystemRuntime + Into<S::Rt>,
        S: 'static + System + Send + Sync,
        H: 'static + Sender<E> + Clone + Send + Sync,
        E: 'static + SupervisorEvent<Arc<RwLock<S>>> + Send + Sync + From<S::SupervisorEvent>,
    {
        let (sender, receiver) = <S::Channel as Channel<S::ChildEvents>>::new();
        let system = Arc::new(RwLock::new(system));
        self.rt.senders_mut().insert(sender.clone());
        self.rt.systems_mut().insert(system.clone());
        let mut child_rt = self.rt.child(S::name()).into();
        let deps = S::Dependencies::instantiate(&mut child_rt)
            .map_err(|e| anyhow::anyhow!("Cannot spawn system {}: {}", std::any::type_name::<S>(), e))
            .unwrap();
        let (oneshot_send, oneshot_recv) = oneshot::channel::<()>();
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        self.rt.shutdown_handles_mut().push((Some(oneshot_send), abort_handle.clone()));
        let child_task = tokio::spawn(async move {
            let res = {
                let mut system_rt =
                    SystemScopedRuntime::supervised(RuntimeScope::new(&mut child_rt), receiver, oneshot_recv, supervisor_handle.clone());
                Abortable::new(
                    AssertUnwindSafe(S::run_supervised(system.clone(), &mut system_rt, deps)).catch_unwind(),
                    abort_registration,
                )
                .await
                .ok()
                .map(Result::ok)
            };
            match res {
                Some(res) => match res {
                    Some(res) => {
                        child_rt.join().await;
                        match res {
                            Ok(_) => match E::report_ok(SuccessReport::new(system, child_rt.service().clone())) {
                                Ok(evt) => supervisor_handle.send(evt).await,
                                Err(e) => {
                                    log::error!("{}", e);
                                    anyhow::bail!(e)
                                }
                            },
                            Err(e) => match E::report_err(ErrorReport::new(system, child_rt.service().clone(), e)) {
                                Ok(evt) => supervisor_handle.send(evt).await,
                                Err(e) => {
                                    log::error!("{}", e);
                                    anyhow::bail!(e)
                                }
                            },
                        }
                    }
                    None => match E::report_err(ErrorReport::new(
                        system,
                        child_rt.service().clone(),
                        ActorError::RuntimeError(ActorRequest::Restart),
                    )) {
                        Ok(evt) => supervisor_handle.send(evt).await,
                        Err(e) => {
                            log::error!("{}", e);
                            anyhow::bail!(e)
                        }
                    },
                },
                None => {
                    log::debug!("Aborting children of system!");
                    child_rt.abort();
                    match E::report_ok(SuccessReport::new(system, child_rt.service().clone())) {
                        Ok(evt) => supervisor_handle.send(evt).await,
                        Err(e) => {
                            log::error!("{}", e);
                            anyhow::bail!(e)
                        }
                    }
                }
            }
        });
        self.rt.join_handles_mut().push(child_task);
        (abort_handle, sender)
    }

    /// Spawn a new system with no supervisor
    pub fn spawn_system_unsupervised<S>(&mut self, system: S) -> (AbortHandle, <S::Channel as Channel<S::ChildEvents>>::Sender)
    where
        Rt: SystemRuntime + Into<S::Rt>,
        S: 'static + System + Send + Sync,
    {
        let (sender, receiver) = <S::Channel as Channel<S::ChildEvents>>::new();
        let system = Arc::new(RwLock::new(system));
        self.rt.senders_mut().insert(sender.clone());
        self.rt.systems_mut().insert(system.clone());
        let mut child_rt = self.rt.child(S::name()).into();
        let deps = S::Dependencies::instantiate(&mut child_rt)
            .map_err(|e| anyhow::anyhow!("Cannot spawn system {}: {}", std::any::type_name::<S>(), e))
            .unwrap();
        let (oneshot_send, oneshot_recv) = oneshot::channel::<()>();
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        self.rt.shutdown_handles_mut().push((Some(oneshot_send), abort_handle.clone()));
        let child_task = tokio::spawn(async move {
            let res = {
                let mut system_rt = SystemScopedRuntime::unsupervised(RuntimeScope::new(&mut child_rt), receiver, oneshot_recv);
                Abortable::new(S::run(system, &mut system_rt, deps), abort_registration).await
            };
            match res {
                Ok(res) => {
                    child_rt.join().await;
                    match res {
                        Ok(_) => Ok(()),
                        Err(e) => anyhow::bail!(e),
                    }
                }
                Err(_) => {
                    log::debug!("Aborting children of system!");
                    child_rt.abort();
                    anyhow::bail!("Aborted!")
                }
            }
        });
        self.rt.join_handles_mut().push(child_task);
        (abort_handle, sender)
    }

    /// Spawn a new pool of actors of a given type
    pub fn spawn_pool<A, H, E, I, F>(&mut self, supervisor_handle: I, f: F)
    where
        Rt: PoolRuntime,
        A: 'static + Actor + Send + Sync,
        H: 'static + Sender<E> + Clone + Send + Sync,
        E: 'static + SupervisorEvent<A> + Send + Sync,
        I: Into<Option<H>>,
        F: FnOnce(&mut ScopedActorPool<Rt, A, H, E>),
    {
        let mut pool = ActorPool::default();
        let mut scoped_pool = ScopedActorPool {
            scope: self,
            pool: &mut pool,
            supervisor_handle: supervisor_handle.into(),
            _evt: PhantomData,
        };
        f(&mut scoped_pool);
        self.rt.pools_mut().insert(Arc::new(RwLock::new(pool)));
    }

    /// Spawn a new actor into a pool, creating a pool if needed
    pub async fn spawn_into_pool<A: 'static + Actor, H, E, I: Into<Option<H>>>(
        &mut self,
        actor: A,
        supervisor_handle: I,
    ) -> (AbortHandle, <A::Channel as Channel<A::Event>>::Sender)
    where
        Rt: PoolRuntime + Into<A::Rt>,
        A: 'static + Actor + Send + Sync,
        H: 'static + Sender<E> + Clone + Send + Sync,
        E: 'static + SupervisorEvent<A> + Send + Sync + From<A::SupervisorEvent> + Debug,
        I: Into<Option<H>>,
    {
        let (abort_handle, sender) = self.spawn_actor(actor, supervisor_handle.into());
        match self.pool::<A>().await {
            Some(res) => {
                res.write().await.push(sender.clone());
            }
            None => {
                let mut pool = ActorPool::<A>::default();
                pool.push(sender.clone());
                self.rt.pools_mut().insert(Arc::new(RwLock::new(pool)));
            }
        };
        (abort_handle, sender)
    }

    /// Add a shared resource and get a reference to it
    pub fn add_resource<R: 'static + Send + Sync + Clone>(&mut self, resource: R) -> Res<R>
    where
        Rt: ResourceRuntime,
    {
        self.rt.resources_mut().insert(resource.clone());
        Res(resource)
    }

    /// Get a shared resource if it exists in the scope
    pub fn resource<R: 'static + Send + Sync + Clone>(&self) -> Option<Res<R>>
    where
        Rt: ResourceRuntime,
    {
        self.rt.resource()
    }

    /// Get a system reference if it exists in the scope
    pub fn system<S: 'static + System + Send + Sync>(&self) -> Option<Sys<S>>
    where
        Rt: SystemRuntime,
    {
        self.rt.system()
    }

    /// Get a pool of actors if it exists in the scope
    pub async fn pool<A>(&mut self) -> Option<Res<Arc<RwLock<ActorPool<A>>>>>
    where
        Rt: PoolRuntime,
        A: 'static + Actor + Send + Sync,
    {
        self.rt.pool::<A>().await
    }

    /// Send an event to an actor in this scope
    pub async fn send_actor_event<A: Actor>(&mut self, event: A::Event) -> anyhow::Result<()>
    where
        Rt: BaseRuntime,
    {
        self.rt.send_actor_event::<A>(event).await
    }

    /// Send an event to a system in this scope
    pub async fn send_system_event<S: System>(&mut self, event: S::ChildEvents) -> anyhow::Result<()>
    where
        Rt: SystemRuntime,
    {
        self.rt.send_system_event::<S>(event).await
    }
}

/// An actor's scope, which provides some helpful functions specific to an actor
pub struct ActorScopedRuntime<'a, A: Actor>
where
    A::Event: 'static,
{
    scope: RuntimeScope<'a, A::Rt>,
    receiver: ShutdownStream<<A::Channel as Channel<A::Event>>::Receiver>,
}

/// A supervised actor's scope. The actor can request its supervisor's handle from here.
pub struct SupervisedActorScopedRuntime<'a, A: Actor, H, E>
where
    A::Event: 'static,
    H: 'static + Sender<E> + Clone + Send + Sync,
    E: 'static + SupervisorEvent<A> + Send + Sync,
{
    pub(crate) scope: ActorScopedRuntime<'a, A>,
    supervisor_handle: H,
    _event: PhantomData<E>,
}

impl<'a, A: Actor> ActorScopedRuntime<'a, A> {
    pub(crate) fn unsupervised(
        runtime: RuntimeScope<'a, A::Rt>,
        receiver: <A::Channel as Channel<A::Event>>::Receiver,
        shutdown: oneshot::Receiver<()>,
    ) -> Self {
        Self {
            scope: runtime,
            receiver: ShutdownStream::new(shutdown, receiver),
        }
    }

    pub(crate) fn supervised<H, E>(
        runtime: RuntimeScope<'a, A::Rt>,
        receiver: <A::Channel as Channel<A::Event>>::Receiver,
        shutdown: oneshot::Receiver<()>,
        supervisor_handle: H,
    ) -> SupervisedActorScopedRuntime<'a, A, H, E>
    where
        H: 'static + Sender<E> + Clone + Send + Sync,
        E: 'static + SupervisorEvent<A> + Send + Sync,
    {
        SupervisedActorScopedRuntime {
            scope: Self::unsupervised(runtime, receiver, shutdown),
            supervisor_handle,
            _event: PhantomData,
        }
    }

    /// Get the next event from the event receiver
    pub async fn next_event(&mut self) -> Option<A::Event> {
        self.receiver.next().await
    }

    pub(crate) async fn startup_dependencies(&mut self) -> Option<A::Dependencies> {
        todo!()
    }

    /// Get this actors's handle
    pub fn my_handle(&self) -> Act<A> {
        self.scope.rt.actor_event_handle::<A>().unwrap()
    }

    /// Shutdown this actor
    pub fn shutdown(&mut self) {
        self.scope.rt.shutdown()
    }

    /// Get the runtime's service
    pub fn service(&self) -> &Service {
        self.scope.rt.service()
    }

    /// Mutably get the runtime's service
    pub fn service_mut(&mut self) -> &mut Service {
        self.scope.rt.service_mut()
    }
}

impl<'a, A: Actor, H, E> SupervisedActorScopedRuntime<'a, A, H, E>
where
    H: 'static + Sender<E> + Clone + Send + Sync,
    E: 'static + SupervisorEvent<A> + Send + Sync,
{
    /// Get this actor's supervisor handle
    pub fn supervisor_handle(&mut self) -> &mut H {
        &mut self.supervisor_handle
    }
}

impl<'a, A: Actor> Deref for ActorScopedRuntime<'a, A> {
    type Target = RuntimeScope<'a, A::Rt>;

    fn deref(&self) -> &Self::Target {
        &self.scope
    }
}

impl<'a, A: Actor> DerefMut for ActorScopedRuntime<'a, A> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.scope
    }
}

impl<'a, A: Actor> Drop for ActorScopedRuntime<'a, A> {
    fn drop(&mut self) {
        self.scope.rt.shutdown();
    }
}

impl<'a, A: Actor, H, E> Deref for SupervisedActorScopedRuntime<'a, A, H, E>
where
    H: 'static + Sender<E> + Clone + Send + Sync,
    E: 'static + SupervisorEvent<A> + Send + Sync,
{
    type Target = ActorScopedRuntime<'a, A>;

    fn deref(&self) -> &Self::Target {
        &self.scope
    }
}

impl<'a, A: Actor, H, E> DerefMut for SupervisedActorScopedRuntime<'a, A, H, E>
where
    H: 'static + Sender<E> + Clone + Send + Sync,
    E: 'static + SupervisorEvent<A> + Send + Sync,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.scope
    }
}

/// A systems's scope, which provides some helpful functions specific to a system
pub struct SystemScopedRuntime<'a, S: System>
where
    S::ChildEvents: 'static,
{
    scope: RuntimeScope<'a, S::Rt>,
    receiver: ShutdownStream<<S::Channel as Channel<S::ChildEvents>>::Receiver>,
}

/// A supervised systems's scope. The system can request its supervisor's handle from here.
pub struct SupervisedSystemScopedRuntime<'a, S: System, H, E>
where
    S::ChildEvents: 'static,
    H: 'static + Sender<E> + Clone + Send + Sync,
    E: 'static + SupervisorEvent<Arc<RwLock<S>>> + Send + Sync,
{
    pub(crate) scope: SystemScopedRuntime<'a, S>,
    supervisor_handle: H,
    _event: PhantomData<E>,
}

impl<'a, S: System> SystemScopedRuntime<'a, S> {
    pub(crate) fn unsupervised(
        runtime: RuntimeScope<'a, S::Rt>,
        receiver: <S::Channel as Channel<S::ChildEvents>>::Receiver,
        shutdown: oneshot::Receiver<()>,
    ) -> Self {
        Self {
            scope: runtime,
            receiver: ShutdownStream::new(shutdown, receiver),
        }
    }

    pub(crate) fn supervised<H, E>(
        runtime: RuntimeScope<'a, S::Rt>,
        receiver: <S::Channel as Channel<S::ChildEvents>>::Receiver,
        shutdown: oneshot::Receiver<()>,
        supervisor_handle: H,
    ) -> SupervisedSystemScopedRuntime<'a, S, H, E>
    where
        H: 'static + Sender<E> + Clone + Send + Sync,
        E: 'static + SupervisorEvent<Arc<RwLock<S>>> + Send + Sync,
    {
        SupervisedSystemScopedRuntime {
            scope: Self::unsupervised(runtime, receiver, shutdown),
            supervisor_handle,
            _event: PhantomData,
        }
    }

    /// Get the next event from the event receiver
    pub async fn next_event(&mut self) -> Option<S::ChildEvents> {
        self.receiver.next().await
    }

    /// Get this system's handle
    pub fn my_handle(&self) -> <S::Channel as Channel<S::ChildEvents>>::Sender {
        self.scope.rt.system_event_handle::<S>().unwrap()
    }

    /// Shutdown this system
    pub fn shutdown(&mut self) {
        self.scope.rt.shutdown()
    }

    /// Get the runtime's service
    pub fn service(&self) -> &Service {
        self.scope.rt.service()
    }

    /// Mutably get the runtime's service
    pub fn service_mut(&mut self) -> &mut Service {
        self.scope.rt.service_mut()
    }
}

impl<'a, S: System, H, E> SupervisedSystemScopedRuntime<'a, S, H, E>
where
    H: 'static + Sender<E> + Clone + Send + Sync,
    E: 'static + SupervisorEvent<Arc<RwLock<S>>> + Send + Sync,
{
    /// Get this systems's supervisor handle
    pub fn supervisor_handle(&mut self) -> &mut H {
        &mut self.supervisor_handle
    }
}

impl<'a, S: System> Drop for SystemScopedRuntime<'a, S> {
    fn drop(&mut self) {
        self.scope.rt.shutdown();
    }
}

impl<'a, S: System> Deref for SystemScopedRuntime<'a, S> {
    type Target = RuntimeScope<'a, S::Rt>;

    fn deref(&self) -> &Self::Target {
        &self.scope
    }
}

impl<'a, S: System> DerefMut for SystemScopedRuntime<'a, S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.scope
    }
}

impl<'a, S: System, H, E> Deref for SupervisedSystemScopedRuntime<'a, S, H, E>
where
    H: 'static + Sender<E> + Clone + Send + Sync,
    E: 'static + SupervisorEvent<Arc<RwLock<S>>> + Send + Sync,
{
    type Target = SystemScopedRuntime<'a, S>;

    fn deref(&self) -> &Self::Target {
        &self.scope
    }
}

impl<'a, S: System, H, E> DerefMut for SupervisedSystemScopedRuntime<'a, S, H, E>
where
    H: 'static + Sender<E> + Clone + Send + Sync,
    E: 'static + SupervisorEvent<Arc<RwLock<S>>> + Send + Sync,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.scope
    }
}

/// A scope for an actor pool, which only allows spawning of the specified actor
pub struct ScopedActorPool<'a, 'b, Rt: BaseRuntime, A: Actor, H, E>
where
    H: 'static + Sender<E> + Clone + Send + Sync,
    E: 'static + SupervisorEvent<A> + Send + Sync,
{
    scope: &'a mut RuntimeScope<'b, Rt>,
    pool: &'a mut ActorPool<A>,
    supervisor_handle: Option<H>,
    _evt: PhantomData<E>,
}

impl<'a, 'b, Rt, A, H, E> ScopedActorPool<'a, 'b, Rt, A, H, E>
where
    H: 'static + Sender<E> + Clone + Send + Sync,
    E: 'static + SupervisorEvent<A> + Send + Sync + std::fmt::Debug + From<A::SupervisorEvent>,
    Rt: 'static + BaseRuntime + Into<A::Rt>,
    A: Actor,
{
    /// Spawn a new actor into this pool
    pub fn spawn(&mut self, actor: A) -> (AbortHandle, <A::Channel as Channel<A::Event>>::Sender)
    where
        A: 'static + Send + Sync,
    {
        let (abort_handle, sender) = self.scope.spawn_actor(actor, self.supervisor_handle.clone());
        self.pool.push(sender.clone());
        (abort_handle, sender)
    }
}
