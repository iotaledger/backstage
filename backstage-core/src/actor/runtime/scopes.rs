use super::*;

pub struct RuntimeScope<'a, Rt> {
    pub rt: &'a mut Rt,
}

impl<'a, Rt: 'static> RuntimeScope<'a, Rt> {
    pub fn new(rt: &'a mut Rt) -> Self {
        Self { rt }
    }

    pub fn spawn_task<F>(&mut self, f: F) -> AbortHandle
    where
        Rt: BaseRuntime,
        for<'b> F: 'static + Send + FnOnce(RuntimeScope<'b, Rt>) -> BoxFuture<'b, anyhow::Result<()>>,
    {
        let child_rt = self.rt.child();
        self.common_spawn_task(child_rt, f)
    }

    pub fn spawn_task_with_runtime<TaskRt, F>(&mut self, f: F) -> AbortHandle
    where
        Rt: BaseRuntime + Into<TaskRt>,
        TaskRt: 'static + BaseRuntime,
        for<'b> F: 'static + Send + FnOnce(RuntimeScope<'b, TaskRt>) -> BoxFuture<'b, anyhow::Result<()>>,
    {
        let child_rt: TaskRt = self.rt.child().into();
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

    pub fn spawn_actor<A, H, E, I: Into<Option<H>>>(
        &mut self,
        actor: A,
        supervisor_handle: I,
    ) -> (AbortHandle, <A::Channel as Channel<A::Event>>::Sender)
    where
        Rt: BaseRuntime + Into<A::Rt>,
        A: 'static + Actor<H, E> + Send + Sync,
        H: 'static + Sender<E> + Clone + Send + Sync,
        E: 'static + SupervisorEvent + Send + Sync,
    {
        let (sender, receiver) = <A::Channel as Channel<A::Event>>::new();
        self.rt.senders_mut().insert(sender.clone());
        let child_rt = self.rt.child().into();
        let abort_handle = self.common_spawn_actor(child_rt, actor, receiver, supervisor_handle);
        (abort_handle, sender)
    }

    pub fn spawn_actor_unsupervised<A>(&mut self, actor: A) -> (AbortHandle, <A::Channel as Channel<A::Event>>::Sender)
    where
        Rt: BaseRuntime + Into<A::Rt>,
        A: 'static + Actor<(), ()> + Send + Sync,
    {
        let (sender, receiver) = <A::Channel as Channel<A::Event>>::new();
        self.rt.senders_mut().insert(sender.clone());
        let child_rt = self.rt.child().into();
        let abort_handle = self.common_spawn_actor(child_rt, actor, receiver, None);
        (abort_handle, sender)
    }

    fn common_spawn_actor<A, H, E, I: Into<Option<H>>>(
        &mut self,
        mut child_rt: A::Rt,
        actor: A,
        receiver: <A::Channel as Channel<A::Event>>::Receiver,
        supervisor_handle: I,
    ) -> AbortHandle
    where
        A: 'static + Actor<H, E> + Send + Sync,
        Rt: BaseRuntime,
        H: 'static + Sender<E> + Clone + Send + Sync,
        E: 'static + SupervisorEvent + Send + Sync,
    {
        let deps = A::Dependencies::instantiate(&mut child_rt)
            .map_err(|e| anyhow::anyhow!("Cannot spawn actor {}: {}", std::any::type_name::<A>(), e))
            .unwrap();
        let (oneshot_send, oneshot_recv) = oneshot::channel::<()>();
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        self.rt.shutdown_handles_mut().push((Some(oneshot_send), abort_handle.clone()));
        let supervisor_handle = supervisor_handle.into();
        let child_task = tokio::spawn(async move {
            let actor_rt = ActorScopedRuntime::new(RuntimeScope::new(&mut child_rt), receiver, oneshot_recv, supervisor_handle);
            let res = Abortable::new(actor.run_then_report(actor_rt, deps), abort_registration).await;
            match res {
                Ok(res) => {
                    child_rt.join().await;
                    res
                }
                Err(_) => {
                    log::debug!("Aborting children of actor!");
                    child_rt.abort();
                    anyhow::bail!("Aborted!")
                }
            }
        });
        self.rt.join_handles_mut().push(child_task);
        abort_handle
    }

    pub fn spawn_system<S, H, E, I: Into<Option<H>>>(
        &mut self,
        system: S,
        supervisor_handle: I,
    ) -> (AbortHandle, <S::Channel as Channel<S::ChildEvents>>::Sender)
    where
        Rt: SystemRuntime + Into<S::Rt>,
        S: 'static + System<H, E> + Send + Sync,
        H: 'static + Sender<E> + Clone + Send + Sync,
        E: 'static + SupervisorEvent + Send + Sync,
    {
        let (sender, receiver) = <S::Channel as Channel<S::ChildEvents>>::new();
        let system = Arc::new(RwLock::new(system));
        self.rt.senders_mut().insert(sender.clone());
        self.rt.systems_mut().insert(system.clone());
        let child_rt = self.rt.child().into();
        let abort_handle = self.common_spawn_system(child_rt, system, receiver, supervisor_handle);
        (abort_handle, sender)
    }

    pub fn spawn_system_unsupervised<S>(&mut self, system: S) -> (AbortHandle, <S::Channel as Channel<S::ChildEvents>>::Sender)
    where
        Rt: SystemRuntime + Into<S::Rt>,
        S: 'static + System<(), ()> + Send + Sync,
    {
        let (sender, receiver) = <S::Channel as Channel<S::ChildEvents>>::new();
        let system = Arc::new(RwLock::new(system));
        self.rt.senders_mut().insert(sender.clone());
        self.rt.systems_mut().insert(system.clone());
        let child_rt = self.rt.child().into();
        let abort_handle = self.common_spawn_system(child_rt, system, receiver, None);
        (abort_handle, sender)
    }

    fn common_spawn_system<S, H, E, I: Into<Option<H>>>(
        &mut self,
        mut child_rt: S::Rt,
        system: Arc<RwLock<S>>,
        receiver: <S::Channel as Channel<S::ChildEvents>>::Receiver,
        supervisor_handle: I,
    ) -> AbortHandle
    where
        S: 'static + System<H, E> + Send + Sync,
        Rt: BaseRuntime,
        H: 'static + Sender<E> + Clone + Send + Sync,
        E: 'static + SupervisorEvent + Send + Sync,
    {
        let deps = S::Dependencies::instantiate(&mut child_rt)
            .map_err(|e| anyhow::anyhow!("Cannot spawn system {}: {}", std::any::type_name::<S>(), e))
            .unwrap();
        let (oneshot_send, oneshot_recv) = oneshot::channel::<()>();
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        self.rt.shutdown_handles_mut().push((Some(oneshot_send), abort_handle.clone()));
        let supervisor_handle = supervisor_handle.into();
        let child_task = tokio::spawn(async move {
            let system_rt = SystemScopedRuntime::new(RuntimeScope::new(&mut child_rt), receiver, oneshot_recv, supervisor_handle);
            let res = Abortable::new(S::run_then_report(system, system_rt, deps), abort_registration).await;
            match res {
                Ok(res) => {
                    child_rt.join().await;
                    res
                }
                Err(_) => {
                    log::debug!("Aborting children of system!");
                    child_rt.abort();
                    anyhow::bail!("Aborted!")
                }
            }
        });
        self.rt.join_handles_mut().push(child_task);
        abort_handle
    }

    pub fn spawn_pool<A: 'static + Actor<H, E>, H, E, I: Into<Option<H>>, F: FnOnce(&mut ScopedActorPool<Rt, A, H, E>)>(
        &mut self,
        supervisor_handle: I,
        f: F,
    ) where
        Rt: PoolRuntime,
        A: Send + Sync,
        H: 'static + Sender<E> + Clone + Send + Sync,
        E: 'static + SupervisorEvent + Send + Sync,
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

    pub fn add_resource<R: 'static + Send + Sync>(&mut self, resource: R) -> Res<R>
    where
        Rt: ResourceRuntime,
    {
        let res = Arc::new(resource);
        self.rt.resources_mut().insert(res.clone());
        Res(res)
    }

    pub fn resource<R: 'static + Send + Sync>(&self) -> Option<Res<R>>
    where
        Rt: ResourceRuntime,
    {
        self.rt.resource()
    }

    pub fn resource_ref<R: 'static + Send + Sync>(&self) -> Option<&R>
    where
        Rt: ResourceRuntime,
    {
        self.rt.resource_ref()
    }

    pub fn system<S: 'static + System<H, E> + Send + Sync, H, E>(&self) -> Option<Sys<S, H, E>>
    where
        Rt: SystemRuntime,
        H: 'static + Sender<E> + Clone + Send + Sync,
        E: 'static + SupervisorEvent + Send + Sync,
    {
        self.rt.system()
    }

    pub fn pool<A, H, E>(&self) -> Option<ResMut<ActorPool<A, H, E>>>
    where
        Rt: PoolRuntime,
        A: 'static + Actor<H, E> + Send + Sync,
        H: 'static + Sender<E> + Clone + Send + Sync,
        E: 'static + SupervisorEvent + Send + Sync,
    {
        self.rt.pool::<A, H, E>()
    }

    pub async fn send_actor_event<A: Actor<H, E>, H, E>(&mut self, event: A::Event) -> anyhow::Result<()>
    where
        Rt: BaseRuntime,
        H: 'static + Sender<E> + Clone + Send + Sync,
        E: 'static + SupervisorEvent + Send + Sync,
    {
        self.rt.send_actor_event::<A, H, E>(event).await
    }

    pub async fn send_system_event<S: System<H, E>, H, E>(&mut self, event: S::ChildEvents) -> anyhow::Result<()>
    where
        Rt: SystemRuntime,
        H: 'static + Sender<E> + Clone + Send + Sync,
        E: 'static + SupervisorEvent + Send + Sync,
    {
        self.rt.send_system_event::<S, H, E>(event).await
    }
}

pub struct ActorScopedRuntime<'a, A: Actor<H, E>, H, E>
where
    A::Event: 'static,
    H: 'static + Sender<E> + Clone + Send + Sync,
    E: 'static + SupervisorEvent + Send + Sync,
{
    scope: RuntimeScope<'a, A::Rt>,
    receiver: ShutdownStream<<A::Channel as Channel<A::Event>>::Receiver>,
    supervisor_handle: Option<H>,
    _event: PhantomData<E>,
}

impl<'a, A: Actor<H, E>, H, E> ActorScopedRuntime<'a, A, H, E>
where
    H: 'static + Sender<E> + Clone + Send + Sync,
    E: 'static + SupervisorEvent + Send + Sync,
{
    pub fn new<I: Into<Option<H>>>(
        runtime: RuntimeScope<'a, A::Rt>,
        receiver: <A::Channel as Channel<A::Event>>::Receiver,
        shutdown: oneshot::Receiver<()>,
        supervisor_handle: I,
    ) -> Self {
        Self {
            scope: runtime,
            receiver: ShutdownStream::new(shutdown, receiver),
            supervisor_handle: supervisor_handle.into(),
            _event: PhantomData,
        }
    }

    pub fn supervisor_handle(&mut self) -> &mut Option<H> {
        &mut self.supervisor_handle
    }

    pub fn child(&self) -> A::Rt {
        self.scope.rt.child()
    }

    pub async fn next_event(&mut self) -> Option<A::Event> {
        self.receiver.next().await
    }

    pub fn my_handle(&self) -> Act<A, H, E> {
        self.scope.rt.actor_event_handle::<A, H, E>().unwrap()
    }

    pub fn shutdown(mut self) {
        self.scope.rt.shutdown()
    }
}

impl<'a, A: Actor<H, E>, H, E> Deref for ActorScopedRuntime<'a, A, H, E>
where
    H: 'static + Sender<E> + Clone + Send + Sync,
    E: 'static + SupervisorEvent + Send + Sync,
{
    type Target = RuntimeScope<'a, A::Rt>;

    fn deref(&self) -> &Self::Target {
        &self.scope
    }
}

impl<'a, A: Actor<H, E>, H, E> DerefMut for ActorScopedRuntime<'a, A, H, E>
where
    H: 'static + Sender<E> + Clone + Send + Sync,
    E: 'static + SupervisorEvent + Send + Sync,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.scope
    }
}

impl<'a, A: Actor<H, E>, H, E> Drop for ActorScopedRuntime<'a, A, H, E>
where
    H: 'static + Sender<E> + Clone + Send + Sync,
    E: 'static + SupervisorEvent + Send + Sync,
{
    fn drop(&mut self) {
        self.scope.rt.shutdown();
    }
}

pub struct SystemScopedRuntime<'a, S: System<H, E>, H, E>
where
    S::ChildEvents: 'static,
    H: 'static + Sender<E> + Clone + Send + Sync,
    E: 'static + SupervisorEvent + Send + Sync,
{
    scope: RuntimeScope<'a, S::Rt>,
    receiver: ShutdownStream<<S::Channel as Channel<S::ChildEvents>>::Receiver>,
    supervisor_handle: Option<H>,
    _event: PhantomData<E>,
}

impl<'a, S: System<H, E>, H, E> SystemScopedRuntime<'a, S, H, E>
where
    H: 'static + Sender<E> + Clone + Send + Sync,
    E: 'static + SupervisorEvent + Send + Sync,
{
    pub fn new<I: Into<Option<H>>>(
        runtime: RuntimeScope<'a, S::Rt>,
        receiver: <S::Channel as Channel<S::ChildEvents>>::Receiver,
        shutdown: oneshot::Receiver<()>,
        supervisor_handle: I,
    ) -> Self {
        Self {
            scope: runtime,
            receiver: ShutdownStream::new(shutdown, receiver),
            supervisor_handle: supervisor_handle.into(),
            _event: PhantomData,
        }
    }

    pub fn supervisor_handle(&mut self) -> &mut Option<H> {
        &mut self.supervisor_handle
    }

    pub fn child(&self) -> S::Rt {
        self.scope.rt.child()
    }

    pub async fn next_event(&mut self) -> Option<S::ChildEvents> {
        self.receiver.next().await
    }

    pub fn my_handle(&self) -> <S::Channel as Channel<S::ChildEvents>>::Sender {
        self.scope.rt.system_event_handle::<S, H, E>().unwrap()
    }

    pub fn children(&mut self) -> SupervisedScope<'_, 'a, S::Rt, <S::Channel as Channel<S::ChildEvents>>::Sender, S::ChildEvents>
    where
        S::ChildEvents: SupervisorEvent,
    {
        SupervisedScope {
            scope: &mut self.scope,
            _data: PhantomData,
        }
    }

    pub fn sublings(&mut self) -> SupervisedScope<'_, 'a, S::Rt, H, E>
    where
        S::ChildEvents: SupervisorEvent,
    {
        SupervisedScope {
            scope: &mut self.scope,
            _data: PhantomData,
        }
    }

    pub fn shutdown(mut self) {
        self.scope.rt.shutdown()
    }
}

impl<'a, S: System<H, E>, H, E> Drop for SystemScopedRuntime<'a, S, H, E>
where
    H: 'static + Sender<E> + Clone + Send + Sync,
    E: 'static + SupervisorEvent + Send + Sync,
{
    fn drop(&mut self) {
        self.scope.rt.shutdown();
    }
}

impl<'a, S: System<H, E>, H, E> Deref for SystemScopedRuntime<'a, S, H, E>
where
    H: 'static + Sender<E> + Clone + Send + Sync,
    E: 'static + SupervisorEvent + Send + Sync,
{
    type Target = RuntimeScope<'a, S::Rt>;

    fn deref(&self) -> &Self::Target {
        &self.scope
    }
}

impl<'a, S: System<H, E>, H, E> DerefMut for SystemScopedRuntime<'a, S, H, E>
where
    H: 'static + Sender<E> + Clone + Send + Sync,
    E: 'static + SupervisorEvent + Send + Sync,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.scope
    }
}

pub struct SupervisedScope<'a, 'b, Rt, H, E>
where
    H: 'static + Sender<E> + Clone + Send + Sync,
    E: 'static + SupervisorEvent + Send + Sync,
{
    scope: &'a mut RuntimeScope<'b, Rt>,
    _data: PhantomData<(H, E)>,
}

impl<'a, 'b, Rt, H, E> SupervisedScope<'a, 'b, Rt, H, E>
where
    Rt: 'static,
    H: 'static + Sender<E> + Clone + Send + Sync,
    E: 'static + SupervisorEvent + Send + Sync,
{
    pub fn system<S: 'static + System<H, E> + Send + Sync>(&self) -> Option<Sys<S, H, E>>
    where
        Rt: SystemRuntime,
    {
        self.scope.system()
    }

    pub fn pool<A>(&self) -> Option<ResMut<ActorPool<A, H, E>>>
    where
        Rt: PoolRuntime,
        A: 'static + Actor<H, E> + Send + Sync,
        H: 'static + Sender<E> + Clone + Send + Sync,
        E: 'static + SupervisorEvent + Send + Sync,
    {
        self.scope.pool::<A, H, E>()
    }

    pub async fn send_actor_event<A: Actor<H, E>>(&mut self, event: A::Event) -> anyhow::Result<()>
    where
        Rt: BaseRuntime,
        H: 'static + Sender<E> + Clone + Send + Sync,
        E: 'static + SupervisorEvent + Send + Sync,
    {
        self.scope.send_actor_event::<A, H, E>(event).await
    }

    pub async fn send_system_event<S: System<H, E>>(&mut self, event: S::ChildEvents) -> anyhow::Result<()>
    where
        Rt: SystemRuntime,
        H: 'static + Sender<E> + Clone + Send + Sync,
        E: 'static + SupervisorEvent + Send + Sync,
    {
        self.scope.send_system_event::<S, H, E>(event).await
    }
}

pub struct ScopedActorPool<'a, 'b, Rt: BaseRuntime, A: Actor<H, E>, H, E>
where
    H: 'static + Sender<E> + Clone + Send + Sync,
    E: 'static + SupervisorEvent + Send + Sync,
{
    scope: &'a mut RuntimeScope<'b, Rt>,
    pool: &'a mut ActorPool<A, H, E>,
    supervisor_handle: Option<H>,
    _evt: PhantomData<E>,
}

impl<'a, 'b, Rt, A, H, E> ScopedActorPool<'a, 'b, Rt, A, H, E>
where
    H: 'static + Sender<E> + Clone + Send + Sync,
    E: 'static + SupervisorEvent + Send + Sync,
    Rt: 'static + BaseRuntime + Into<A::Rt>,
    A: Actor<H, E>,
{
    fn child(&self) -> A::Rt {
        self.scope.rt.child().into()
    }

    pub fn spawn(&mut self, actor: A) -> (AbortHandle, <A::Channel as Channel<A::Event>>::Sender)
    where
        A: 'static + Send + Sync,
    {
        let (sender, receiver) = <A::Channel as Channel<A::Event>>::new();
        let child_rt = self.child();
        let abort_handle = self
            .scope
            .common_spawn_actor(child_rt, actor, receiver, self.supervisor_handle.clone());
        self.pool.push(sender.clone());
        (abort_handle, sender)
    }
}
