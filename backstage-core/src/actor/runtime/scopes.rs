use super::*;

pub struct RuntimeScope<'a, Rt> {
    pub rt: &'a mut Rt,
}

impl<'a, Rt: 'static> RuntimeScope<'a, Rt> {
    pub fn new(rt: &'a mut Rt) -> Self {
        Self { rt }
    }

    pub fn with_runtime<'p, ChildRt>(&'p mut self) -> ChildRuntimeScope<'p, 'a, Rt, ChildRt>
    where
        Rt: BaseRuntime + Into<ChildRt>,
    {
        ChildRuntimeScope {
            scope: self,
            _child_rt: PhantomData::<ChildRt>,
        }
    }

    pub fn spawn_task<F>(&mut self, f: F) -> AbortHandle
    where
        Rt: BaseRuntime,
        for<'b> F: 'static + Send + FnOnce(RuntimeScope<'b, Rt>) -> BoxFuture<'b, anyhow::Result<()>>,
    {
        let child_rt = self.rt.child();
        self.spawn_task_with_runtime(child_rt, f)
    }

    pub fn spawn_task_with_runtime<ChildRt, F>(&mut self, mut child_rt: ChildRt, f: F) -> AbortHandle
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
                    child_rt.shutdown();
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
        Rt: BaseRuntime,
        A: 'static + Actor<Rt, H, E> + Send + Sync,
        H: 'static + Sender<E> + Clone + Send + Sync,
        E: 'static + SupervisorEvent + Send + Sync,
    {
        let (sender, receiver) = <A::Channel as Channel<A::Event>>::new();
        self.rt.senders_mut().insert(sender.clone());
        let child_rt = self.rt.child();
        let abort_handle = self.spawn_actor_with_runtime(child_rt, actor, receiver, supervisor_handle);
        (abort_handle, sender)
    }

    pub fn spawn_actor_without_supervisor<A>(&mut self, actor: A) -> (AbortHandle, <A::Channel as Channel<A::Event>>::Sender)
    where
        Rt: BaseRuntime,
        A: 'static + Actor<Rt, (), ()> + Send + Sync,
    {
        let (sender, receiver) = <A::Channel as Channel<A::Event>>::new();
        self.rt.senders_mut().insert(sender.clone());
        let child_rt = self.rt.child();
        let abort_handle = self.spawn_actor_with_runtime(child_rt, actor, receiver, None);
        (abort_handle, sender)
    }

    pub fn spawn_actor_with_runtime<ChildRt, A, H, E, I: Into<Option<H>>>(
        &mut self,
        mut child_rt: ChildRt,
        actor: A,
        receiver: <A::Channel as Channel<A::Event>>::Receiver,
        supervisor_handle: I,
    ) -> AbortHandle
    where
        A: 'static + Actor<ChildRt, H, E> + Send + Sync,
        Rt: BaseRuntime,
        ChildRt: 'static + BaseRuntime,
        H: 'static + Sender<E> + Clone + Send + Sync,
        E: 'static + SupervisorEvent + Send + Sync,
    {
        let deps = <A::Dependencies as Dependencies<ChildRt>>::instantiate(&mut child_rt)
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
                    child_rt.shutdown();
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
        Rt: SystemRuntime,
        S: 'static + System<Rt, H, E> + Send + Sync,
        H: 'static + Sender<E> + Clone + Send + Sync,
        E: 'static + SupervisorEvent + Send + Sync,
    {
        let (sender, receiver) = <S::Channel as Channel<S::ChildEvents>>::new();
        let system = Arc::new(RwLock::new(system));
        self.rt.senders_mut().insert(sender.clone());
        self.rt.systems_mut().insert(system.clone());
        let child_rt = self.rt.child();
        let abort_handle = self.spawn_system_with_runtime(child_rt, system, receiver, supervisor_handle);
        (abort_handle, sender)
    }

    pub fn spawn_system_without_supervisor<S>(&mut self, system: S) -> (AbortHandle, <S::Channel as Channel<S::ChildEvents>>::Sender)
    where
        Rt: SystemRuntime,
        S: 'static + System<Rt, (), ()> + Send + Sync,
    {
        let (sender, receiver) = <S::Channel as Channel<S::ChildEvents>>::new();
        let system = Arc::new(RwLock::new(system));
        self.rt.senders_mut().insert(sender.clone());
        self.rt.systems_mut().insert(system.clone());
        let child_rt = self.rt.child();
        let abort_handle = self.spawn_system_with_runtime(child_rt, system, receiver, None);
        (abort_handle, sender)
    }

    pub fn spawn_system_with_runtime<ChildRt, S, H, E, I: Into<Option<H>>>(
        &mut self,
        mut child_rt: ChildRt,
        system: Arc<RwLock<S>>,
        receiver: <S::Channel as Channel<S::ChildEvents>>::Receiver,
        supervisor_handle: I,
    ) -> AbortHandle
    where
        S: 'static + System<ChildRt, H, E> + Send + Sync,
        Rt: BaseRuntime,
        ChildRt: 'static + SystemRuntime,
        H: 'static + Sender<E> + Clone + Send + Sync,
        E: 'static + SupervisorEvent + Send + Sync,
    {
        let deps = <S::Dependencies as Dependencies<ChildRt>>::instantiate(&mut child_rt)
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
                    child_rt.shutdown();
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

    pub fn spawn_pool<A: 'static + Actor<Rt, H, E>, H, E, I: Into<Option<H>>, F: FnOnce(&mut ScopedActorPool<Rt, Rt, A, H, E>)>(
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

    pub fn spawn_pool_with_runtime<
        ChildRt,
        A: 'static + Actor<ChildRt, H, E>,
        H,
        E,
        I: Into<Option<H>>,
        F: FnOnce(&mut ScopedActorPool<Rt, ChildRt, A, H, E>),
    >(
        &mut self,
        supervisor_handle: I,
        f: F,
    ) where
        Rt: PoolRuntime,
        ChildRt: 'static + BaseRuntime,
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

    pub fn system<S: 'static + System<Rt, H, E> + Send + Sync, H, E>(&self) -> Option<Sys<Rt, S, H, E>>
    where
        Rt: SystemRuntime,
        H: 'static + Sender<E> + Clone + Send + Sync,
        E: 'static + SupervisorEvent + Send + Sync,
    {
        self.rt.system()
    }

    pub fn pool<ChildRt, A, H, E>(&self) -> Option<ResMut<ActorPool<ChildRt, A, H, E>>>
    where
        Rt: PoolRuntime,
        ChildRt: 'static + BaseRuntime,
        A: 'static + Actor<ChildRt, H, E> + Send + Sync,
        H: 'static + Sender<E> + Clone + Send + Sync,
        E: 'static + SupervisorEvent + Send + Sync,
    {
        self.rt.pool::<ChildRt, A, H, E>()
    }

    pub async fn send_actor_event<A: Actor<Rt, H, E>, H, E>(&mut self, event: A::Event) -> anyhow::Result<()>
    where
        Rt: BaseRuntime,
        H: 'static + Sender<E> + Clone + Send + Sync,
        E: 'static + SupervisorEvent + Send + Sync,
    {
        self.rt.send_actor_event::<A, H, E>(event).await
    }

    pub async fn send_system_event<S: System<Rt, H, E>, H, E>(&mut self, event: S::ChildEvents) -> anyhow::Result<()>
    where
        Rt: SystemRuntime,
        H: 'static + Sender<E> + Clone + Send + Sync,
        E: 'static + SupervisorEvent + Send + Sync,
    {
        self.rt.send_system_event::<S, H, E>(event).await
    }
}

pub struct ChildRuntimeScope<'a, 's, Rt, ChildRt>
where
    Rt: Into<ChildRt>,
{
    scope: &'a mut RuntimeScope<'s, Rt>,
    _child_rt: PhantomData<ChildRt>,
}

impl<'a, 's, Rt, ChildRt> ChildRuntimeScope<'a, 's, Rt, ChildRt>
where
    Rt: 'static + Into<ChildRt>,
    ChildRt: 'static,
{
    pub fn spawn_task<F>(&'a mut self, f: F) -> AbortHandle
    where
        for<'b> F: 'static + Send + FnOnce(RuntimeScope<'b, ChildRt>) -> BoxFuture<'b, anyhow::Result<()>>,
        Rt: BaseRuntime,
        ChildRt: BaseRuntime,
    {
        let child_rt: ChildRt = self.scope.rt.child().into();
        self.scope.spawn_task_with_runtime(child_rt, f)
    }

    pub fn spawn_actor<A, H, E, I: Into<Option<H>>>(
        &mut self,
        actor: A,
        supervisor_handle: I,
    ) -> (AbortHandle, <A::Channel as Channel<A::Event>>::Sender)
    where
        A: 'static + Actor<ChildRt, H, E> + Send + Sync,
        H: 'static + Sender<E> + Clone + Send + Sync,
        E: 'static + SupervisorEvent + Send + Sync,
        Rt: BaseRuntime,
        ChildRt: BaseRuntime,
    {
        let (sender, receiver) = <A::Channel as Channel<A::Event>>::new();
        self.scope.rt.senders_mut().insert(sender.clone());
        let mut child_rt: ChildRt = self.scope.rt.child().into();
        let abort_handle = self.scope.spawn_actor_with_runtime(child_rt, actor, receiver, supervisor_handle);
        (abort_handle, sender)
    }

    pub fn spawn_system<S, H, E, I: Into<Option<H>>>(
        &mut self,
        system: S,
        supervisor_handle: I,
    ) -> (AbortHandle, <S::Channel as Channel<S::ChildEvents>>::Sender)
    where
        Rt: SystemRuntime,
        ChildRt: SystemRuntime,
        S: 'static + System<ChildRt, H, E> + Send + Sync,
        H: 'static + Sender<E> + Clone + Send + Sync,
        E: 'static + SupervisorEvent + Send + Sync,
    {
        let (sender, receiver) = <S::Channel as Channel<S::ChildEvents>>::new();
        let system = Arc::new(RwLock::new(system));
        self.scope.rt.senders_mut().insert(sender.clone());
        self.scope.rt.systems_mut().insert(system.clone());
        let mut child_rt: ChildRt = self.scope.rt.child().into();
        let abort_handle = self.scope.spawn_system_with_runtime(child_rt, system, receiver, supervisor_handle);
        (abort_handle, sender)
    }

    pub fn spawn_pool<A, H, E, I, F>(&mut self, supervisor_handle: I, f: F)
    where
        Rt: PoolRuntime,
        ChildRt: BaseRuntime,
        H: 'static + Sender<E> + Clone + Send + Sync,
        E: 'static + SupervisorEvent + Send + Sync,
        A: 'static + Actor<ChildRt, H, E> + Send + Sync,
        I: Into<Option<H>>,
        F: FnOnce(&mut ScopedActorPool<Rt, ChildRt, A, H, E>),
    {
        self.scope.spawn_pool_with_runtime(supervisor_handle, |rt| f(rt));
    }
}

pub struct ActorScopedRuntime<'a, A: Actor<Rt, H, E>, Rt: BaseRuntime, H, E>
where
    A::Event: 'static,
    H: 'static + Sender<E> + Clone + Send + Sync,
    E: 'static + SupervisorEvent + Send + Sync,
{
    pub scope: RuntimeScope<'a, Rt>,
    receiver: ShutdownStream<<A::Channel as Channel<A::Event>>::Receiver>,
    supervisor_handle: Option<H>,
    _event: PhantomData<E>,
}

impl<'a, A: Actor<Rt, H, E>, Rt: BaseRuntime, H, E> ActorScopedRuntime<'a, A, Rt, H, E>
where
    H: 'static + Sender<E> + Clone + Send + Sync,
    E: 'static + SupervisorEvent + Send + Sync,
{
    pub fn new<I: Into<Option<H>>>(
        runtime: RuntimeScope<'a, Rt>,
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

    pub fn with_runtime<'p, ChildRt>(&'p mut self) -> ChildRuntimeScope<'p, 'a, Rt, ChildRt>
    where
        Rt: Into<ChildRt>,
    {
        ChildRuntimeScope {
            scope: &mut self.scope,
            _child_rt: PhantomData::<ChildRt>,
        }
    }

    pub async fn send_actor_event<OtherA: Actor<Rt, H, E>>(&mut self, event: OtherA::Event) -> anyhow::Result<()>
    where
        Rt: BaseRuntime,
        H: 'static + Sender<E> + Clone + Send + Sync,
        E: 'static + SupervisorEvent + Send + Sync,
    {
        self.scope.rt.send_actor_event::<OtherA, H, E>(event).await
    }

    pub async fn send_system_event<S: System<Rt, H, E>>(&mut self, event: S::ChildEvents) -> anyhow::Result<()>
    where
        Rt: SystemRuntime,
        H: 'static + Sender<E> + Clone + Send + Sync,
        E: 'static + SupervisorEvent + Send + Sync,
    {
        self.scope.rt.send_system_event::<S, H, E>(event).await
    }

    pub fn child(&self) -> Rt {
        self.scope.rt.child()
    }

    pub async fn next_event(&mut self) -> Option<A::Event> {
        self.receiver.next().await
    }

    pub fn my_handle(&self) -> Act<Rt, A, H, E> {
        self.scope.rt.actor_event_handle::<A, H, E>().unwrap()
    }

    pub fn shutdown(mut self) {
        self.scope.rt.shutdown()
    }

    pub fn add_resource<R: 'static + Send + Sync>(&mut self, resource: R) -> Res<R>
    where
        Rt: 'static + ResourceRuntime,
    {
        self.scope.add_resource(resource)
    }

    pub fn resource<R: 'static + Send + Sync>(&self) -> Option<Res<R>>
    where
        Rt: 'static + ResourceRuntime,
    {
        self.scope.resource()
    }

    pub fn resource_ref<R: 'static + Send + Sync>(&self) -> Option<&R>
    where
        Rt: 'static + ResourceRuntime,
    {
        self.scope.resource_ref()
    }

    pub fn system<S: 'static + System<Rt, <A::Channel as Channel<A::Event>>::Sender, A::Event> + Send + Sync>(
        &self,
    ) -> Option<Sys<Rt, S, <A::Channel as Channel<A::Event>>::Sender, A::Event>>
    where
        Rt: 'static + SystemRuntime,
        A::Event: SupervisorEvent,
    {
        self.scope.system()
    }

    pub fn pool<OtherA>(&self) -> Option<ResMut<ActorPool<Rt, OtherA, <A::Channel as Channel<A::Event>>::Sender, A::Event>>>
    where
        Rt: 'static + PoolRuntime,
        OtherA: 'static + Actor<Rt, <A::Channel as Channel<A::Event>>::Sender, A::Event> + Send + Sync,
        A::Event: SupervisorEvent,
    {
        self.scope.pool()
    }
}

impl<'a, A: Actor<Rt, H, E>, Rt: BaseRuntime, H, E> Drop for ActorScopedRuntime<'a, A, Rt, H, E>
where
    H: 'static + Sender<E> + Clone + Send + Sync,
    E: 'static + SupervisorEvent + Send + Sync,
{
    fn drop(&mut self) {
        self.scope.rt.shutdown();
    }
}

pub struct SystemScopedRuntime<'a, S: System<Rt, H, E>, Rt: SystemRuntime, H, E>
where
    S::ChildEvents: 'static,
    H: 'static + Sender<E> + Clone + Send + Sync,
    E: 'static + SupervisorEvent + Send + Sync,
{
    pub scope: RuntimeScope<'a, Rt>,
    receiver: ShutdownStream<<S::Channel as Channel<S::ChildEvents>>::Receiver>,
    supervisor_handle: Option<H>,
    _event: PhantomData<E>,
}

impl<'a, S: System<Rt, H, E>, Rt: SystemRuntime, H, E> SystemScopedRuntime<'a, S, Rt, H, E>
where
    H: 'static + Sender<E> + Clone + Send + Sync,
    E: 'static + SupervisorEvent + Send + Sync,
{
    pub fn new<I: Into<Option<H>>>(
        runtime: RuntimeScope<'a, Rt>,
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

    pub fn with_runtime<'p, ChildRt>(&'p mut self) -> ChildRuntimeScope<'p, 'a, Rt, ChildRt>
    where
        Rt: Into<ChildRt>,
    {
        ChildRuntimeScope {
            scope: &mut self.scope,
            _child_rt: PhantomData::<ChildRt>,
        }
    }

    pub async fn send_actor_event<A: Actor<Rt, H, E>>(&mut self, event: A::Event) -> anyhow::Result<()>
    where
        Rt: BaseRuntime,
        H: 'static + Sender<E> + Clone + Send + Sync,
        E: 'static + SupervisorEvent + Send + Sync,
    {
        self.scope.rt.send_actor_event::<A, H, E>(event).await
    }

    pub async fn send_system_event<OtherS: System<Rt, H, E>>(&mut self, event: OtherS::ChildEvents) -> anyhow::Result<()>
    where
        Rt: SystemRuntime,
        H: 'static + Sender<E> + Clone + Send + Sync,
        E: 'static + SupervisorEvent + Send + Sync,
    {
        self.scope.rt.send_system_event::<OtherS, H, E>(event).await
    }

    pub fn child(&self) -> Rt {
        self.scope.rt.child()
    }

    pub async fn next_event(&mut self) -> Option<S::ChildEvents> {
        self.receiver.next().await
    }

    pub fn my_handle(&self) -> <S::Channel as Channel<S::ChildEvents>>::Sender {
        self.scope.rt.system_event_handle::<S, H, E>().unwrap()
    }

    pub fn shutdown(mut self) {
        self.scope.rt.shutdown()
    }

    pub fn add_resource<R: 'static + Send + Sync>(&mut self, resource: R) -> Res<R>
    where
        Rt: 'static + ResourceRuntime,
    {
        self.scope.add_resource(resource)
    }

    pub fn resource<R: 'static + Send + Sync>(&self) -> Option<Res<R>>
    where
        Rt: 'static + ResourceRuntime,
    {
        self.scope.resource()
    }

    pub fn resource_ref<R: 'static + Send + Sync>(&self) -> Option<&R>
    where
        Rt: 'static + ResourceRuntime,
    {
        self.scope.resource_ref()
    }

    pub fn system<OtherS: 'static + System<Rt, <S::Channel as Channel<S::ChildEvents>>::Sender, S::ChildEvents> + Send + Sync>(
        &self,
    ) -> Option<Sys<Rt, OtherS, <S::Channel as Channel<S::ChildEvents>>::Sender, S::ChildEvents>>
    where
        Rt: 'static + SystemRuntime,
        S::ChildEvents: SupervisorEvent,
    {
        self.scope.system()
    }

    pub fn pool<A, ChildRt>(&self) -> Option<ResMut<ActorPool<ChildRt, A, <S::Channel as Channel<S::ChildEvents>>::Sender, S::ChildEvents>>>
    where
        Rt: 'static + PoolRuntime,
        A: 'static + Actor<ChildRt, <S::Channel as Channel<S::ChildEvents>>::Sender, S::ChildEvents> + Send + Sync,
        ChildRt: 'static + BaseRuntime,
        S::ChildEvents: SupervisorEvent,
    {
        self.scope.pool()
    }
}

impl<'a, S: System<Rt, H, E>, Rt: SystemRuntime, H, E> Drop for SystemScopedRuntime<'a, S, Rt, H, E>
where
    H: 'static + Sender<E> + Clone + Send + Sync,
    E: 'static + SupervisorEvent + Send + Sync,
{
    fn drop(&mut self) {
        self.scope.rt.shutdown();
    }
}
