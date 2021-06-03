use crate::{actor::shutdown_stream::ShutdownStream, Actor, ActorError, ActorRequest, Channel, Dependencies, Sender, System};
use anymap::{any::CloneAny, Map};
use async_trait::async_trait;
use futures::{
    future::{AbortHandle, AbortRegistration, Abortable, BoxFuture},
    StreamExt,
};
use lru::LruCache;
use std::{
    marker::PhantomData,
    ops::{Deref, DerefMut},
    sync::Arc,
};
use tokio::{
    sync::{oneshot, RwLock},
    task::{JoinError, JoinHandle},
};

pub use basic_runtime::*;
pub use full_runtime::*;
pub use system_runtime::*;

mod basic_runtime;
mod full_runtime;
mod system_runtime;

#[async_trait]
pub trait BaseRuntime: Send + Sync {
    fn child(&self) -> Self;

    fn join_handles(&self) -> &Vec<JoinHandle<Result<(), ActorError>>>;

    fn join_handles_mut(&mut self) -> &mut Vec<JoinHandle<Result<(), ActorError>>>;

    fn shutdown_handles(&self) -> &Vec<(Option<oneshot::Sender<()>>, AbortHandle)>;

    fn shutdown_handles_mut(&mut self) -> &mut Vec<(Option<oneshot::Sender<()>>, AbortHandle)>;

    fn senders(&self) -> &Map<dyn CloneAny + Send + Sync>;

    fn senders_mut(&mut self) -> &mut Map<dyn CloneAny + Send + Sync>;

    fn shutdown(&mut self) {
        for handle in self.shutdown_handles_mut().iter_mut() {
            handle.0.take().map(|h| h.send(()));
        }
    }

    async fn scope<O: Send + Sync, F: Send + FnOnce(&mut RuntimeScope<'_, Self>) -> O>(&mut self, f: F) -> anyhow::Result<O>
    where
        Self: 'static + Sized,
    {
        let res = f(&mut RuntimeScope(self));
        self.join().await?;
        Ok(res)
    }

    async fn join(&mut self) -> Result<Vec<Result<(), ActorError>>, JoinError> {
        let mut results = Vec::new();
        for handle in self.join_handles_mut().drain(..) {
            results.push(handle.await?);
        }
        Ok(results)
    }

    fn abort(mut self)
    where
        Self: Sized,
    {
        for handles in self.shutdown_handles_mut().iter_mut() {
            if let Some(shutdown_handle) = handles.0.take() {
                shutdown_handle.send(());
            } else {
                handles.1.abort();
            }
        }
    }

    fn event_handle<H: 'static + Clone + Send + Sync>(&self) -> Option<H> {
        self.senders().get::<H>().map(|handle| handle.clone())
    }

    fn actor_event_handle<A: Actor<Self>>(&self) -> Option<Act<Self, A>>
    where
        Self: Sized,
        <A::Channel as Channel<A::Event>>::Sender: 'static,
    {
        self.senders().get::<<A::Channel as Channel<A::Event>>::Sender>().map(|handle| Act {
            actor: handle.clone(),
            _rt: PhantomData::<Self>,
        })
    }

    async fn send_actor_event<A: Actor<Self>>(&mut self, event: A::Event) -> anyhow::Result<()>
    where
        Self: Sized,
        <A::Channel as Channel<A::Event>>::Sender: 'static,
    {
        let handle = self
            .senders_mut()
            .get_mut::<<A::Channel as Channel<A::Event>>::Sender>()
            .ok_or_else(|| anyhow::anyhow!("No channel for this actor!"))?;
        Sender::<A::Event>::send(handle, event).await
    }

    fn consume(self) -> BasicRuntime
    where
        Self: Into<BasicRuntime>,
    {
        self.into()
    }
}

#[async_trait]
pub trait SystemRuntime: BaseRuntime {
    fn systems(&self) -> &Map<dyn CloneAny + Send + Sync>;

    fn systems_mut(&mut self) -> &mut Map<dyn CloneAny + Send + Sync>;

    fn system<S: 'static + System<Self> + Send + Sync>(&self) -> Option<Sys<Self, S>>
    where
        Self: Sized,
    {
        self.systems().get::<Arc<RwLock<S>>>().map(|sys| Sys {
            system: sys.clone(),
            _rt: PhantomData::<Self>,
        })
    }

    fn system_event_handle<S: System<Self>>(&self) -> Option<<S::Channel as Channel<S::ChildEvents>>::Sender>
    where
        Self: Sized,
        <S::Channel as Channel<S::ChildEvents>>::Sender: 'static,
    {
        self.senders()
            .get::<<S::Channel as Channel<S::ChildEvents>>::Sender>()
            .map(|handle| handle.clone())
    }

    async fn send_system_event<S: System<Self>>(&mut self, event: S::ChildEvents) -> anyhow::Result<()>
    where
        Self: Sized,
        <S::Channel as Channel<S::ChildEvents>>::Sender: 'static,
    {
        let handle = self
            .senders_mut()
            .get_mut::<<S::Channel as Channel<S::ChildEvents>>::Sender>()
            .ok_or_else(|| anyhow::anyhow!("No channel for this actor!"))?;
        Sender::<S::ChildEvents>::send(handle, event).await
    }

    fn consume(self) -> SystemsRuntime
    where
        Self: Into<SystemsRuntime>,
    {
        self.into()
    }
}

pub trait ResourceRuntime: BaseRuntime {
    fn resources(&self) -> &Map<dyn CloneAny + Send + Sync>;

    fn resources_mut(&mut self) -> &mut Map<dyn CloneAny + Send + Sync>;

    fn resource<R: 'static + Send + Sync>(&self) -> Option<Res<R>> {
        self.resources().get::<Arc<R>>().map(|res| Res(res.clone()))
    }

    fn resource_ref<R: 'static + Send + Sync>(&self) -> Option<&R> {
        self.resources().get::<Arc<R>>().map(Arc::as_ref)
    }
}

pub trait PoolRuntime: BaseRuntime {
    fn pools(&self) -> &Map<dyn CloneAny + Send + Sync>;

    fn pools_mut(&mut self) -> &mut Map<dyn CloneAny + Send + Sync>;

    fn pool<A: Actor<Self>>(&self) -> Option<ResMut<ActorPool<'static, <A::Channel as Channel<A::Event>>::Sender>>>
    where
        Self: Sized,
    {
        self.pools()
            .get::<Arc<RwLock<ActorPool<<A::Channel as Channel<A::Event>>::Sender>>>>()
            .map(|pool| ResMut(pool.clone()))
    }
}

pub struct RuntimeScope<'a, Rt>(pub &'a mut Rt);

pub struct ChildRuntimeScope<'a, 's, Rt, ChildRt>
where
    Rt: Into<ChildRt>,
{
    scope: &'a mut RuntimeScope<'s, Rt>,
    _child_rt: PhantomData<ChildRt>,
}

impl<'a, Rt: 'static + BaseRuntime> RuntimeScope<'a, Rt> {
    pub fn with_runtime<'p, ChildRt>(&'p mut self) -> ChildRuntimeScope<'p, 'a, Rt, ChildRt>
    where
        Rt: Into<ChildRt>,
    {
        ChildRuntimeScope {
            scope: self,
            _child_rt: PhantomData::<ChildRt>,
        }
    }

    pub fn spawn_task<F>(&mut self, f: F) -> AbortHandle
    where
        for<'b> F: 'static + Send + FnOnce(RuntimeScope<'b, Rt>) -> BoxFuture<'b, Result<(), ActorError>>,
    {
        let child_rt = self.0.child();
        self.spawn_task_with_runtime(child_rt, f)
    }

    fn spawn_task_with_runtime<ChildRt, F>(&mut self, mut child_rt: ChildRt, f: F) -> AbortHandle
    where
        for<'b> F: 'static + Send + FnOnce(RuntimeScope<'b, ChildRt>) -> BoxFuture<'b, Result<(), ActorError>>,
        ChildRt: 'static + BaseRuntime,
    {
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        let child_task = tokio::spawn(async move {
            let scope = RuntimeScope(&mut child_rt);
            let res = Abortable::new(f(scope), abort_registration).await;
            match res {
                Ok(res) => {
                    child_rt.join().await;
                    res
                }
                Err(_) => {
                    log::debug!("Aborting children of task!");
                    child_rt.abort();
                    Err(ActorError::Other {
                        source: anyhow::anyhow!("Aborted!"),
                        request: ActorRequest::Finish,
                    })
                }
            }
        });
        self.0.join_handles_mut().push(child_task);
        self.0.shutdown_handles_mut().push((None, abort_handle.clone()));
        abort_handle
    }

    pub fn spawn_actor<A>(&mut self, actor: A) -> (AbortHandle, <A::Channel as Channel<A::Event>>::Sender)
    where
        A: 'static + Actor<Rt> + Send + Sync,
    {
        let (sender, receiver) = <A::Channel as Channel<A::Event>>::new();
        self.0.senders_mut().insert(sender.clone());
        let mut child_rt = self.0.child();
        let abort_handle = self.spawn_actor_with_runtime(child_rt, actor, receiver);
        (abort_handle, sender)
    }

    fn spawn_actor_with_runtime<ChildRt, A>(
        &mut self,
        mut child_rt: ChildRt,
        actor: A,
        receiver: <A::Channel as Channel<A::Event>>::Receiver,
    ) -> AbortHandle
    where
        A: 'static + Actor<ChildRt> + Send + Sync,
        ChildRt: 'static + BaseRuntime,
    {
        let deps = <A::Dependencies as Dependencies<ChildRt>>::instantiate(&mut child_rt)
            .map_err(|e| anyhow::anyhow!("Cannot spawn actor {}: {}", std::any::type_name::<A>(), e))
            .unwrap();
        let (oneshot_send, oneshot_recv) = oneshot::channel::<()>();
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        self.0.shutdown_handles_mut().push((Some(oneshot_send), abort_handle.clone()));
        let child_task = tokio::spawn(async move {
            let actor_rt = ActorScopedRuntime::new(RuntimeScope(&mut child_rt), receiver, oneshot_recv);
            let res = Abortable::new(actor.run(actor_rt, deps), abort_registration).await;
            match res {
                Ok(res) => {
                    child_rt.join().await;
                    res
                }
                Err(_) => {
                    log::debug!("Aborting children of actor!");
                    child_rt.abort();
                    Err(ActorError::Other {
                        source: anyhow::anyhow!("Aborted!"),
                        request: ActorRequest::Finish,
                    })
                }
            }
        });
        self.0.join_handles_mut().push(child_task);
        abort_handle
    }

    pub fn spawn_system<S>(&mut self, system: S) -> (AbortHandle, <S::Channel as Channel<S::ChildEvents>>::Sender)
    where
        Rt: SystemRuntime,
        S: 'static + System<Rt> + Send + Sync,
    {
        let (sender, receiver) = <S::Channel as Channel<S::ChildEvents>>::new();
        let system = Arc::new(RwLock::new(system));
        self.0.senders_mut().insert(sender.clone());
        self.0.systems_mut().insert(system.clone());
        let mut child_rt = self.0.child();
        let abort_handle = self.spawn_system_with_runtime(child_rt, system, receiver);
        (abort_handle, sender)
    }

    fn spawn_system_with_runtime<ChildRt, S>(
        &mut self,
        mut child_rt: ChildRt,
        system: Arc<RwLock<S>>,
        receiver: <S::Channel as Channel<S::ChildEvents>>::Receiver,
    ) -> AbortHandle
    where
        S: 'static + System<ChildRt> + Send + Sync,
        ChildRt: 'static + SystemRuntime,
    {
        let deps = <S::Dependencies as Dependencies<ChildRt>>::instantiate(&mut child_rt)
            .map_err(|e| anyhow::anyhow!("Cannot spawn system {}: {}", std::any::type_name::<S>(), e))
            .unwrap();
        let (oneshot_send, oneshot_recv) = oneshot::channel::<()>();
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        self.0.shutdown_handles_mut().push((Some(oneshot_send), abort_handle.clone()));
        let child_task = tokio::spawn(async move {
            let system_rt = SystemScopedRuntime::new(RuntimeScope((&mut child_rt).into()), receiver, oneshot_recv);
            let res = Abortable::new(S::run(system, system_rt, deps), abort_registration).await;
            match res {
                Ok(res) => {
                    child_rt.join().await;
                    res
                }
                Err(_) => {
                    log::debug!("Aborting children of system!");
                    child_rt.abort();
                    Err(ActorError::Other {
                        source: anyhow::anyhow!("Aborted!"),
                        request: ActorRequest::Finish,
                    })
                }
            }
        });
        self.0.join_handles_mut().push(child_task);
        abort_handle
    }

    pub fn spawn_pool<H: 'static + Send + Sync, F: FnOnce(&mut ActorPool<'_, H>)>(&'static mut self, f: F)
    where
        Rt: PoolRuntime,
    {
        let mut pool = ActorPool::default();
        f(&mut pool);
        self.0.pools_mut().insert(Arc::new(RwLock::new(pool)));
    }

    pub fn add_resource<R: 'static + Send + Sync>(&mut self, resource: R) -> Res<R>
    where
        Rt: ResourceRuntime,
    {
        let res = Arc::new(resource);
        self.0.resources_mut().insert(res.clone());
        Res(res)
    }

    pub fn resource<R: 'static + Send + Sync>(&self) -> Option<Res<R>>
    where
        Rt: ResourceRuntime,
    {
        self.0.resource()
    }

    pub fn resource_ref<R: 'static + Send + Sync>(&self) -> Option<&R>
    where
        Rt: ResourceRuntime,
    {
        self.0.resource_ref()
    }

    pub fn system<S: 'static + System<Rt> + Send + Sync>(&self) -> Option<Sys<Rt, S>>
    where
        Rt: SystemRuntime,
    {
        self.0.system()
    }

    pub fn pool<A: Actor<Rt>>(&self) -> Option<ResMut<ActorPool<'static, <A::Channel as Channel<A::Event>>::Sender>>>
    where
        Rt: PoolRuntime,
    {
        self.0.pool::<A>()
    }

    pub async fn send_actor_event<A: Actor<Rt>>(&mut self, event: A::Event) -> anyhow::Result<()>
    where
        <A::Channel as Channel<A::Event>>::Sender: 'static,
    {
        self.0.send_actor_event::<A>(event).await
    }

    pub async fn send_system_event<S: System<Rt>>(&mut self, event: S::ChildEvents) -> anyhow::Result<()>
    where
        Rt: SystemRuntime,
        <S::Channel as Channel<S::ChildEvents>>::Sender: 'static,
    {
        self.0.send_system_event::<S>(event).await
    }
}

impl<'a, 's, Rt, ChildRt> ChildRuntimeScope<'a, 's, Rt, ChildRt>
where
    Rt: 'static + BaseRuntime + Into<ChildRt>,
    ChildRt: 'static + BaseRuntime,
{
    pub fn spawn_task<F>(&'a mut self, f: F) -> AbortHandle
    where
        for<'b> F: 'static + Send + FnOnce(RuntimeScope<'b, ChildRt>) -> BoxFuture<'b, Result<(), ActorError>>,
    {
        let child_rt: ChildRt = self.scope.0.child().into();
        self.scope.spawn_task_with_runtime(child_rt, f)
    }

    pub fn spawn_actor<A>(&mut self, actor: A) -> (AbortHandle, <A::Channel as Channel<A::Event>>::Sender)
    where
        A: 'static + Actor<ChildRt> + Send + Sync,
    {
        let (sender, receiver) = <A::Channel as Channel<A::Event>>::new();
        self.scope.0.senders_mut().insert(sender.clone());
        let mut child_rt: ChildRt = self.scope.0.child().into();
        let abort_handle = self.scope.spawn_actor_with_runtime(child_rt, actor, receiver);
        (abort_handle, sender)
    }

    pub fn spawn_system<S>(&mut self, system: S) -> (AbortHandle, <S::Channel as Channel<S::ChildEvents>>::Sender)
    where
        Rt: SystemRuntime,
        ChildRt: SystemRuntime,
        S: 'static + System<ChildRt> + Send + Sync,
    {
        let (sender, receiver) = <S::Channel as Channel<S::ChildEvents>>::new();
        let system = Arc::new(RwLock::new(system));
        self.scope.0.senders_mut().insert(sender.clone());
        self.scope.0.systems_mut().insert(system.clone());
        let mut child_rt: ChildRt = self.scope.0.child().into();
        let abort_handle = self.scope.spawn_system_with_runtime(child_rt, system, receiver);
        (abort_handle, sender)
    }
}

pub struct ActorScopedRuntime<'a, A: Actor<Rt>, Rt: BaseRuntime> {
    runtime: RuntimeScope<'a, Rt>,
    receiver: ShutdownStream<<A::Channel as Channel<A::Event>>::Receiver>,
}

impl<'a, A: Actor<Rt>, Rt: BaseRuntime> ActorScopedRuntime<'a, A, Rt> {
    pub fn new(
        runtime: RuntimeScope<'a, Rt>,
        receiver: <A::Channel as Channel<A::Event>>::Receiver,
        shutdown: oneshot::Receiver<()>,
    ) -> Self {
        Self {
            runtime,
            receiver: ShutdownStream::new(shutdown, receiver),
        }
    }

    pub async fn next_event(&mut self) -> Option<A::Event> {
        self.receiver.next().await
    }

    pub fn shutdown(mut self) {
        self.0.shutdown()
    }
}

impl<'a, A: Actor<Rt>, Rt: BaseRuntime> Deref for ActorScopedRuntime<'a, A, Rt> {
    type Target = RuntimeScope<'a, Rt>;

    fn deref(&self) -> &Self::Target {
        &self.runtime
    }
}

impl<'a, A: Actor<Rt>, Rt: BaseRuntime> DerefMut for ActorScopedRuntime<'a, A, Rt> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.runtime
    }
}

impl<'a, A: Actor<Rt>, Rt: BaseRuntime> Drop for ActorScopedRuntime<'a, A, Rt> {
    fn drop(&mut self) {
        self.0.shutdown()
    }
}

pub struct SystemScopedRuntime<'a, S: System<Rt>, Rt: SystemRuntime> {
    runtime: RuntimeScope<'a, Rt>,
    receiver: ShutdownStream<<S::Channel as Channel<S::ChildEvents>>::Receiver>,
}

impl<'a, S: System<Rt>, Rt: SystemRuntime> SystemScopedRuntime<'a, S, Rt> {
    pub fn new(
        runtime: RuntimeScope<'a, Rt>,
        receiver: <S::Channel as Channel<S::ChildEvents>>::Receiver,
        shutdown: oneshot::Receiver<()>,
    ) -> Self {
        Self {
            runtime,
            receiver: ShutdownStream::new(shutdown, receiver),
        }
    }

    pub async fn next_event(&mut self) -> Option<S::ChildEvents> {
        self.receiver.next().await
    }

    pub async fn my_handle(&self) -> <S::Channel as Channel<S::ChildEvents>>::Sender
    where
        <S::Channel as Channel<S::ChildEvents>>::Sender: 'static,
    {
        self.runtime.0.system_event_handle::<S>().unwrap()
    }

    pub fn shutdown(mut self) {
        self.0.shutdown()
    }
}

impl<'a, S: System<Rt>, Rt: SystemRuntime> Deref for SystemScopedRuntime<'a, S, Rt> {
    type Target = RuntimeScope<'a, Rt>;

    fn deref(&self) -> &Self::Target {
        &self.runtime
    }
}

impl<'a, S: System<Rt>, Rt: SystemRuntime> DerefMut for SystemScopedRuntime<'a, S, Rt> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.runtime
    }
}

impl<'a, S: System<Rt>, Rt: SystemRuntime> Drop for SystemScopedRuntime<'a, S, Rt> {
    fn drop(&mut self) {
        self.0.shutdown()
    }
}

pub struct Res<R>(Arc<R>);

impl<R> Deref for Res<R> {
    type Target = R;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

pub struct ResMut<R>(Arc<RwLock<R>>);

impl<R> Deref for ResMut<R> {
    type Target = RwLock<R>;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

pub struct Sys<Rt: SystemRuntime, S: System<Rt>> {
    system: Arc<RwLock<S>>,
    _rt: PhantomData<Rt>,
}

impl<Rt: SystemRuntime, S: System<Rt>> Deref for Sys<Rt, S> {
    type Target = RwLock<S>;

    fn deref(&self) -> &Self::Target {
        self.system.deref()
    }
}

pub struct Act<Rt: BaseRuntime, A: Actor<Rt>> {
    actor: <A::Channel as Channel<A::Event>>::Sender,
    _rt: PhantomData<Rt>,
}

impl<Rt: BaseRuntime, A: Actor<Rt>> Deref for Act<Rt, A> {
    type Target = <A::Channel as Channel<A::Event>>::Sender;

    fn deref(&self) -> &Self::Target {
        &self.actor
    }
}

impl<Rt: BaseRuntime, A: Actor<Rt>> DerefMut for Act<Rt, A> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.actor
    }
}

pub struct ActorPool<'a, H> {
    handles: Vec<H>,
    lru: LruCache<usize, &'a H>,
}

impl<'a, H> Default for ActorPool<'a, H> {
    fn default() -> Self {
        Self {
            handles: Default::default(),
            lru: LruCache::unbounded(),
        }
    }
}

impl<'a, H> ActorPool<'a, H> {
    pub fn push(&'a mut self, handle: H) {
        let idx = self.handles.len();
        self.handles.push(handle);
        self.lru.put(idx, &self.handles[idx]);
    }

    pub fn get_lru(&mut self) -> Option<&H> {
        self.lru.pop_lru().map(|(idx, handle)| {
            self.lru.put(idx, handle);
            handle
        })
    }
}

impl<'a, H> Deref for ActorPool<'a, H> {
    type Target = Vec<H>;

    fn deref(&self) -> &Self::Target {
        &self.handles
    }
}
