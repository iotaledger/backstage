use crate::{actor::shutdown_stream::ShutdownStream, Actor, ActorError, ActorRequest, Channel, Dependencies, Sender, System};
use anymap::{any::CloneAny, Map};
use async_trait::async_trait;
use futures::{
    future::{AbortHandle, Abortable, BoxFuture},
    StreamExt,
};
use lru::LruCache;
#[cfg(feature = "rand_pool")]
use rand::Rng;
use std::{
    cell::RefCell,
    marker::PhantomData,
    ops::{Deref, DerefMut},
    rc::Rc,
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

    fn pool<A>(&self) -> Option<ResMut<ActorPool<Self, A>>>
    where
        Self: 'static + Sized,
        A: 'static + Actor<Self> + Send + Sync,
    {
        self.pools()
            .get::<Arc<RwLock<ActorPool<Self, A>>>>()
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

    pub fn spawn_pool<A: 'static + Actor<Rt>, F: FnOnce(&mut ScopedActorPool<Rt, A>)>(&mut self, f: F)
    where
        Rt: PoolRuntime,
        <A::Channel as Channel<A::Event>>::Sender: 'static,
        A: Send + Sync,
    {
        let mut pool = ActorPool::default();
        let mut scoped_pool = ScopedActorPool {
            scope: self,
            pool: &mut pool,
        };
        f(&mut scoped_pool);
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

    pub fn pool<A>(&self) -> Option<ResMut<ActorPool<Rt, A>>>
    where
        Rt: PoolRuntime,
        A: 'static + Actor<Rt> + Send + Sync,
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

pub struct ScopedActorPool<'a, 'b, Rt: BaseRuntime, A: Actor<Rt>>
where
    <A::Channel as Channel<A::Event>>::Sender: 'static,
{
    scope: &'a mut RuntimeScope<'b, Rt>,
    pool: &'a mut ActorPool<Rt, A>,
}

impl<'a, 'b, Rt: BaseRuntime, A: Actor<Rt>> ScopedActorPool<'a, 'b, Rt, A> {
    pub fn spawn(&mut self, actor: A) -> (AbortHandle, <A::Channel as Channel<A::Event>>::Sender)
    where
        A: 'static + Send + Sync,
        Rt: 'static,
    {
        let (sender, receiver) = <A::Channel as Channel<A::Event>>::new();
        let child_rt = self.scope.0.child();
        let abort_handle = self.scope.spawn_actor_with_runtime(child_rt, actor, receiver);
        self.pool.push(sender.clone());
        (abort_handle, sender)
    }
}

pub struct ActorPool<Rt: BaseRuntime, A: Actor<Rt>>
where
    <A::Channel as Channel<A::Event>>::Sender: 'static,
{
    handles: Vec<Rc<RefCell<<A::Channel as Channel<A::Event>>::Sender>>>,
    lru: LruCache<usize, Rc<RefCell<<A::Channel as Channel<A::Event>>::Sender>>>,
}

impl<Rt: BaseRuntime, A: Actor<Rt>> Clone for ActorPool<Rt, A> {
    fn clone(&self) -> Self {
        let handles = self.handles.iter().map(|rc| Rc::new(RefCell::new(rc.borrow().clone()))).collect();
        let mut lru = LruCache::unbounded();
        for (idx, lru_rc) in self.lru.iter().rev() {
            lru.put(*idx, Rc::new(RefCell::new(lru_rc.borrow().clone())));
        }
        Self { handles: handles, lru }
    }
}

unsafe impl<Rt: Send + BaseRuntime, A: Actor<Rt> + Send> Send for ActorPool<Rt, A> {}

unsafe impl<Rt: Sync + BaseRuntime, A: Actor<Rt> + Sync> Sync for ActorPool<Rt, A> {}

impl<'a, Rt: BaseRuntime, A: Actor<Rt>> Default for ActorPool<Rt, A> {
    fn default() -> Self {
        Self {
            handles: Default::default(),
            lru: LruCache::unbounded(),
        }
    }
}

impl<'a, Rt: BaseRuntime, A: Actor<Rt>> ActorPool<Rt, A> {
    fn push(&mut self, handle: <A::Channel as Channel<A::Event>>::Sender) {
        let idx = self.handles.len();
        let handle_rc = Rc::new(RefCell::new(handle));
        self.handles.push(handle_rc.clone());
        self.lru.put(idx, handle_rc);
    }

    pub fn get_lru(&mut self) -> Option<<A::Channel as Channel<A::Event>>::Sender> {
        self.lru.pop_lru().map(|(idx, handle)| {
            let res = handle.borrow().clone();
            self.lru.put(idx, handle);
            res
        })
    }

    pub async fn send_lru(&mut self, event: A::Event) -> anyhow::Result<()> {
        if let Some(mut handle) = self.get_lru() {
            handle.send(event).await
        } else {
            anyhow::bail!("No handles in pool!");
        }
    }

    #[cfg(feature = "rand_pool")]
    pub fn get_random(&mut self) -> Option<<A::Channel as Channel<A::Event>>::Sender> {
        let mut rng = rand::thread_rng();
        self.handles.get(rng.gen_range(0..self.handles.len())).map(|rc| rc.borrow().clone())
    }

    #[cfg(feature = "rand_pool")]
    pub async fn send_random(&mut self, event: A::Event) -> anyhow::Result<()> {
        if let Some(mut handle) = self.get_random() {
            handle.send(event).await
        } else {
            anyhow::bail!("No handles in pool!");
        }
    }

    pub fn iter(&mut self) -> std::vec::IntoIter<<A::Channel as Channel<A::Event>>::Sender> {
        self.handles.iter().map(|rc| rc.borrow().clone()).collect::<Vec<_>>().into_iter()
    }

    pub async fn send_all(&mut self, event: A::Event) -> anyhow::Result<()>
    where
        A::Event: Clone,
    {
        for mut handle in self.iter() {
            handle.send(event.clone()).await?;
        }
        Ok(())
    }
}
