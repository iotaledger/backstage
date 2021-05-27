use crate::{Actor, ActorError, Channel, Receiver, Sender, System};
use anymap::{any::CloneAny, Map};
use futures::Future;
use log::debug;
use lru::LruCache;
use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
};
use tokio::{
    sync::RwLock,
    task::{JoinError, JoinHandle},
};
pub struct BackstageRuntime {
    child_handles: Vec<JoinHandle<Result<(), ActorError>>>,
    resources: Map<dyn CloneAny + Send + Sync>,
    senders: Map<dyn CloneAny + Send + Sync>,
    systems: Map<dyn CloneAny + Send + Sync>,
    pools: Map<dyn CloneAny + Send + Sync>,
}

pub struct ActorRuntime<'a, A: Actor> {
    runtime: RuntimeScope<'a>,
    receiver: <A::Channel as Channel<A::Event>>::Receiver,
}

impl<'a, A: Actor> ActorRuntime<'a, A> {
    pub fn new(runtime: RuntimeScope<'a>, receiver: <A::Channel as Channel<A::Event>>::Receiver) -> Self {
        Self { runtime, receiver }
    }

    pub async fn next_event(&mut self) -> Option<A::Event> {
        Receiver::<A::Event>::recv(&mut self.receiver).await
    }
}

impl<'a, A: Actor> Deref for ActorRuntime<'a, A> {
    type Target = RuntimeScope<'a>;

    fn deref(&self) -> &Self::Target {
        &self.runtime
    }
}

impl<'a, A: Actor> DerefMut for ActorRuntime<'a, A> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.runtime
    }
}

pub struct SystemRuntime<'a, S: System> {
    runtime: RuntimeScope<'a>,
    receiver: <S::Channel as Channel<S::ChildEvents>>::Receiver,
}

impl<'a, S: System> SystemRuntime<'a, S> {
    pub fn new(runtime: RuntimeScope<'a>, receiver: <S::Channel as Channel<S::ChildEvents>>::Receiver) -> Self {
        Self { runtime, receiver }
    }

    pub async fn next_event(&mut self) -> Option<S::ChildEvents> {
        Receiver::<S::ChildEvents>::recv(&mut self.receiver).await
    }

    pub async fn my_handle(&self) -> <S::Channel as Channel<S::ChildEvents>>::Sender
    where
        <S::Channel as Channel<S::ChildEvents>>::Sender: 'static,
    {
        self.runtime.0.system_event_handle::<S>().unwrap()
    }
}

impl<'a, S: System> Deref for SystemRuntime<'a, S> {
    type Target = RuntimeScope<'a>;

    fn deref(&self) -> &Self::Target {
        &self.runtime
    }
}

impl<'a, S: System> DerefMut for SystemRuntime<'a, S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.runtime
    }
}

pub struct RuntimeScope<'a>(pub &'a mut BackstageRuntime);

impl<'a> RuntimeScope<'a> {
    pub fn spawn_task<F: FnOnce(BackstageRuntime) -> O, O: 'static + Send + Future<Output = Result<(), ActorError>>>(&mut self, f: F) {
        let child_rt = self.0.child();
        let child_task = tokio::spawn(f(child_rt));
        self.0.child_handles.push(child_task);
    }

    pub fn spawn_actor<A: 'static + Actor + Send + Sync>(&mut self, actor: A) -> <A::Channel as Channel<A::Event>>::Sender {
        let (sender, receiver) = <A::Channel as Channel<A::Event>>::new();
        self.0.senders.insert(sender.clone());
        let mut child_rt = self.0.child();
        let child_task = tokio::spawn(async move {
            let actor_rt = ActorRuntime::new(RuntimeScope(&mut child_rt), receiver);
            let res = actor.run(actor_rt).await;
            child_rt.join().await;
            res
        });
        self.0.child_handles.push(child_task);
        sender
    }

    pub fn spawn_system<S: 'static + System + Send + Sync>(&mut self, system: S) -> <S::Channel as Channel<S::ChildEvents>>::Sender {
        let (sender, receiver) = <S::Channel as Channel<S::ChildEvents>>::new();
        let system = Arc::new(RwLock::new(system));
        self.0.senders.insert(sender.clone());
        self.0.systems.insert(system.clone());
        let mut child_rt = self.0.child();
        let child_task = tokio::spawn(async move {
            let system_rt = SystemRuntime::new(RuntimeScope((&mut child_rt).into()), receiver);
            let res = S::run(system, system_rt).await;
            child_rt.join().await;
            res
        });
        self.0.child_handles.push(child_task);
        sender
    }

    pub fn add_resource<R: 'static + Send + Sync>(&mut self, resource: R) -> Res<R> {
        let res = Arc::new(resource);
        self.0.resources.insert(res.clone());
        Res(res)
    }

    pub fn pool<H: 'static + Send + Sync, F: FnOnce(&mut Pool<'_, H>)>(&'static mut self, f: F) {
        let mut pool = Pool::default();
        f(&mut pool);
        self.0.pools.insert(Arc::new(RwLock::new(pool)));
    }

    pub async fn send_event<A: Actor>(&mut self, event: A::Event) -> anyhow::Result<()>
    where
        <A::Channel as Channel<A::Event>>::Sender: 'static,
    {
        self.0.send_event::<A>(event).await
    }
}

impl BackstageRuntime {
    pub fn new() -> Self {
        Self {
            child_handles: Default::default(),
            resources: Map::new(),
            senders: Map::new(),
            systems: Map::new(),
            pools: Map::new(),
        }
    }

    fn child(&self) -> Self {
        Self {
            child_handles: Default::default(),
            resources: self.resources.clone(),
            senders: self.senders.clone(),
            systems: self.systems.clone(),
            pools: self.pools.clone(),
        }
    }

    pub async fn scope<O, F: FnOnce(&mut RuntimeScope) -> O>(&mut self, f: F) -> anyhow::Result<O> {
        let res = f(&mut RuntimeScope(self.into()));
        self.join().await?;
        Ok(res)
    }

    async fn join(&mut self) -> Result<Vec<Result<(), ActorError>>, JoinError> {
        let mut results = Vec::new();
        for handle in self.child_handles.drain(..) {
            results.push(handle.await?);
        }
        Ok(results)
    }

    pub fn resource<R: 'static + Send + Sync>(&self) -> Option<&R> {
        self.resources.get::<Arc<R>>().map(Arc::as_ref)
    }

    pub fn system<S: 'static + System + Send + Sync>(&self) -> Option<Arc<RwLock<S>>> {
        self.systems.get::<Arc<RwLock<S>>>().map(|sys| sys.clone())
    }

    pub fn pool<A: Actor>(&self) -> Option<ResMut<Pool<'static, <A::Channel as Channel<A::Event>>::Sender>>> {
        self.pools
            .get::<Arc<RwLock<Pool<<A::Channel as Channel<A::Event>>::Sender>>>>()
            .map(|pool| ResMut(pool.clone()))
    }

    pub fn event_handle<H: 'static + Clone + Send + Sync>(&self) -> Option<H> {
        self.senders.get::<H>().map(|handle| handle.clone())
    }

    pub fn system_event_handle<S: System>(&self) -> Option<<S::Channel as Channel<S::ChildEvents>>::Sender>
    where
        <S::Channel as Channel<S::ChildEvents>>::Sender: 'static,
    {
        self.senders
            .get::<<S::Channel as Channel<S::ChildEvents>>::Sender>()
            .map(|handle| handle.clone())
    }

    pub fn actor_event_handle<A: Actor>(&self) -> Option<<A::Channel as Channel<A::Event>>::Sender>
    where
        <A::Channel as Channel<A::Event>>::Sender: 'static,
    {
        self.senders
            .get::<<A::Channel as Channel<A::Event>>::Sender>()
            .map(|handle| handle.clone())
    }

    pub async fn send_event<A: Actor>(&mut self, event: A::Event) -> anyhow::Result<()>
    where
        <A::Channel as Channel<A::Event>>::Sender: 'static,
    {
        let handle = self
            .senders
            .get_mut::<<A::Channel as Channel<A::Event>>::Sender>()
            .ok_or_else(|| anyhow::anyhow!("No channel for this actor!"))?;
        Sender::<A::Event>::send(handle, event).await
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

impl<R> ResMut<R> {
    pub async fn read(&self) -> tokio::sync::RwLockReadGuard<'_, R> {
        self.0.read().await
    }

    pub async fn write(&self) -> tokio::sync::RwLockWriteGuard<'_, R> {
        self.0.write().await
    }
}

pub struct Pool<'a, H> {
    handles: Vec<H>,
    lru: LruCache<usize, &'a H>,
}

impl<'a, H> Default for Pool<'a, H> {
    fn default() -> Self {
        Self {
            handles: Default::default(),
            lru: LruCache::unbounded(),
        }
    }
}

impl<'a, H> Pool<'a, H> {
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

impl<'a, H> Deref for Pool<'a, H> {
    type Target = Vec<H>;

    fn deref(&self) -> &Self::Target {
        &self.handles
    }
}
