use crate::{Actor, ActorError, ActorRequest, Channel, Dependencies, Receiver, Sender, System};
use anymap::{any::CloneAny, Map};
use futures::future::{AbortHandle, Abortable, BoxFuture};
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
    join_handles: Vec<JoinHandle<Result<(), ActorError>>>,
    abort_handles: Vec<AbortHandle>,
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
    pub fn spawn_task<F>(&mut self, f: F) -> AbortHandle
    where
        for<'b> F: 'static + Send + FnOnce(RuntimeScope<'b>) -> BoxFuture<'b, Result<(), ActorError>>,
    {
        let mut child_rt = self.0.child();
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        let child_task = tokio::spawn(async move {
            let scope = RuntimeScope(&mut child_rt);
            let res = Abortable::new(f(scope), abort_registration).await;
            match res {
                Ok(res) => {
                    child_rt.join().await;
                    res
                }
                Err(a) => {
                    log::debug!("Aborting children of task!");
                    child_rt.abort();
                    Err(ActorError::Other {
                        source: anyhow::anyhow!("Aborted!"),
                        request: ActorRequest::Finish,
                    })
                }
            }
        });
        self.0.join_handles.push(child_task);
        self.0.abort_handles.push(abort_handle.clone());
        abort_handle
    }

    pub fn spawn_actor<A: 'static + Actor + Send + Sync>(&mut self, actor: A) -> (AbortHandle, <A::Channel as Channel<A::Event>>::Sender) {
        let deps = <A::Dependencies as Dependencies>::instantiate(&self.0)
            .map_err(|e| anyhow::anyhow!("Cannot spawn actor {}: {}", std::any::type_name::<A>(), e))
            .unwrap();
        let (sender, receiver) = <A::Channel as Channel<A::Event>>::new();
        self.0.senders.insert(sender.clone());
        let mut child_rt = self.0.child();
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        let child_task = tokio::spawn(async move {
            let actor_rt = ActorRuntime::new(RuntimeScope(&mut child_rt), receiver);

            let res = Abortable::new(actor.run(actor_rt, deps), abort_registration).await;
            match res {
                Ok(res) => {
                    child_rt.join().await;
                    res
                }
                Err(a) => {
                    log::debug!("Aborting children of actor!");
                    child_rt.abort();
                    Err(ActorError::Other {
                        source: anyhow::anyhow!("Aborted!"),
                        request: ActorRequest::Finish,
                    })
                }
            }
        });
        self.0.join_handles.push(child_task);
        self.0.abort_handles.push(abort_handle.clone());
        (abort_handle, sender)
    }

    pub fn spawn_system<S: 'static + System + Send + Sync>(
        &mut self,
        system: S,
    ) -> (AbortHandle, <S::Channel as Channel<S::ChildEvents>>::Sender) {
        let deps = <S::Dependencies as Dependencies>::instantiate(&self.0)
            .map_err(|e| anyhow::anyhow!("Cannot spawn system {}: {}", std::any::type_name::<S>(), e))
            .unwrap();
        let (sender, receiver) = <S::Channel as Channel<S::ChildEvents>>::new();
        let system = Arc::new(RwLock::new(system));
        self.0.senders.insert(sender.clone());
        self.0.systems.insert(system.clone());
        let mut child_rt = self.0.child();
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        let child_task = tokio::spawn(async move {
            let system_rt = SystemRuntime::new(RuntimeScope((&mut child_rt).into()), receiver);
            let res = Abortable::new(S::run(system, system_rt, deps), abort_registration).await;
            match res {
                Ok(res) => {
                    child_rt.join().await;
                    res
                }
                Err(a) => {
                    log::debug!("Aborting children of system!");
                    child_rt.abort();
                    Err(ActorError::Other {
                        source: anyhow::anyhow!("Aborted!"),
                        request: ActorRequest::Finish,
                    })
                }
            }
        });
        self.0.join_handles.push(child_task);
        self.0.abort_handles.push(abort_handle.clone());
        (abort_handle, sender)
    }

    pub fn spawn_pool<H: 'static + Send + Sync, F: FnOnce(&mut ActorPool<'_, H>)>(&'static mut self, f: F) {
        let mut pool = ActorPool::default();
        f(&mut pool);
        self.0.pools.insert(Arc::new(RwLock::new(pool)));
    }

    pub fn add_resource<R: 'static + Send + Sync>(&mut self, resource: R) -> Res<R> {
        let res = Arc::new(resource);
        self.0.resources.insert(res.clone());
        Res(res)
    }

    pub fn resource<R: 'static + Send + Sync>(&self) -> Option<Res<R>> {
        self.0.resource::<R>()
    }

    pub fn system<S: 'static + System + Send + Sync>(&self) -> Option<Sys<S>> {
        self.0.system::<S>()
    }

    pub fn pool<A: Actor>(&self) -> Option<ResMut<ActorPool<'static, <A::Channel as Channel<A::Event>>::Sender>>> {
        self.0.pool::<A>()
    }

    pub async fn send_actor_event<A: Actor>(&mut self, event: A::Event) -> anyhow::Result<()>
    where
        <A::Channel as Channel<A::Event>>::Sender: 'static,
    {
        self.0.send_actor_event::<A>(event).await
    }

    pub async fn send_system_event<S: System>(&mut self, event: S::ChildEvents) -> anyhow::Result<()>
    where
        <S::Channel as Channel<S::ChildEvents>>::Sender: 'static,
    {
        self.0.send_system_event::<S>(event).await
    }
}

impl Default for BackstageRuntime {
    fn default() -> Self {
        Self {
            join_handles: Default::default(),
            abort_handles: Default::default(),
            resources: Map::new(),
            senders: Map::new(),
            systems: Map::new(),
            pools: Map::new(),
        }
    }
}

impl BackstageRuntime {
    pub fn new() -> Self {
        Self::default()
    }

    fn child(&self) -> Self {
        Self {
            resources: self.resources.clone(),
            senders: self.senders.clone(),
            systems: self.systems.clone(),
            pools: self.pools.clone(),
            ..Default::default()
        }
    }

    pub async fn scope<O, F: FnOnce(&mut RuntimeScope) -> O>(&mut self, f: F) -> anyhow::Result<O> {
        let res = f(&mut RuntimeScope(self.into()));
        self.join().await?;
        Ok(res)
    }

    async fn join(&mut self) -> Result<Vec<Result<(), ActorError>>, JoinError> {
        let mut results = Vec::new();
        for handle in self.join_handles.drain(..) {
            results.push(handle.await?);
        }
        Ok(results)
    }

    fn abort(mut self) {
        for handle in self.abort_handles.drain(..) {
            handle.abort();
        }
    }

    pub fn resource<R: 'static + Send + Sync>(&self) -> Option<Res<R>> {
        self.resources.get::<Arc<R>>().map(|res| Res(res.clone()))
    }

    pub fn system<S: 'static + System + Send + Sync>(&self) -> Option<Sys<S>> {
        self.systems.get::<Arc<RwLock<S>>>().map(|sys| Sys(sys.clone()))
    }

    pub fn pool<A: Actor>(&self) -> Option<ResMut<ActorPool<'static, <A::Channel as Channel<A::Event>>::Sender>>> {
        self.pools
            .get::<Arc<RwLock<ActorPool<<A::Channel as Channel<A::Event>>::Sender>>>>()
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

    pub fn actor_event_handle<A: Actor>(&self) -> Option<Act<A>>
    where
        <A::Channel as Channel<A::Event>>::Sender: 'static,
    {
        self.senders
            .get::<<A::Channel as Channel<A::Event>>::Sender>()
            .map(|handle| Act(handle.clone()))
    }

    pub async fn send_actor_event<A: Actor>(&mut self, event: A::Event) -> anyhow::Result<()>
    where
        <A::Channel as Channel<A::Event>>::Sender: 'static,
    {
        let handle = self
            .senders
            .get_mut::<<A::Channel as Channel<A::Event>>::Sender>()
            .ok_or_else(|| anyhow::anyhow!("No channel for this actor!"))?;
        Sender::<A::Event>::send(handle, event).await
    }

    pub async fn send_system_event<S: System>(&mut self, event: S::ChildEvents) -> anyhow::Result<()>
    where
        <S::Channel as Channel<S::ChildEvents>>::Sender: 'static,
    {
        let handle = self
            .senders
            .get_mut::<<S::Channel as Channel<S::ChildEvents>>::Sender>()
            .ok_or_else(|| anyhow::anyhow!("No channel for this actor!"))?;
        Sender::<S::ChildEvents>::send(handle, event).await
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

pub struct Sys<S: System>(Arc<RwLock<S>>);

impl<S: System> Deref for Sys<S> {
    type Target = RwLock<S>;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

pub struct Act<A: Actor>(<A::Channel as Channel<A::Event>>::Sender);

impl<A: Actor> Deref for Act<A> {
    type Target = <A::Channel as Channel<A::Event>>::Sender;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<A: Actor> DerefMut for Act<A> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
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
