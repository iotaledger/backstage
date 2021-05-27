use crate::{Actor, ActorError, Channel, Receiver, Sender, System};
use anymap::{any::CloneAny, Map};
use futures::Future;
use lru::LruCache;
use std::{
    borrow::Cow,
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

pub struct ActorRuntime<A: Actor> {
    runtime: BackstageRuntime,
    receiver: <A::Channel as Channel<A::Event>>::Receiver,
}

impl<A: Actor> ActorRuntime<A> {
    pub fn new(runtime: BackstageRuntime, receiver: <A::Channel as Channel<A::Event>>::Receiver) -> Self {
        Self { runtime, receiver }
    }

    pub async fn next_event(&mut self) -> Option<A::Event> {
        Receiver::<A::Event>::recv(&mut self.receiver).await
    }

    pub async fn actor_scope<O, F: FnOnce(&mut RuntimeScope, &mut <A::Channel as Channel<A::Event>>::Receiver) -> O>(
        &mut self,
        f: F,
    ) -> anyhow::Result<O> {
        let res = f(&mut RuntimeScope((&mut self.runtime).into()), &mut self.receiver);
        self.join().await?;
        Ok(res)
    }
}

impl<A: Actor> Deref for ActorRuntime<A> {
    type Target = BackstageRuntime;

    fn deref(&self) -> &Self::Target {
        &self.runtime
    }
}

impl<A: Actor> DerefMut for ActorRuntime<A> {
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

pub enum Boo<'a, B: 'a> {
    Borrowed(&'a mut B),
    Owned(B),
}

impl<'a, B: 'a> Deref for Boo<'a, B> {
    type Target = B;

    fn deref(&self) -> &Self::Target {
        match self {
            Boo::Borrowed(b) => *b,
            Boo::Owned(o) => o,
        }
    }
}

impl<'a, B: 'a> DerefMut for Boo<'a, B> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Boo::Borrowed(b) => *b,
            Boo::Owned(ref mut o) => o,
        }
    }
}

impl<'a, O: 'a> From<O> for Boo<'a, O> {
    fn from(owned: O) -> Self {
        Self::Owned(owned)
    }
}

impl<'a, B: 'a> From<&'a mut B> for Boo<'a, B> {
    fn from(borrowed: &'a mut B) -> Self {
        Self::Borrowed(borrowed)
    }
}

pub struct RuntimeScope<'a>(pub Boo<'a, BackstageRuntime>);

impl<'a> RuntimeScope<'a> {
    pub fn spawn_task<F: FnOnce(BackstageRuntime) -> O, O: 'static + Send + Future<Output = Result<(), ActorError>>>(&mut self, f: F) {
        let child_rt = self.0.child();
        let child_task = tokio::spawn(f(child_rt));
        self.0.child_handles.push(child_task);
    }

    pub fn spawn_actor<A: 'static + Actor>(&mut self, actor: A) -> <A::Channel as Channel<A::Event>>::Sender {
        let (sender, receiver) = <A::Channel as Channel<A::Event>>::new();
        let child_rt = ActorRuntime::new(self.0.child(), receiver);
        self.0.senders.insert(sender.clone());
        let child_task = tokio::spawn(actor.run(child_rt));
        self.0.child_handles.push(child_task);
        sender
    }

    pub fn spawn_system<S: 'static + System + Send + Sync>(&mut self, system: S) -> <S::Channel as Channel<S::ChildEvents>>::Sender {
        let (sender, receiver) = <S::Channel as Channel<S::ChildEvents>>::new();
        let child_rt = SystemRuntime::new(RuntimeScope(self.0.child().into()), receiver);
        let system = Arc::new(RwLock::new(system));
        self.0.senders.insert(sender.clone());
        self.0.systems.insert(system.clone());
        let child_task = tokio::spawn(S::run(system, child_rt));
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

mod test {
    use super::*;
    use crate::TokioChannel;

    struct MyResource {
        n: u32,
    }

    #[derive(Clone, Debug)]
    enum MyEvent {
        Shutdown,
        Text(String),
    }

    struct MyActor;

    #[async_trait::async_trait]
    impl Actor for MyActor {
        type Dependencies = (Res<MyResource>,);

        type Event = MyEvent;

        type Channel = TokioChannel<Self::Event>;

        async fn run(self, mut rt: ActorRuntime<Self>) -> Result<(), ActorError> {
            while let Some(evt) = rt.next_event().await {
                let res = rt.resource::<MyResource>();
                match evt {
                    MyEvent::Shutdown => break,
                    MyEvent::Text(s) => {
                        println!("Test received: {}", s);
                    }
                }
            }

            Ok(())
        }
    }
    #[tokio::test]
    async fn runtime() {
        let mut rt = BackstageRuntime::new();
        rt.scope(|scope| {
            let res = scope.add_resource(MyResource { n: 0 });
            let actor_handle = scope.spawn_actor(MyActor);
        })
        .await
        .unwrap();
    }
}
