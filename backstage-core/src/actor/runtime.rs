use crate::{Actor, ActorError, Channel, System};
use anymap::{any::CloneAny, Map};
use lru::LruCache;
use std::{
    marker::PhantomData,
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
    _data: PhantomData<A>,
}

impl<A: Actor> ActorRuntime<A> {
    pub fn new(runtime: BackstageRuntime, receiver: <A::Channel as Channel<A::Event>>::Receiver) -> Self {
        Self {
            runtime,
            receiver,
            _data: PhantomData,
        }
    }

    pub async fn next_event(&mut self) -> Option<A::Event> {
        todo!()
        // self.receiver.recv().await
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

pub struct RuntimeScope<'a>(&'a mut BackstageRuntime);

impl<'a> RuntimeScope<'a> {
    pub fn spawn_actor<A: 'static + Actor>(&mut self, actor: A) {
        let (sender, receiver) = <A::Channel as Channel<A::Event>>::new();
        let child_rt = ActorRuntime::new(self.0.child(), receiver);
        self.0.senders.insert(sender);
        let child_task = tokio::spawn(actor.run(child_rt));

        self.0.child_handles.push(child_task);
    }

    pub fn spawn_system<S: 'static + System + Send + Sync>(&mut self, system: S) {
        let child_rt = self.0.child();
        let system = Arc::new(RwLock::new(system));
        self.0.systems.insert(system.clone());
        let child_task = tokio::spawn(S::run(system, child_rt));

        self.0.child_handles.push(child_task);
    }

    pub fn add_resource<R: 'static + Send + Sync>(&mut self, resource: R) -> Res<R> {
        let res = Arc::new(resource);
        self.0.resources.insert(res.clone());
        Res(res)
    }

    pub fn pool<H: 'static + Send + Sync, O, F: FnOnce(&mut Pool<'_, H>)>(&'static mut self, f: F) {
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
        let res = f(&mut RuntimeScope(self));
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
            scope.spawn_actor(MyActor);
        })
        .await
        .unwrap();
    }
}
