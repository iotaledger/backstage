use crate::{actor::shutdown_stream::ShutdownStream, Actor, Channel, Dependencies, Sender, SupervisorEvent, System};
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
pub use null_runtime::*;
pub use scopes::*;
pub use system_runtime::*;

mod basic_runtime;
mod full_runtime;
mod null_runtime;
mod scopes;
mod system_runtime;

#[async_trait]
pub trait BaseRuntime: Send + Sync {
    fn join_handles(&self) -> &Vec<JoinHandle<anyhow::Result<()>>>;

    fn join_handles_mut(&mut self) -> &mut Vec<JoinHandle<anyhow::Result<()>>>;

    fn shutdown_handles(&self) -> &Vec<(Option<oneshot::Sender<()>>, AbortHandle)>;

    fn shutdown_handles_mut(&mut self) -> &mut Vec<(Option<oneshot::Sender<()>>, AbortHandle)>;

    fn senders(&self) -> &Map<dyn CloneAny + Send + Sync>;

    fn senders_mut(&mut self) -> &mut Map<dyn CloneAny + Send + Sync>;

    fn child(&self) -> Self;

    async fn scope<O: Send + Sync, F: Send + FnOnce(&mut RuntimeScope<'_, Self>) -> O>(&mut self, f: F) -> anyhow::Result<O>
    where
        Self: 'static + Sized,
    {
        let res = f(&mut RuntimeScope::new(self));
        self.join().await?;
        Ok(res)
    }

    fn shutdown(&mut self) {
        for handle in self.shutdown_handles_mut().iter_mut() {
            handle.0.take().map(|h| h.send(()));
        }
    }

    async fn join(&mut self) -> Result<(), JoinError> {
        for handle in self.join_handles_mut().drain(..) {
            if let Err(e) = handle.await? {
                log::error!("{}", e);
            }
        }
        Ok(())
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

    fn event_handle<T: 'static + Clone + Send + Sync>(&self) -> Option<T> {
        self.senders().get::<T>().map(|handle| handle.clone())
    }

    fn actor_event_handle<A: Actor<H, E>, H, E>(&self) -> Option<Act<A, H, E>>
    where
        Self: Sized,
        H: 'static + Sender<E> + Clone + Send + Sync,
        E: 'static + SupervisorEvent + Send + Sync,
    {
        self.senders()
            .get::<<A::Channel as Channel<A::Event>>::Sender>()
            .map(|handle| Act { actor: handle.clone() })
    }

    async fn send_actor_event<A: Actor<H, E>, H, E>(&mut self, event: A::Event) -> anyhow::Result<()>
    where
        Self: Sized,
        H: 'static + Sender<E> + Clone + Send + Sync,
        E: 'static + SupervisorEvent + Send + Sync,
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

    fn system<S: 'static + System<H, E> + Send + Sync, H, E>(&self) -> Option<Sys<S, H, E>>
    where
        Self: Sized,
        H: 'static + Sender<E> + Clone + Send + Sync,
        E: 'static + SupervisorEvent + Send + Sync,
    {
        self.systems().get::<Arc<RwLock<S>>>().map(|sys| Sys::new(sys.clone()))
    }

    fn system_event_handle<S: System<H, E>, H, E>(&self) -> Option<<S::Channel as Channel<S::ChildEvents>>::Sender>
    where
        Self: Sized,
        H: 'static + Sender<E> + Clone + Send + Sync,
        E: 'static + SupervisorEvent + Send + Sync,
    {
        self.senders()
            .get::<<S::Channel as Channel<S::ChildEvents>>::Sender>()
            .map(|handle| handle.clone())
    }

    async fn send_system_event<S: System<H, E>, H, E>(&mut self, event: S::ChildEvents) -> anyhow::Result<()>
    where
        Self: Sized,
        H: 'static + Sender<E> + Clone + Send + Sync,
        E: 'static + SupervisorEvent + Send + Sync,
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

    fn pool<A, H, E>(&self) -> Option<ResMut<ActorPool<A, H, E>>>
    where
        Self: 'static + Sized,
        A: 'static + Actor<H, E> + Send + Sync,
        H: 'static + Sender<E> + Clone + Send + Sync,
        E: 'static + SupervisorEvent + Send + Sync,
    {
        self.pools()
            .get::<Arc<RwLock<ActorPool<A, H, E>>>>()
            .map(|pool| ResMut(pool.clone()))
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

pub struct Sys<S: System<H, E>, H, E>
where
    H: 'static + Sender<E> + Clone + Send + Sync,
    E: 'static + SupervisorEvent + Send + Sync,
{
    system: Arc<RwLock<S>>,
    _data: PhantomData<(H, E)>,
}

impl<S: System<H, E>, H, E> Sys<S, H, E>
where
    H: 'static + Sender<E> + Clone + Send + Sync,
    E: 'static + SupervisorEvent + Send + Sync,
{
    pub fn new(system: Arc<RwLock<S>>) -> Self {
        Self {
            system,
            _data: PhantomData,
        }
    }
}

impl<S: System<H, E>, H, E> Deref for Sys<S, H, E>
where
    H: 'static + Sender<E> + Clone + Send + Sync,
    E: 'static + SupervisorEvent + Send + Sync,
{
    type Target = RwLock<S>;

    fn deref(&self) -> &Self::Target {
        self.system.deref()
    }
}

pub struct Act<A: Actor<H, E>, H, E>
where
    H: 'static + Sender<E> + Clone + Send + Sync,
    E: 'static + SupervisorEvent + Send + Sync,
{
    actor: <A::Channel as Channel<A::Event>>::Sender,
}

impl<A: Actor<H, E>, H, E> Deref for Act<A, H, E>
where
    H: 'static + Sender<E> + Clone + Send + Sync,
    E: 'static + SupervisorEvent + Send + Sync,
{
    type Target = <A::Channel as Channel<A::Event>>::Sender;

    fn deref(&self) -> &Self::Target {
        &self.actor
    }
}

impl<A: Actor<H, E>, H, E> DerefMut for Act<A, H, E>
where
    H: 'static + Sender<E> + Clone + Send + Sync,
    E: 'static + SupervisorEvent + Send + Sync,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.actor
    }
}

pub struct ActorPool<A: Actor<H, E>, H, E>
where
    H: 'static + Sender<E> + Clone + Send + Sync,
    E: 'static + SupervisorEvent + Send + Sync,
{
    handles: Vec<Rc<RefCell<<A::Channel as Channel<A::Event>>::Sender>>>,
    lru: LruCache<usize, Rc<RefCell<<A::Channel as Channel<A::Event>>::Sender>>>,
}

impl<A: Actor<H, E>, H, E> Clone for ActorPool<A, H, E>
where
    H: 'static + Sender<E> + Clone + Send + Sync,
    E: 'static + SupervisorEvent + Send + Sync,
{
    fn clone(&self) -> Self {
        let handles = self.handles.iter().map(|rc| Rc::new(RefCell::new(rc.borrow().clone()))).collect();
        let mut lru = LruCache::unbounded();
        for (idx, lru_rc) in self.lru.iter().rev() {
            lru.put(*idx, Rc::new(RefCell::new(lru_rc.borrow().clone())));
        }
        Self { handles: handles, lru }
    }
}

unsafe impl<A: Actor<H, E> + Send, H, E> Send for ActorPool<A, H, E>
where
    H: 'static + Sender<E> + Clone + Send + Sync,
    E: 'static + SupervisorEvent + Send + Sync,
{
}

unsafe impl<A: Actor<H, E> + Sync, H, E> Sync for ActorPool<A, H, E>
where
    H: 'static + Sender<E> + Clone + Send + Sync,
    E: 'static + SupervisorEvent + Send + Sync,
{
}

impl<A: Actor<H, E>, H, E> Default for ActorPool<A, H, E>
where
    H: 'static + Sender<E> + Clone + Send + Sync,
    E: 'static + SupervisorEvent + Send + Sync,
{
    fn default() -> Self {
        Self {
            handles: Default::default(),
            lru: LruCache::unbounded(),
        }
    }
}

impl<A: Actor<H, E>, H, E> ActorPool<A, H, E>
where
    H: 'static + Sender<E> + Clone + Send + Sync,
    E: 'static + SupervisorEvent + Send + Sync,
{
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
