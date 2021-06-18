use crate::{actor::shutdown_stream::ShutdownStream, Actor, Channel, Dependencies, IdPool, Sender, Service, SupervisorEvent, System};
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
    task::JoinHandle,
};

pub use basic_runtime::*;
pub use full_runtime::*;
pub use scopes::*;
pub use system_runtime::*;

mod basic_runtime;
mod full_runtime;
mod scopes;
mod system_runtime;

/// Defines the bare essentials of a scoped runtime which can spawn and
/// manage actors as well as shut them down appropriately.
#[async_trait]
pub trait BaseRuntime: Send + Sync {
    /// Create a new runtime with a given service
    fn new(service: Service) -> Self;

    /// Get the join handles of this runtime's scoped tasks
    fn join_handles(&self) -> &Vec<JoinHandle<anyhow::Result<()>>>;

    /// Mutably get the join handles of this runtime's scoped tasks
    fn join_handles_mut(&mut self) -> &mut Vec<JoinHandle<anyhow::Result<()>>>;

    /// Get the shutdown handles of this runtime's scoped tasks
    fn shutdown_handles(&self) -> &Vec<(Option<oneshot::Sender<()>>, AbortHandle)>;

    /// Mutably get the shutdown handles of this runtime's scoped tasks
    fn shutdown_handles_mut(&mut self) -> &mut Vec<(Option<oneshot::Sender<()>>, AbortHandle)>;

    /// Get the anymap of senders (handles) for this runtime's scoped tasks
    fn senders(&self) -> &Map<dyn CloneAny + Send + Sync>;

    /// Mutably get the anymap of senders (handles) for this runtime's scoped tasks
    fn senders_mut(&mut self) -> &mut Map<dyn CloneAny + Send + Sync>;

    /// Get the service for this runtime
    fn service(&self) -> &Service;

    /// Mutably get the service for this runtime
    fn service_mut(&mut self) -> &mut Service;

    /// Create a child of this runtime
    fn child<S: Into<String>>(&mut self, name: S) -> Self;

    /// Spawn this runtime as the runtime of a given actor
    async fn as_actor<A: 'static + Actor + Send + Sync>(mut actor: A) -> anyhow::Result<()>
    where
        Self: Into<A::Rt>,
    {
        let mut rt: A::Rt = Self::new(Service::new(A::name())).into();
        let (sender, receiver) = <A::Channel as Channel<A::Event>>::new();
        rt.senders_mut().insert(sender.clone());
        let (oneshot_send, oneshot_recv) = oneshot::channel::<()>();
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        rt.shutdown_handles_mut().push((Some(oneshot_send), abort_handle.clone()));
        let deps = A::Dependencies::instantiate(&mut rt)
            .map_err(|e| anyhow::anyhow!("Cannot spawn actor {}: {}", std::any::type_name::<A>(), e))
            .unwrap();
        let res = {
            let scope = RuntimeScope::new(&mut rt);
            let mut actor_rt = ActorScopedRuntime::unsupervised(scope, receiver, oneshot_recv);
            Abortable::new(actor.run(&mut actor_rt, deps), abort_registration).await
        };
        match res {
            Ok(res) => {
                rt.join().await;
                match res {
                    Ok(_) => Ok(()),
                    Err(e) => anyhow::bail!(e),
                }
            }
            Err(_) => {
                log::debug!("Aborting children of actor!");
                rt.abort();
                anyhow::bail!("Aborted!")
            }
        }
    }

    /// Create a new scope within this one
    async fn scope<O: Send + Sync, F: Send + FnOnce(&mut RuntimeScope<'_, Self>) -> O>(&mut self, f: F) -> anyhow::Result<O>
    where
        Self: 'static + Sized,
    {
        let res = f(&mut RuntimeScope::new(self));
        self.join().await;
        Ok(res)
    }

    /// Shutdown the tasks in this runtime's scope
    fn shutdown(&mut self) {
        for handle in self.shutdown_handles_mut().iter_mut() {
            handle.0.take().map(|h| h.send(()));
        }
    }

    /// Await the tasks in this runtime's scope
    async fn join(&mut self) {
        for handle in self.join_handles_mut().drain(..) {
            handle.await.ok();
        }
    }

    /// Abort the tasks in this runtime's scope. This will shutdown tasks that have
    /// shutdown handles instead.
    fn abort(&mut self)
    where
        Self: Sized,
    {
        for handles in self.shutdown_handles_mut().iter_mut() {
            if let Some(shutdown_handle) = handles.0.take() {
                if let Err(_) = shutdown_handle.send(()) {
                    handles.1.abort();
                }
            } else {
                handles.1.abort();
            }
        }
    }

    /// Get an event handle of the given type, if it exists in this scope
    fn event_handle<H: 'static + Sender<E> + Clone + Send + Sync, E: 'static + Send + Sync>(&self) -> Option<H> {
        self.senders()
            .get::<H>()
            .and_then(|handle| (!handle.is_closed()).then(|| handle.clone()))
    }

    /// Get an actor's event handle, if it exists in this scope.
    /// Note: This will only return a handle if the actor exists outside of a pool.
    fn actor_event_handle<A: Actor>(&self) -> Option<Act<A>>
    where
        Self: Sized,
    {
        self.senders()
            .get::<<A::Channel as Channel<A::Event>>::Sender>()
            .and_then(|handle| (!handle.is_closed()).then(|| Act(handle.clone())))
    }

    /// Send an event to a given actor, if it exists in this scope
    async fn send_actor_event<A: Actor>(&mut self, event: A::Event) -> anyhow::Result<()>
    where
        Self: Sized,
    {
        let handle = self
            .senders_mut()
            .get_mut::<<A::Channel as Channel<A::Event>>::Sender>()
            .and_then(|handle| (!handle.is_closed()).then(|| handle))
            .ok_or_else(|| anyhow::anyhow!("No channel for this actor!"))?;
        Sender::<A::Event>::send(handle, event).await
    }
}

/// A runtime which manages systems in addition to actors
#[async_trait]
pub trait SystemRuntime: BaseRuntime {
    /// Get the anymap of systems for this runtime's scope
    fn systems(&self) -> &Map<dyn CloneAny + Send + Sync>;

    /// Mutably get the anymap of systems for this runtime's scope
    fn systems_mut(&mut self) -> &mut Map<dyn CloneAny + Send + Sync>;

    /// Spawn this runtime as the runtime of a given system
    async fn as_system<S: 'static + System + Send + Sync>(system: S) -> anyhow::Result<()>
    where
        Self: Into<S::Rt>,
    {
        let mut rt: S::Rt = Self::new(Service::new(S::name())).into();
        let system = Arc::new(RwLock::new(system));
        rt.systems_mut().insert(system.clone());
        let (sender, receiver) = <S::Channel as Channel<S::ChildEvents>>::new();
        rt.senders_mut().insert(sender.clone());
        let (oneshot_send, oneshot_recv) = oneshot::channel::<()>();
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        rt.shutdown_handles_mut().push((Some(oneshot_send), abort_handle.clone()));
        let deps = S::Dependencies::instantiate(&mut rt)
            .map_err(|e| anyhow::anyhow!("Cannot spawn system {}: {}", std::any::type_name::<S>(), e))
            .unwrap();
        let res = {
            let scope = RuntimeScope::new(&mut rt);
            let mut system_rt = SystemScopedRuntime::unsupervised(scope, receiver, oneshot_recv);
            Abortable::new(S::run(system, &mut system_rt, deps), abort_registration).await
        };
        match res {
            Ok(res) => {
                rt.join().await;
                match res {
                    Ok(_) => Ok(()),
                    Err(e) => anyhow::bail!(e),
                }
            }
            Err(_) => {
                log::debug!("Aborting children of actor!");
                rt.abort();
                anyhow::bail!("Aborted!")
            }
        }
    }

    /// Get a shared reference to a system if it exists in this runtime's scope
    fn system<S: 'static + System + Send + Sync>(&self) -> Option<Sys<S>>
    where
        Self: Sized,
    {
        self.systems().get::<Arc<RwLock<S>>>().map(|sys| Sys(sys.clone()))
    }

    /// Get a system's event handle if the system exists in this runtime's scope
    fn system_event_handle<S: System>(&self) -> Option<<S::Channel as Channel<S::ChildEvents>>::Sender>
    where
        Self: Sized,
    {
        self.senders()
            .get::<<S::Channel as Channel<S::ChildEvents>>::Sender>()
            .and_then(|handle| (!handle.is_closed()).then(|| handle.clone()))
    }

    /// Send an event to a system if it exists within this runtime's scope
    async fn send_system_event<S: System>(&mut self, event: S::ChildEvents) -> anyhow::Result<()>
    where
        Self: Sized,
    {
        let handle = self
            .senders_mut()
            .get_mut::<<S::Channel as Channel<S::ChildEvents>>::Sender>()
            .and_then(|handle| (!handle.is_closed()).then(|| handle))
            .ok_or_else(|| anyhow::anyhow!("No channel for this actor!"))?;
        Sender::<S::ChildEvents>::send(handle, event).await
    }
}

/// A runtime which manages shared resources
pub trait ResourceRuntime: BaseRuntime {
    /// Get the runtime scope's shared resources anymap
    fn resources(&self) -> &Map<dyn CloneAny + Send + Sync>;

    /// Mutably get the runtime scope's shared resources anymap
    fn resources_mut(&mut self) -> &mut Map<dyn CloneAny + Send + Sync>;

    /// Get a shared resource if it exists in this runtime's scope
    fn resource<R: 'static + Send + Sync + Clone>(&self) -> Option<Res<R>> {
        self.resources().get::<R>().map(|res| Res(res.clone()))
    }
}

/// A runtime which manages pools of actors
#[async_trait]
pub trait PoolRuntime: BaseRuntime {
    /// Get the anymap of pools from this runtimes's scope
    fn pools(&self) -> &Map<dyn CloneAny + Send + Sync>;

    /// Mutably get the anymap of pools from this runtimes's scope
    fn pools_mut(&mut self) -> &mut Map<dyn CloneAny + Send + Sync>;

    /// Get the pool of a specified actor if it exists in this runtime's scope
    async fn pool<A>(&mut self) -> Option<Res<Arc<RwLock<ActorPool<A>>>>>
    where
        Self: 'static + Sized,
        A: 'static + Actor + Send + Sync,
    {
        match self.pools().get::<Arc<RwLock<ActorPool<A>>>>().cloned() {
            Some(arc) => {
                if arc.write().await.verify() {
                    Some(Res(arc))
                } else {
                    self.pools_mut().remove::<Arc<RwLock<ActorPool<A>>>>();
                    None
                }
            }
            None => None,
        }
    }
}

/// A shared resource
pub struct Res<R: Clone>(R);

impl<R: Deref + Clone> Deref for Res<R> {
    type Target = R::Target;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl<R: DerefMut + Clone> DerefMut for Res<R> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.deref_mut()
    }
}

/// A shared system reference
pub struct Sys<S: System>(Arc<RwLock<S>>);

impl<S: System> Deref for Sys<S> {
    type Target = RwLock<S>;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

/// An actor handle, used to send events
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

/// A pool of actors which can be queried for actor handles
pub struct ActorPool<A: Actor> {
    handles: Vec<Option<Rc<RefCell<<A::Channel as Channel<A::Event>>::Sender>>>>,
    lru: LruCache<usize, Rc<RefCell<<A::Channel as Channel<A::Event>>::Sender>>>,
    id_pool: IdPool<usize>,
}

impl<A: Actor> Clone for ActorPool<A> {
    fn clone(&self) -> Self {
        let handles = self
            .handles
            .iter()
            .map(|opt_rc| opt_rc.as_ref().map(|rc| Rc::new(RefCell::new(rc.borrow().clone()))))
            .collect();
        let mut lru = LruCache::unbounded();
        for (idx, lru_rc) in self.lru.iter().rev() {
            lru.put(*idx, Rc::new(RefCell::new(lru_rc.borrow().clone())));
        }
        Self {
            handles,
            lru,
            id_pool: self.id_pool.clone(),
        }
    }
}

unsafe impl<A: Actor + Send> Send for ActorPool<A> {}

unsafe impl<A: Actor + Sync> Sync for ActorPool<A> {}

impl<A: Actor> Default for ActorPool<A> {
    fn default() -> Self {
        Self {
            handles: Default::default(),
            lru: LruCache::unbounded(),
            id_pool: Default::default(),
        }
    }
}

impl<A: Actor> ActorPool<A> {
    fn push(&mut self, handle: <A::Channel as Channel<A::Event>>::Sender) {
        let id = self.id_pool.get_id();
        let handle_rc = Rc::new(RefCell::new(handle));
        if id >= self.handles.len() {
            self.handles.resize(id + 1, None);
        }
        self.handles[id] = Some(handle_rc.clone());
        self.lru.put(id, handle_rc);
    }

    /// Get the least recently used actor handle from this pool
    pub fn get_lru(&mut self) -> Option<Act<A>> {
        self.lru.pop_lru().map(|(id, handle)| {
            let res = handle.borrow().clone();
            self.lru.put(id, handle);
            Act(res)
        })
    }

    /// Send to the least recently used actor handle in this pool
    pub async fn send_lru(&mut self, event: A::Event) -> anyhow::Result<()> {
        if let Some(mut handle) = self.get_lru() {
            handle.send(event).await
        } else {
            anyhow::bail!("No handles in pool!");
        }
    }

    #[cfg(feature = "rand_pool")]
    /// Get a random actor handle from this pool
    pub fn get_random(&mut self) -> Option<Act<A>> {
        let mut rng = rand::thread_rng();
        let handles = self.handles.iter().filter_map(|h| h.as_ref()).collect::<Vec<_>>();
        handles.get(rng.gen_range(0..handles.len())).map(|rc| Act(rc.borrow().clone()))
    }

    #[cfg(feature = "rand_pool")]
    /// Send to a random actor handle from this pool
    pub async fn send_random(&mut self, event: A::Event) -> anyhow::Result<()> {
        if let Some(mut handle) = self.get_random() {
            handle.send(event).await
        } else {
            anyhow::bail!("No handles in pool!");
        }
    }

    /// Get an iterator over the actor handles in this pool
    pub fn iter(&mut self) -> std::vec::IntoIter<Act<A>> {
        self.handles
            .iter()
            .filter_map(|opt_rc| opt_rc.as_ref().map(|rc| Act(rc.borrow().clone())))
            .collect::<Vec<_>>()
            .into_iter()
    }

    /// Send to every actor handle in this pool
    pub async fn send_all(&mut self, event: A::Event) -> anyhow::Result<()>
    where
        A::Event: Clone,
    {
        for mut handle in self.iter() {
            handle.send(event.clone()).await?;
        }
        Ok(())
    }

    pub(crate) fn verify(&mut self) -> bool {
        for (id, opt) in self.handles.iter_mut().enumerate() {
            if opt.is_some() {
                if opt.as_ref().unwrap().borrow().is_closed() {
                    *opt = None;
                    self.lru.pop(&id);
                    self.id_pool.return_id(id);
                }
            }
        }
        if self.handles.iter().all(|opt| opt.is_none()) {
            false
        } else {
            true
        }
    }
}
