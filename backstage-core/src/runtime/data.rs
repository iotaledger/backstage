use crate::actor::{Actor, Channel, EventDriven, IdPool, Sender, System};
use lru::LruCache;
use std::{
    collections::HashMap,
    hash::Hash,
    ops::{Deref, DerefMut},
    sync::Arc,
};
use tokio::sync::RwLock;

/// Wrapper for data types
pub trait DataWrapper<T> {
    /// Get the wrapper value, consuming the wrapper
    fn into_inner(self) -> T;
}

/// A shared resource
#[derive(Clone)]
pub struct Res<R>(pub R);

impl<R: Deref> Deref for Res<R> {
    type Target = R::Target;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl<R: DerefMut> DerefMut for Res<R> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.deref_mut()
    }
}

impl<R> DataWrapper<R> for Res<R> {
    fn into_inner(self) -> R {
        self.0
    }
}

/// A shared system reference
pub struct Sys<S: System> {
    /// The actor handle
    pub actor: Act<S>,
    /// The shared state of the system
    pub state: Res<S::State>,
}

impl<S: System> Clone for Sys<S> {
    fn clone(&self) -> Self {
        Self {
            actor: self.actor.clone(),
            state: self.state.clone(),
        }
    }
}

/// An actor handle, used to send events
pub struct Act<A: EventDriven>(pub <A::Channel as Channel<A, A::Event>>::Sender);

impl<A: EventDriven> Deref for Act<A> {
    type Target = <A::Channel as Channel<A, A::Event>>::Sender;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<A: EventDriven> DerefMut for Act<A> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<A: EventDriven> Clone for Act<A>
where
    <A::Channel as Channel<A, A::Event>>::Sender: Clone,
{
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<A: EventDriven> DataWrapper<<A::Channel as Channel<A, A::Event>>::Sender> for Act<A> {
    fn into_inner(self) -> <A::Channel as Channel<A, A::Event>>::Sender {
        self.0
    }
}

#[async_trait::async_trait]
impl<A: EventDriven> Sender<A::Event> for Act<A> {
    async fn send(&mut self, event: A::Event) -> anyhow::Result<()> {
        self.0.send(event).await
    }

    fn is_closed(&self) -> bool {
        self.0.is_closed()
    }
}

/// A pool of actors which can be used as a dependency
pub struct Pool<A: Actor, M: Hash + Clone>(pub Arc<RwLock<ActorPool<A, M>>>);

impl<A: Actor, M: Hash + Clone> Clone for Pool<A, M> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<A: Actor, M: Hash + Clone> Deref for Pool<A, M> {
    type Target = Arc<RwLock<ActorPool<A, M>>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<A: Actor, M: Hash + Clone> DerefMut for Pool<A, M> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<A: Actor, M: Hash + Clone> DataWrapper<Arc<RwLock<ActorPool<A, M>>>> for Pool<A, M> {
    fn into_inner(self) -> Arc<RwLock<ActorPool<A, M>>> {
        self.0
    }
}

/// A basic actor pool with no metrics
pub type BasicPool<A> = Pool<A, ()>;

/// A pool of actors which can be queried for actor handles
pub struct ActorPool<A: Actor, M: Hash + Clone> {
    handles: Vec<Option<Act<A>>>,
    lru: LruCache<usize, usize>,
    map: HashMap<M, usize>,
    id_pool: IdPool<usize>,
}

impl<A: Actor, M: Hash + Clone> Default for ActorPool<A, M> {
    fn default() -> Self {
        Self {
            handles: Default::default(),
            lru: LruCache::unbounded(),
            map: Default::default(),
            id_pool: Default::default(),
        }
    }
}

impl<A: Actor, M: Hash + Eq + Clone + Default> ActorPool<A, M> {
    pub(crate) fn push_default_metric(&mut self, handle: Act<A>) {
        self.push(handle, M::default());
    }
}

impl<A: Actor, M: Hash + Eq + Clone> ActorPool<A, M> {
    pub(crate) fn push(&mut self, handle: Act<A>, metric: M) {
        let id = self.id_pool.get_id();
        if id >= self.handles.len() {
            self.handles.resize(id + 1, None);
        }
        self.handles[id] = Some(handle);
        self.lru.put(id, id);
        self.map.insert(metric, id);
    }

    /// Get an actor handle from this pool by a given metric
    pub fn get_by_metric(&self, metric: &M) -> Option<&Act<A>> {
        if let Some(&id) = self.map.get(metric) {
            self.handles[id].as_ref()
        } else {
            None
        }
    }

    /// Mutably get an actor handle from this pool by a given metric
    pub fn get_by_metric_mut(&mut self, metric: &M) -> Option<&mut Act<A>> {
        if let Some(&id) = self.map.get(metric) {
            self.handles[id].as_mut()
        } else {
            None
        }
    }

    /// Send a message to an actor handle from this pool by a given metric
    pub async fn send_by_metric(&mut self, metric: &M, event: A::Event) -> anyhow::Result<()> {
        if let Some(handle) = self.get_by_metric_mut(metric) {
            handle.send(event).await
        } else {
            anyhow::bail!("No handles in pool!");
        }
    }

    /// Get an iterator over the actor handles in this pool
    pub fn iter_with_metrics(&self) -> std::vec::IntoIter<(&M, &Act<A>)> {
        self.map
            .iter()
            .filter_map(|(metric, &id)| self.handles[id].as_ref().map(|h| (metric, h)))
            .collect::<Vec<_>>()
            .into_iter()
    }

    pub(crate) fn verify(&mut self) -> bool {
        for (id, opt) in self.handles.iter_mut().enumerate() {
            if opt.is_some() {
                if opt.as_ref().unwrap().is_closed() {
                    *opt = None;
                    self.lru.pop(&id);
                    self.id_pool.return_id(id);
                    self.map.retain(|_, idx| *idx != id);
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

impl<A: Actor, M: Hash + Clone> ActorPool<A, M> {
    pub(crate) fn push_no_metric(&mut self, handle: Act<A>) {
        let id = self.id_pool.get_id();
        if id >= self.handles.len() {
            self.handles.resize(id + 1, None);
        }
        self.handles[id] = Some(handle);
        self.lru.put(id, id);
    }

    /// Get the least recently used actor handle from this pool
    pub fn get_lru(&mut self) -> Option<&Act<A>> {
        if let Some((id, _)) = self.lru.pop_lru() {
            let res = self.handles[id].as_ref();
            self.lru.put(id, id);
            res
        } else {
            None
        }
    }

    /// Mutably get the least recently used actor handle from this pool
    pub fn get_lru_mut(&mut self) -> Option<&mut Act<A>> {
        if let Some((id, _)) = self.lru.pop_lru() {
            let res = self.handles[id].as_mut();
            self.lru.put(id, id);
            res
        } else {
            None
        }
    }

    /// Send to the least recently used actor handle in this pool
    pub async fn send_lru(&mut self, event: A::Event) -> anyhow::Result<()> {
        if let Some(handle) = self.get_lru_mut() {
            handle.send(event).await
        } else {
            anyhow::bail!("No handles in pool!");
        }
    }

    #[cfg(feature = "rand_pool")]
    /// Get a random actor handle from this pool
    pub fn get_random(&self) -> Option<&Act<A>> {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let mut handles = self.handles.iter().filter_map(|h| h.as_ref()).collect::<Vec<_>>();
        (handles.len() != 0).then(|| handles.remove(rng.gen_range(0..handles.len())))
    }

    #[cfg(feature = "rand_pool")]
    /// Get a random actor handle from this pool
    pub fn get_random_mut(&mut self) -> Option<&mut Act<A>> {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let mut handles = self.handles.iter_mut().filter_map(|h| h.as_mut()).collect::<Vec<_>>();
        (handles.len() != 0).then(|| handles.remove(rng.gen_range(0..handles.len())))
    }

    #[cfg(feature = "rand_pool")]
    /// Send to a random actor handle from this pool
    pub async fn send_random(&mut self, event: A::Event) -> anyhow::Result<()> {
        if let Some(handle) = self.get_random_mut() {
            handle.send(event).await
        } else {
            anyhow::bail!("No handles in pool!");
        }
    }

    /// Get an iterator over the actor handles in this pool
    pub fn iter(&self) -> std::vec::IntoIter<&Act<A>> {
        self.handles.iter().filter_map(|opt| opt.as_ref()).collect::<Vec<_>>().into_iter()
    }

    /// Get an iterator over the actor handles in this pool
    pub fn iter_mut(&mut self) -> std::vec::IntoIter<&mut Act<A>> {
        self.handles
            .iter_mut()
            .filter_map(|opt| opt.as_mut())
            .collect::<Vec<_>>()
            .into_iter()
    }

    /// Send to every actor handle in this pool
    pub async fn send_all(&mut self, event: A::Event) -> anyhow::Result<()>
    where
        A::Event: Clone,
    {
        for handle in self.iter_mut() {
            handle.send(event.clone()).await?;
        }
        Ok(())
    }
}
