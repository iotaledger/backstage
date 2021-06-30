use crate::actor::{Actor, Channel, IdPool, Sender, System};
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
pub struct Res<R: Clone>(pub R);

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

impl<R: Clone> DataWrapper<R> for Res<R> {
    fn into_inner(self) -> R {
        self.0
    }
}

/// A shared system reference
#[derive(Clone)]
pub struct Sys<S: System> {
    /// The actor handle
    pub actor: Act<S>,
    /// The shared state of the system
    pub state: Res<S::State>,
}

/// An actor handle, used to send events
pub struct Act<A: Actor>(pub <A::Channel as Channel<A::Event>>::Sender);

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

impl<A: Actor> Clone for Act<A>
where
    <A::Channel as Channel<A::Event>>::Sender: Clone,
{
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<A: Actor> DataWrapper<<A::Channel as Channel<A::Event>>::Sender> for Act<A> {
    fn into_inner(self) -> <A::Channel as Channel<A::Event>>::Sender {
        self.0
    }
}

#[async_trait::async_trait]
impl<A: Actor> Sender<A::Event> for Act<A> {
    async fn send(&mut self, event: A::Event) -> anyhow::Result<()> {
        self.0.send(event).await
    }

    fn is_closed(&self) -> bool {
        self.0.is_closed()
    }
}

/// A pool of actors which can be used as a dependency
#[derive(Clone)]
pub struct Pool<A: Actor, M: Hash + Clone>(pub Arc<RwLock<ActorPool<A, M>>>);

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

pub type BasicPool<A: Actor> = Pool<A, ()>;

/// A pool of actors which can be queried for actor handles
pub struct ActorPool<A: Actor, M: Hash + Clone> {
    handles: Vec<Option<Act<A>>>,
    lru: LruCache<usize, usize>,
    map: HashMap<M, usize>,
    id_pool: IdPool<usize>,
}

impl<A: Actor, M: Hash + Clone> Clone for ActorPool<A, M> {
    fn clone(&self) -> Self {
        let mut lru = LruCache::unbounded();
        for (idx, _) in self.lru.iter().rev() {
            lru.put(*idx, *idx);
        }
        Self {
            handles: self.handles.clone(),
            lru,
            map: self.map.clone(),
            id_pool: self.id_pool.clone(),
        }
    }
}

//unsafe impl<A: Actor + Send, M: Hash + Clone + Send> Send for ActorPool<A, M> {}
//unsafe impl<A: Actor + Sync, M: Hash + Clone + Sync> Sync for ActorPool<A, M> {}

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

    pub fn get_by_metric(&mut self, metric: &M) -> Option<&mut Act<A>> {
        if let Some(&id) = self.map.get(metric) {
            self.handles[id].as_mut()
        } else {
            None
        }
    }

    pub async fn send_by_metric(&mut self, metric: &M, event: A::Event) -> anyhow::Result<()> {
        if let Some(handle) = self.get_by_metric(metric) {
            handle.send(event).await
        } else {
            anyhow::bail!("No handles in pool!");
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
    pub fn get_lru(&mut self) -> Option<&mut Act<A>> {
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
        if let Some(handle) = self.get_lru() {
            handle.send(event).await
        } else {
            anyhow::bail!("No handles in pool!");
        }
    }

    #[cfg(feature = "rand_pool")]
    /// Get a random actor handle from this pool
    pub fn get_random(&mut self) -> Option<&mut Act<A>> {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let mut handles = self.handles.iter_mut().filter_map(|h| h.as_mut()).collect::<Vec<_>>();
        (handles.len() != 0).then(|| handles.remove(rng.gen_range(0..handles.len())))
    }

    #[cfg(feature = "rand_pool")]
    /// Send to a random actor handle from this pool
    pub async fn send_random(&mut self, event: A::Event) -> anyhow::Result<()> {
        if let Some(handle) = self.get_random() {
            handle.send(event).await
        } else {
            anyhow::bail!("No handles in pool!");
        }
    }

    /// Get an iterator over the actor handles in this pool
    pub fn iter(&mut self) -> std::vec::IntoIter<&mut Act<A>> {
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
        for handle in self.iter() {
            handle.send(event.clone()).await?;
        }
        Ok(())
    }

    pub(crate) fn verify(&mut self) -> bool {
        for (id, opt) in self.handles.iter_mut().enumerate() {
            if opt.is_some() {
                if opt.as_ref().unwrap().is_closed() {
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
