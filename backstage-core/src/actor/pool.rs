use crate::prelude::*;
use async_trait::async_trait;
use lru::LruCache;
use std::ops::DerefMut;
use std::{collections::HashMap, hash::Hash};
use tokio::sync::RwLock;

#[async_trait]
pub trait ActorPool: Default {
    type Actor: Actor;

    async fn verify(&self) -> bool;

    async fn handles(&self) -> Vec<Act<Self::Actor>>;

    async fn send_all(&self, event: <Self::Actor as Actor>::Event) -> anyhow::Result<()>
    where
        <Self::Actor as Actor>::Event: Clone;
}

#[async_trait]
pub trait BasicActorPool: ActorPool {
    async fn push(&self, handle: Act<Self::Actor>);

    async fn get(&self) -> Option<Act<Self::Actor>>;

    async fn send(&self, event: <Self::Actor as Actor>::Event) -> anyhow::Result<()>;
}

#[async_trait]
pub trait KeyedActorPool: ActorPool {
    type Key: Send + Sync;

    async fn push(&self, key: Self::Key, handle: Act<Self::Actor>);

    async fn get(&self, key: &Self::Key) -> Option<Act<Self::Actor>>;

    async fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = (Self::Key, Act<Self::Actor>)> + 'a>;

    async fn send(&self, key: &Self::Key, event: <Self::Actor as Actor>::Event) -> anyhow::Result<()>;
}

#[cfg(feature = "rand_pool")]
pub struct RandomPool<A: Actor> {
    handles: RwLock<Vec<Act<A>>>,
}

impl<A: Actor> Default for RandomPool<A> {
    fn default() -> Self {
        Self {
            handles: RwLock::new(Vec::new()),
        }
    }
}

#[cfg(feature = "rand_pool")]
#[async_trait]
impl<A: Actor> ActorPool for RandomPool<A> {
    type Actor = A;

    async fn verify(&self) -> bool {
        let mut handles = self.handles.write().await;
        handles.retain(|handle| !handle.is_closed());
        handles.len() != 0
    }

    async fn handles(&self) -> Vec<Act<A>> {
        self.handles.read().await.iter().filter(|h| !h.is_closed()).cloned().collect()
    }

    async fn send_all(&self, event: <Self::Actor as Actor>::Event) -> anyhow::Result<()>
    where
        <Self::Actor as Actor>::Event: Clone,
    {
        for handle in self.handles.read().await.iter().filter(|h| !h.is_closed()) {
            handle.send(event.clone())?;
        }
        Ok(())
    }
}

#[cfg(feature = "rand_pool")]
use rand::Rng;

#[cfg(feature = "rand_pool")]
#[async_trait]
impl<A: Actor> BasicActorPool for RandomPool<A> {
    async fn push(&self, handle: Act<Self::Actor>) {
        self.handles.write().await.push(handle);
    }

    async fn get(&self) -> Option<Act<Self::Actor>> {
        if !self.verify().await {
            return None;
        }
        let handles = self.handles.read().await;
        if handles.len() != 0 {
            let mut rng = rand::thread_rng();
            handles.get(rng.gen_range(0..handles.len())).cloned()
        } else {
            None
        }
    }

    async fn send(&self, event: <Self::Actor as Actor>::Event) -> anyhow::Result<()> {
        let handles = self.handles.read().await;
        if let Some(handle) = if handles.len() != 0 {
            let mut rng = rand::thread_rng();
            handles.get(rng.gen_range(0..handles.len()))
        } else {
            None
        } {
            handle.send(event)
        } else {
            anyhow::bail!("No handles to send to!");
        }
    }
}

pub struct LruPool<A: Actor> {
    inner: RwLock<LruInner<A>>,
}

pub struct LruInner<A: Actor> {
    handles: Vec<Option<Act<A>>>,
    lru: LruCache<usize, usize>,
    id_pool: IdPool<usize>,
}

#[async_trait]
impl<A: Actor> ActorPool for LruPool<A> {
    type Actor = A;

    async fn verify(&self) -> bool {
        let mut lock = self.inner.write().await;
        let inner = lock.deref_mut();
        for (id, opt) in inner.handles.iter_mut().enumerate() {
            if opt.is_some() {
                if opt.as_ref().unwrap().is_closed() {
                    *opt = None;
                    inner.lru.pop(&id);
                    inner.id_pool.return_id(id);
                }
            }
        }
        if inner.handles.iter().all(|opt| opt.is_none()) {
            false
        } else {
            true
        }
    }

    async fn handles(&self) -> Vec<Act<A>> {
        self.inner
            .read()
            .await
            .handles
            .iter()
            .filter_map(Option::as_ref)
            .filter(|h| !h.is_closed())
            .cloned()
            .collect()
    }

    async fn send_all(&self, event: <Self::Actor as Actor>::Event) -> anyhow::Result<()>
    where
        <Self::Actor as Actor>::Event: Clone,
    {
        for handle in self
            .inner
            .read()
            .await
            .handles
            .iter()
            .filter_map(Option::as_ref)
            .filter(|h| !h.is_closed())
        {
            handle.send(event.clone())?;
        }
        Ok(())
    }
}

#[async_trait]
impl<A: Actor> BasicActorPool for LruPool<A> {
    async fn push(&self, handle: Act<Self::Actor>) {
        let mut inner = self.inner.write().await;
        let id = inner.id_pool.get_id();
        if id >= inner.handles.len() {
            inner.handles.resize(id + 1, None);
        }
        inner.handles[id] = Some(handle);
        inner.lru.put(id, id);
    }

    /// Get the least recently used handle
    async fn get(&self) -> Option<Act<Self::Actor>> {
        if !self.verify().await {
            return None;
        }
        let mut inner = self.inner.write().await;
        if let Some((id, _)) = inner.lru.pop_lru() {
            let res = inner.handles[id].clone();
            inner.lru.put(id, id);
            res
        } else {
            None
        }
    }

    async fn send(&self, event: <Self::Actor as Actor>::Event) -> anyhow::Result<()> {
        let mut inner = self.inner.write().await;
        if let Some(handle) = if let Some((id, _)) = inner.lru.pop_lru() {
            let res = inner.handles[id].clone();
            inner.lru.put(id, id);
            res
        } else {
            None
        } {
            handle.send(event)
        } else {
            anyhow::bail!("No handles to send to!");
        }
    }
}

impl<A: Actor> Default for LruPool<A> {
    fn default() -> Self {
        Self { inner: Default::default() }
    }
}

impl<A: Actor> Default for LruInner<A> {
    fn default() -> Self {
        Self {
            handles: Default::default(),
            lru: LruCache::unbounded(),
            id_pool: Default::default(),
        }
    }
}

pub struct MapPool<A: Actor, M: Hash + Clone> {
    map: RwLock<HashMap<M, Act<A>>>,
}

impl<A: Actor, M: Hash + Clone> Default for MapPool<A, M> {
    fn default() -> Self {
        Self { map: Default::default() }
    }
}

#[async_trait]
impl<A: Actor, M: Hash + Clone + Send + Sync + Eq> ActorPool for MapPool<A, M> {
    type Actor = A;

    async fn verify(&self) -> bool {
        let mut map = self.map.write().await;
        map.retain(|_, handle| !handle.is_closed());
        map.len() != 0
    }

    async fn handles(&self) -> Vec<Act<A>> {
        self.map.read().await.values().filter(|h| !h.is_closed()).cloned().collect()
    }

    async fn send_all(&self, event: <Self::Actor as Actor>::Event) -> anyhow::Result<()>
    where
        <Self::Actor as Actor>::Event: Clone,
    {
        for handle in self.map.read().await.values().filter(|h| !h.is_closed()) {
            handle.send(event.clone())?;
        }
        Ok(())
    }
}

#[async_trait]
impl<A: Actor, M: Hash + Clone + Send + Sync + Eq> KeyedActorPool for MapPool<A, M> {
    type Key = M;

    async fn push(&self, key: Self::Key, handle: Act<Self::Actor>) {
        self.map.write().await.insert(key, handle);
    }

    async fn get(&self, key: &Self::Key) -> Option<Act<Self::Actor>> {
        if !self.verify().await {
            return None;
        }
        self.map.read().await.get(key).cloned()
    }

    async fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = (Self::Key, Act<Self::Actor>)> + 'a> {
        Box::new(
            self.map
                .read()
                .await
                .iter()
                .filter_map(|(k, v)| (!v.is_closed()).then(|| (k.clone(), v.clone())))
                .collect::<Vec<_>>()
                .into_iter(),
        )
    }

    async fn send(&self, key: &Self::Key, event: <Self::Actor as Actor>::Event) -> anyhow::Result<()> {
        if let Some(handle) = self.map.read().await.get(key) {
            handle.send(event)
        } else {
            anyhow::bail!("No handle for the given key!");
        }
    }
}
