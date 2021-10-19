// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use crate::prelude::*;
use async_trait::async_trait;
use lru::LruCache;
use std::{collections::HashMap, hash::Hash, ops::DerefMut};
use tokio::sync::RwLock;

/// An actor pool which contains some number of handles
#[async_trait]
pub trait ActorPool: Default
where
    Act<Self::Actor>: Clone,
{
    /// The actor type of this pool
    type Actor: Actor;

    /// Verify the pool by removing any closed handles. An empty pool is invalid.
    async fn verify(&self) -> bool;

    /// Get a copy of the handles in this pool
    async fn handles(&self) -> Vec<Act<Self::Actor>>;

    /// Send a cloneable event to all handles in this pool
    async fn send_all<E>(&self, event: E) -> anyhow::Result<()>
    where
        E: 'static + Clone + Send + Sync + DynEvent<Self::Actor>,
        Self::Actor: 'static + Send + Sync + HandleEvent<E>;
}

/// A basic actor pool which allows retrieving handles via its internal method
#[async_trait]
pub trait BasicActorPool: ActorPool
where
    Act<Self::Actor>: Clone,
{
    /// Push a handle into the pool
    async fn push(&self, handle: Act<Self::Actor>);

    /// Get a handle from the pool
    async fn get(&self) -> Option<Act<Self::Actor>>;

    /// Send to a handle in the pool
    async fn send<E>(&self, event: E) -> anyhow::Result<()>
    where
        E: 'static + Send + Sync + DynEvent<Self::Actor>,
        Self::Actor: 'static + Send + Sync + HandleEvent<E>;
}

/// A keyed actor pool which can be accessed via some metric
#[async_trait]
pub trait KeyedActorPool: ActorPool
where
    Act<Self::Actor>: Clone,
{
    /// The key that identifies each handle in the pool
    type Key: Send + Sync;

    /// Push a handle into the pool
    async fn push(&self, key: Self::Key, handle: Act<Self::Actor>);

    /// Get a handle from the pool via a key
    async fn get(&self, key: &Self::Key) -> Option<Act<Self::Actor>>;

    /// Iterate the key/handle pairs in the pool
    async fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = (Self::Key, Act<Self::Actor>)> + 'a>;

    /// Send to a handle in the pool with a key
    async fn send<E>(&self, key: &Self::Key, event: E) -> anyhow::Result<()>
    where
        E: 'static + Send + Sync + DynEvent<Self::Actor>,
        Self::Actor: 'static + Send + Sync + HandleEvent<E>;
}

#[cfg(feature = "rand_pool")]
/// A pool which randomly returns a handle
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
impl<A: 'static + Actor + Send + Sync> ActorPool for RandomPool<A>
where
    Act<A>: Clone,
{
    type Actor = A;

    async fn verify(&self) -> bool {
        let mut handles = self.handles.write().await;
        handles.retain(|handle| !handle.is_closed());
        handles.len() != 0
    }

    async fn handles(&self) -> Vec<Act<A>> {
        self.handles
            .read()
            .await
            .iter()
            .filter(|h| !h.is_closed())
            .cloned()
            .collect()
    }

    async fn send_all<E>(&self, event: E) -> anyhow::Result<()>
    where
        E: 'static + Clone + Send + Sync + DynEvent<Self::Actor>,
        Self::Actor: HandleEvent<E>,
    {
        for handle in self.handles.read().await.iter().filter(|h| !h.is_closed()) {
            handle.send(Box::new(event.clone()))?;
        }
        Ok(())
    }
}

#[cfg(feature = "rand_pool")]
use rand::Rng;

#[cfg(feature = "rand_pool")]
#[async_trait]
impl<A: 'static + Actor + Send + Sync> BasicActorPool for RandomPool<A>
where
    Act<A>: Clone,
{
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

    async fn send<E>(&self, event: E) -> anyhow::Result<()>
    where
        E: 'static + Send + Sync + DynEvent<Self::Actor>,
        Self::Actor: 'static + Send + Sync + HandleEvent<E>,
    {
        let handles = self.handles.read().await;
        if let Some(handle) = if handles.len() != 0 {
            let mut rng = rand::thread_rng();
            handles.get(rng.gen_range(0..handles.len()))
        } else {
            None
        } {
            handle.send(Box::new(event))
        } else {
            anyhow::bail!("No handles to send to!");
        }
    }
}

/// A pool which returns the least recently used handle
pub struct LruPool<A: Actor> {
    inner: RwLock<LruInner<A>>,
}

struct LruInner<A: Actor> {
    handles: Vec<Option<Act<A>>>,
    lru: LruCache<usize, usize>,
    id_pool: IdPool<usize>,
}

#[async_trait]
impl<A: 'static + Actor + Send + Sync> ActorPool for LruPool<A>
where
    Act<A>: Clone,
{
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

    async fn send_all<E>(&self, event: E) -> anyhow::Result<()>
    where
        E: 'static + Clone + Send + Sync + DynEvent<Self::Actor>,
        Self::Actor: 'static + Send + Sync + HandleEvent<E>,
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
            handle.send(Box::new(event.clone()))?;
        }
        Ok(())
    }
}

#[async_trait]
impl<A: 'static + Actor + Send + Sync> BasicActorPool for LruPool<A>
where
    Act<A>: Clone,
{
    async fn push(&self, handle: Act<Self::Actor>) {
        let mut inner = self.inner.write().await;
        let id = inner.id_pool.get_id().expect("Out of space for handles!");
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

    async fn send<E>(&self, event: E) -> anyhow::Result<()>
    where
        E: 'static + Send + Sync + DynEvent<Self::Actor>,
        Self::Actor: 'static + Send + Sync + HandleEvent<E>,
    {
        let mut inner = self.inner.write().await;
        if let Some(handle) = if let Some((id, _)) = inner.lru.pop_lru() {
            let res = inner.handles[id].clone();
            inner.lru.put(id, id);
            res
        } else {
            None
        } {
            handle.send(Box::new(event))
        } else {
            anyhow::bail!("No handles to send to!");
        }
    }
}

impl<A: Actor> Default for LruPool<A> {
    fn default() -> Self {
        Self {
            inner: Default::default(),
        }
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

/// A keyed pool which stores handles in a HashMap
pub struct MapPool<A: Actor, M: Hash + Clone> {
    map: RwLock<HashMap<M, Act<A>>>,
}

impl<A: Actor, M: Hash + Clone> Default for MapPool<A, M> {
    fn default() -> Self {
        Self {
            map: Default::default(),
        }
    }
}

#[async_trait]
impl<A: 'static + Actor + Send + Sync, M: Hash + Clone + Send + Sync + Eq> ActorPool for MapPool<A, M>
where
    Act<A>: Clone,
{
    type Actor = A;

    async fn verify(&self) -> bool {
        let mut map = self.map.write().await;
        map.retain(|_, handle| !handle.is_closed());
        map.len() != 0
    }

    async fn handles(&self) -> Vec<Act<A>> {
        self.map
            .read()
            .await
            .values()
            .filter(|h| !h.is_closed())
            .cloned()
            .collect()
    }

    async fn send_all<E>(&self, event: E) -> anyhow::Result<()>
    where
        E: 'static + Clone + Send + Sync + DynEvent<Self::Actor>,
        Self::Actor: 'static + Send + Sync + HandleEvent<E>,
    {
        for handle in self.map.read().await.values().filter(|h| !h.is_closed()) {
            handle.send(Box::new(event.clone()))?;
        }
        Ok(())
    }
}

#[async_trait]
impl<A: 'static + Actor + Send + Sync, M: Hash + Clone + Send + Sync + Eq> KeyedActorPool for MapPool<A, M>
where
    Act<A>: Clone,
{
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

    async fn send<E>(&self, key: &Self::Key, event: E) -> anyhow::Result<()>
    where
        E: 'static + Send + Sync + DynEvent<Self::Actor>,
        Self::Actor: 'static + Send + Sync + HandleEvent<E>,
    {
        if let Some(handle) = self.map.read().await.get(key) {
            handle.send(Box::new(event))
        } else {
            anyhow::bail!("No handle for the given key!");
        }
    }
}
