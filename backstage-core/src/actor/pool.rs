use crate::prelude::*;
use async_trait::async_trait;
use lru::LruCache;
use std::{collections::HashMap, hash::Hash};

#[async_trait]
pub trait ActorPool: Default {
    type Actor: Actor;

    fn verify(&mut self) -> bool;

    fn handles<'a>(&'a self) -> Box<dyn std::iter::Iterator<Item = &Act<Self::Actor>> + 'a + Send>;

    fn handles_mut<'a>(&'a mut self) -> Box<dyn std::iter::Iterator<Item = &mut Act<Self::Actor>> + 'a + Send>;

    async fn send_all(&mut self, event: <Self::Actor as Actor>::Event) -> anyhow::Result<()>
    where
        <Self::Actor as Actor>::Event: Clone,
    {
        for handle in self.handles_mut() {
            handle.send(event.clone()).await?;
        }
        Ok(())
    }
}

#[async_trait]
pub trait BasicActorPool: ActorPool {
    fn push(&mut self, handle: Act<Self::Actor>);

    fn get(&self) -> Option<&Act<Self::Actor>>;

    fn get_mut(&mut self) -> Option<&mut Act<Self::Actor>>;

    async fn send(&mut self, event: <Self::Actor as Actor>::Event) -> anyhow::Result<()> {
        if let Some(handle) = self.get_mut() {
            handle.send(event).await
        } else {
            anyhow::bail!("No handles to send to!");
        }
    }
}

#[async_trait]
pub trait KeyedActorPool: ActorPool {
    type Key: Send + Sync;

    fn push(&mut self, key: Self::Key, handle: Act<Self::Actor>);

    fn get(&self, key: &Self::Key) -> Option<&Act<Self::Actor>>;

    fn get_mut(&mut self, key: &Self::Key) -> Option<&mut Act<Self::Actor>>;

    fn iter<'a>(&'a self) -> Box<dyn std::iter::Iterator<Item = (&Self::Key, &Act<Self::Actor>)> + 'a + Send>;

    async fn send(&mut self, key: &Self::Key, event: <Self::Actor as Actor>::Event) -> anyhow::Result<()> {
        if let Some(handle) = KeyedActorPool::get_mut(self, key) {
            handle.send(event).await
        } else {
            anyhow::bail!("No handle for the given key!");
        }
    }
}

#[cfg(feature = "rand_pool")]
pub struct RandomPool<A: Actor> {
    handles: Vec<Act<A>>,
}

impl<A: Actor> Default for RandomPool<A> {
    fn default() -> Self {
        Self { handles: Vec::new() }
    }
}

#[cfg(feature = "rand_pool")]
impl<A: Actor> ActorPool for RandomPool<A> {
    type Actor = A;

    fn verify(&mut self) -> bool {
        self.handles.retain(|handle| !handle.is_closed());
        self.handles.len() != 0
    }

    fn handles<'a>(&'a self) -> Box<dyn std::iter::Iterator<Item = &Act<Self::Actor>> + 'a + Send> {
        Box::new(self.handles.iter())
    }

    fn handles_mut<'a>(&'a mut self) -> Box<dyn std::iter::Iterator<Item = &mut Act<Self::Actor>> + 'a + Send> {
        Box::new(self.handles.iter_mut())
    }
}

#[cfg(feature = "rand_pool")]
impl<A: Actor> BasicActorPool for RandomPool<A> {
    fn push(&mut self, handle: Act<Self::Actor>) {
        self.handles.push(handle);
    }

    fn get(&self) -> Option<&Act<Self::Actor>> {
        use rand::Rng;
        if self.handles.len() != 0 {
            let mut rng = rand::thread_rng();
            self.handles.get(rng.gen_range(0..self.handles.len()))
        } else {
            None
        }
    }

    fn get_mut(&mut self) -> Option<&mut Act<Self::Actor>> {
        use rand::Rng;
        if self.handles.len() != 0 {
            let mut rng = rand::thread_rng();
            let idx = rng.gen_range(0..self.handles.len());
            self.handles.get_mut(idx)
        } else {
            None
        }
    }
}

pub struct LruPool<A: Actor> {
    handles: Vec<Option<Act<A>>>,
    lru: LruCache<usize, usize>,
    id_pool: IdPool<usize>,
}

impl<A: Actor> ActorPool for LruPool<A> {
    type Actor = A;

    fn verify(&mut self) -> bool {
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

    fn handles<'a>(&'a self) -> Box<dyn std::iter::Iterator<Item = &Act<Self::Actor>> + 'a + Send> {
        Box::new(self.handles.iter().filter_map(Option::as_ref))
    }

    fn handles_mut<'a>(&'a mut self) -> Box<dyn std::iter::Iterator<Item = &mut Act<Self::Actor>> + 'a + Send> {
        Box::new(self.handles.iter_mut().filter_map(Option::as_mut))
    }
}

impl<A: Actor> BasicActorPool for LruPool<A> {
    fn push(&mut self, handle: Act<Self::Actor>) {
        let id = self.id_pool.get_id();
        if id >= self.handles.len() {
            self.handles.resize(id + 1, None);
        }
        self.handles[id] = Some(handle);
        self.lru.put(id, id);
    }

    /// Get the least recently used handle. WARNING: This will not
    /// update the LRU as this call does not grant mutable access to
    /// the pool!
    fn get(&self) -> Option<&Act<Self::Actor>> {
        if let Some((&id, _)) = self.lru.peek_lru() {
            let res = self.handles[id].as_ref();
            res
        } else {
            None
        }
    }

    fn get_mut(&mut self) -> Option<&mut Act<Self::Actor>> {
        if let Some((id, _)) = self.lru.pop_lru() {
            let res = self.handles[id].as_mut();
            self.lru.put(id, id);
            res
        } else {
            None
        }
    }
}

impl<A: Actor> Default for LruPool<A> {
    fn default() -> Self {
        Self {
            handles: Default::default(),
            lru: LruCache::unbounded(),
            id_pool: Default::default(),
        }
    }
}

pub struct MapPool<A: Actor, M: Hash + Clone> {
    map: HashMap<M, Act<A>>,
}

impl<A: Actor, M: Hash + Clone> Default for MapPool<A, M> {
    fn default() -> Self {
        Self { map: Default::default() }
    }
}

impl<A: Actor, M: Hash + Clone + Send + Sync + Eq> ActorPool for MapPool<A, M> {
    type Actor = A;

    fn verify(&mut self) -> bool {
        self.map.retain(|_, handle| !handle.is_closed());
        self.map.len() != 0
    }

    fn handles<'a>(&'a self) -> Box<dyn std::iter::Iterator<Item = &Act<Self::Actor>> + 'a + Send> {
        Box::new(self.map.values())
    }

    fn handles_mut<'a>(&'a mut self) -> Box<dyn std::iter::Iterator<Item = &mut Act<Self::Actor>> + 'a + Send> {
        Box::new(self.map.values_mut())
    }
}

impl<A: Actor, M: Hash + Clone + Send + Sync + Eq> KeyedActorPool for MapPool<A, M> {
    type Key = M;

    fn push(&mut self, key: Self::Key, handle: Act<Self::Actor>) {
        self.map.insert(key, handle);
    }

    fn get(&self, key: &Self::Key) -> Option<&Act<Self::Actor>> {
        self.map.get(key)
    }

    fn get_mut(&mut self, key: &Self::Key) -> Option<&mut Act<Self::Actor>> {
        self.map.get_mut(key)
    }

    fn iter<'a>(&'a self) -> Box<dyn std::iter::Iterator<Item = (&Self::Key, &Act<Self::Actor>)> + 'a + Send> {
        Box::new(self.map.iter())
    }
}
