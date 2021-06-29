use crate::actor::{Actor, Channel, IdPool, Sender, System};
use lru::LruCache;
use std::{
    cell::RefCell,
    ops::{Deref, DerefMut},
    rc::Rc,
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
pub struct Res<R: Clone>(pub(crate) R);

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
pub struct Sys<S: System>(pub(crate) Arc<RwLock<S>>);

impl<S: System> Deref for Sys<S> {
    type Target = RwLock<S>;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl<S: System> DataWrapper<Arc<RwLock<S>>> for Sys<S> {
    fn into_inner(self) -> Arc<RwLock<S>> {
        self.0
    }
}

/// An actor handle, used to send events
pub struct Act<A: Actor>(pub(crate) <A::Channel as Channel<A::Event>>::Sender);

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

/// A pool of actors which can be used as a dependency
#[derive(Clone)]
pub struct Pool<A: Actor>(pub(crate) Arc<RwLock<ActorPool<A>>>);

impl<A: Actor> Deref for Pool<A> {
    type Target = Arc<RwLock<ActorPool<A>>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<A: Actor> DerefMut for Pool<A> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<A: Actor> DataWrapper<Arc<RwLock<ActorPool<A>>>> for Pool<A> {
    fn into_inner(self) -> Arc<RwLock<ActorPool<A>>> {
        self.0
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
    pub(crate) fn push(&mut self, handle: <A::Channel as Channel<A::Event>>::Sender) {
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
        use rand::Rng;
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
