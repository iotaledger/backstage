use crate::{actor::shutdown_stream::ShutdownStream, Actor, Channel, IdPool, Sender, SupervisorEvent, System};
use anymap::any::{Any, UncheckedAnyExt};
use async_recursion::async_recursion;
use futures::{
    future::{AbortHandle, Abortable, BoxFuture},
    StreamExt,
};
use lru::LruCache;
use ptree::{write_tree, TreeItem};
#[cfg(feature = "rand_pool")]
use rand::Rng;
use std::{
    any::TypeId,
    cell::RefCell,
    collections::HashMap,
    marker::PhantomData,
    ops::{Deref, DerefMut},
    rc::Rc,
    sync::Arc,
};
use tokio::{
    sync::{oneshot, RwLock},
    task::JoinHandle,
};

mod scopes;
pub use scopes::*;

/// An alias type indicating that this is a scope id
pub type ScopeId = usize;
type DataId = usize;

/// A scope, which marks data as usable for a given task
pub struct Scope {
    id: ScopeId,
    data: HashMap<TypeId, DataId>,
}

impl Scope {
    fn new(id: usize, parent: Option<&Scope>) -> Self {
        Scope {
            id,
            data: parent.map(|p| p.data.clone()).unwrap_or_default(),
        }
    }
}

/// The central registry that stores all data for the application.
/// Data is accessable via scopes which track its location within
/// the global dyn vector. Two id pools manage the reusable indexes
/// for this data.
pub struct Registry {
    data: Vec<Option<Box<dyn Any + Send + Sync>>>,
    data_source: HashMap<DataId, ScopeId>,
    parents: HashMap<ScopeId, ScopeId>,
    children: HashMap<ScopeId, Vec<ScopeId>>,
    scopes: HashMap<ScopeId, Scope>,
    scope_pool: IdPool<ScopeId>,
    data_pool: IdPool<DataId>,
}

impl Default for Registry {
    fn default() -> Self {
        Self {
            data: Vec::with_capacity(100),
            data_source: Default::default(),
            parents: Default::default(),
            children: Default::default(),
            scopes: Default::default(),
            scope_pool: Default::default(),
            data_pool: Default::default(),
        }
    }
}

impl Registry {
    /// Create a new scope with an optional parent
    pub fn new_scope<P: Into<Option<ScopeId>>>(&mut self, parent: P) -> usize {
        let scope_id = self.scope_pool.get_id();
        //log::debug!("Creating scope {}", scope_id);
        let parent = parent.into();
        if let Some(parent_id) = parent {
            self.parents.insert(scope_id, parent_id);
            self.children.entry(parent_id).or_default().push(scope_id);
        }
        let scope = Scope::new(scope_id, parent.and_then(|p| self.scopes.get(&p)));
        self.scopes.insert(scope_id, scope);
        scope_id
    }

    /// Drop a scope and all of its children recursively
    #[async_recursion]
    pub async fn drop_scope(&mut self, scope_id: &ScopeId) {
        if let Some(children) = self.children.remove(scope_id) {
            let children = children
                .iter()
                .map(|id| self.scopes.get(id).map(|s| s.id))
                .filter_map(|v| v)
                .collect::<Vec<_>>();
            for child in children.iter() {
                self.drop_scope(&child).await;
            }
        }
        self.scopes.remove(scope_id);
        for (&data_id, _) in self.data_source.iter().filter(|&(_, id)| id == scope_id) {
            if self.data[data_id].take().is_some() {
                self.data_pool.return_id(data_id);
            }
        }
        if let Some(parent) = self.parents.remove(scope_id) {
            self.children.get_mut(&parent).unwrap().retain(|id| id != scope_id);
        }
        self.scope_pool.return_id(*scope_id);
    }

    /// Add some arbitrary data to a scope and its children recursively
    pub fn add_data<T: 'static + Send + Sync>(&mut self, scope_id: &ScopeId, data: T) -> anyhow::Result<()> {
        log::debug!("Adding {} to scope {}", std::any::type_name::<T>(), scope_id);
        let scope = self
            .scopes
            .get_mut(scope_id)
            .ok_or_else(|| anyhow::anyhow!("No scope with id {}!", scope_id))?;
        let data_id = self.data_pool.get_id();
        if self.data.len() <= data_id {
            self.data.resize_with(data_id + 11, || None);
        }
        self.data[data_id].replace(Box::new(data));
        self.data_source.insert(data_id, *scope_id);
        scope.data.insert(TypeId::of::<T>(), data_id);
        if let Some(children) = self.children.get(scope_id).cloned() {
            for child in children.iter() {
                self.propagate_data::<T>(child, Propagation::Add(data_id));
            }
        }
        Ok(())
    }

    /// Remove some data from this scope and its children.
    /// NOTE: This will only remove data if this scope originally added it! Otherwise,
    /// this fn will return an error.
    pub fn remove_data<T: 'static + Send + Sync>(&mut self, scope_id: &ScopeId) -> anyhow::Result<Option<T>> {
        log::debug!("Removing {} from scope {}", std::any::type_name::<T>(), scope_id);
        if let Some(data_id) = self
            .scopes
            .get(scope_id)
            .and_then(|scope| scope.data.get(&TypeId::of::<T>()))
            .and_then(|data_id| {
                self.data_source
                    .get(data_id)
                    .and_then(|source| (source == scope_id).then(|| *data_id))
            })
        {
            let data = self.data[data_id].take();
            Ok(data.map(|data| {
                self.data_pool.return_id(data_id);
                if let Some(children) = self.children.get(scope_id).cloned() {
                    for child in children.iter() {
                        self.propagate_data::<T>(child, Propagation::Remove);
                    }
                }
                *unsafe { data.downcast_unchecked::<T>() }
            }))
        } else {
            anyhow::bail!("This scope does not own this data!")
        }
    }

    fn propagate_data<T: 'static + Send + Sync>(&mut self, scope_id: &ScopeId, prop: Propagation) {
        log::debug!("Propagating {} to scope {}", std::any::type_name::<T>(), scope_id);
        if let Some(scope) = self.scopes.get_mut(scope_id) {
            match prop {
                Propagation::Add(data_id) => {
                    scope.data.insert(TypeId::of::<T>(), data_id);
                }
                Propagation::Remove => {
                    scope.data.remove(&TypeId::of::<T>());
                }
            }
        }
        if let Some(children) = self.children.get(scope_id).cloned() {
            for child in children.iter() {
                self.propagate_data::<T>(child, prop);
            }
        }
    }

    /// Get a reference to some arbitrary data from the given scope
    pub fn get_data<T: 'static + Send + Sync>(&self, scope_id: &ScopeId) -> Option<&T> {
        self.scopes
            .get(scope_id)
            .and_then(|scope| scope.data.get(&TypeId::of::<T>()))
            .and_then(|data_id| self.data[*data_id].as_ref())
            .map(|t| unsafe { t.downcast_ref_unchecked::<T>() })
    }

    /// Get a mutable reference to some arbitrary data from the given scope
    pub fn get_data_mut<T: 'static + Send + Sync>(&mut self, scope_id: &ScopeId) -> Option<&mut T> {
        self.scopes
            .get(scope_id)
            .and_then(|scope| scope.data.get(&TypeId::of::<T>()).cloned())
            .and_then(move |data_id| self.data[data_id].as_mut().map(|t| unsafe { t.downcast_mut_unchecked::<T>() }))
    }
}

#[derive(Clone)]
pub(crate) struct PrintableRegistry<'a>(&'a Registry, ScopeId);

impl<'a> TreeItem for PrintableRegistry<'a> {
    type Child = Self;

    fn write_self<W: std::io::Write>(&self, f: &mut W, _style: &ptree::Style) -> std::io::Result<()> {
        let PrintableRegistry(registry, scope_id) = self;
        write!(
            f,
            "Scope {}, Data {:?}",
            scope_id,
            registry
                .data_source
                .iter()
                .filter_map(|(data, id)| (id == scope_id).then(|| data))
                .collect::<Vec<_>>()
        )
    }

    fn children(&self) -> std::borrow::Cow<[Self::Child]> {
        let PrintableRegistry(registry, scope_id) = self;
        registry
            .children
            .get(scope_id)
            .cloned()
            .unwrap_or_default()
            .iter()
            .map(|&c| PrintableRegistry(registry.clone(), c))
            .collect::<Vec<_>>()
            .into()
    }
}

impl<'a> std::fmt::Display for PrintableRegistry<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut buf = std::io::Cursor::new(Vec::<u8>::new());
        write_tree(self, &mut buf).ok();
        write!(f, "{}", String::from_utf8_lossy(&buf.into_inner()))
    }
}

#[derive(Copy, Clone)]
enum Propagation {
    Add(DataId),
    Remove,
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
