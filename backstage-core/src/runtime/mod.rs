use crate::actor::{Actor, Channel, IdPool, Sender, Service, ServiceStatus, ServiceTree, ShutdownHandle, System};
use anymap::{
    any::{CloneAny, UncheckedAnyExt},
    raw::Entry,
};
use async_recursion::async_recursion;
use async_trait::async_trait;
use futures::{
    future::{AbortHandle, Abortable, BoxFuture},
    task::AtomicWaker,
    Future,
};
use ptree::{write_tree, TreeItem};
use std::{
    any::TypeId,
    borrow::Cow,
    collections::{HashMap, HashSet},
    marker::PhantomData,
    ops::{Deref, DerefMut},
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task::{Context, Poll},
};
use tokio::{sync::RwLock, task::JoinHandle};

mod data;
pub use data::*;
mod scopes;
pub use scopes::*;
mod access;
pub use access::*;

/// An alias type indicating that this is a scope id
pub type ScopeId = usize;
type DataId = usize;

#[derive(Clone)]
enum Dependency {
    Once(DepSignal),
    Linked(DepSignal),
}

impl Dependency {
    pub fn upgrade(&mut self) {
        match self {
            Dependency::Once(flag) => *self = Dependency::Linked(flag.clone()),
            _ => (),
        }
    }
}

/// The status of a dependency
pub enum DepStatus<T> {
    /// The dependency is ready to be used
    Ready(T),
    /// The dependency is not ready, here is a flag to await
    Waiting(DepHandle<T>),
}

impl<T: 'static + Clone + Send + Sync> Into<Option<T>> for DepStatus<T> {
    fn into(self) -> Option<T> {
        match self {
            DepStatus::Ready(t) => Some(t),
            DepStatus::Waiting(h) => {
                if h.flag.set.load(Ordering::Relaxed) {
                    h.flag.val.try_read().ok().and_then(|lock| {
                        lock.clone().map(|d| {
                            log::trace!("About to downcast a ready dep to type {}", std::any::type_name::<T>());
                            *unsafe { d.downcast_unchecked() }
                        })
                    })
                } else {
                    None
                }
            }
        }
    }
}

impl<T: 'static + Clone + Send + Sync> DepStatus<T> {
    /// Wait for a dependency to become ready.
    /// Will return immediately if it is already ready.
    /// Will return an Err if the containing scope is dropped.
    pub async fn get(self) -> anyhow::Result<T> {
        match self {
            DepStatus::Ready(t) => Ok(t),
            DepStatus::Waiting(h) => h.await,
        }
    }

    /// Get the value of a dependency if it is ready, otherwise return None.
    pub fn get_opt(self) -> Option<T> {
        self.into()
    }
}

pub(crate) enum RawDepStatus {
    /// The dependency is ready to be used
    Ready(Box<dyn CloneAny + Send + Sync>),
    /// The dependency is not ready, here is a flag to await
    Waiting(DepSignal),
}

impl RawDepStatus {
    pub fn with_type<T: 'static + Clone + Send + Sync>(self) -> DepStatus<T> {
        match self {
            RawDepStatus::Ready(t) => {
                log::trace!("About to downcast a raw dep to type {}", std::any::type_name::<T>());
                DepStatus::Ready(*unsafe { t.downcast_unchecked() })
            }
            RawDepStatus::Waiting(s) => DepStatus::Waiting(s.handle()),
        }
    }
}

/// A scope, which marks data as usable for a given task
pub struct Scope {
    id: ScopeId,
    // TODO: Maybe use a stack here to allow overwriting parent's
    // data without deleting it from the child scope?
    // Of course this means figuring out how to propagate removal properly
    data: HashMap<TypeId, DataId>,
    service: Service,
    shutdown_handle: Option<ShutdownHandle>,
    abort_handle: Option<AbortHandle>,
    dependencies: anymap::Map<dyn CloneAny + Send + Sync>,
    created: HashSet<DataId>,
    parent: Option<ScopeId>,
    children: HashSet<ScopeId>,
}

impl Scope {
    fn new(
        id: usize,
        parent: Option<&Scope>,
        name: String,
        shutdown_handle: Option<ShutdownHandle>,
        abort_handle: Option<AbortHandle>,
    ) -> Self {
        Scope {
            id,
            data: parent.map(|p| p.data.clone()).unwrap_or_default(),
            service: Service::new(name),
            shutdown_handle,
            abort_handle,
            dependencies: anymap::Map::new(),
            created: Default::default(),
            parent: parent.map(|p| p.id),
            children: Default::default(),
        }
    }

    /// Abort the tasks in this scope. This will shutdown tasks that have
    /// shutdown handles instead.
    fn abort(&mut self) {
        log::debug!("Aborting scope {} ({})", self.id, self.service.name);
        let (shutdown_handle, abort_handle) = (&mut self.shutdown_handle, &mut self.abort_handle);
        if let Some(handle) = shutdown_handle.take() {
            handle.shutdown();
        } else if let Some(abort) = abort_handle.take() {
            abort.abort();
        }
        for dep in self.dependencies.as_mut().drain() {
            log::trace!("About to downcast dependency while aborting scope {}", self.id);
            match *unsafe { dep.downcast_unchecked() } {
                Dependency::Once(f) | Dependency::Linked(f) => {
                    f.cancel();
                }
            }
        }
    }
}

/// Defines how the registry is accessed so that various wrappers can
/// be implemented. This trait assumes that the registry will be wrapped
/// in some synchronizeable structure which can be accessed via interior
/// mutability.
#[async_trait]
pub trait RegistryAccess: Clone {
    /// Create a new runtime scope using this registry implementation
    async fn instantiate<S: Into<String> + Send>(
        name: S,
        shutdown_handle: Option<ShutdownHandle>,
        abort_handle: Option<AbortHandle>,
    ) -> RuntimeScope<Self>
    where
        Self: Send + Sized;

    /// Create a new scope with an optional parent scope and name function
    async fn new_scope<P: Send + Into<Option<ScopeId>>, F: 'static + Send + Sync + FnOnce(ScopeId) -> String>(
        &self,
        parent: P,
        name_fn: F,
        shutdown_handle: Option<ShutdownHandle>,
        abort_handle: Option<AbortHandle>,
    ) -> anyhow::Result<usize>;

    /// Drop the scope and all child scopes
    async fn drop_scope(&self, scope_id: &ScopeId) -> anyhow::Result<()>;

    /// Add arbitrary data to this scope
    async fn add_data<T: 'static + Send + Sync + Clone>(&self, scope_id: &ScopeId, data: T) -> anyhow::Result<()>;

    /// Force this scope to depend on some arbitrary data, and shut down if it is ever removed
    async fn depend_on<T: 'static + Send + Sync + Clone>(&self, scope_id: &ScopeId) -> anyhow::Result<DepStatus<T>>;

    /// Remove arbitrary data from this scope
    async fn remove_data<T: 'static + Send + Sync + Clone>(&self, scope_id: &ScopeId) -> anyhow::Result<Option<T>>;

    /// Get arbitrary data from this scope
    async fn get_data<T: 'static + Send + Sync + Clone>(&self, scope_id: &ScopeId) -> anyhow::Result<DepStatus<T>>;

    /// Get this scope's service
    async fn get_service(&self, scope_id: &ScopeId) -> anyhow::Result<Service>;

    /// Update the status of this scope
    async fn update_status(&self, scope_id: &ScopeId, status: Cow<'static, str>) -> anyhow::Result<()>;

    /// Abort this scope
    async fn abort(&self, scope_id: &ScopeId) -> anyhow::Result<()>;

    /// Print the tree hierarchy starting with this scope
    /// NOTE: To print the entire tree, use scope id `&0` as it will always refer to the root
    async fn print(&self, scope_id: &ScopeId);

    /// Request the service tree from this scope
    async fn service_tree(&self, scope_id: &ScopeId) -> anyhow::Result<ServiceTree>;
}

/// The central registry that stores all data for the application.
/// Data is accessable via scopes which track its location within
/// the global dyn vector. Two id pools manage the reusable indexes
/// for this data.
pub struct Registry {
    data: Vec<Option<Box<dyn CloneAny + Send + Sync>>>,
    data_pool: IdPool<DataId>,
    scopes: HashMap<ScopeId, Scope>,
    scope_pool: IdPool<ScopeId>,
}

impl Default for Registry {
    fn default() -> Self {
        Self {
            data: Vec::with_capacity(100),
            data_pool: Default::default(),
            scopes: Default::default(),
            scope_pool: Default::default(),
        }
    }
}

impl Registry {
    /// Create a new scope with an optional parent
    pub(crate) fn new_scope<P: Into<Option<ScopeId>>, F: FnOnce(ScopeId) -> String>(
        &mut self,
        parent: P,
        name_fn: F,
        shutdown_handle: Option<ShutdownHandle>,
        abort_handle: Option<AbortHandle>,
    ) -> usize {
        let scope_id = self.scope_pool.get_id();
        let parent = parent.into();
        if let Some(parent_id) = parent {
            self.scopes.entry(parent_id).and_modify(|scope| {
                scope.children.insert(scope_id);
            });
        }
        let scope = Scope::new(
            scope_id,
            parent.and_then(|p| self.scopes.get(&p)),
            name_fn(scope_id),
            shutdown_handle,
            abort_handle,
        );
        self.scopes.insert(scope_id, scope);
        scope_id
    }

    /// Drop a scope and all of its children recursively
    pub(crate) fn drop_scope(&mut self, scope_id: &ScopeId) -> anyhow::Result<()> {
        let scope = self
            .scopes
            .remove(scope_id)
            .ok_or_else(|| anyhow::anyhow!("No scope with id {}!", scope_id))?;
        log::debug!("Dropping scope {} ({})", scope_id, scope.service.name);
        for child in scope.children.iter() {
            self.drop_scope(child)?;
        }
        for &data_id in scope.created.iter() {
            if self.data[data_id].take().is_some() {
                self.data_pool.return_id(data_id);
            }
        }
        if let Some(parent) = scope.parent.and_then(|parent_id| self.scopes.get_mut(&parent_id)) {
            parent.children.retain(|id| id != scope_id);
        }
        self.scope_pool.return_id(*scope_id);
        Ok(())
    }

    pub(crate) async fn depend_on<T: 'static + Send + Sync + Clone>(&mut self, scope_id: &ScopeId) -> anyhow::Result<DepStatus<T>> {
        self.depend_on_raw(scope_id, TypeId::of::<T>()).await.map(|s| s.with_type())
    }

    pub(crate) async fn depend_on_raw(&mut self, scope_id: &ScopeId, data_type: TypeId) -> anyhow::Result<RawDepStatus> {
        let status = self.get_data_raw(scope_id, data_type)?;
        let scope = self.scopes.get_mut(scope_id).unwrap();
        Ok(match scope.dependencies.as_mut().entry(data_type) {
            Entry::Occupied(mut e) => {
                log::trace!("About to downcast for upgrading a dep in scope {}", scope_id);
                let val = unsafe { e.get_mut().downcast_mut_unchecked::<Dependency>() };
                val.upgrade();
                match val {
                    Dependency::Linked(f) => RawDepStatus::Waiting(f.clone()),
                    _ => panic!(),
                }
            }
            Entry::Vacant(v) => {
                let flag = DepSignal::default();
                log::trace!("About to insert linked dependency in scope {}", scope_id);
                unsafe { v.insert(Box::new(Dependency::Linked(flag.clone()))) };
                if let RawDepStatus::Ready(t) = status {
                    flag.signal_raw(t).await;
                }
                RawDepStatus::Waiting(flag)
            }
        })
    }

    /// Add some arbitrary data to a scope and its children recursively
    pub(crate) async fn add_data<T: 'static + Send + Sync + Clone>(&mut self, scope_id: &ScopeId, data: T) -> anyhow::Result<()> {
        self.add_data_raw(scope_id, TypeId::of::<T>(), Box::new(data)).await
    }

    pub(crate) async fn add_data_raw(
        &mut self,
        scope_id: &ScopeId,
        data_type: TypeId,
        data: Box<dyn CloneAny + Send + Sync>,
    ) -> anyhow::Result<()> {
        let scope = self
            .scopes
            .get_mut(scope_id)
            .ok_or_else(|| anyhow::anyhow!("No scope with id {}!", scope_id))?;
        let data_id = self.data_pool.get_id();
        if self.data.len() <= data_id {
            self.data.resize_with(data_id + 10, || None);
        }
        self.data[data_id].replace(data);
        scope.created.insert(data_id);
        self.propagate_data_raw(scope_id, data_type, Propagation::Add(data_id)).await;
        Ok(())
    }

    /// Remove some data from this scope and its children.
    pub(crate) async fn remove_data<T: 'static + Send + Sync + Clone>(&mut self, scope_id: &ScopeId) -> anyhow::Result<Option<T>> {
        self.remove_data_raw(scope_id, TypeId::of::<T>()).await.map(|o| {
            o.map(|data| {
                log::trace!("About to downcast for remove_data call in scope {}", scope_id);
                *unsafe { data.downcast_unchecked::<T>() }
            })
        })
    }

    pub(crate) async fn remove_data_raw(
        &mut self,
        scope_id: &ScopeId,
        data_type: TypeId,
    ) -> anyhow::Result<Option<Box<dyn CloneAny + Send + Sync>>> {
        if let Some((creator_scope, data)) = {
            self.scopes
                .get(scope_id)
                .ok_or_else(|| anyhow::anyhow!("No scope with id {}!", scope_id))?
                .data
                .get(&data_type)
                .cloned()
                .and_then(|data_id| {
                    self.data[data_id].take().map(|data| {
                        let mut creator_scope = self.scopes.get(scope_id).unwrap();
                        self.data_pool.return_id(data_id);
                        // Check if we created the data
                        // If not, climb the parent tree until we find the scope that did
                        // If we never find that scope, just remove it anyway I guess
                        while !creator_scope.created.contains(&data_id) {
                            if let Some(parent) = creator_scope.parent.and_then(|parent_id| self.scopes.get(&parent_id)) {
                                creator_scope = parent;
                            } else {
                                break;
                            }
                        }
                        (creator_scope.id, data)
                    })
                })
        }
        .map(|(scope_id, data)| (self.scopes.get_mut(&scope_id).unwrap(), data))
        {
            let scope_id = creator_scope.id;
            self.propagate_data_raw(&scope_id, data_type, Propagation::Remove).await;
            Ok(Some(data))
        } else {
            Ok(None)
        }
    }

    #[async_recursion]
    async fn propagate_data_raw(&mut self, scope_id: &ScopeId, data_type: TypeId, prop: Propagation) {
        if let Some(scope) = self.scopes.get_mut(scope_id) {
            match prop {
                Propagation::Add(data_id) => {
                    scope.data.insert(data_type, data_id);
                    if let Some(dep) = scope.dependencies.as_mut().remove(&data_type).map(|d| {
                        log::trace!("About to downcast for add propagation in scope {}", scope_id);
                        *unsafe { d.downcast_unchecked::<Dependency>() }
                    }) {
                        match dep {
                            Dependency::Once(ref f) | Dependency::Linked(ref f) => {
                                f.signal_raw(self.data[data_id].clone().unwrap()).await;
                            }
                        }
                        if let Dependency::Linked(_) = dep {
                            scope.dependencies.insert(dep);
                        }
                    }
                    for child in scope.children.clone() {
                        self.propagate_data_raw(&child, data_type, prop).await;
                    }
                }
                Propagation::Remove => {
                    scope.data.remove(&data_type);
                    if let Some(Dependency::Linked(_)) = scope.dependencies.as_mut().remove(&data_type).map(|d| {
                        log::trace!("About to downcast for remove propagation in scope {}", scope_id);
                        *unsafe { d.downcast_unchecked() }
                    }) {
                        log::debug!(
                            "Aborting scope {} ({}) due to a removed critical dependency!",
                            scope.id,
                            scope.service.name
                        );
                        scope.abort();
                    } else {
                        for child in scope.children.clone() {
                            self.propagate_data_raw(&child, data_type, prop).await;
                        }
                    }
                }
            }
        }
    }

    /// Get a reference to some arbitrary data from the given scope
    pub(crate) fn get_data<T: 'static + Send + Sync + Clone>(&mut self, scope_id: &ScopeId) -> anyhow::Result<DepStatus<T>> {
        Ok(
            match self
                .scopes
                .get(scope_id)
                .ok_or_else(|| anyhow::anyhow!("No scope with id {}!", scope_id))?
                .data
                .get(&TypeId::of::<T>())
                .and_then(|&data_id| self.data[data_id].as_ref())
                .map(|d| {
                    log::trace!("About to downcast for get_data call in scope {}", scope_id);
                    *unsafe { d.clone().downcast_unchecked::<T>() }
                }) {
                Some(d) => DepStatus::Ready(d),
                None => {
                    let flag = DepSignal::default();
                    self.scopes
                        .get_mut(scope_id)
                        .unwrap()
                        .dependencies
                        .insert(Dependency::Once(flag.clone()));
                    DepStatus::Waiting(flag.handle())
                }
            },
        )
    }

    pub(crate) fn get_data_raw(&mut self, scope_id: &ScopeId, data_type: TypeId) -> anyhow::Result<RawDepStatus> {
        Ok(
            match self
                .scopes
                .get(scope_id)
                .ok_or_else(|| anyhow::anyhow!("No scope with id {}!", scope_id))?
                .data
                .get(&data_type)
                .and_then(|&data_id| self.data[data_id].clone())
            {
                Some(d) => RawDepStatus::Ready(d),
                None => {
                    let flag = DepSignal::default();
                    self.scopes
                        .get_mut(scope_id)
                        .unwrap()
                        .dependencies
                        .insert(Dependency::Once(flag.clone()));
                    RawDepStatus::Waiting(flag)
                }
            },
        )
    }

    pub(crate) fn get_service(&self, scope_id: &ScopeId) -> anyhow::Result<Service> {
        self.scopes
            .get(scope_id)
            .ok_or_else(|| anyhow::anyhow!("No scope with id {}!", scope_id))
            .map(|scope| scope.service.clone())
    }

    pub(crate) fn update_status(&mut self, scope_id: &ScopeId, status: Cow<'static, str>) -> anyhow::Result<()> {
        self.scopes
            .get_mut(scope_id)
            .ok_or_else(|| anyhow::anyhow!("No scope with id {}!", scope_id))
            .map(|scope| scope.service.update_status(status))
    }

    pub(crate) fn abort(&mut self, scope_id: &ScopeId) -> anyhow::Result<()> {
        for child in self
            .scopes
            .get(&scope_id)
            .ok_or_else(|| anyhow::anyhow!("No scope with id {}!", scope_id))?
            .children
            .iter()
            .cloned()
            .collect::<Vec<_>>()
        {
            self.abort(&child).ok();
        }
        self.scopes.get_mut(scope_id).map(|scope| scope.abort());
        Ok(())
    }

    pub(crate) fn print(&self, scope_id: &ScopeId) {
        log::debug!("Registry ({}):\n{}", scope_id, PrintableRegistry(self, *scope_id))
    }

    pub(crate) fn service_tree(&self, scope_id: &ScopeId) -> anyhow::Result<ServiceTree> {
        let scope = self
            .scopes
            .get(scope_id)
            .ok_or_else(|| anyhow::anyhow!("No scope with id {}!", scope_id))?;
        Ok(ServiceTree {
            service: scope.service.clone(),
            children: scope
                .children
                .iter()
                .map(|id| self.service_tree(id))
                .collect::<anyhow::Result<Vec<ServiceTree>>>()?,
        })
    }
}

#[derive(Default)]
pub(crate) struct DepFlag {
    waker: AtomicWaker,
    set: AtomicBool,
    val: Arc<RwLock<Option<Box<dyn CloneAny + Send + Sync>>>>,
}

impl DepFlag {
    pub(crate) async fn signal<T: 'static + Clone + Send + Sync>(&self, val: T) {
        self.set.store(true, Ordering::Relaxed);
        self.waker.wake();
        *self.val.write().await = Some(Box::new(val));
    }

    pub(crate) async fn signal_raw(&self, val: Box<dyn CloneAny + Send + Sync>) {
        self.set.store(true, Ordering::Relaxed);
        self.waker.wake();
        *self.val.write().await = Some(val);
    }

    pub(crate) fn cancel(&self) {
        self.set.store(true, Ordering::Relaxed);
        self.waker.wake();
    }
}

#[derive(Clone, Default)]
pub(crate) struct DepSignal {
    flag: Arc<DepFlag>,
}

impl DepSignal {
    pub(crate) async fn signal<T: 'static + Clone + Send + Sync>(self, val: T) {
        self.flag.signal(val).await
    }

    pub(crate) async fn signal_raw(&self, val: Box<dyn CloneAny + Send + Sync>) {
        self.flag.signal_raw(val).await
    }

    pub(crate) fn cancel(self) {
        self.flag.cancel();
    }

    pub(crate) fn handle<T: 'static + Clone + Send + Sync>(self) -> DepHandle<T> {
        DepHandle {
            flag: self.flag,
            _type: PhantomData,
        }
    }
}

/// A handle to an awaitable dependency
#[derive(Clone, Default)]
pub struct DepHandle<T> {
    flag: Arc<DepFlag>,
    _type: PhantomData<fn(T) -> T>,
}

impl<T: 'static + Clone> Future for DepHandle<T> {
    type Output = anyhow::Result<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // quick check to avoid registration if already done.
        if self.flag.set.load(Ordering::Relaxed) {
            return match self.flag.val.try_read() {
                Ok(lock) => Poll::Ready(
                    lock.clone()
                        .ok_or_else(|| anyhow::anyhow!("Dependency notification canceled!"))
                        .map(|d| {
                            log::trace!("About to downcast dependency (1/2)");
                            *unsafe { d.downcast_unchecked::<T>() }
                        }),
                ),
                Err(_) => Poll::Pending,
            };
        }

        self.flag.waker.register(cx.waker());

        // Need to check condition **after** `register` to avoid a race
        // condition that would result in lost notifications.
        if self.flag.set.load(Ordering::Relaxed) {
            match self.flag.val.try_read() {
                Ok(lock) => Poll::Ready(
                    lock.clone()
                        .ok_or_else(|| anyhow::anyhow!("Dependency notification canceled!"))
                        .map(|d| {
                            log::trace!("About to downcast dependency (2/2)");
                            *unsafe { d.downcast_unchecked::<T>() }
                        }),
                ),
                Err(_) => Poll::Pending,
            }
        } else {
            Poll::Pending
        }
    }
}

#[derive(Clone)]
pub(crate) struct PrintableRegistry<'a>(&'a Registry, ScopeId);

impl<'a> TreeItem for PrintableRegistry<'a> {
    type Child = Self;

    fn write_self<W: std::io::Write>(&self, f: &mut W, _style: &ptree::Style) -> std::io::Result<()> {
        let PrintableRegistry(registry, scope_id) = self;
        if let Some(scope) = registry.scopes.get(&scope_id) {
            write!(
                f,
                "{} ({}) - {}, Uptime {} ms, Data {:?}",
                scope_id,
                scope.service.name,
                scope.service.status,
                scope.service.up_since.elapsed().unwrap().as_millis(),
                scope.created
            )
        } else {
            write!(f, "{}", scope_id)
        }
    }

    fn children(&self) -> std::borrow::Cow<[Self::Child]> {
        let PrintableRegistry(registry, scope_id) = self;
        registry
            .scopes
            .get(scope_id)
            .map(|scope| {
                scope
                    .children
                    .iter()
                    .map(|&c| PrintableRegistry(registry.clone(), c))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default()
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
