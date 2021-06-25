use crate::actor::{Actor, Channel, IdPool, Sender, Service, ServiceStatus, System};
use anymap::any::{CloneAny, UncheckedAnyExt};
use async_trait::async_trait;
use futures::{
    future::{AbortHandle, Abortable, BoxFuture},
    StreamExt,
};
use ptree::{write_tree, TreeItem};
use std::{
    any::TypeId,
    collections::{HashMap, HashSet},
    marker::PhantomData,
    ops::{Deref, DerefMut},
    sync::Arc,
};
use tokio::{
    sync::{oneshot, RwLock},
    task::JoinHandle,
};

mod data;
pub use data::*;
mod scopes;
pub use scopes::*;
mod access;
pub use access::*;

/// An alias type indicating that this is a scope id
pub type ScopeId = usize;
type DataId = usize;

/// A scope, which marks data as usable for a given task
pub struct Scope {
    id: ScopeId,
    // TODO: Maybe use a stack here to allow overwriting parent's
    // data without deleting it from the child scope?
    // Of course this means figuring out how to propagate removal properly
    data: HashMap<TypeId, DataId>,
    service: Service,
    shutdown_handle: Option<oneshot::Sender<()>>,
    abort_handle: Option<AbortHandle>,
    dependencies: HashSet<TypeId>,
    created: HashSet<DataId>,
    parent: Option<ScopeId>,
    children: HashSet<ScopeId>,
}

impl Scope {
    fn new(
        id: usize,
        parent: Option<&Scope>,
        name: String,
        shutdown_handle: Option<oneshot::Sender<()>>,
        abort_handle: Option<AbortHandle>,
    ) -> Self {
        Scope {
            id,
            data: parent.map(|p| p.data.clone()).unwrap_or_default(),
            service: Service::new(name),
            shutdown_handle,
            abort_handle,
            dependencies: Default::default(),
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
            if let (Err(_), Some(abort)) = (handle.send(()), abort_handle.take()) {
                abort.abort();
            }
        } else {
            if let Some(abort) = abort_handle.take() {
                abort.abort();
            }
        }
    }
}

#[async_trait]
pub trait RegistryAccess: Clone {
    /// Create a new runtime scope using this registry implementation
    async fn instantiate<S: Into<String> + Send>(
        name: S,
        shutdown_handle: Option<oneshot::Sender<()>>,
        abort_handle: Option<AbortHandle>,
    ) -> RuntimeScope<Self>
    where
        Self: Send + Sized;

    /// Create a new scope with an optional parent scope and name function
    async fn new_scope<P: Send + Into<Option<ScopeId>>, F: 'static + Send + Sync + FnOnce(ScopeId) -> String>(
        &mut self,
        parent: P,
        name_fn: F,
        shutdown_handle: Option<oneshot::Sender<()>>,
        abort_handle: Option<AbortHandle>,
    ) -> usize;

    /// Drop the scope and all child scopes
    async fn drop_scope(&mut self, scope_id: &ScopeId);

    /// Add arbitrary data to this scope
    async fn add_data<T: 'static + Send + Sync + Clone>(&mut self, scope_id: &ScopeId, data: T) -> anyhow::Result<()>;

    /// Force this scope to depend on some arbitrary data, and shut down if it is ever removed
    async fn depend_on<T: 'static + Send + Sync + Clone>(&mut self, scope_id: &ScopeId) -> anyhow::Result<()>;

    /// Remove arbitrary data from this scope
    async fn remove_data<T: 'static + Send + Sync + Clone>(&mut self, scope_id: &ScopeId) -> anyhow::Result<Option<T>>;

    /// Get arbitrary data from this scope
    async fn get_data<T: 'static + Send + Sync + Clone>(&mut self, scope_id: &ScopeId) -> Option<T>;

    /// Get this scope's service
    async fn get_service(&mut self, scope_id: &ScopeId) -> Option<Service>;

    /// Update the status of this scope
    async fn update_status(&mut self, scope_id: &ScopeId, status: ServiceStatus) -> anyhow::Result<()>;

    /// Abort this scope
    async fn abort(&mut self, scope_id: &ScopeId);

    /// Print the tree hierarchy starting with this scope
    /// NOTE: To print the entire tree, use scope id `&0` as it will always refer to the root
    async fn print(&mut self, scope_id: &ScopeId);
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
        shutdown_handle: Option<oneshot::Sender<()>>,
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
    pub(crate) fn drop_scope(&mut self, scope_id: &ScopeId) {
        if let Some(scope) = self.scopes.remove(scope_id) {
            log::debug!("Dropping scope {} ({})", scope_id, scope.service.name);
            for child in scope.children.iter() {
                self.drop_scope(child);
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
        }
    }

    pub(crate) fn depend_on<T: 'static + Send + Sync + Clone>(&mut self, scope_id: &ScopeId) -> anyhow::Result<()> {
        self.depend_on_raw(scope_id, TypeId::of::<T>())
    }

    pub(crate) fn depend_on_raw(&mut self, scope_id: &ScopeId, data_type: TypeId) -> anyhow::Result<()> {
        self.scopes
            .get_mut(scope_id)
            .map(|scope| {
                scope.dependencies.insert(data_type);
            })
            .ok_or_else(|| anyhow::anyhow!("No scope with id {}!", scope_id))
    }

    /// Add some arbitrary data to a scope and its children recursively
    pub(crate) fn add_data<T: 'static + Send + Sync + Clone>(&mut self, scope_id: &ScopeId, data: T) -> anyhow::Result<()> {
        self.add_data_raw(scope_id, TypeId::of::<T>(), Box::new(data))
    }

    pub(crate) fn add_data_raw(
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
        scope.data.insert(data_type, data_id);
        for child in scope.children.clone() {
            self.propagate_data_raw(&child, data_type, Propagation::Add(data_id));
        }
        Ok(())
    }

    /// Remove some data from this scope and its children.
    /// NOTE: This will only remove data if this scope originally added it! Otherwise,
    /// this fn will return an error.
    pub(crate) fn remove_data<T: 'static + Send + Sync + Clone>(&mut self, scope_id: &ScopeId) -> anyhow::Result<Option<T>> {
        self.remove_data_raw(scope_id, TypeId::of::<T>())
            .map(|o| o.map(|data| *unsafe { data.downcast_unchecked::<T>() }))
    }

    pub(crate) fn remove_data_raw(
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
            creator_scope.data.remove(&data_type);
            for child in creator_scope.children.clone() {
                self.propagate_data_raw(&child, data_type, Propagation::Remove);
            }
            Ok(Some(data))
        } else {
            Ok(None)
        }
    }

    fn propagate_data_raw(&mut self, scope_id: &ScopeId, data_type: TypeId, prop: Propagation) {
        if let Some(scope) = self.scopes.get_mut(scope_id) {
            match prop {
                Propagation::Add(data_id) => {
                    scope.data.insert(data_type, data_id);
                    for child in scope.children.clone() {
                        self.propagate_data_raw(&child, data_type, prop);
                    }
                }
                Propagation::Remove => {
                    scope.data.remove(&data_type);
                    if scope.dependencies.contains(&data_type) {
                        log::info!(
                            "Aborting scope {} ({}) due to a removed critical dependency!",
                            scope.id,
                            scope.service.name
                        );
                        scope.abort();
                    } else {
                        for child in scope.children.clone() {
                            self.propagate_data_raw(&child, data_type, prop);
                        }
                    }
                }
            }
        }
    }

    /// Get a reference to some arbitrary data from the given scope
    pub(crate) fn get_data<T: 'static + Send + Sync + Clone>(&self, scope_id: &ScopeId) -> Option<&T> {
        self.scopes
            .get(scope_id)
            .and_then(|scope| scope.data.get(&TypeId::of::<T>()))
            .and_then(|data_id| self.data[*data_id].as_ref())
            .map(|t| unsafe { t.downcast_ref_unchecked::<T>() })
    }

    pub(crate) fn get_data_raw(&self, scope_id: &ScopeId, data_type: TypeId) -> Option<&Box<dyn CloneAny + Send + Sync>> {
        self.scopes
            .get(scope_id)
            .and_then(|scope| scope.data.get(&data_type))
            .and_then(|data_id| self.data[*data_id].as_ref())
    }

    /// Get a mutable reference to some arbitrary data from the given scope
    pub(crate) fn get_data_mut<T: 'static + Send + Sync + Clone>(&mut self, scope_id: &ScopeId) -> Option<&mut T> {
        self.scopes
            .get(scope_id)
            .and_then(|scope| scope.data.get(&TypeId::of::<T>()).cloned())
            .and_then(move |data_id| self.data[data_id].as_mut().map(|t| unsafe { t.downcast_mut_unchecked::<T>() }))
    }

    pub(crate) fn get_service(&self, scope_id: &ScopeId) -> Option<Service> {
        self.scopes.get(scope_id).map(|scope| scope.service.clone())
    }

    pub(crate) fn update_status(&mut self, scope_id: &ScopeId, status: ServiceStatus) -> anyhow::Result<()> {
        self.scopes
            .get_mut(scope_id)
            .map(|scope| scope.service.update_status(status))
            .ok_or_else(|| anyhow::anyhow!("Scope {} does not exist!", scope_id))
    }

    pub(crate) fn abort(&mut self, scope_id: &ScopeId) {
        for child in self
            .scopes
            .get(&scope_id)
            .map(|scope| scope.children.iter().cloned().collect::<Vec<_>>())
            .unwrap_or_default()
        {
            self.abort(&child);
        }
        self.scopes.get_mut(scope_id).map(|scope| scope.abort());
    }

    pub(crate) fn print(&self, scope_id: &ScopeId) {
        log::debug!("Registry ({}):\n{}", scope_id, PrintableRegistry(self, *scope_id))
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
                "{} ({}), Uptime {} ms, Data {:?}",
                scope_id,
                scope.service.name,
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
