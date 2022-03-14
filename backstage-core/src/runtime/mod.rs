// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use crate::actor::{Actor, Channel, Service, ServiceStatus, ServiceTree, ShutdownHandle, System};
use anymap::any::{CloneAny, UncheckedAnyExt};
use async_recursion::async_recursion;
use async_trait::async_trait;
use dyn_clone::DynClone;
use futures::{
    future::{AbortHandle, Abortable, BoxFuture},
    task::AtomicWaker,
    Future,
};
use std::{
    any::TypeId,
    borrow::Cow,
    collections::{hash_map::Entry, HashMap, HashSet},
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
pub use uuid::Uuid;

mod data;
pub use data::*;
mod scopes;
pub use scopes::*;
mod access;
pub use access::*;

/// An alias type indicating that this is a scope id
pub type ScopeId = Uuid;

/// The root scope id, which is always a zeroed uuid
pub const ROOT_SCOPE: Uuid = Uuid::nil();

#[derive(Debug, Clone)]
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
                        lock.clone()
                            .map(|d| *unsafe { d.clone_to_any_send_sync().downcast_unchecked() })
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

#[derive(Debug)]
pub enum RawDepStatus {
    /// The dependency is ready to be used
    Ready(Box<dyn CloneAny + Send + Sync>),
    /// The dependency is not ready, here is a flag to await
    Waiting(DepSignal),
}

impl RawDepStatus {
    pub fn with_type<T: 'static + Clone + Send + Sync>(self) -> DepStatus<T> {
        match self {
            RawDepStatus::Ready(t) => DepStatus::Ready(*unsafe { t.downcast_unchecked() }),
            RawDepStatus::Waiting(s) => DepStatus::Waiting(s.handle()),
        }
    }
}

/// A scope, which marks data as usable for a given task
#[derive(Debug)]
pub struct Scope {
    id: ScopeId,
    created_data: HashMap<TypeId, Arc<Box<dyn CloneAny + Send + Sync>>>,
    visible_data: HashMap<TypeId, Arc<Box<dyn CloneAny + Send + Sync>>>,
    service: Service,
    shutdown_handle: Option<ShutdownHandle>,
    abort_handle: Option<AbortHandle>,
    dependencies: HashMap<TypeId, Dependency>,
    parent: Option<ScopeId>,
    children: HashSet<ScopeId>,
}

impl Scope {
    fn new(
        id: ScopeId,
        parent: Option<&Scope>,
        name: String,
        shutdown_handle: Option<ShutdownHandle>,
        abort_handle: Option<AbortHandle>,
    ) -> Self {
        Scope {
            id,
            created_data: Default::default(),
            visible_data: parent.map(|p| p.visible_data.clone()).unwrap_or_default(),
            service: Service::new(id, name),
            shutdown_handle,
            abort_handle,
            dependencies: Default::default(),
            parent: parent.map(|p| p.id),
            children: Default::default(),
        }
    }

    /// Abort the tasks in this scope. This will shutdown tasks that have
    /// shutdown handles instead.
    fn abort(&mut self) {
        log::debug!("Aborting scope {:x} ({})", self.id.as_fields().0, self.service.name());
        let (shutdown_handle, abort_handle) = (&mut self.shutdown_handle, &mut self.abort_handle);
        if let Some(handle) = shutdown_handle.take() {
            handle.shutdown();
        } else if let Some(abort) = abort_handle.take() {
            abort.abort();
        }
        for (_, dep) in self.dependencies.drain() {
            match dep {
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
pub trait RegistryAccess: DynClone {
    /// Create a new runtime scope using this registry implementation
    async fn instantiate<S: 'static + Into<String> + Send + Sync>(
        name: S,
        shutdown_handle: Option<ShutdownHandle>,
        abort_handle: Option<AbortHandle>,
    ) -> RuntimeScope
    where
        Self: Send + Sized;

    /// Create a new scope with an optional parent scope and name function
    async fn new_scope(
        &self,
        parent: Option<ScopeId>,
        name_fn: Box<dyn 'static + Send + Sync + FnOnce(ScopeId) -> String>,
        shutdown_handle: Option<ShutdownHandle>,
        abort_handle: Option<AbortHandle>,
    ) -> anyhow::Result<ScopeId>;

    /// Drop the scope and all child scopes
    async fn drop_scope(&self, scope_id: &ScopeId) -> anyhow::Result<()>;

    /// Add arbitrary data to this scope
    async fn add_data(
        &self,
        scope_id: &ScopeId,
        data_type: TypeId,
        data: Box<dyn CloneAny + Send + Sync>,
    ) -> anyhow::Result<()>;

    /// Force this scope to depend on some arbitrary data, and shut down if it is ever removed
    async fn depend_on(&self, scope_id: &ScopeId, data_type: TypeId) -> anyhow::Result<RawDepStatus>;

    /// Remove arbitrary data from this scope
    async fn remove_data(
        &self,
        scope_id: &ScopeId,
        data_type: TypeId,
    ) -> anyhow::Result<Box<dyn CloneAny + Send + Sync>>;

    /// Get arbitrary data from this scope
    async fn get_data(&self, scope_id: &ScopeId, data_type: TypeId) -> anyhow::Result<RawDepStatus>;

    /// Get this scope's service
    async fn get_service(&self, scope_id: &ScopeId) -> anyhow::Result<Service>;

    /// Update the status of this scope
    async fn update_status(&self, scope_id: &ScopeId, status: Cow<'static, str>) -> anyhow::Result<()>;

    /// Abort this scope
    async fn abort(&self, scope_id: &ScopeId) -> anyhow::Result<()>;

    /// Request the service tree from this scope
    async fn service_tree(&self, scope_id: &ScopeId) -> anyhow::Result<ServiceTree>;
}
dyn_clone::clone_trait_object!(RegistryAccess);

/// The central registry that stores all data for the application.
/// Data is accessable via scopes organized as a tree structure.
#[derive(Debug)]
pub struct Registry {
    scopes: HashMap<ScopeId, RwLock<Scope>>,
}

impl Default for Registry {
    fn default() -> Self {
        Self {
            scopes: Default::default(),
        }
    }
}

impl Registry {
    /// Create a new scope with an optional parent
    pub(crate) async fn new_scope(
        &mut self,
        parent: Option<ScopeId>,
        name_fn: Box<dyn Send + Sync + FnOnce(ScopeId) -> String>,
        shutdown_handle: Option<ShutdownHandle>,
        abort_handle: Option<AbortHandle>,
    ) -> ScopeId {
        let scope_id = if self.scopes.is_empty() {
            ROOT_SCOPE
        } else {
            loop {
                let id = Uuid::new_v4();
                if !self.scopes.contains_key(&id) {
                    break id;
                }
            }
        };
        let scope = {
            if let Some(parent_lock) = parent.and_then(|ref p| self.scopes.get(p)) {
                let mut parent_scope = parent_lock.write().await;
                parent_scope.children.insert(scope_id);
                Scope::new(
                    scope_id,
                    Some(&*parent_scope),
                    name_fn(scope_id),
                    shutdown_handle,
                    abort_handle,
                )
            } else {
                Scope::new(scope_id, None, name_fn(scope_id), shutdown_handle, abort_handle)
            }
        };
        self.scopes.insert(scope_id, RwLock::new(scope));
        scope_id
    }

    /// Drop a scope and all of its children recursively
    #[async_recursion]
    pub(crate) async fn drop_scope(&mut self, scope_id: &ScopeId) -> anyhow::Result<()> {
        let lock = self
            .scopes
            .remove(scope_id)
            .ok_or_else(|| anyhow::anyhow!("No scope with id {}!", scope_id))?;
        let scope = lock.read().await;
        log::debug!("Dropping scope {:x} ({})", scope_id.as_fields().0, scope.service.name());
        let children = scope.children.clone();
        // Make sure we drop the lock before calling any children as they will obtain
        // a lock on their parent. This will deadlock otherwise.
        drop(scope);
        for child in children {
            self.drop_scope(&child).await?;
        }
        let scope = lock.write().await;
        if let Some(parent) = scope.parent.and_then(|parent_id| self.scopes.get(&parent_id)) {
            let mut parent = parent.write().await;
            parent.children.retain(|id| id != scope_id);
        }
        Ok(())
    }

    pub(crate) async fn depend_on<T: 'static + Send + Sync + Clone>(
        &self,
        scope_id: &ScopeId,
    ) -> anyhow::Result<DepStatus<T>> {
        self.depend_on_raw(scope_id, TypeId::of::<T>())
            .await
            .map(|s| s.with_type())
    }

    pub(crate) async fn depend_on_raw(&self, scope_id: &ScopeId, data_type: TypeId) -> anyhow::Result<RawDepStatus> {
        let status = self.get_data_raw(scope_id, data_type).await?;
        let mut scope = self.scopes.get(scope_id).unwrap().write().await;
        Ok(match scope.dependencies.entry(data_type) {
            Entry::Occupied(mut e) => {
                let val = e.get_mut();
                val.upgrade();
                match val {
                    Dependency::Linked(f) => RawDepStatus::Waiting(f.clone()),
                    _ => panic!(),
                }
            }
            Entry::Vacant(v) => {
                let flag = DepSignal::default();
                v.insert(Dependency::Linked(flag.clone()));
                if let RawDepStatus::Ready(t) = status {
                    flag.signal_raw(t.into()).await;
                }
                RawDepStatus::Waiting(flag)
            }
        })
    }

    /// Add some arbitrary data to a scope and its children recursively
    pub(crate) async fn add_data<T: 'static + Send + Sync + Clone>(
        &self,
        scope_id: &ScopeId,
        data: T,
    ) -> anyhow::Result<()> {
        self.add_data_raw(scope_id, TypeId::of::<T>(), Box::new(data)).await
    }

    pub(crate) async fn add_data_raw(
        &self,
        scope_id: &ScopeId,
        data_type: TypeId,
        data: Box<dyn CloneAny + Send + Sync>,
    ) -> anyhow::Result<()> {
        let mut scope = self
            .scopes
            .get(scope_id)
            .ok_or_else(|| anyhow::anyhow!("No scope with id {}!", scope_id))?
            .write()
            .await;

        let arc_data = Arc::new(data);
        scope.created_data.insert(data_type, arc_data.clone());
        self.propagate_data_raw(scope.deref_mut(), data_type, Propagation::Add(arc_data))
            .await;
        Ok(())
    }

    /// Remove some data from this scope and its children.
    pub(crate) async fn remove_data<T: 'static + Send + Sync + Clone>(&self, scope_id: &ScopeId) -> anyhow::Result<T> {
        self.remove_data_raw(scope_id, TypeId::of::<T>())
            .await
            .map(|o| *unsafe { o.downcast_unchecked::<T>() })
    }

    pub(crate) async fn remove_data_raw(
        &self,
        scope_id: &ScopeId,
        data_type: TypeId,
    ) -> anyhow::Result<Box<dyn CloneAny + Send + Sync>> {
        let mut scope = self
            .scopes
            .get(scope_id)
            .ok_or_else(|| anyhow::anyhow!("No scope with id {}!", scope_id))?
            .write()
            .await;
        if let Some(arc_data) = scope.created_data.remove(&data_type) {
            self.propagate_data_raw(scope.deref_mut(), data_type, Propagation::Remove)
                .await;
            Ok(Arc::try_unwrap(arc_data).unwrap())
        } else {
            anyhow::bail!("Tried to remove data not created by scope {}!", scope_id);
        }
    }

    #[async_recursion]
    async fn propagate_data_raw(&self, scope: &mut Scope, data_type: TypeId, prop: Propagation) {
        match prop {
            Propagation::Add(ref arc_data) => {
                scope.visible_data.insert(data_type, arc_data.clone());
                if let Some(dep) = scope.dependencies.remove(&data_type) {
                    match dep {
                        Dependency::Once(ref f) | Dependency::Linked(ref f) => {
                            f.signal_raw(arc_data.deref().clone()).await;
                        }
                    }
                    if let Dependency::Linked(_) = dep {
                        scope.dependencies.insert(data_type, dep);
                    }
                }
                for child in scope.children.iter() {
                    if let Some(child_lock) = self.scopes.get(child) {
                        let mut child_scope = child_lock.write().await;
                        self.propagate_data_raw(child_scope.deref_mut(), data_type, prop.clone())
                            .await;
                    }
                }
            }
            Propagation::Remove => {
                scope.visible_data.remove(&data_type);
                if let Some(Dependency::Linked(_)) = scope.dependencies.remove(&data_type) {
                    log::debug!(
                        "Aborting scope {} ({}) due to a removed critical dependency!",
                        scope.id,
                        scope.service.name()
                    );
                    scope.abort();
                } else {
                    for child in scope.children.iter() {
                        if let Some(child_lock) = self.scopes.get(child) {
                            let mut child_scope = child_lock.write().await;
                            self.propagate_data_raw(child_scope.deref_mut(), data_type, prop.clone())
                                .await;
                        }
                    }
                }
            }
        }
    }

    /// Get a reference to some arbitrary data from the given scope
    pub(crate) async fn get_data<T: 'static + Send + Sync + Clone>(
        &self,
        scope_id: &ScopeId,
    ) -> anyhow::Result<DepStatus<T>> {
        self.get_data_raw(scope_id, TypeId::of::<T>())
            .await
            .map(|s| s.with_type::<T>())
    }

    pub(crate) async fn get_data_raw(&self, scope_id: &ScopeId, data_type: TypeId) -> anyhow::Result<RawDepStatus> {
        let lock = self
            .scopes
            .get(scope_id)
            .ok_or_else(|| anyhow::anyhow!("No scope with id {}!", scope_id))?;
        let scope = lock.read().await;
        Ok(
            match scope
                .visible_data
                .get(&data_type)
                .map(|arc_data| arc_data.deref().clone())
            {
                Some(d) => RawDepStatus::Ready(d),
                None => {
                    // Drop the current lock because we're going to aquire a write lock here
                    drop(scope);
                    let flag = match lock.write().await.dependencies.entry(data_type) {
                        Entry::Occupied(o) => match o.get() {
                            Dependency::Once(f) | Dependency::Linked(f) => f.clone(),
                        },
                        Entry::Vacant(v) => {
                            let flag = DepSignal::default();
                            v.insert(Dependency::Once(flag.clone()));
                            flag
                        }
                    };
                    RawDepStatus::Waiting(flag)
                }
            },
        )
    }

    pub(crate) async fn get_service(&self, scope_id: &ScopeId) -> anyhow::Result<Service> {
        let scope = self
            .scopes
            .get(scope_id)
            .ok_or_else(|| anyhow::anyhow!("No scope with id {}!", scope_id))?;
        let scope = scope.read().await;
        Ok(scope.service.clone())
    }

    pub(crate) async fn update_status<S: Into<Cow<'static, str>>>(
        &self,
        scope_id: &ScopeId,
        status: S,
    ) -> anyhow::Result<()> {
        let scope = self
            .scopes
            .get(scope_id)
            .ok_or_else(|| anyhow::anyhow!("No scope with id {}!", scope_id))?;
        let mut scope = scope.write().await;
        scope.service.update_status(status);
        Ok(())
    }

    #[async_recursion]
    pub(crate) async fn abort(&self, scope_id: &ScopeId) -> anyhow::Result<()> {
        let mut scope = self
            .scopes
            .get(scope_id)
            .ok_or_else(|| anyhow::anyhow!("No scope with id {}!", scope_id))?
            .write()
            .await;
        for child in scope.children.iter() {
            self.abort(child).await.ok();
        }
        scope.abort();
        Ok(())
    }

    #[async_recursion]
    pub(crate) async fn service_tree(&self, scope_id: &ScopeId) -> anyhow::Result<ServiceTree> {
        let scope = self
            .scopes
            .get(scope_id)
            .ok_or_else(|| anyhow::anyhow!("No scope with id {}!", scope_id))?
            .read()
            .await;
        let mut children = Vec::new();
        for child in scope.children.iter() {
            children.push(self.service_tree(child).await?);
        }
        Ok(ServiceTree {
            service: scope.service.clone(),
            children,
        })
    }
}

#[derive(Default, Debug)]
pub(crate) struct DepFlag {
    waker: AtomicWaker,
    set: AtomicBool,
    val: RwLock<Option<Box<dyn CloneAny + Send + Sync>>>,
}

impl DepFlag {
    pub(crate) async fn signal_raw(&self, val: Box<dyn CloneAny + Send + Sync>) {
        *self.val.write().await = Some(val);
        self.set.store(true, Ordering::Relaxed);
        self.waker.wake();
    }

    pub(crate) fn cancel(&self) {
        self.set.store(true, Ordering::Relaxed);
        self.waker.wake();
    }
}

#[derive(Clone, Default, Debug)]
pub struct DepSignal {
    flag: Arc<DepFlag>,
}

impl DepSignal {
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
                        .map(|d| *unsafe { d.downcast_unchecked::<T>() }),
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
                        .map(|d| *unsafe { d.downcast_unchecked::<T>() }),
                ),
                Err(_) => Poll::Pending,
            }
        } else {
            Poll::Pending
        }
    }
}

#[derive(Clone)]
enum Propagation {
    Add(Arc<Box<dyn CloneAny + Send + Sync>>),
    Remove,
}
