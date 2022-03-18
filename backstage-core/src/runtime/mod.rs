// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use crate::actor::{Channel, Service, ServiceStatus, ServiceTree, ServiceView, ShutdownHandle, System};
use anymap::any::{CloneAny, UncheckedAnyExt};
use async_recursion::async_recursion;
use async_trait::async_trait;
use futures::{
    future::{AbortHandle, Abortable, BoxFuture},
    task::AtomicWaker,
    Future,
};
use std::{
    any::TypeId,
    borrow::Cow,
    collections::{hash_map::Entry, HashMap},
    marker::PhantomData,
    ops::{Deref, DerefMut},
    path::Path,
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

/// An alias type indicating that this is a scope id
pub type ScopeId = Uuid;

/// The root scope id, which is always a zeroed uuid
pub const ROOT_SCOPE: Uuid = Uuid::nil();

/// A scope, which marks data as usable for a given task
#[derive(Clone, Debug)]
pub struct Scope {
    inner: Arc<ScopeInner>,
    valid: Arc<AtomicBool>,
    service: Service,
}

/// Shared scope information
#[derive(Debug)]
pub struct ScopeInner {
    id: ScopeId,
    data: RwLock<ScopeMutData>,
    shutdown_handle: Option<ShutdownHandle>,
    abort_handle: Option<AbortHandle>,
    parent: Option<Scope>,
    path: Option<&'static str>,
}

/// Scope data
#[derive(Debug, Default)]
pub struct ScopeMutData {
    created: HashMap<TypeId, Box<dyn CloneAny + Send + Sync>>,
    visible: HashMap<TypeId, Scope>,
    dependencies: HashMap<TypeId, Dependency>,
    children: HashMap<ScopeId, Scope>,
}

impl Scope {
    fn root(abort_handle: AbortHandle) -> Scope {
        Scope {
            inner: Arc::new(ScopeInner {
                id: ROOT_SCOPE,
                data: Default::default(),
                shutdown_handle: Default::default(),
                abort_handle: Some(abort_handle),
                parent: None,
                path: None,
            }),
            service: Service::new(ROOT_SCOPE, "root"),
            valid: Arc::new(AtomicBool::new(true)),
        }
    }

    async fn child<N: FnOnce(ScopeId) -> String>(
        &self,
        name: N,
        shutdown_handle: Option<ShutdownHandle>,
        abort_handle: Option<AbortHandle>,
        path: Option<&'static str>,
    ) -> Self {
        let id = Uuid::new_v4();
        let parent = self.clone();
        let visible_data = parent.data.read().await.visible.clone();
        let child = Scope {
            inner: Arc::new(ScopeInner {
                id,
                data: RwLock::new(ScopeMutData {
                    created: Default::default(),
                    visible: visible_data,
                    dependencies: Default::default(),
                    children: Default::default(),
                }),
                shutdown_handle,
                abort_handle,
                parent: Some(parent),
                path,
            }),
            service: Service::new(id, name(id)),
            valid: Arc::new(AtomicBool::new(true)),
        };
        self.data.write().await.children.insert(id, child.clone());
        child
    }

    /// Find a scope by id
    pub fn find(&self, id: ScopeId) -> Option<Scope> {
        if id == self.id {
            Some(self.clone())
        } else {
            self.parent.as_ref().and_then(|p| p.find(id))
        }
    }

    /// Find a scope by path
    pub async fn find_by_path(&self, path: &Path) -> Option<Scope> {
        if path.is_absolute() {
            let mut curr = self.find(ROOT_SCOPE).unwrap();
            for segment in path.iter() {
                let children = curr.data.read().await.children.values().cloned().collect::<Vec<_>>();
                if children.is_empty() {
                    return None;
                }
                for child in children.iter() {
                    // TODO: somehow handle skipping levels if there are no matching paths
                    if segment.to_str() == self.path {
                        let found = child.clone();
                        curr = found;
                        break;
                    }
                }
            }
            Some(curr)
        } else {
            todo!()
        }
    }

    pub(crate) fn parent(&self) -> Option<&Scope> {
        self.parent.as_ref()
    }

    pub(crate) async fn children(&self) -> Vec<Scope> {
        self.data.read().await.children.values().cloned().collect()
    }

    pub(crate) async fn drop(&self) {
        if let Some(parent) = self.parent.as_ref() {
            parent.data.write().await.children.remove(&self.id);
        }
    }

    pub(crate) async fn add_data(&self, data_type: TypeId, data: Box<dyn CloneAny + Send + Sync>) {
        if !self.valid.load(Ordering::Relaxed) {
            log::warn!("Tried to add data to invalid scope {:x}", self.id.as_fields().0);
            return;
        }
        self.data.write().await.created.insert(data_type, data);
        self.propagate_data(data_type, self).await;
    }

    pub(crate) async fn remove_data(&self, data_type: TypeId) -> Option<DepReady> {
        if !self.valid.load(Ordering::Relaxed) {
            log::warn!("Tried to remove data from invalid scope {:x}", self.id.as_fields().0);
            return None;
        }
        let mut scope_data = self.data.write().await;
        if let Some(data) = scope_data.created.remove(&data_type) {
            scope_data.visible.remove(&data_type);
            if let Some(Dependency::Linked(_)) = scope_data.dependencies.remove(&data_type) {
                drop(scope_data);
                log::debug!(
                    "Aborting scope {:x} ({}) due to a removed critical dependency!",
                    self.id.as_fields().0,
                    self.service.name()
                );
                self.abort().await;
            }
            Some(DepReady(data))
        } else {
            None
        }
    }

    #[async_recursion]
    async fn propagate_data(&self, data_type: TypeId, creator_scope: &Scope) {
        log::debug!("Propagating data to {:x}", self.id.as_fields().0);
        let mut scope_data = self.data.write().await;
        scope_data.visible.insert(data_type, creator_scope.clone());
        let dep = scope_data.dependencies.remove(&data_type);
        let children = scope_data.children.values().cloned().collect::<Vec<_>>();
        drop(scope_data);
        if let Some(dep) = dep {
            let data = creator_scope.data.read().await.created.get(&data_type).unwrap().clone();
            dep.get_signal().signal(data).await;

            if let Dependency::Linked(_) = dep {
                self.data.write().await.dependencies.insert(data_type, dep);
            }
        }
        for child_scope in children.iter() {
            child_scope.propagate_data(data_type, creator_scope).await;
        }
    }

    /// Get some arbitrary data from the given scope
    pub(crate) async fn get_data(&self, data_type: TypeId) -> Option<DepReady> {
        if !self.valid.load(Ordering::Relaxed) {
            log::warn!("Tried to get data from invalid scope {:x}", self.id.as_fields().0);
            return None;
        }
        let scope_data = self.data.read().await;
        let visible = scope_data.visible.get(&data_type).cloned();
        drop(scope_data);
        match visible {
            Some(creator_scope) => {
                let scope_data = creator_scope.data.read().await;
                scope_data.created.get(&data_type).cloned().map(|data| DepReady(data))
            }
            None => None,
        }
    }

    /// Get some arbitrary data from the given scope or a signal to await its creation
    pub(crate) async fn get_data_promise(&self, data_type: TypeId) -> anyhow::Result<RawDepStatus> {
        if !self.valid.load(Ordering::Relaxed) {
            anyhow::bail!("Scope is invalid");
        }
        let data = self.get_data(data_type).await;
        Ok(match data {
            Some(data) => RawDepStatus::Ready(data),
            None => {
                let mut scope_data = self.data.write().await;
                let flag = match scope_data.dependencies.entry(data_type) {
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
        })
    }

    pub(crate) async fn depend_on(&self, data_type: TypeId) -> anyhow::Result<RawDepStatus> {
        if !self.valid.load(Ordering::Relaxed) {
            anyhow::bail!("Scope is invalid");
        }
        let status = self.get_data(data_type).await;
        let mut scope_data = self.data.write().await;
        Ok(match scope_data.dependencies.entry(data_type) {
            Entry::Occupied(mut e) => {
                let val = e.get_mut();
                RawDepStatus::Waiting(val.upgrade().get_signal().clone())
            }
            Entry::Vacant(v) => {
                let flag = DepSignal::default();
                v.insert(Dependency::Linked(flag.clone()));
                drop(scope_data);
                if let Some(DepReady(t)) = status {
                    flag.signal(t.into()).await;
                }
                RawDepStatus::Waiting(flag)
            }
        })
    }

    pub(crate) async fn get_service(&self) -> ServiceView {
        self.service.view().await
    }

    pub(crate) async fn update_status<S: Into<Cow<'static, str>>>(&self, status: S) {
        self.service.update_status(status).await;
    }

    #[async_recursion]
    pub(crate) async fn service_tree(&self) -> ServiceTree {
        let children = self.data.read().await.children.values().cloned().collect::<Vec<_>>();
        let mut child_svs = Vec::new();
        for child_scope in children.iter() {
            child_svs.push(child_scope.service_tree().await);
        }
        ServiceTree {
            service: self.get_service().await,
            children: child_svs,
        }
    }

    pub(crate) async fn shutdown(&self) {
        log::debug!("Shutting down scope {:x}", self.id.as_fields().0);
        self.valid.store(false, Ordering::Relaxed);
        let data = self.data.write().await.dependencies.drain().collect::<Vec<_>>();
        for (_, dep) in data {
            dep.into_signal().cancel()
        }
        if let Some(handle) = self.shutdown_handle.as_ref() {
            handle.shutdown();
        } else if let Some(abort) = self.abort_handle.as_ref() {
            abort.abort();
        }
    }

    /// Abort the tasks in this scope.
    #[async_recursion]
    async fn abort(&self) {
        log::debug!("Aborting scope {:x}", self.id.as_fields().0);
        let data = self.data.write().await.dependencies.drain().collect::<Vec<_>>();
        for (_, dep) in data {
            dep.into_signal().cancel()
        }
        let children = self.data.read().await.children.values().cloned().collect::<Vec<_>>();
        for child_scope in children.iter() {
            child_scope.abort().await;
        }
        if let Some(handle) = self.shutdown_handle.as_ref() {
            handle.shutdown();
        }
        if let Some(abort) = self.abort_handle.as_ref() {
            abort.abort();
        }
    }
}

impl Deref for Scope {
    type Target = ScopeInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[derive(Debug, Clone)]
enum Dependency {
    Once(DepSignal),
    Linked(DepSignal),
}

impl Dependency {
    pub fn upgrade(&mut self) -> &mut Self {
        match self {
            Dependency::Once(flag) => {
                *self = Dependency::Linked(std::mem::take(flag));
                self
            }
            Dependency::Linked(_) => self,
        }
    }

    pub fn get_signal(&self) -> &DepSignal {
        match self {
            Dependency::Once(flag) | Dependency::Linked(flag) => flag,
        }
    }

    pub fn into_signal(self) -> DepSignal {
        match self {
            Dependency::Once(flag) | Dependency::Linked(flag) => flag,
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

/// A dynamic dependency status
#[derive(Debug)]
pub(crate) enum RawDepStatus {
    /// The dependency is ready to be used
    Ready(DepReady),
    /// The dependency is not ready, here is a flag to await
    Waiting(DepSignal),
}

impl RawDepStatus {
    /// Convert this dynamic status to a typed one
    pub fn with_type<T: 'static + Clone + Send + Sync>(self) -> DepStatus<T> {
        match self {
            RawDepStatus::Ready(t) => DepStatus::Ready(t.with_type()),
            RawDepStatus::Waiting(s) => DepStatus::Waiting(s.handle()),
        }
    }
}

/// A ready dependency
#[derive(Debug)]
pub(crate) struct DepReady(Box<dyn CloneAny + Send + Sync>);

impl DepReady {
    /// Convert this ready dependency to a type
    pub fn with_type<T: 'static + Clone + Send + Sync>(self) -> T {
        *unsafe { self.0.downcast_unchecked() }
    }
}

#[derive(Default, Debug)]
pub(crate) struct DepFlag {
    waker: AtomicWaker,
    set: AtomicBool,
    val: RwLock<Option<Box<dyn CloneAny + Send + Sync>>>,
}

impl DepFlag {
    pub(crate) async fn signal(&self, val: Box<dyn CloneAny + Send + Sync>) {
        *self.val.write().await = Some(val);
        self.set.store(true, Ordering::Relaxed);
        self.waker.wake();
    }

    pub(crate) fn cancel(&self) {
        self.set.store(true, Ordering::Relaxed);
        self.waker.wake();
    }
}

/// A signal for a dependency
#[derive(Clone, Default, Debug)]
pub struct DepSignal {
    flag: Arc<DepFlag>,
}

impl DepSignal {
    pub(crate) async fn signal(&self, val: Box<dyn CloneAny + Send + Sync>) {
        self.flag.signal(val).await
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
