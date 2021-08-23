use super::{Channel, Message, Route, Service, Shutdown};
use std::{any::TypeId, collections::HashMap};
use tokio::sync::RwLock;

pub type ScopeId = usize;
pub type PartitionedScopes = RwLock<HashMap<ScopeId, Scope>>;

lazy_static::lazy_static! {
    pub static ref PROMETHEUS_REGISTRY: prometheus::Registry = {
        prometheus::Registry::new_custom(Some("Backstage".into()), None).expect("PROMETHEUS_REGISTRY to be created")
    };
    pub static ref BACKSTAGE_PARTITIONS: usize = {
        if let Ok(p) = std::env::var("BACKSTAGE_PARTITIONS") {
            p.parse().expect("Invalid BACKSTAGE_PARTITIONS env")
        } else {
            num_cpus::get()
        }
    };
    pub static ref SCOPES: Vec<PartitionedScopes> = {
        let mut scopes = Vec::with_capacity(*BACKSTAGE_PARTITIONS);
        for _ in 0..*BACKSTAGE_PARTITIONS {
            scopes.push(RwLock::default());
        }
        scopes
    };
}

pub(crate) struct Scoped {
    pub(crate) scope_id: Option<ScopeId>,
    pub(crate) pendings: Vec<tokio::sync::oneshot::Sender<ScopeId>>,
}

impl Scoped {
    pub fn with_scope_id(scope_id: ScopeId) -> Self {
        Self {
            scope_id: Some(scope_id),
            pendings: Vec::new(),
        }
    }
    pub fn new() -> Self {
        Self {
            scope_id: None,
            pendings: Vec::new(),
        }
    }
    pub fn add_pending(&mut self, oneshot: tokio::sync::oneshot::Sender<ScopeId>) {
        self.pendings.push(oneshot)
    }
    pub fn scope_id(&self) -> &Option<ScopeId> {
        &self.scope_id
    }
}
pub trait Resource: Clone + Send + Sync + 'static {}
impl<T> Resource for T where T: Clone + Send + Sync + 'static {}

pub struct Data<T: Resource> {
    pub(crate) resource: Option<T>,
    pub(crate) subscribers: HashMap<ScopeId, Subscriber<T>>,
}

impl<T: Resource> Data<T> {
    pub fn with_resource(resource: T) -> Self {
        Self {
            resource: Some(resource),
            subscribers: HashMap::new(),
        }
    }
    pub fn with_subscriber(scope_id: ScopeId, subscriber: Subscriber<T>) -> Self {
        let mut subscribers = HashMap::new();
        subscribers.insert(scope_id, subscriber);
        Self {
            resource: None,
            subscribers,
        }
    }
}
pub enum Subscriber<T: Resource> {
    /// OneCopy subscriber will receive one copy of the resource once it's available
    OneCopy(tokio::sync::oneshot::Sender<anyhow::Result<T>>),
    /// LinkedOneCopy subscriber will receive one copy of the resource once it's available,
    /// subscriber will get shutdown if the resource is replaced or dropped.
    LinkedOneCopy(Option<tokio::sync::oneshot::Sender<anyhow::Result<T>>>, Box<dyn Shutdown>),
    DynCopy,
    /* LinkedDynCopy
     * LinkedDynCopy(Box<dyn Shutdown>), */
}

impl<T: Resource> Subscriber<T> {
    pub fn one_copy(one_shot: tokio::sync::oneshot::Sender<anyhow::Result<T>>) -> Self {
        Self::OneCopy(one_shot)
    }
    pub fn linked_one_copy(one_shot: tokio::sync::oneshot::Sender<anyhow::Result<T>>, shutdown_handle: Box<dyn Shutdown>) -> Self {
        Self::LinkedOneCopy(Some(one_shot), shutdown_handle)
    }
    pub fn dyn_copy() -> Self {
        Self::DynCopy
    }
}
pub struct Cleanup<T: Resource> {
    _marker: std::marker::PhantomData<T>,
    resource_scope_id: ScopeId,
}
impl<T: Resource> Cleanup<T> {
    pub fn new(resource_scope_id: ScopeId) -> Self {
        Self {
            _marker: std::marker::PhantomData::<T>,
            resource_scope_id,
        }
    }
}
#[async_trait::async_trait]
pub trait CleanupFromOther: Send + Sync {
    async fn cleanup_from_other(self: Box<Self>, my_scope_id: ScopeId);
}
#[async_trait::async_trait]
impl<T: Resource> CleanupFromOther for Cleanup<T> {
    async fn cleanup_from_other(self: Box<Self>, my_scope_id: ScopeId) {
        let resource_scopes_index = self.resource_scope_id % num_cpus::get();
        let mut lock = SCOPES[resource_scopes_index].write().await;
        if let Some(resource_scope) = lock.get_mut(&self.resource_scope_id) {
            if let Some(data) = resource_scope.data_and_subscribers.get_mut::<Data<T>>() {
                data.subscribers.remove(&my_scope_id);
            }
        };
        drop(lock);
    }
}

#[async_trait::async_trait]
pub trait CleanupSelf: Send + Sync {
    async fn cleanup_self(self: Box<Self>, data_and_subscribers: &mut anymap::Map<dyn anymap::any::Any + Send + Sync>);
}
pub struct CleanupData<T: Resource> {
    _marker: std::marker::PhantomData<T>,
}
impl<T: Resource> CleanupData<T> {
    pub fn new() -> Self {
        Self {
            _marker: std::marker::PhantomData::<T>,
        }
    }
}
#[async_trait::async_trait]
impl<T: Resource> CleanupSelf for CleanupData<T> {
    async fn cleanup_self(self: Box<Self>, data_and_subscribers: &mut anymap::Map<dyn anymap::any::Any + Send + Sync>) {
        if let Some(mut data) = data_and_subscribers.remove::<Data<T>>() {
            for subscriber in data.subscribers.drain() {
                match subscriber {
                    // Subscriber:: todo
                    _ => {}
                }
                todo!("finish cleanup")
            }
        }
    }
}

pub struct Scope {
    pub(crate) parent_id: Option<ScopeId>,
    pub(crate) shutdown_handle: Box<dyn Shutdown>,
    pub(crate) cleanup: HashMap<ScopeId, Box<dyn CleanupFromOther>>,
    pub(crate) cleanup_data: HashMap<TypeId, Box<dyn CleanupSelf>>,
    pub(crate) data_and_subscribers: anymap::Map<dyn anymap::any::Any + Send + Sync>,
    pub(crate) active_scopes: HashMap<String, Scoped>,
    pub(crate) router: anymap::Map<dyn anymap::any::Any + Send + Sync>,
}

impl Scope {
    /// Create new scope
    pub fn new(parent_id: Option<ScopeId>, shutdown_handle: Box<dyn Shutdown>) -> Self {
        Self {
            parent_id,
            shutdown_handle,
            cleanup: HashMap::new(),
            cleanup_data: HashMap::new(),
            data_and_subscribers: anymap::Map::new(),
            active_scopes: HashMap::new(),
            router: anymap::Map::new(),
        }
    }
}
