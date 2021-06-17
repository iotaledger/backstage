use crate::core::*;
use std::collections::HashMap;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
#[derive(Clone)]
/// GlobalRegistry handle used to access the registry functionality (ie registry actor handle, request handle, etc)
pub struct GlobalRegistryHandle(pub UnboundedSender<GlobalRegistryEvent>);
/// Inbox used by GlobalRegistry actor to receive events
pub struct GlobalRegistryInbox(UnboundedReceiver<GlobalRegistryEvent>);
/// Channel implementation
impl Channel for GlobalRegistry {
    type Handle = GlobalRegistryHandle;
    type Inbox = GlobalRegistryInbox;
    fn channel(&mut self) -> Result<(Self::Handle, Self::Inbox), anyhow::Error> {
        let (tx, rx) = unbounded_channel();
        Ok((GlobalRegistryHandle(tx), GlobalRegistryInbox(rx)))
    }
}

impl ActorHandle for GlobalRegistryHandle {
    fn shutdown(self: Box<Self>) {
        self.0.send(GlobalRegistryEvent::Shutdown).ok();
    }
    fn aknshutdown(&self, _service: Service, _r: ActorResult) {
        // do nothing
    }
    fn service(&self, _service: &Service) {
        // do nothing
    }
    fn send(&self, event: Box<dyn std::any::Any>) -> Result<(), Box<dyn std::any::Any>> {
        let my_event: GlobalRegistryEvent = *event.downcast()?;
        self.0.send(my_event).ok();
        Ok(())
    }
}
/// GlobalRegistry actor state
pub struct GlobalRegistry {
    store: anymap::Map<dyn anymap::any::CloneAny + Send + Sync>,
    pendings: anymap::Map<dyn anymap::any::Any + Send + Sync>,
    cleanups: HashMap<NamedType, Vec<Box<dyn CleanupFromStore>>>,
}
impl GlobalRegistry {
    /// Create new GlobalRegistry actor's struct
    pub fn new() -> Self {
        Self {
            store: anymap::Map::new(),
            pendings: anymap::Map::new(),
            cleanups: HashMap::new(),
        }
    }
}
/// Anymap key used to store more than one V for the same K type
#[derive(Clone)]
pub struct Mapped<K> {
    values: std::collections::HashMap<String, K>,
}
/// Hold the interested parties with a given T
pub struct Pending<T> {
    // clients interested in precise T for Name String
    precise: std::collections::HashMap<String, Vec<tokio::sync::oneshot::Sender<Result<T, anyhow::Error>>>>,
}
/// GlobalRegistry event type to be used by consumers to access the registry functionality
pub enum GlobalRegistryEvent {
    /// Register actor handle
    Boxed(Box<dyn RegistryEvent>),
    /// Shutdown registry variant
    Shutdown,
}
/// The event type of registry
pub trait RegistryEvent: Send {
    /// implement how registry supposed to handle the boxed event
    fn handle(self: Box<Self>, registry: &mut GlobalRegistry);
}
/// Register T event
pub struct Register<T: Clone> {
    named_type: NamedType,
    t: T,
    response_handle: tokio::sync::oneshot::Sender<Result<(), anyhow::Error>>,
}

impl<T: Clone> Register<T> {
    /// Create new Register T struct
    pub fn new<A: 'static + Channel>(name: String, t: T, response_handle: tokio::sync::oneshot::Sender<Result<(), anyhow::Error>>) -> Self {
        Self {
            named_type: NamedType::new::<A>(name),
            t,
            response_handle,
        }
    }
    /// Insert cleanup record for a given T from named actor
    pub(crate) fn insert_cleanup(registry: &mut GlobalRegistry, named_type: NamedType)
    where
        T: Sync + Send + Clone + 'static,
    {
        if let Some(existing_cleanups) = registry.cleanups.get_mut(&named_type) {
            existing_cleanups.push(Box::new(Cleanup::<T>::new()));
        } else {
            registry.cleanups.insert(named_type, vec![Box::new(Cleanup::<T>::new())]);
        }
    }
}

impl<T: 'static + Sync + Send + Clone> RegistryEvent for Register<T> {
    fn handle(self: Box<Self>, registry: &mut GlobalRegistry) {
        // check if there is any pendings for this T type;
        if let Some(pendings) = registry.pendings.get_mut::<Pending<T>>() {
            if let Some(response_handles) = pendings.precise.remove(&self.named_type.name) {
                for h in response_handles {
                    h.send(Ok(self.t.clone())).ok();
                }
            }
        };
        if let Some(hash_map) = registry.store.get_mut::<Mapped<T>>() {
            if hash_map.values.contains_key(&self.named_type.name) {
                self.response_handle
                    .send(Err(anyhow::Error::msg("duplicated name for same value")))
                    .ok();
            } else {
                hash_map.values.insert(self.named_type.name.clone(), self.t);
                self.response_handle.send(Ok(())).ok();
            }
        } else {
            let mut values = HashMap::new();
            values.insert(self.named_type.name.clone(), self.t);
            let mapped = Mapped { values };
            registry.store.insert::<Mapped<T>>(mapped);
            self.response_handle.send(Ok(())).ok();
        };
        // store cleanup record
        Self::insert_cleanup(registry, self.named_type.clone());
    }
}

/// Lookup T event by name
pub struct Lookup<T> {
    name: String,
    response_handle: tokio::sync::oneshot::Sender<Option<T>>,
}

impl<T> Lookup<T> {
    /// Create new lookup T struct
    pub fn new(name: String, response_handle: tokio::sync::oneshot::Sender<Option<T>>) -> Self {
        Self { name, response_handle }
    }
}

impl<T: 'static + Sync + Send + Clone> RegistryEvent for Lookup<T> {
    fn handle(self: Box<Self>, registry: &mut GlobalRegistry) {
        if let Some(hash_map) = registry.store.get::<Mapped<T>>() {
            if let Some(requested_t) = hash_map.values.get(&self.name) {
                self.response_handle.send(Some(requested_t.clone())).ok();
                return ();
            }
        };
        // nothing found
        self.response_handle.send(None).ok();
    }
}

/// LookupAll T event
pub struct LookupAll<T> {
    response_handle: tokio::sync::oneshot::Sender<Option<HashMap<String, T>>>,
}

impl<T> LookupAll<T> {
    /// Create new lookupall T struct
    pub fn new(response_handle: tokio::sync::oneshot::Sender<Option<HashMap<String, T>>>) -> Self {
        Self { response_handle }
    }
}

impl<T: 'static + Sync + Send + Clone> RegistryEvent for LookupAll<T> {
    fn handle(self: Box<Self>, registry: &mut GlobalRegistry) {
        if let Some(hash_map) = registry.store.get::<Mapped<T>>() {
            self.response_handle.send(Some(hash_map.values.clone())).ok();
            return ();
        };
        // nothing found
        self.response_handle.send(None).ok();
    }
}
/// Remove T event by name
pub struct Remove<T> {
    name: String,
    _marker: std::marker::PhantomData<T>,
}

impl<T> Remove<T> {
    /// Create new Remove T struct
    pub fn new(name: String) -> Self {
        Self {
            name,
            _marker: std::marker::PhantomData::<T>,
        }
    }
}

impl<T: 'static + Sync + Send + Clone> RegistryEvent for Remove<T> {
    fn handle(self: Box<Self>, registry: &mut GlobalRegistry) {
        if let Some(mut hash_map) = registry.store.remove::<Mapped<T>>() {
            let _ = hash_map.values.remove(&self.name);
            if hash_map.values.len() != 0 {
                // reinsert
                registry.store.insert(hash_map);
            }
        };
    }
}

/// DependsOn T event
pub struct DependsOn<T> {
    named_type: NamedType,
    name: String,
    response_handle: tokio::sync::oneshot::Sender<Result<T, anyhow::Error>>,
}

impl<T> DependsOn<T> {
    /// Create new DependsOn T struct
    pub fn new<A: 'static + Channel>(name: String, response_handle: tokio::sync::oneshot::Sender<Result<T, anyhow::Error>>) -> Self {
        let named_type = NamedType::new::<A>(name.clone());
        Self {
            name,
            response_handle,
            named_type,
        }
    }
}

impl<T: 'static + Sync + Send + Clone> RegistryEvent for DependsOn<T> {
    fn handle(self: Box<Self>, registry: &mut GlobalRegistry) {
        if let Some(hash_map) = registry.store.get::<Mapped<T>>() {
            if let Some(requested_t) = hash_map.values.get(&self.name) {
                self.response_handle.send(Ok(requested_t.clone())).ok();
                return ();
            }
        };
        if let Some(pendings) = registry.pendings.get_mut::<Pending<T>>() {
            // check if we already have precise request
            if let Some(response_handles) = pendings.precise.get_mut(&self.name) {
                response_handles.push(self.response_handle);
            } else {
                pendings.precise.insert(self.name, vec![self.response_handle]);
            }
        } else {
            let mut precise = HashMap::new();
            precise.insert(self.name, vec![self.response_handle]);
            let p = Pending { precise };
            registry.pendings.insert::<Pending<T>>(p);
        };
        // todo insert cleanup object to release close self.response handle
    }
}
/// Unique id for a named typeid;
#[derive(Eq, PartialEq, Clone)]
pub struct NamedType {
    name: String,
    type_id: std::any::TypeId,
}

impl std::hash::Hash for NamedType {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.type_id.hash(state);
    }
}

impl NamedType {
    fn new<T: 'static>(name: String) -> Self {
        Self {
            name,
            type_id: std::any::TypeId::of::<T>(),
        }
    }
}
/// Cleanup registred types by Actor T
pub struct CleanupTypes<T: Channel> {
    _marker: std::marker::PhantomData<T>,
    name: String,
}
impl<T: Channel> CleanupTypes<T> {
    pub fn new(name: String) -> Self {
        Self {
            _marker: std::marker::PhantomData,
            name,
        }
    }
}

impl<T: Channel + 'static> RegistryEvent for CleanupTypes<T> {
    fn handle(self: Box<Self>, registry: &mut GlobalRegistry) {
        let named_type = NamedType::new::<T>(self.name);
        if let Some(cleanups) = registry.cleanups.remove(&named_type) {
            for c in cleanups {
                c.cleanup(&named_type.name, registry);
            }
        };
    }
}

/// Close any pending/dependency on the actor
pub struct ClosePending<T: Channel> {
    _marker: std::marker::PhantomData<T>,
    name: String,
}

impl<T: Channel> ClosePending<T> {
    pub fn new(name: String) -> Self {
        Self {
            _marker: std::marker::PhantomData,
            name,
        }
    }
}

impl<T: Channel + 'static> RegistryEvent for ClosePending<T> {
    fn handle(self: Box<Self>, registry: &mut GlobalRegistry) {
        let named_type = NamedType::new::<T>(self.name);
        if let Some(mut cleanups) = registry.cleanups.remove(&named_type) {
            for c in cleanups.iter_mut() {
                c.cleanup_pendings(&named_type.name, registry);
            }
            // reinsert
            registry.cleanups.insert(named_type, cleanups);
        };
    }
}

struct Cleanup<T> {
    _marker: std::marker::PhantomData<T>,
}

impl<T> Cleanup<T> {
    /// create cleanup event
    fn new() -> Self {
        Self {
            _marker: std::marker::PhantomData::<T>,
        }
    }
}
trait CleanupFromStore: Send + Sync {
    fn cleanup(self: Box<Self>, name: &str, registry: &mut GlobalRegistry);
    fn cleanup_pendings(&mut self, name: &str, registry: &mut GlobalRegistry);
    fn of(&self, type_id: &std::any::TypeId) -> bool;
}

impl<T: 'static + Sync + Send + Clone> CleanupFromStore for Cleanup<T> {
    fn cleanup_pendings(&mut self, name: &str, registry: &mut GlobalRegistry) {
        // check if there is any pendings for this T type;
        if let Some(pendings) = registry.pendings.get_mut::<Pending<T>>() {
            if let Some(response_handles) = pendings.precise.remove(name) {
                for h in response_handles {
                    h.send(Err(anyhow::Error::msg("Cleanup process dropped response handle"))).ok();
                }
            }
        };
    }
    fn cleanup(mut self: Box<Self>, name: &str, registry: &mut GlobalRegistry) {
        self.cleanup_pendings(name, registry);
        if let Some(mut hash_map) = registry.store.remove::<Mapped<T>>() {
            let _ = hash_map.values.remove(name);
            if hash_map.values.len() != 0 {
                // reinsert as there are other T don't belong to the provided name
                registry.store.insert(hash_map);
            }
        };
    }
    fn of(&self, type_id: &std::any::TypeId) -> bool {
        &std::any::TypeId::of::<T>() == type_id
    }
}
/// GlobalRegistry Actor implementation
#[async_trait::async_trait]
impl<C> Actor<C> for GlobalRegistry
where
    C: Essential<Actor = Self>,
    C::Supervisor: ActorHandle,
{
    async fn run(mut self, context: &mut C) -> ActorResult {
        context.handle().take();
        context.service().update_status(ServiceStatus::Running);
        context.propagate_service();
        while let Some(event) = context.inbox().0.recv().await {
            match event {
                GlobalRegistryEvent::Boxed(registry_event) => {
                    registry_event.handle(&mut self);
                }
                GlobalRegistryEvent::Shutdown => {
                    self.pendings.as_mut().drain();
                }
            }
        }
        Ok(())
    }
}
