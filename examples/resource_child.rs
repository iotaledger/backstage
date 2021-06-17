use backstage::{core::*, runtime::BackstageRuntime};

///// common traits START ////
pub trait GenericStorage {
    type Backend: Clone + Sync + Send + 'static;
}

pub trait Storage {
    fn new() -> Self;
}

pub trait Insert<K, V>: Storage {
    fn insert(&self, k: &K, v: &V);
}

// +++ other access operation ..........
///// common traits END ////

//// consider this implementation of common traits START /////
#[derive(Clone)]
pub struct Rocksdb;
impl Storage for Rocksdb {
    fn new() -> Self {
        Self
    }
}
impl Insert<u8, u64> for Rocksdb {
    fn insert(&self, k: &u8, v: &u64) {
        log::info!("Inserted {}, {}", k, v)
    }
}
// +++ impl for other access operation ..........
//// consider this implementation of common traits END /////

/////////////////// Resource child START /////////////////////
#[derive(Clone)]
/// The ResourceChild's State
pub struct ResourceChild;

enum ResourceChildEvent {
    Shutdown,
}

#[derive(Clone, Debug)]
/// The ResourceChildHandle
pub struct ResourceChildHandle {
    tx: tokio::sync::mpsc::UnboundedSender<ResourceChildEvent>,
}
/// The ResourceChildInbox
pub struct ResourceChildInbox {
    rx: tokio::sync::mpsc::UnboundedReceiver<ResourceChildEvent>,
}

impl ActorHandle for ResourceChildHandle {
    fn service(&self, _service: &Service) {}
    fn shutdown(self: Box<Self>) {
        self.tx.send(ResourceChildEvent::Shutdown).ok();
    }
    fn aknshutdown(&self, _service: Service, _r: ActorResult) {}
    fn send(&self, _event: Box<dyn std::any::Any>) -> Result<(), Box<dyn std::any::Any>> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl<C> Actor<C> for ResourceChild
where
    C: Registry<Actor = Self>,
{
    async fn run(self, context: &mut C) -> ActorResult {
        let storage = Rocksdb;
        context.register(storage).await.map_err(|e| {
            log::error!("{} unable to register Rocksdb {}", context.service().name(), e);
            Reason::Exit
        })?;
        log::info!("{} registered Rocksdb", context.service().name());

        while let Some(ResourceChildEvent::Shutdown) = context.inbox().rx.recv().await {
            break;
        }
        Ok(())
    }
}

impl Channel for ResourceChild {
    type Handle = ResourceChildHandle;
    type Inbox = ResourceChildInbox;
    fn channel(&mut self) -> Result<(Self::Handle, Self::Inbox), anyhow::Error> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        Ok((ResourceChildHandle { tx }, ResourceChildInbox { rx }))
    }
}

/////////////////// Resouce child END /////////////////////

/////////////////// Some child START /////////////////////

#[derive(Clone)]
/// The SomeChild's State
struct SomeChild;
#[derive(Clone)]
/// The SomeChildHandle
struct SomeChildHandle;
/// The SomeChildInbox
struct SomeChildInbox;

impl ActorHandle for SomeChildHandle {
    fn service(&self, _service: &Service) {}
    fn shutdown(self: Box<Self>) {}
    fn aknshutdown(&self, _service: Service, _r: ActorResult) {}
    fn send(&self, _event: Box<dyn std::any::Any>) -> Result<(), Box<dyn std::any::Any>> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl<C, B> Actor<C> for SomeChild
where
    C: Registry<Actor = Self>,
    C::Generic: GenericStorage<Backend = B>,
    B: Insert<u8, u64> + Sync + Send + Clone + 'static, // these suppsed to be replaced with supertrait
{
    async fn run(self, context: &mut C) -> ActorResult {
        let storage: B = context.depends_on("Storage".into()).await.map_err(|e| {
            log::error!("{} unable to get StorageBackend {}", context.service().name(), e);
            Reason::Exit
        })?;
        log::info!("{} acquired StorageBackend", context.service().name());
        // make use of the storage
        storage.insert(&1, &2);
        Ok(())
    }
}

impl Channel for SomeChild {
    type Handle = SomeChildHandle;
    type Inbox = SomeChildInbox;
    fn channel(&mut self) -> Result<(Self::Handle, Self::Inbox), anyhow::Error> {
        Ok((SomeChildHandle, SomeChildInbox))
    }
}

/////////////////// Second child END /////////////////////
// defines your extra bounds struct
#[derive(Clone)]
pub struct Bounds;
// impl your bounds
// Consider this enabled by cfg feature
impl GenericStorage for Bounds {
    type Backend = Rocksdb;
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let mut runtime = BackstageRuntime::new("two-child-example", Bounds).expect("runtime to get created");
    runtime.add("Storage", ResourceChild).await.expect("Storage to get spawned");
    runtime.add("SomeChild", SomeChild).await.expect("SomeChild to get spawned");
    runtime.block_on().await;
}
