use crate::core::{Actor, ActorHandle, ActorResult, Channel, Essential, Service, Spawn};
use futures::future::AbortHandle;
/// Backstage default runtime struct
pub struct Backstage<C: Channel, S: ActorHandle, G: Clone + Send> {
    /// The supervisor handle which is used to keep the supervisor up to date with child status
    supervisor: S,
    /// The actor's handle
    handle: Option<C::Handle>,
    /// The actor's inbox
    inbox: C::Inbox,
    /// The actor service
    service: Service,
    /// The actor children handles
    children_handles: std::collections::HashMap<String, Box<dyn ActorHandle>>,
    /// The children joins handle, to ensure the child reached its EOL
    children_joins: std::collections::HashMap<String, tokio::task::JoinHandle<()>>,
    /// Abort handles for children started in abortable mode
    children_aborts: std::collections::HashMap<String, AbortHandle>,
    /// Registry handle
    registry: Option<crate::core::registry::GlobalRegistryHandle>,
    /// passed generic bounds
    generic: G,
}

impl<C: Channel, S: ActorHandle, G: Clone + Send> Backstage<C, S, G> {
    /// Create backstage context
    pub fn new(
        supervisor: S,
        handle: C::Handle,
        inbox: C::Inbox,
        service: Service,
        registry: Option<crate::core::registry::GlobalRegistryHandle>,
        generic: G,
    ) -> Self {
        Self {
            supervisor,
            handle: Some(handle),
            inbox,
            service,
            children_handles: std::collections::HashMap::new(),
            children_joins: std::collections::HashMap::new(),
            children_aborts: std::collections::HashMap::new(),
            registry,
            generic,
        }
    }
}

#[async_trait::async_trait]
impl<S: ActorHandle, C: Channel + 'static, G: Clone + Send> Essential for Backstage<C, S, G>
where
    C::Handle: Clone + Sync,
{
    type Supervisor = S;
    type Actor = C;
    type Generic = G;
    /// Defines how to breakdown the context and it should aknowledge shutdown to its supervisor
    async fn breakdown(mut self, r: ActorResult) {
        // shutdown children
        self.shutdown();
        // await on children_joins just to force the contract
        for (_, c) in self.children_joins.drain() {
            let _ = c.await;
        }
        // update service to be stopped
        self.service.update_status(crate::core::ServiceStatus::Stopped);
        // cleanup all associated registered types from the registry
        if let Some(r) = self.registry.as_ref() {
            let cleanup = crate::core::registry::CleanupTypes::<C>::new(self.service.name().into());
            let event = crate::core::GlobalRegistryEvent::Boxed(Box::new(cleanup));
            r.0.send(event).ok();
        }
        // aknshutdown to supervisor
        self.supervisor.aknshutdown(self.service, r);
    }
    fn shutdown(&mut self) {
        // drop self handle (if any)
        self.handle.take();
        // shutdown children handles (if any)
        for (_, c) in self.children_handles.drain() {
            c.shutdown();
        }
        // abort children which have abort handle
        for (_, c) in self.children_aborts.drain() {
            c.abort();
        }
        if !self.service.is_stopping() {
            self.service.update_status(crate::core::ServiceStatus::Stopping);
        }
    }
    fn propagate_service(&mut self) {
        self.supervisor.service(&self.service);
    }
    fn shutdown_microservice(&mut self, name: &str, abort: bool) {
        if let Some(h) = self.children_handles.remove(name) {
            h.shutdown();
        };
        if abort {
            if let Some(h) = self.children_aborts.remove(name) {
                h.abort();
            }
        }
    }
    /// Get the service from the actor context
    fn service(&mut self) -> &mut Service {
        &mut self.service
    }
    /// Get the supervisor handle from the actor context
    fn supervisor(&mut self) -> &mut Self::Supervisor {
        &mut self.supervisor
    }
    /// Get the actor's handle from the actor context
    fn handle(&mut self) -> &mut Option<<Self::Actor as Channel>::Handle> {
        &mut self.handle
    }
    /// Get the actor's inbox from the actor context
    fn inbox(&mut self) -> &mut <Self::Actor as Channel>::Inbox {
        &mut self.inbox
    }
    fn requested_to_shutdown(&self, name: &str) -> bool {
        !self.children_handles.contains_key(name)
    }
    fn status_change(&mut self, microservice: Service) {
        let name = microservice.name();
        if !self.service.contains_microservice(name) {
            // Skip non microservices/child
            // this will happen when the microservice above belong to sibling/other dep app;
            return ();
        }
        if microservice.is_stopped() {
            self.children_joins.remove(name);
            self.children_handles.remove(name);
            self.children_aborts.remove(name);
        }
        for (n, h) in self.children_handles.iter_mut() {
            if name != n {
                h.service(&microservice);
            }
        }
        self.service.update_microservice(name.clone().into(), microservice);
        // recompute service status only when the service is not stopping
        if !self.service.is_stopping() {
            if (self.service.is_starting() || self.service.is_initializing())
                && self.service.microservices.values().any(|ms| ms.is_initializing())
            {
                self.service.update_status(crate::core::ServiceStatus::Initializing);
            } else if self.service.microservices.values().all(|ms| ms.is_running()) {
                self.service.update_status(crate::core::ServiceStatus::Running);
            } else if self.service.microservices.values().all(|ms| ms.is_maintenance()) {
                self.service.update_status(crate::core::ServiceStatus::Maintenance);
            } else {
                self.service.update_status(crate::core::ServiceStatus::Degraded);
            }
        }
        self.supervisor.service(&self.service);
    }
    fn generic(&mut self) -> &mut Self::Generic {
        &mut self.generic
    }
}
use crate::core::Registry;
// implementation of spawn contract
impl<PA: Channel, PS: ActorHandle, A, S: ActorHandle, G: Send + Clone + 'static> Spawn<A, S> for Backstage<PA, PS, G>
where
    Self: Essential<Actor = PA, Generic = G> + crate::core::Registry,
    A: Actor<Backstage<A, S, G>>,
    A::Handle: ActorHandle + Clone,
{
    fn spawn(&mut self, mut actor: A, supervisor: S, service: Service) -> Result<A::Handle, anyhow::Error> {
        // try to create the actor's channel
        let (handle, inbox) = actor.channel()?;
        // get the name of the actor service
        let name: String = service.name().into();
        // update microservice
        self.service.update_microservice(name.clone(), service.clone());
        self.children_handles.insert(name.clone(), Box::new(handle.clone()));
        // clone registry (if any)
        let registry_handle = self.registry.clone();
        let generic = self.generic.clone();
        let mut child_context = Backstage::<A, S, G>::new(supervisor, handle.clone(), inbox, service.clone(), registry_handle, generic);
        // register the handle in registry (if registry available)
        if child_context.registry.is_some() {
            let f = child_context.register::<A::Handle>(handle.clone());
            tokio::task::block_in_place(move || tokio::runtime::Handle::current().block_on(async move { f.await }))?;
        }
        let wrapped_fut = async move {
            let mut context = child_context;
            let child_fut = actor.run(&mut context);
            let r = child_fut.await;
            context.breakdown(r).await;
        };
        let join_handle = tokio::spawn(wrapped_fut);
        self.children_joins.insert(name.clone(), join_handle);
        Ok(handle)
    }
}

#[async_trait::async_trait]
impl<C: Channel + 'static, S: ActorHandle, G: Clone + Send + 'static> crate::core::Registry for Backstage<C, S, G> {
    async fn link<T: 'static + Sync + Send + Clone>(&mut self, name: String) -> Result<T, anyhow::Error> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let event = crate::core::registry::Link::<C, T>::new(self.service().name().into(), name, tx);
        if let Some(r) = self.registry.as_ref() {
            r.0.send(crate::core::registry::GlobalRegistryEvent::Boxed(Box::new(event))).ok();
        } else {
            anyhow::bail!("Registry doesn't exist")
        }
        rx.await?
    }
    async fn depends_on<T: 'static + Sync + Send + Clone>(&mut self, name: String) -> Result<T, anyhow::Error> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let event = crate::core::registry::DependsOn::<T>::new::<C>(name, tx);
        if let Some(r) = self.registry.as_ref() {
            r.0.send(crate::core::registry::GlobalRegistryEvent::Boxed(Box::new(event))).ok();
        } else {
            anyhow::bail!("Registry doesn't exist")
        }
        rx.await?
    }
    async fn register<T: 'static + Sync + Send + Clone>(&mut self, t: T) -> Result<(), anyhow::Error> {
        let name = self.service().name();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let event = crate::core::registry::Register::<T>::new::<C>(name.into(), t, tx);
        if let Some(r) = self.registry.as_ref() {
            r.0.send(crate::core::registry::GlobalRegistryEvent::Boxed(Box::new(event))).ok();
        } else {
            anyhow::bail!("Registry doesn't exist")
        }
        rx.await?
    }
    async fn lookup<T: 'static + Sync + Send + Clone>(&mut self, name: String) -> Result<Option<T>, anyhow::Error> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let event = crate::core::registry::Lookup::<T>::new(name, tx);
        if let Some(r) = self.registry.as_ref() {
            r.0.send(crate::core::registry::GlobalRegistryEvent::Boxed(Box::new(event))).ok();
        } else {
            anyhow::bail!("Registry doesn't exist")
        }
        Ok(rx.await?)
    }
    async fn lookup_all<T: 'static + Sync + Send + Clone>(
        &mut self,
    ) -> Result<Option<std::collections::HashMap<String, T>>, anyhow::Error> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let event = crate::core::registry::LookupAll::<T>::new(tx);
        if let Some(r) = self.registry.as_ref() {
            r.0.send(crate::core::registry::GlobalRegistryEvent::Boxed(Box::new(event))).ok();
        } else {
            anyhow::bail!("Registry doesn't exist")
        }
        Ok(rx.await?)
    }
    async fn remove<T: 'static + Sync + Send + Clone>(&mut self, name: String) {
        let event = crate::core::registry::Remove::<T>::new(name);
        if let Some(r) = self.registry.as_ref() {
            r.0.send(crate::core::registry::GlobalRegistryEvent::Boxed(Box::new(event))).ok();
        };
    }
}

pub mod launcher;

pub struct BackstageRuntime<G: Clone + Send + 'static> {
    context: Backstage<BackstageActor, crate::core::NullSupervisor, G>,
    launcher_handle: Option<launcher::LauncherHandle<G>>,
}

#[derive(Clone)]
pub struct BackstageHandle {
    tx: tokio::sync::mpsc::UnboundedSender<BackstageEvent>,
}

impl BackstageHandle {
    pub fn new(tx: tokio::sync::mpsc::UnboundedSender<BackstageEvent>) -> Self {
        Self { tx }
    }
}

impl ActorHandle for BackstageHandle {
    fn service(&self, service: &Service) {
        self.tx.send(BackstageEvent::Service(service.clone())).ok();
    }
    fn shutdown(self: Box<Self>) {
        // do nothing
    }
    fn aknshutdown(&self, service: Service, _r: ActorResult) {
        self.tx.send(BackstageEvent::Service(service)).ok();
    }
    fn send(&self, event: Box<dyn std::any::Any>) -> Result<(), Box<dyn std::any::Any>> {
        // do nothing
        Ok(())
    }
}

pub struct BackstageActor;
pub enum BackstageEvent {
    Service(Service),
    ExitProgram,
}
/// Backstage inbox (wrapper around tokio receiver)
pub struct BackstageInbox {
    rx: tokio::sync::mpsc::UnboundedReceiver<BackstageEvent>,
}

impl BackstageInbox {
    /// Create new backstage inbox (wrapper around tokio receiver)
    pub fn new(rx: tokio::sync::mpsc::UnboundedReceiver<BackstageEvent>) -> Self {
        Self { rx }
    }
}
impl Channel for BackstageActor {
    type Handle = BackstageHandle;
    type Inbox = BackstageInbox;
    fn channel(&mut self) -> Result<(Self::Handle, Self::Inbox), anyhow::Error> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let handle = BackstageHandle::new(tx);
        let inbox = BackstageInbox::new(rx);
        Ok((handle, inbox))
    }
}

impl<G: Send + Clone + 'static> BackstageRuntime<G> {
    /// Create new runtime
    pub fn new(name: &str, generic: G) -> Result<Self, anyhow::Error> {
        // create backstage
        let (handle, inbox) = BackstageActor.channel()?;
        let mut backstage = Backstage::<BackstageActor, _, _>::new(
            crate::core::NullSupervisor,
            handle.clone(),
            inbox,
            Service::new(name),
            None,
            generic,
        );
        // spawn registry as runtime child
        // Initialize registery
        let registry = crate::core::GlobalRegistry::new();
        // spawn registry as child
        let supervisor_handle = backstage.handle().clone().expect("BackstageActor Handle");
        let registry_handle = backstage.spawn(registry, supervisor_handle, Service::new("Registry"))?;
        // store the registry handle
        backstage.registry.replace(registry_handle);
        log::info!("{} spawned registry", backstage.service().name());
        // create and spawn launcher
        let launcher = launcher::Launcher::new();
        let launcher_handle = backstage.spawn(launcher, Box::new(handle.clone()), Service::new("Launcher"))?;
        // spawn ctrl_c
        Backstage::<BackstageActor, crate::core::NullSupervisor, G>::spawn_task(ctrl_c(handle));

        Ok(Self {
            context: backstage,
            launcher_handle: Some(launcher_handle),
        })
    }
    /// Return context reference
    pub fn context(&mut self) -> &mut Backstage<BackstageActor, crate::core::NullSupervisor, G> {
        &mut self.context
    }
    /// Add actor to the launcher
    pub async fn add<T: Clone + Channel + crate::core::Actor<Backstage<T, Box<(dyn crate::core::ActorHandle + 'static)>, G>>>(
        &mut self,
        name: &str,
        actor: T,
    ) -> Result<T::Handle, anyhow::Error>
    where
        T::Handle: crate::core::ActorHandle,
        G: Send + Clone + 'static,
    {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let add_event = launcher::Add::new(name.into(), actor, tx);
        self.launcher_handle
            .as_mut()
            .expect("Launcher Handle")
            .0
            .send(launcher::LauncherEvent::<G>::Boxed(Box::new(add_event)))
            .map_err(|e| anyhow::Error::msg(format!("{}", e)))?;
        rx.await?
    }
    /// Block on the system
    pub async fn block_on(mut self) {
        self.context.handle().take();
        // drop registry handle as it's no longer needed
        self.context.registry.take();
        while let Some(event) = self.context.inbox().rx.recv().await {
            match event {
                BackstageEvent::Service(service) => {
                    if service.is_stopped() {
                        self.context.shutdown();
                    }
                    self.context.status_change(service);
                }
                BackstageEvent::ExitProgram => self.context.shutdown(),
            }
        }
        let name = self.context.service().name().to_string();
        self.context.breakdown(Ok(())).await;
        log::info!("Thank you for running {} using Backstage, Goodbye!", name);
    }
}

/// Useful function to exit program using ctrl_c signal
async fn ctrl_c(handle: BackstageHandle) {
    // await on ctrl_c
    if let Ok(_) = tokio::signal::ctrl_c().await {
        // exit program using launcher
        let exit_program_event = BackstageEvent::ExitProgram;
        let _ = handle.tx.send(exit_program_event);
    };
}

///////////
#[derive(Clone)]
struct TestActor;
#[derive(Clone)]
struct TestActorHandle;
impl ActorHandle for TestActorHandle {
    fn service(&self, _service: &Service) {}
    fn shutdown(self: Box<Self>) {}
    fn aknshutdown(&self, _service: Service, _r: ActorResult) {}
    fn send(&self, _event: Box<dyn std::any::Any>) -> Result<(), Box<dyn std::any::Any>> {
        Ok(())
    }
}
struct TestActorInbox;
#[async_trait::async_trait]
impl<C> Actor<C> for TestActor
where
    C: Essential<Actor = Self>,
{
    async fn run(self, _context: &mut C) -> ActorResult {
        Ok(())
    }
}

impl Channel for TestActor {
    type Handle = TestActorHandle;
    type Inbox = TestActorInbox;
    fn channel(&mut self) -> Result<(Self::Handle, Self::Inbox), anyhow::Error> {
        Ok((TestActorHandle, TestActorInbox))
    }
}

#[tokio::test]
async fn backstage_test() {
    env_logger::init();
    #[derive(Clone)]
    pub struct NoBounds;
    // build runtime and spawn launcher
    let mut runtime = BackstageRuntime::new("backstage-test", NoBounds).expect("runtime to get created");
    runtime.add("TestActor", TestActor).await.unwrap();
    runtime.block_on().await;
}
