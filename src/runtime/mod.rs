use crate::core::{Actor, ActorHandle, ActorResult, BoxedActorHandle, Channel, Essential, Service, Spawn};
use futures::future::AbortHandle;
/// Backstage default runtime struct
pub struct Backstage<C: Channel, S: ActorHandle> {
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
}

impl<C: Channel, S: ActorHandle> Backstage<C, S> {
    /// Create backstage context
    pub fn new(
        supervisor: S,
        handle: C::Handle,
        inbox: C::Inbox,
        service: Service,
        registry: Option<crate::core::registry::GlobalRegistryHandle>,
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
        }
    }
}

#[async_trait::async_trait]
impl<S: ActorHandle, C: Channel> Essential for Backstage<C, S>
where
    C::Handle: Clone + Sync,
{
    type Supervisor = S;
    type Actor = C;
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
        // remove itself from the registry (if any)
        if let Some(r) = self.registry.as_ref() {
            let event = crate::core::GlobalRegistryEvent::Boxed(Box::new(crate::core::registry::Remove::<C::Handle>::new(
                self.service.name().into(),
            )));
            r.0.send(event).ok();
        }
        // aknshutdown to supervisor
        self.supervisor.aknshutdown(self.service, r);
    }
    fn shutdown(&mut self) {
        // drop registty handle (if any)
        self.registry.take();
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
        self.children_handles.contains_key(name)
    }
    fn status_change(&mut self, microservice: Service) {
        let name = microservice.name();
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
}

// implementation of spawn contract
impl<PA: Channel, PS: ActorHandle, A, S: ActorHandle> Spawn<A, S> for Backstage<PA, PS>
where
    Self: Essential<Actor = PA>,
    A: Actor<Backstage<A, S>>,
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
        let child_context = Backstage::<A, S>::new(supervisor, handle.clone(), inbox, service.clone(), registry_handle);
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
impl<C: Channel, S: ActorHandle> crate::core::Registry for Backstage<C, S> {
    async fn register<T: 'static + Sync + Send + Clone>(&mut self, name: String, handle: T) -> Result<(), anyhow::Error> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let event = crate::core::registry::Register::<T>::new(name, handle, tx);
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

pub struct BackstageRuntime {
    context: Backstage<BackStageActor, crate::core::NullSupervisor>,
    launcher_handle: Option<launcher::LauncherHandle>,
}

#[derive(Clone)]
pub struct BackstageHandle {
    tx: tokio::sync::mpsc::UnboundedSender<Service>,
}

impl BackstageHandle {
    pub fn new(tx: tokio::sync::mpsc::UnboundedSender<Service>) -> Self {
        Self { tx }
    }
}

impl ActorHandle for BackstageHandle {
    fn service(&self, service: &Service) {
        self.tx.send(service.clone());
    }
    fn shutdown(self: Box<Self>) {
        // do nothing
    }
    fn aknshutdown(&self, service: Service, r: ActorResult) {
        self.tx.send(service);
    }
    fn send(&self, event: Box<dyn std::any::Any>) -> Result<(), Box<dyn std::any::Any>> {
        // do nothing
        Ok(())
    }
}

pub struct BackStageActor;
pub struct BackstageInbox {
    rx: tokio::sync::mpsc::UnboundedReceiver<Service>,
}

impl BackstageInbox {
    pub fn new(rx: tokio::sync::mpsc::UnboundedReceiver<Service>) -> Self {
        Self { rx }
    }
}
impl Channel for BackStageActor {
    type Handle = BackstageHandle;
    type Inbox = BackstageInbox;
    fn channel(&mut self) -> Result<(Self::Handle, Self::Inbox), anyhow::Error> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let handle = BackstageHandle::new(tx);
        let inbox = BackstageInbox::new(rx);
        Ok((handle, inbox))
    }
}
impl BackstageRuntime {
    /// Create new runtime
    pub fn new(name: &str) -> Result<Self, anyhow::Error> {
        // create backstage
        let (handle, inbox) = BackStageActor.channel()?;
        let mut backstage =
            Backstage::<BackStageActor, _>::new(crate::core::NullSupervisor, handle.clone(), inbox, Service::new(name), None);
        // create and spawn launcher
        let launcher = launcher::Launcher::new();
        let launcher_handle = backstage.spawn(launcher, Box::new(handle), Service::new("Launcher"))?;
        // spawn launcher
        Ok(Self {
            context: backstage,
            launcher_handle: Some(launcher_handle),
        })
    }
    /// Add actor to the launcher
    pub async fn add<T: Clone + Channel + crate::core::Actor<Backstage<T, Box<(dyn crate::core::ActorHandle + 'static)>>>>(
        &mut self,
        name: &str,
        actor: T,
    ) -> Result<T::Handle, anyhow::Error>
    where
        T::Handle: crate::core::ActorHandle,
    {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let add_event = launcher::Add::new(name.into(), actor, tx);
        self.launcher_handle
            .as_mut()
            .expect("Launcher Handle")
            .0
            .send(launcher::LauncherEvent::Boxed(Box::new(add_event)))
            .map_err(|e| anyhow::Error::msg(format!("{}", e)))?;
        rx.await?
    }
    /// Block on the system
    pub async fn block_on(mut self) {
        self.context.handle().take();
        while let Some(service) = self.context.inbox().rx.recv().await {
            self.context.status_change(service);
        }
        let name = self.context.service().name().to_string();
        self.context.breakdown(Ok(())).await;
        log::info!("Thank for you running {} using Backstage, Goodbye!", name);
    }
}

#[tokio::test]
async fn no_children() {
    env_logger::init();
    // build runtime and spawn launcher
    let runtime = BackstageRuntime::new("backstage-test").expect("runtime to get created");
    runtime.block_on().await;
}
