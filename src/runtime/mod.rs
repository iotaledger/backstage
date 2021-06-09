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
        // drop self handle (if any)
        self.handle.take();
        // these just to enforce the contract,
        // as the children shutdown supposed to be handled by the actor run method
        // shutdown children handles (if any)
        for (_, c) in self.children_handles.drain() {
            c.shutdown();
        }
        // abort children which have abort handle
        for (_, c) in self.children_aborts.drain() {
            c.abort();
        }
        // await on children_joins
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
