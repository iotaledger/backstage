use super::{
    AbortRegistration, Abortable, Actor, ActorResult, Channel, Cleanup, CleanupData, Data, Reason, Resource, Route, Scope, ScopeId,
    Service, ServiceStatus, ServiceTree, Shutdown, Subscriber, Supervise, SCOPES,
};
use prometheus::core::Collector;
use std::collections::HashMap;
pub struct Rt<A: Actor, S: super::Supervise<A>>
where
    Self: Send,
{
    pub(crate) name: String,
    pub(crate) scopes_index: usize,
    pub(crate) scope_id: usize,
    pub(crate) parent_scope_id: usize,
    pub(crate) supervisor: S,
    pub(crate) handle: <A::Channel as Channel>::Handle,
    pub(crate) inbox: <A::Channel as Channel>::Inbox,
    pub(crate) children_handles: std::collections::HashMap<usize, Box<dyn Shutdown>>,
    pub(crate) children_joins: std::collections::HashMap<usize, tokio::task::JoinHandle<()>>,
    pub(crate) abort_registration: AbortRegistration,
    pub(crate) registered_metrics: Vec<Box<dyn prometheus::core::Collector>>,
}

pub struct InitializedRx(tokio::sync::oneshot::Receiver<Result<(), super::Reason>>);

impl InitializedRx {
    pub async fn initialized(self) -> Result<(), super::Reason> {
        self.0.await.expect("Expected functional CheckInit oneshot")
    }
}

impl<A: Actor, S: super::Supervise<A>> Rt<A, S> {
    pub async fn spawn<N: Into<String>, Child>(
        &mut self,
        name: N,
        mut child: Child,
    ) -> Result<(<Child::Channel as Channel>::Handle, InitializedRx), Reason>
    where
        Child: Actor<Context<<A::Channel as Channel>::Handle> = Rt<Child, <A::Channel as Channel>::Handle>>
            + super::ChannelBuilder<Child::Channel>,
        <A::Channel as Channel>::Handle: super::Supervise<Child>,
        Self: Send,
    {
        // try to create the actor's channel
        let channel = child.build_channel().await?;
        let parent_id = self.scope_id;
        let mut scopes_index;
        let mut child_scope_id;
        let handle;
        let inbox;
        let abort_registration;
        let mut metric;
        // create the service
        let name: String = name.into();
        let mut service = Service::new(name.clone());
        loop {
            // generate unique scope_id
            child_scope_id = rand::random::<ScopeId>();
            // check which scopes partition owns the scope_id
            scopes_index = child_scope_id % num_cpus::get();
            // access the scope
            let mut lock = SCOPES[scopes_index].write().await;
            if lock.contains_key(&child_scope_id) {
                drop(lock);
                continue;
            }
            // finialize the channel
            let r = channel.channel::<Child>(child_scope_id);
            handle = r.0;
            inbox = r.1;
            abort_registration = r.2;
            metric = r.3;
            service.update_status(ServiceStatus::Initializing);
            let scope = Scope::new(parent_id, service.clone(), Box::new(handle.clone()));
            lock.insert(child_scope_id, scope);
            drop(lock);
            break;
        }
        // update microservices with the new child
        let mut lock = SCOPES[self.scopes_index].write().await;
        let my_scope = lock.get_mut(&self.scope_id).expect("Expected self scope to exist");
        my_scope.microservices.insert(child_scope_id, service);
        if !my_scope.scope_id_by_ms_name.contains_key(&name) {
            my_scope.scope_id_by_ms_name.insert(name.clone(), child_scope_id);
        }
        drop(lock);
        // add the child handle to
        self.children_handles.insert(child_scope_id, Box::new(handle.clone()));
        let sup = self.handle.clone();
        // create child context
        let mut child_context = Rt::<Child, <A::Channel as Channel>::Handle>::new(
            name,
            scopes_index,
            child_scope_id,
            parent_id,
            sup,
            handle.clone(),
            inbox,
            abort_registration,
        );
        if let Some(metric) = metric.take() {
            child_context.register(metric).expect("Metric to be registered");
        }
        let (tx_oneshot, rx_oneshot) = tokio::sync::oneshot::channel::<Result<(), Reason>>();
        // create child future;
        let wrapped_fut = async move {
            let mut child = child;
            let mut rt = child_context;
            let check_init = tx_oneshot;
            let f = child.init(&mut rt);
            match f.await {
                Err(reason) => {
                    // inform oneshot receiver
                    check_init.send(Err(reason.clone())).ok();
                    // breakdown the child
                    let f = rt.breakdown(child, Err(reason));
                    f.await;
                }
                Ok(deps) => {
                    // update the status change in the scope and inform supervisor afterward
                    let mut lock = SCOPES[scopes_index].write().await;
                    let child_scope = lock.get_mut(&child_scope_id).expect("Child scope to exist");
                    // the child might set its service to be degraded, therefore we don't overwrite
                    if child_scope.service.is_initializing() {
                        child_scope.service.update_status(ServiceStatus::Running);
                    }
                    drop(lock);
                    // inform oneshot receiver
                    check_init.send(Ok(())).ok();
                    let f = child.run(&mut rt, deps);
                    let r = f.await;
                    let f = rt.breakdown(child, r);
                    f.await
                }
            }
        };
        let join_handle = tokio::spawn(wrapped_fut);
        self.children_joins.insert(child_scope_id, join_handle);
        Ok((handle, InitializedRx(rx_oneshot)))
    }
    pub fn inbox_mut(&mut self) -> &mut <A::Channel as Channel>::Inbox {
        &mut self.inbox
    }
    pub fn handle_mut(&mut self) -> &mut <A::Channel as Channel>::Handle {
        &mut self.handle
    }
    pub fn handle(&self) -> &<A::Channel as Channel>::Handle {
        &self.handle
    }
    pub fn supervisor_handle_mut(&mut self) -> &mut S {
        &mut self.supervisor
    }
    pub async fn microservices_stopped(&self) -> bool {
        let lock = SCOPES[self.scopes_index].read().await;
        let my_scope = lock.get(&self.scope_id).expect("expected self scope to exist");
        let r = my_scope.microservices.iter().all(|(_, s)| s.is_stopped());
        drop(lock);
        r
    }
    pub async fn stop(&mut self) {
        let mut lock = SCOPES[self.scopes_index].write().await;
        let my_scope = lock.get_mut(&self.scope_id).expect("expected self scope to exist");
        my_scope.service.update_status(ServiceStatus::Stopping);
        drop(lock);
        self.shutdown_children().await;
    }
    pub async fn shutdown_children(&mut self) {
        for (_, c) in self.children_handles.drain() {
            c.shutdown().await;
        }
    }
    pub async fn handle_microservice(&self, scope_id: ScopeId, service: Service, r: Option<ActorResult>) {
        let mut lock = SCOPES[self.scopes_index].write().await;
        let my_scope = lock.get_mut(&self.scope_id).expect("expected self scope to exist");
        if let Some(Ok(_)) = r {
            my_scope.microservices.remove(&scope_id);
            my_scope.scope_id_by_ms_name.remove(service.name().as_ref());
        } else {
            my_scope.microservices.insert(scope_id, service);
        }
        // recompute the status change only if self service not stopping
        if !my_scope.service.is_stopping() {
            if my_scope.microservices.iter().all(|(_, s)| s.is_running()) {
                my_scope.service.update_status(ServiceStatus::Running);
            } else {
                if !my_scope.service.is_maintenance() {
                    my_scope.service.update_status(ServiceStatus::Degraded);
                }
            };
        }
        let updated_service = my_scope.service.clone();
        drop(lock);
        self.supervisor.report(self.scope_id, updated_service).await;
    }
    /// Defines how to breakdown the context and it should aknowledge shutdown to its supervisor
    async fn breakdown(mut self, actor: A, r: super::ActorResult)
    where
        Self: Send,
    {
        // shutdown children handles (if any)
        for (_, c) in self.children_handles.drain() {
            c.shutdown().await;
        }
        // await on children_joins just to force the contract
        for (_, c) in self.children_joins.drain() {
            let _ = c.await;
        }
        // unregister registered_metrics
        self.unregister_metrics().expect("To unregister metrics");
        // update service to be stopped
        let mut lock = SCOPES[self.scopes_index].write().await;
        let mut my_scope = lock.remove(&self.scope_id).expect("expected self scope to exist");
        drop(lock);
        let mut my_service = my_scope.service;
        my_service.update_status(crate::core::ServiceStatus::Stopped);
        for (_type_id, cleanup_self) in my_scope.cleanup_data.drain() {
            cleanup_self.cleanup_self(&mut my_scope.data_and_subscribers).await;
        }
        for (scope_id, cleanup_from_other) in my_scope.cleanup.drain() {
            cleanup_from_other.cleanup_from_other(scope_id).await;
        }
        // aknshutdown to supervisor
        self.supervisor.eol(self.scope_id, my_service, actor, r).await;
    }
    pub async fn add_route<T: Send + 'static>(&self) -> anyhow::Result<()>
    where
        <A::Channel as Channel>::Handle: Route<T>,
    {
        let route: Box<dyn Route<T>> = Box::new(self.handle.clone());
        let mut lock = SCOPES[self.scopes_index].write().await;
        let my_scope = lock.get_mut(&self.scope_id).expect("expected self scope to exist");
        my_scope.router.insert(route);
        Ok(())
    }
    pub async fn remove_route<T: Send + 'static>(&self) -> anyhow::Result<()> {
        let mut lock = SCOPES[self.scopes_index].write().await;
        let my_scope = lock.get_mut(&self.scope_id).expect("expected self scope to exist");
        my_scope.router.remove::<Box<dyn Route<T>>>();
        Ok(())
    }
    pub async fn send<T: Send + 'static>(&self, scope_id: ScopeId, message: T) -> anyhow::Result<()> {
        let scopes_index = scope_id % num_cpus::get();
        let lock = SCOPES[scopes_index].read().await;
        if let Some(scope) = lock.get(&scope_id) {
            if let Some(route) = scope.router.get::<Box<dyn Route<T>>>() {
                if let Some(message) = route.try_send_msg(message).await? {
                    let cloned_route = route.clone();
                    drop(lock);
                    cloned_route.send_msg(message).await?;
                    Ok(())
                } else {
                    Ok(())
                }
            } else {
                anyhow::bail!("No route available")
            }
        } else {
            anyhow::bail!("No scope available")
        }
    }
}

impl<A: Actor, S: super::Supervise<A>> Rt<A, S> {
    /// Create backstage context
    pub fn new(
        name: String,
        scopes_index: usize,
        scope_id: usize,
        parent_scope_id: usize,
        supervisor: S,
        handle: <A::Channel as Channel>::Handle,
        inbox: <A::Channel as Channel>::Inbox,
        abort_registration: AbortRegistration,
    ) -> Self {
        Self {
            name,
            scopes_index,
            scope_id,
            parent_scope_id,
            supervisor,
            handle,
            inbox,
            children_handles: std::collections::HashMap::new(),
            children_joins: std::collections::HashMap::new(),
            abort_registration,
            registered_metrics: Vec::new(),
        }
    }
    /// Register a metric
    pub fn register<T: Collector + Clone + 'static>(&mut self, metric: T) -> prometheus::Result<()> {
        let r = super::registry::PROMETHEUS_REGISTRY.register(Box::new(metric.clone()));
        if r.is_ok() {
            self.registered_metrics.push(Box::new(metric))
        }
        r
    }
    /// Unregister all the metrics
    pub(crate) fn unregister_metrics(&mut self) -> prometheus::Result<()> {
        for m in self.registered_metrics.pop() {
            super::registry::PROMETHEUS_REGISTRY.unregister(m)?
        }
        Ok(())
    }
    pub fn name(&self) -> std::borrow::Cow<'static, str> {
        self.name.clone().into()
    }
}

impl<A: Actor, S: Supervise<A>> Rt<A, S> {
    pub async fn publish<T: Resource>(&self, resource: T) {
        let scope_id = self.scope_id;
        let scopes_index = self.scopes_index;
        let mut lock = SCOPES[scopes_index].write().await;
        let my_scope = lock.get_mut(&scope_id).expect("Self scope to exist");
        if let Some(data) = my_scope.data_and_subscribers.get_mut::<Data<T>>() {
            let previous = data.resource.replace(resource.clone());
            let mut active_subscribers = HashMap::<ScopeId, Subscriber<T>>::new();
            // require further read locks
            let mut should_get_shutdown = Vec::<Box<dyn Shutdown>>::new();
            let mut should_get_resource = Vec::<ScopeId>::new();
            // publish copy to existing subscriber(s)
            for (sub_scope_id, subscriber) in data.subscribers.drain() {
                match subscriber {
                    Subscriber::OneCopy(one_sender) => {
                        one_sender.send(Ok(resource.clone())).ok();
                    }
                    Subscriber::LinkedOneCopy(mut one_sender_opt, shutdown_handle) => {
                        if let Some(one_sender) = one_sender_opt.take() {
                            one_sender.send(Ok(resource.clone())).ok();
                            // reinsert into our new subscribers
                            active_subscribers.insert(sub_scope_id, Subscriber::LinkedOneCopy(None, shutdown_handle));
                        } else {
                            // check if the resource already existed, which mean the actor already cloned cloned a copy
                            if previous.is_some() {
                                // note: we don't shut it down yet as it might cause deadlock
                                should_get_shutdown.push(shutdown_handle);
                            }
                        };
                    }
                    subscriber => {
                        should_get_resource.push(sub_scope_id);
                        active_subscribers.insert(sub_scope_id, subscriber);
                    }
                }
            }
            data.subscribers = active_subscribers;
            drop(lock);
            for shutdown_handle in should_get_shutdown.pop() {
                shutdown_handle.shutdown().await;
            }
            // publish the resource to other scopes
            for sub_id in should_get_resource.pop() {
                let sub_scopes_index = sub_id % num_cpus::get();
                let lock = SCOPES[sub_scopes_index].read().await;
                if let Some(sub_scope) = lock.get(&sub_id) {
                    if let Some(route) = sub_scope.router.get::<Box<dyn Route<T>>>() {
                        match route.try_send_msg(resource.clone()).await {
                            Ok(Some(message)) => {
                                let boxed_route = route.clone();
                                drop(lock);
                                if let Err(e) = boxed_route.send_msg(message).await {
                                    log::error!("{}", e);
                                } else {
                                    log::debug!("Message published to subscriber: {}", sub_id);
                                }
                            }
                            Ok(None) => {
                                log::debug!("Message published to subscriber: {}", sub_id);
                            }
                            Err(e) => {
                                log::error!("{}", e);
                            }
                        };
                    } else {
                        // no route available,
                        log::error!("Subscriber: {}, doesn't have route", sub_id);
                    };
                } else {
                    // sub dropped its scope, nothing is required
                    log::warn!("Subscriber: {} scope is dropped", sub_id);
                };
            }
        } else {
            let data = Data::<T>::with_resource(resource);
            let cleanup_data = CleanupData::<T>::new();
            let type_id = std::any::TypeId::of::<T>();
            my_scope.data_and_subscribers.insert(data);
            my_scope.cleanup_data.insert(type_id, Box::new(cleanup_data));
            drop(lock);
        };
    }
    pub async fn depends_on<T: Resource>(&self, resource_scope_id: ScopeId) -> anyhow::Result<T, anyhow::Error> {
        let my_scope_id = self.scope_id;
        let resource_scopes_index = resource_scope_id % num_cpus::get();
        let mut lock = SCOPES[resource_scopes_index].write().await;
        if let Some(resource_scope) = lock.get_mut(&resource_scope_id) {
            // get the resource if it's available
            if let Some(data) = resource_scope.data_and_subscribers.get_mut::<Data<T>>() {
                if let Some(resource) = data.resource.clone().take() {
                    Ok(resource)
                } else {
                    let (tx, rx) = tokio::sync::oneshot::channel::<anyhow::Result<T>>();
                    let subscriber = Subscriber::<T>::OneCopy(tx);
                    data.subscribers.insert(my_scope_id, subscriber);
                    drop(lock);
                    let abortable = Abortable::new(rx, self.abort_registration.clone());
                    if let Ok(r) = abortable.await {
                        r?
                    } else {
                        anyhow::bail!("Aborted")
                    }
                }
            } else {
                let (tx, rx) = tokio::sync::oneshot::channel::<anyhow::Result<T>>();
                let subscriber = Subscriber::<T>::OneCopy(tx);
                let data = Data::<T>::with_subscriber(my_scope_id, subscriber);
                let cleanup_data = CleanupData::<T>::new();
                let type_id = std::any::TypeId::of::<T>();
                resource_scope.cleanup_data.insert(type_id, Box::new(cleanup_data));
                resource_scope.data_and_subscribers.insert(data);
                drop(lock);
                let abortable = Abortable::new(rx, self.abort_registration.clone());
                if let Ok(r) = abortable.await {
                    r?
                } else {
                    anyhow::bail!("Aborted")
                }
            }
        } else {
            // resource_scope got dropped or it doesn't exist
            anyhow::bail!("Resource scope doesn't exist");
        }
    }
    pub async fn link<T: Resource>(&self, resource_scope_id: ScopeId) -> anyhow::Result<T, anyhow::Error> {
        let my_scope_id = self.scope_id;
        let resource_scopes_index = resource_scope_id % num_cpus::get();
        let mut lock = SCOPES[resource_scopes_index].write().await;
        if let Some(resource_scope) = lock.get_mut(&resource_scope_id) {
            // get the resource if it's available
            if let Some(data) = resource_scope.data_and_subscribers.get_mut::<Data<T>>() {
                if let Some(resource) = data.resource.clone().take() {
                    let shutdown_handle = Box::new(self.handle.clone());
                    let subscriber = Subscriber::<T>::LinkedOneCopy(None, shutdown_handle);
                    data.subscribers.insert(self.scope_id, subscriber);
                    drop(lock);
                    self.add_cleanup_from_other_obj::<T>(resource_scope_id).await;
                    Ok(resource)
                } else {
                    let (tx, rx) = tokio::sync::oneshot::channel::<anyhow::Result<T>>();
                    let shutdown_handle = Box::new(self.handle.clone());
                    let subscriber = Subscriber::<T>::LinkedOneCopy(Some(tx), shutdown_handle);
                    data.subscribers.insert(my_scope_id, subscriber);
                    drop(lock);
                    let abortable = Abortable::new(rx, self.abort_registration.clone());
                    if let Ok(r) = abortable.await {
                        let r = r?;
                        if r.is_ok() {
                            self.add_cleanup_from_other_obj::<T>(resource_scope_id).await;
                        };
                        r
                    } else {
                        anyhow::bail!("Aborted")
                    }
                }
            } else {
                let (tx, rx) = tokio::sync::oneshot::channel::<anyhow::Result<T>>();
                let shutdown_handle = Box::new(self.handle.clone());
                let subscriber = Subscriber::<T>::LinkedOneCopy(Some(tx), shutdown_handle);
                let cleanup_data = CleanupData::<T>::new();
                let type_id = std::any::TypeId::of::<T>();
                let data = Data::<T>::with_subscriber(my_scope_id, subscriber);
                resource_scope.data_and_subscribers.insert(data);
                resource_scope.cleanup_data.insert(type_id, Box::new(cleanup_data));
                drop(lock);
                let abortable = Abortable::new(rx, self.abort_registration.clone());
                if let Ok(r) = abortable.await {
                    let r = r?;
                    if r.is_ok() {
                        self.add_cleanup_from_other_obj::<T>(resource_scope_id).await;
                    };
                    r
                } else {
                    anyhow::bail!("Aborted")
                }
            }
        } else {
            // resource_scope got dropped or it doesn't exist
            anyhow::bail!("Resource scope doesn't exist");
        }
    }
    pub async fn depends_on_dyn<T: Resource>(&self, resource_scope_id: ScopeId) -> anyhow::Result<Option<T>, anyhow::Error>
    where
        <A::Channel as Channel>::Handle: Route<T>,
    {
        let my_scope_id = self.scope_id;
        // add route first
        let mut lock = SCOPES[self.scopes_index].write().await;
        let my_scope = lock.get_mut(&my_scope_id).expect("Self scope to exist");
        let route: Box<dyn Route<T>> = Box::new(self.handle.clone());
        my_scope.router.insert(route);
        drop(lock);
        let resource_scopes_index = resource_scope_id % num_cpus::get();
        let mut lock = SCOPES[resource_scopes_index].write().await;
        if let Some(resource_scope) = lock.get_mut(&resource_scope_id) {
            // get the resource if it's available
            if let Some(data) = resource_scope.data_and_subscribers.get_mut::<Data<T>>() {
                let subscriber = Subscriber::<T>::DynCopy;
                data.subscribers.insert(my_scope_id, subscriber);
                if let Some(resource) = data.resource.clone().take() {
                    drop(lock);
                    Ok(Some(resource))
                } else {
                    drop(lock);
                    Ok(None)
                }
            } else {
                let subscriber = Subscriber::<T>::DynCopy;
                let cleanup_data = CleanupData::<T>::new();
                let type_id = std::any::TypeId::of::<T>();
                let data = Data::<T>::with_subscriber(my_scope_id, subscriber);
                resource_scope.data_and_subscribers.insert(data);
                resource_scope.cleanup_data.insert(type_id, Box::new(cleanup_data));
                drop(lock);
                Ok(None)
            }
        } else {
            // resource_scope got dropped or it doesn't exist
            anyhow::bail!("Resource scope doesn't exist");
        }
    }
    async fn add_cleanup_from_other_obj<T: Resource>(&self, resource_scope_id: ScopeId) {
        let cleanup = Cleanup::<T>::new(resource_scope_id);
        let mut lock = SCOPES[self.scopes_index].write().await;
        let my_scope = lock.get_mut(&self.scope_id).expect("Self scope to exist");
        my_scope.cleanup.insert(resource_scope_id, Box::new(cleanup));
        drop(lock);
    }
}

pub struct Runtime<A: Actor> {
    join_handle: tokio::task::JoinHandle<()>,
    handle: <A::Channel as Channel>::Handle,
    initialized_rx: Option<InitializedRx>,
}

impl<A: Actor> Runtime<A> {
    pub async fn new<T>(name: T, child: A) -> Result<Self, Reason>
    where
        A: super::ChannelBuilder<A::Channel> + Actor<Context<super::NullSupervisor> = Rt<A, super::NullSupervisor>>,
        T: Into<String>,
    {
        Self::with_supervisor(name, child, super::NullSupervisor).await
    }
    /// Create new runtime
    pub async fn with_supervisor<T, S>(name: T, mut child: A, supervisor: S) -> Result<Self, Reason>
    where
        A: super::ChannelBuilder<A::Channel> + Actor<Context<S> = Rt<A, S>>,
        T: Into<String>,
        S: super::Supervise<A>,
    {
        // try to create the actor's channel
        let (handle, inbox, abort_registration, mut metric) = child.build_channel().await?.channel::<A>(0);
        // this is the root runtime, so we are going to use 0 as the parent_id
        let parent_id = 0;
        let shutdown_handle = Box::new(handle.clone());
        let scopes_index = 0;
        let child_scope_id = 0;
        // create the service
        let name: String = name.into();
        let mut service = Service::new(name.clone());
        let mut lock = SCOPES[0].write().await;
        service.update_status(ServiceStatus::Initializing);
        let scope = Scope::new(parent_id, service.clone(), shutdown_handle.clone());
        lock.insert(child_scope_id, scope);
        drop(lock);
        // create child context
        let mut child_context = Rt::<A, S>::new(
            name,
            scopes_index,
            child_scope_id,
            parent_id,
            supervisor,
            handle.clone(),
            inbox,
            abort_registration,
        );
        if let Some(metric) = metric.take() {
            child_context.register(metric).expect("Metric to be registered");
        }
        let (tx_oneshot, rx_oneshot) = tokio::sync::oneshot::channel::<Result<(), Reason>>();
        // create child future;
        let wrapped_fut = async move {
            let mut child = child;
            let mut rt = child_context;
            let check_init = tx_oneshot;
            let f = child.init(&mut rt);
            match f.await {
                Err(reason) => {
                    // inform oneshot receiver
                    check_init.send(Err(reason.clone())).ok();
                    // breakdown the child
                    let f = rt.breakdown(child, Err(reason));
                    f.await;
                }
                Ok(deps) => {
                    // update the status change in the scope and inform supervisor afterward
                    let mut lock = SCOPES[scopes_index].write().await;
                    let child_scope = lock.get_mut(&child_scope_id).expect("Child scope to exist");
                    // the child might set its service to be degraded, therefore we don't overwrite
                    if !child_scope.service.is_degraded() {
                        child_scope.service.update_status(ServiceStatus::Running);
                    }
                    drop(lock);
                    // inform oneshot receiver
                    check_init.send(Ok(())).ok();
                    let f = child.run(&mut rt, deps);
                    let r = f.await;
                    let f = rt.breakdown(child, r);
                    f.await
                }
            }
        };
        let join_handle = tokio::spawn(wrapped_fut);
        tokio::spawn(ctrl_c(handle.clone()));
        Ok(Self {
            handle,
            join_handle,
            initialized_rx: Some(InitializedRx(rx_oneshot)),
        })
    }
    pub fn handle(&self) -> &<A::Channel as Channel>::Handle {
        &self.handle
    }
    pub fn handle_mut(&mut self) -> &<A::Channel as Channel>::Handle {
        &mut self.handle
    }
    pub fn take_initialized_rx(&mut self) -> Option<InitializedRx> {
        self.initialized_rx.take()
    }
    pub async fn block_on(self) -> Result<(), tokio::task::JoinError> {
        self.join_handle.await
    }
}

/// Useful function to exit program using ctrl_c signal
async fn ctrl_c<H: Shutdown>(handle: H) {
    let boxed = Box::new(handle);
    // await on ctrl_c
    if let Err(e) = tokio::signal::ctrl_c().await {
        log::error!("Tokio ctrl_c signal error: {}", e)
    };
    boxed.shutdown().await;
}

#[cfg(test)]
mod tests {
    use crate::core::*;

    ////////// Custom backstage start /////////////;
    struct Backstage;
    #[derive(Debug)]
    enum BackstageEvent {
        // #[backstage_macros::shutdown] todo impl macro
        Shutdown,
    }
    impl ShutdownEvent for BackstageEvent {
        fn shutdown_event() -> Self {
            BackstageEvent::Shutdown
        }
    }
    #[async_trait::async_trait]
    impl Actor for Backstage {
        type Channel = UnboundedChannel<BackstageEvent>;
        async fn init<S: Supervise<Self>>(&mut self, rt: &mut Self::Context<S>) -> Result<Self::Deps, Reason> {
            // build and spawn your apps actors using the rt
            rt.handle().shutdown().await;
            Ok(())
        }
        async fn run<S: Supervise<Self>>(&mut self, rt: &mut Self::Context<S>, _deps: Self::Deps) -> ActorResult {
            while let Some(event) = rt.inbox_mut().next().await {
                match event {
                    BackstageEvent::Shutdown => {
                        log::info!("{} got shutdown", rt.name());
                        break;
                    }
                }
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn custom_backstage() {
        env_logger::init();
        let backstage = Backstage;
        let runtime = Runtime::new("Backstage", backstage).await.expect("Runtime to build");
        runtime.block_on().await.expect("Runtime to run");
    }
    ////////// Custom backstage end /////////
}
