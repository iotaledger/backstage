use super::{
    AbortRegistration, Abortable, Actor, ActorResult, Channel, Cleanup, CleanupData, Data, Reason, Resource, Route, Scope, ScopeId,
    Service, ServiceStatus, Shutdown, Subscriber, Supervise, BACKSTAGE_PARTITIONS, SCOPES, VISIBLE_DATA,
};

use prometheus::core::Collector;
use std::collections::HashMap;
pub struct Rt<A: Actor, S: super::Supervise<A>>
where
    Self: Send,
{
    /// The level depth of the context, it's the scope depth from the root
    pub(crate) depth: usize,
    /// The service of the actor
    pub(crate) service: Service,
    pub(crate) scopes_index: usize,
    pub(crate) scope_id: ScopeId,
    pub(crate) parent_scope_id: Option<ScopeId>,
    pub(crate) supervisor: S,
    pub(crate) handle: <A::Channel as Channel>::Handle,
    pub(crate) inbox: <A::Channel as Channel>::Inbox,
    pub(crate) children_handles: std::collections::HashMap<ScopeId, Box<dyn Shutdown>>,
    pub(crate) children_joins: std::collections::HashMap<ScopeId, tokio::task::JoinHandle<()>>,
    pub(crate) abort_registration: AbortRegistration,
    pub(crate) registered_metrics: Vec<Box<dyn prometheus::core::Collector>>,
    pub(crate) visible_data: std::collections::HashSet<std::any::TypeId>,
}

pub struct InitializedRx(ScopeId, tokio::sync::oneshot::Receiver<Result<Service, super::Reason>>);

impl InitializedRx {
    pub async fn initialized(self) -> Result<(ScopeId, Service), super::Reason> {
        let service = self.1.await.expect("Expected functional CheckInit oneshot")?;
        Ok((self.0, service))
    }
}

impl<A: Actor, S: super::Supervise<A>> Rt<A, S>
where
    Self: Send + Sync,
{
    pub fn abort_registration(&self) -> AbortRegistration {
        self.abort_registration.clone()
    }
    /// Create abortable future which will be aborted if the actor got shutdown signal.
    pub fn abortable<F>(&self, fut: F) -> Abortable<F>
    where
        F: std::future::Future + Send + Sync,
    {
        let abort_registration = self.abort_registration();
        Abortable::new(fut, abort_registration)
    }
    pub async fn start<Dir: Into<Option<String>>, Child>(
        &mut self,
        directory: Dir,
        child: Child,
    ) -> Result<<Child::Channel as Channel>::Handle, Reason>
    where
        Child: Actor<Context<<A::Channel as Channel>::Handle> = Rt<Child, <A::Channel as Channel>::Handle>>
            + super::ChannelBuilder<Child::Channel>,
        <A::Channel as Channel>::Handle: super::Supervise<Child>,
        Self: Send,
    {
        let (h, init_signal) = self.spawn(directory, child).await?;
        if let Ok(Ok((scope_id, service))) = self.abortable(init_signal.initialized()).await {
            self.upsert_microservice(scope_id, service);
            Ok(h)
        } else {
            Err(Reason::Aborted)
        }
    }
    pub async fn spawn<Dir: Into<Option<String>>, Child>(
        &mut self,
        directory: Dir,
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
        let mut dir = directory.into();
        let mut service = Service::new::<Child>(dir.clone());
        loop {
            // generate unique scope_id
            child_scope_id = rand::random::<ScopeId>();
            // check which scopes partition owns the scope_id
            scopes_index = child_scope_id % *BACKSTAGE_PARTITIONS;
            // access the scope
            let mut lock = SCOPES[scopes_index].write().await;
            if lock.contains_key(&child_scope_id) {
                drop(lock);
                continue;
            }
            // finialize the channel
            let mut r = channel.channel::<Child>(child_scope_id);
            handle = r.0;
            inbox = r.1;
            abort_registration = r.2;
            metric = r.3;
            service.update_status(ServiceStatus::Initializing);
            let mut scope = Scope::new(Some(parent_id), Box::new(handle.clone()));
            // add route (if any)
            if let Some(route) = r.4.take() {
                scope.router.insert(route);
            }
            let data = Data::with_resource(service.clone());
            scope.data_and_subscribers.insert(data);
            let data = Data::with_resource(handle.clone());
            scope.data_and_subscribers.insert(data);
            lock.insert(child_scope_id, scope);
            drop(lock);
            break;
        }
        self.service.microservices.insert(child_scope_id, service.clone());
        if let Some(dir_name) = dir.take() {
            let mut lock = SCOPES[self.scopes_index].write().await;
            let my_scope = lock.get_mut(&self.scope_id).expect("Self scope to exist");
            my_scope.active_directories.insert(dir_name, child_scope_id);
            drop(lock);
        }
        // add the child handle to the children_handles
        self.children_handles.insert(child_scope_id, Box::new(handle.clone()));
        // create supervisor handle
        let sup = self.handle.clone();
        // created visible data
        let visible_data = std::collections::HashSet::new();
        // the child depth, relative to the supervision tree height
        let child_depth = self.depth + 1;
        // create child context
        let mut child_context = Rt::<Child, <A::Channel as Channel>::Handle>::new(
            child_depth,
            service,
            scopes_index,
            child_scope_id,
            Some(parent_id),
            sup,
            handle.clone(),
            inbox,
            abort_registration,
            visible_data,
        );
        if let Some(metric) = metric.take() {
            child_context.register(metric).expect("Metric to be registered");
        }
        let (tx_oneshot, rx_oneshot) = tokio::sync::oneshot::channel::<Result<Service, Reason>>();
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
                    if rt.service().is_initializing() {
                        rt.update_status(ServiceStatus::Running).await
                    }
                    let service = rt.service().clone();
                    // inform oneshot receiver
                    check_init.send(Ok(service)).ok();
                    let f = child.run(&mut rt, deps);
                    let r = f.await;
                    let f = rt.breakdown(child, r);
                    f.await
                }
            }
        };
        let join_handle = tokio::spawn(wrapped_fut);
        self.children_joins.insert(child_scope_id, join_handle);
        Ok((handle, InitializedRx(child_scope_id, rx_oneshot)))
    }
    pub fn upsert_microservice(&mut self, scope_id: ScopeId, service: Service) {
        if service.is_stopped() {
            self.children_handles.remove(&scope_id);
            self.children_joins.remove(&scope_id);
        }
        self.service.microservices.insert(scope_id, service);
    }
    pub fn remove_microservice(&mut self, scope_id: ScopeId) {
        self.service.microservices.remove(&scope_id);
        self.children_handles.remove(&scope_id);
        self.children_joins.remove(&scope_id);
    }
    // Update context service status
    pub async fn update_status(&mut self, service_status: ServiceStatus) {
        self.service.update_status(service_status);
        let service = self.service.clone();
        // report to supervisor
        self.supervisor.report(self.scope_id, service.clone()).await;
        // publish the service to subscribers
        self.publish(service).await;
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
    pub fn supervisor_handle(&self) -> &S {
        &self.supervisor
    }
    /// Get the service
    pub fn service(&self) -> &Service {
        &self.service
    }
    /// Return the actor's scope id
    pub fn scope_id(&self) -> ScopeId {
        self.scope_id
    }
    /// Return the actor parent's scope id
    pub fn parent_id(&self) -> Option<ScopeId> {
        self.parent_scope_id
    }
    pub async fn get_directory_scope_id(&self, parent_scope_id: ScopeId, dir_name: &String) -> Option<ScopeId> {
        let scopes_index = parent_scope_id % *BACKSTAGE_PARTITIONS;
        let lock = SCOPES[scopes_index].read().await;
        if let Some(scope) = lock.get(&parent_scope_id) {
            let dir_name = scope.active_directories.get(dir_name).and_then(|s| Some(s.to_owned()));
            drop(lock);
            dir_name
        } else {
            None
        }
    }
    pub async fn shutdown_scope(&self, scope_id: ScopeId) -> anyhow::Result<()>
    where
        Self: Send,
    {
        let scopes_index = scope_id % *BACKSTAGE_PARTITIONS;
        let lock = SCOPES[scopes_index].read().await;
        if let Some(scope) = lock.get(&scope_id) {
            // we clone the handle to prevent very rare deadlock (which might only happen if channel is bounded)
            let shutdown_handle = scope.shutdown_handle.clone();
            drop(lock);
            shutdown_handle.shutdown().await;
        } else {
            anyhow::bail!("scope doesn't exist");
        };
        Ok(())
    }
    pub fn microservices_stopped(&self) -> bool {
        self.service.microservices.iter().all(|(_, s)| s.is_stopped())
    }
    pub fn microservices_all(&self, is: fn(&Service) -> bool) -> bool {
        self.service.microservices.iter().all(|(_, s)| is(s))
    }
    pub fn microservices_any(&self, is: fn(&Service) -> bool) -> bool {
        self.service.microservices.iter().any(|(_, s)| is(s))
    }
    pub async fn stop(&mut self) {
        self.shutdown_children().await;
        self.update_status(ServiceStatus::Stopping).await;
    }
    /// Shutdown all the children within this actor context
    pub async fn shutdown_children(&mut self) {
        for (_, c) in self.children_handles.drain() {
            c.shutdown().await;
        }
    }
    /// Shutdown all the children of a given type within this actor context
    pub async fn shutdown_children_type<T: Actor>(&mut self) {
        // extract the scopes for a given type
        let mut iter = self.service.scopes_iter::<T>();
        while let Some(scope_id) = iter.next() {
            if let Some(h) = self.children_handles.remove(&scope_id) {
                h.shutdown().await;
            };
        }
    }
    /// Defines how to breakdown the context and it should aknowledge shutdown to its supervisor
    async fn breakdown(mut self, actor: A, r: ActorResult)
    where
        Self: Send,
    {
        // shutdown children handles (if any)
        self.shutdown_children().await;
        // await on children_joins just to force the contract
        for (_, c) in self.children_joins.drain() {
            let _ = c.await;
        }
        // unregister registered_metrics
        self.unregister_metrics().expect("To unregister metrics");
        // update service to be Stopped
        self.service.update_status(ServiceStatus::Stopped);
        // clone service
        let service = self.service.clone();
        // delete the active scope from the supervisor (if any)
        if let Some(parent_id) = self.parent_scope_id.take() {
            if let Some(dir_name) = service.directory.as_ref() {
                let parent_scopes_index = parent_id % *BACKSTAGE_PARTITIONS;
                let mut lock = SCOPES[parent_scopes_index].write().await;
                if let Some(parent_scope) = lock.get_mut(&parent_id) {
                    parent_scope.active_directories.remove(dir_name);
                }
                drop(lock);
            }
        }
        // publish the service before cleaning up any data
        self.publish(service.clone()).await;
        // drop any visible data
        let depth = self.depth;
        let scope_id = self.scope_id;
        if !self.visible_data.is_empty() {
            let mut lock = VISIBLE_DATA.write().await;
            for type_id in self.visible_data.drain() {
                if let Some(mut vec) = lock.remove(&type_id) {
                    // check if there are other resources for the same type_id
                    if vec.len() != 1 {
                        vec.retain(|&x| x != (depth, scope_id));
                        lock.insert(type_id, vec);
                    }
                };
            }
            drop(lock);
        }
        // drop scope
        let mut lock = SCOPES[self.scopes_index].write().await;
        let mut my_scope = lock.remove(&self.scope_id).expect("Self scope to exist on drop");
        drop(lock);
        for (_type_id, cleanup_self) in my_scope.cleanup_data.drain() {
            cleanup_self.cleanup_self(&mut my_scope.data_and_subscribers).await;
        }
        for (_type_id_scope_id, cleanup_from_other) in my_scope.cleanup.drain() {
            cleanup_from_other.cleanup_from_other(self.scope_id).await;
        }
        // aknshutdown to supervisor
        self.supervisor.eol(self.scope_id, service, actor, r).await;
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
    pub async fn send<T: Send + 'static>(&self, scope_id: ScopeId, message: T) -> anyhow::Result<()>
    where
        Self: Send + Sync,
    {
        let scopes_index = scope_id % *BACKSTAGE_PARTITIONS;
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
        depth: usize,
        service: Service,
        scopes_index: usize,
        scope_id: ScopeId,
        parent_scope_id: Option<ScopeId>,
        supervisor: S,
        handle: <A::Channel as Channel>::Handle,
        inbox: <A::Channel as Channel>::Inbox,
        abort_registration: AbortRegistration,
        visible_data: std::collections::HashSet<std::any::TypeId>,
    ) -> Self {
        Self {
            depth,
            service,
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
            visible_data,
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
}

impl<A: Actor, S: Supervise<A>> Rt<A, S> {
    /// Add exposed resource
    pub async fn add_resource<T: Resource>(&mut self, resource: T) {
        // publish the resource in the local scope data_store
        self.publish(resource).await;
        // index the scope_id by the resource type_id
        self.expose::<T>().await
    }
    /// Expose resource as visible data
    pub async fn expose<T: Resource>(&mut self) {
        let type_id = std::any::TypeId::of::<T>();
        self.visible_data.insert(type_id.clone());
        let depth = self.depth;
        let scope_id = self.scope_id;
        // expose globally
        let mut lock = VISIBLE_DATA.write().await;
        lock.entry(type_id)
            .and_modify(|data_vec| {
                data_vec.push((depth, scope_id));
                data_vec.sort_by(|a, b| a.0.cmp(&b.0));
            })
            .or_insert(vec![(depth, scope_id)]);
        drop(lock);
    }
    /// Get the highest resource (at the most top level)
    pub async fn highest_scope_id<T: Resource>(&self) -> Option<ScopeId> {
        let type_id = std::any::TypeId::of::<T>();
        let lock = VISIBLE_DATA.read().await;
        if let Some(vec) = lock.get(&type_id) {
            Some(vec[0].clone().1)
        } else {
            None
        }
    }
    pub async fn lowest_scope_id<T: Resource>(&self) -> Option<ScopeId> {
        let type_id = std::any::TypeId::of::<T>();
        let lock = VISIBLE_DATA.read().await;
        if let Some(vec) = lock.get(&type_id) {
            Some(vec.last().expect("not empty scopes").1)
        } else {
            None
        }
    }
    pub async fn try_borrow<'a, T: Resource, R>(&self, resource_scope_id: ScopeId, fn_once: fn(&T) -> R) -> Option<R::Output>
    where
        R: std::future::Future + Send + 'static,
    {
        let scopes_index = resource_scope_id % *BACKSTAGE_PARTITIONS;
        let lock = SCOPES[scopes_index].read().await;
        if let Some(my_scope) = lock.get(&resource_scope_id) {
            if let Some(data) = my_scope.data_and_subscribers.get::<Data<T>>() {
                if let Some(resource) = data.resource.as_ref() {
                    return Some(fn_once(resource).await);
                }
            }
        }
        None
    }
    pub async fn try_borrow_mut<'a, T: Resource, R>(&self, resource_scope_id: ScopeId, fn_once: fn(&T) -> R) -> Option<R::Output>
    where
        R: std::future::Future + Send + 'static,
    {
        let scopes_index = resource_scope_id % *BACKSTAGE_PARTITIONS;
        let mut lock = SCOPES[scopes_index].write().await;
        if let Some(my_scope) = lock.get_mut(&resource_scope_id) {
            if let Some(data) = my_scope.data_and_subscribers.get::<Data<T>>() {
                if let Some(resource) = data.resource.as_ref() {
                    return Some(fn_once(resource).await);
                }
            }
        }
        None
    }
    /// Publish resource
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
                let sub_scopes_index = sub_id % *BACKSTAGE_PARTITIONS;
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
    pub async fn lookup<T: Resource>(&self, resource_scope_id: ScopeId) -> Option<T> {
        let resource_scopes_index = resource_scope_id % *BACKSTAGE_PARTITIONS;
        let mut lock = SCOPES[resource_scopes_index].write().await;
        if let Some(resource_scope) = lock.get_mut(&resource_scope_id) {
            if let Some(data) = resource_scope.data_and_subscribers.get::<Data<T>>() {
                data.resource.clone()
            } else {
                None
            }
        } else {
            None
        }
    }
    pub async fn drop_resource<T: Resource>(&self) {
        // require further read locks
        let mut should_get_shutdown = Vec::<Box<dyn Shutdown>>::new();
        // dynamic subscribers who should be notified because the resource got dropped
        let mut should_get_notification = Vec::<Box<dyn Route<Option<T>>>>::new();
        let mut lock = SCOPES[self.scopes_index].write().await;
        let my_scope = lock.get_mut(&self.scope_id).expect("self scope to exist");
        if let Some(mut data) = my_scope.data_and_subscribers.remove::<Data<T>>() {
            let resource = data.resource.take();
            // for peace of mind we drop the resource;
            drop(resource);
            // iterate the subscribers
            // drain subscribers
            for (_sub_scope_id, subscriber) in data.subscribers.drain() {
                match subscriber {
                    // this happen when the actor drops a resource before it publishes it,
                    // it might happen because it had some critical dep that were required to create this resource.
                    Subscriber::OneCopy(one_sender) => {
                        one_sender.send(Err(anyhow::Error::msg("Resource got dropped"))).ok();
                    }
                    Subscriber::LinkedOneCopy(mut one_sender_opt, shutdown_handle) => {
                        if let Some(one_sender) = one_sender_opt.take() {
                            one_sender.send(Err(anyhow::Error::msg("Resource got dropped"))).ok();
                            should_get_shutdown.push(shutdown_handle);
                        } else {
                            should_get_shutdown.push(shutdown_handle);
                        };
                    }
                    Subscriber::DynCopy(boxed_route) => {
                        should_get_notification.push(boxed_route);
                    }
                }
            }
        }; // else no active subscribers, so nothing to do;
        for route_opt_resource in should_get_notification.drain(..) {
            route_opt_resource.send_msg(None).await.ok();
        }
        for shutdown_handle in should_get_shutdown.drain(..) {
            shutdown_handle.shutdown().await;
        }
    }
    pub async fn depends_on<T: Resource>(&self, resource_scope_id: ScopeId) -> anyhow::Result<T, anyhow::Error> {
        let my_scope_id = self.scope_id;
        let resource_scopes_index = resource_scope_id % *BACKSTAGE_PARTITIONS;
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
        let resource_scopes_index = resource_scope_id % *BACKSTAGE_PARTITIONS;
        let mut lock = SCOPES[resource_scopes_index].write().await;
        if let Some(resource_scope) = lock.get_mut(&resource_scope_id) {
            // get the resource if it's available
            if let Some(data) = resource_scope.data_and_subscribers.get_mut::<Data<T>>() {
                if let Some(resource) = data.resource.clone().take() {
                    let shutdown_handle = Box::new(self.handle.clone());
                    let subscriber = Subscriber::<T>::LinkedOneCopy(None, shutdown_handle);
                    data.subscribers.insert(self.scope_id, subscriber);
                    drop(lock);
                    // the self actor might get shutdown before the resource provider,
                    // therefore we should cleanup self Subscriber::<T>::LinkedOneCopy from the provider
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
                            // as mentioned above, this needed to cleanup the provider if the linked subscriber shutdown before the provider
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
        <A::Channel as Channel>::Handle: Route<Option<T>>,
    {
        let my_scope_id = self.scope_id;
        // add route first
        let mut lock = SCOPES[self.scopes_index].write().await;
        let my_scope = lock.get_mut(&my_scope_id).expect("Self scope to exist");
        let route: Box<dyn Route<Option<T>>> = Box::new(self.handle.clone());
        my_scope.router.insert(route.clone());
        drop(lock);
        let resource_scopes_index = resource_scope_id % *BACKSTAGE_PARTITIONS;
        let mut lock = SCOPES[resource_scopes_index].write().await;
        if let Some(resource_scope) = lock.get_mut(&resource_scope_id) {
            // add cleanup from other obj.
            self.add_cleanup_from_other_obj::<T>(resource_scope_id).await;
            // get the resource if it's available
            if let Some(data) = resource_scope.data_and_subscribers.get_mut::<Data<T>>() {
                let subscriber = Subscriber::<T>::DynCopy(route);
                data.subscribers.insert(my_scope_id, subscriber);
                if let Some(resource) = data.resource.clone().take() {
                    drop(lock);
                    Ok(Some(resource))
                } else {
                    drop(lock);
                    Ok(None)
                }
            } else {
                let subscriber = Subscriber::<T>::DynCopy(route);
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
        let type_id = std::any::TypeId::of::<T>();
        let mut lock = SCOPES[self.scopes_index].write().await;
        let my_scope = lock.get_mut(&self.scope_id).expect("Self scope to exist");
        my_scope.cleanup.insert((type_id, resource_scope_id), Box::new(cleanup));
        drop(lock);
    }
}

pub struct Runtime<A: Actor> {
    join_handle: tokio::task::JoinHandle<()>,
    handle: <A::Channel as Channel>::Handle,
    initialized_rx: Option<InitializedRx>,
    websocket_server: Option<Box<dyn Shutdown>>,
    websocket_join_handle: Option<tokio::task::JoinHandle<()>>,
}

impl<A: Actor> Runtime<A> {
    pub async fn new<T>(root_dir: T, child: A) -> Result<Self, Reason>
    where
        A: super::ChannelBuilder<A::Channel> + Actor<Context<super::NullSupervisor> = Rt<A, super::NullSupervisor>>,
        T: Into<Option<String>>,
    {
        Self::with_supervisor(root_dir, child, super::NullSupervisor).await
    }

    /// Create new runtime
    pub async fn with_supervisor<T, S>(dir: T, mut child: A, supervisor: S) -> Result<Self, Reason>
    where
        A: super::ChannelBuilder<A::Channel> + Actor<Context<S> = Rt<A, S>>,
        T: Into<Option<String>>,
        S: super::Supervise<A>,
    {
        // try to create the actor's channel
        let (handle, inbox, abort_registration, mut metric, mut route) = child.build_channel().await?.channel::<A>(0);
        // this is the root runtime, so we are going to use 0 as the parent_id
        let shutdown_handle = Box::new(handle.clone());
        let scopes_index = 0;
        let child_scope_id = 0;
        // create the service
        let dir = dir.into();
        let mut service = Service::new::<A>(dir.clone());
        let mut lock = SCOPES[0].write().await;
        service.update_status(ServiceStatus::Initializing);
        let mut scope = Scope::new(None, shutdown_handle.clone());
        if let Some(route) = route.take() {
            scope.router.insert(route);
        }
        let data = Data::with_resource(service.clone());
        scope.data_and_subscribers.insert(data);
        let data = Data::with_resource(handle.clone());
        scope.data_and_subscribers.insert(data);
        lock.insert(child_scope_id, scope);
        drop(lock);
        let visible_data = std::collections::HashSet::new();
        let depth = 0;
        // create child context
        let mut child_context = Rt::<A, S>::new(
            depth,
            service,
            scopes_index,
            child_scope_id,
            None,
            supervisor,
            handle.clone(),
            inbox,
            abort_registration,
            visible_data,
        );
        if let Some(metric) = metric.take() {
            child_context.register(metric).expect("Metric to be registered");
        }
        let (tx_oneshot, rx_oneshot) = tokio::sync::oneshot::channel::<Result<Service, Reason>>();
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
                    if rt.service().is_initializing() {
                        rt.update_status(ServiceStatus::Running).await
                    }
                    // inform oneshot receiver
                    check_init.send(Ok(rt.service().clone())).ok();
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
            initialized_rx: Some(InitializedRx(child_scope_id, rx_oneshot)),
            websocket_server: None,
            websocket_join_handle: None,
        })
    }
    pub fn handle(&self) -> &<A::Channel as Channel>::Handle {
        &self.handle
    }
    pub fn handle_mut(&mut self) -> &<A::Channel as Channel>::Handle {
        &mut self.handle
    }
    pub async fn websocket_server(mut self, addr: std::net::SocketAddr, mut ttl: Option<u32>) -> Result<Self, Reason> {
        use crate::{
            core::{channel::ChannelBuilder, NullSupervisor},
            prefab::websocket::Websocket,
        };
        let websocket_scope_id = 1;
        let mut websocket = Websocket::new(addr.clone()).link_to(Box::new(self.handle.clone()));
        if let Some(ttl) = ttl.take() {
            websocket = websocket.set_ttl(ttl);
        }
        let (handle, inbox, abort_registration, mut metric, mut route) =
            websocket.build_channel().await?.channel::<Websocket>(websocket_scope_id);
        let shutdown_handle = Box::new(handle.clone());
        let scopes_index = websocket_scope_id % *BACKSTAGE_PARTITIONS;
        // create the service
        let dir_name: String = format!("ws@{}", addr);
        let mut service = Service::new::<Websocket>(Some(dir_name));
        let mut lock = SCOPES[scopes_index].write().await;
        service.update_status(ServiceStatus::Initializing);
        let mut scope = Scope::new(None, shutdown_handle.clone());
        if let Some(route) = route.take() {
            scope.router.insert(route);
        }
        lock.insert(websocket_scope_id, scope);
        drop(lock);
        let visible_data = std::collections::HashSet::new();
        let depth = 0;
        // create child context
        let mut child_context = Rt::<Websocket, NullSupervisor>::new(
            depth,
            service,
            scopes_index,
            1,
            None,
            NullSupervisor,
            handle.clone(),
            inbox,
            abort_registration,
            visible_data,
        );
        if let Some(metric) = metric.take() {
            child_context.register(metric).expect("Metric to be registered");
        }
        let wrapped_fut = async move {
            let mut child = websocket;
            let mut rt = child_context;
            let f = child.init(&mut rt);
            match f.await {
                Err(reason) => {
                    // breakdown the child
                    let f = rt.breakdown(child, Err(reason));
                    f.await;
                }
                Ok(deps) => {
                    if rt.service().is_initializing() {
                        rt.update_status(ServiceStatus::Running).await
                    }
                    let f = child.run(&mut rt, deps);
                    let r = f.await;
                    let f = rt.breakdown(child, r);
                    f.await
                }
            }
        };
        let join_handle = tokio::spawn(wrapped_fut);
        self.websocket_join_handle.replace(join_handle);
        self.websocket_server.replace(Box::new(handle.clone()));
        Ok(self)
    }

    pub fn take_initialized_rx(&mut self) -> Option<InitializedRx> {
        self.initialized_rx.take()
    }
    pub async fn block_on(mut self) -> Result<(), tokio::task::JoinError> {
        let r = self.join_handle.await;
        if let Some(ws_server_handle) = self.websocket_server.take() {
            ws_server_handle.shutdown().await;
            self.websocket_join_handle.expect("websocket join handle").await.ok();
        }
        r
    }
}

/// Useful function to exit program using ctrl_c signal
async fn ctrl_c<H: Shutdown>(handle: H) {
    // await on ctrl_c
    if let Err(e) = tokio::signal::ctrl_c().await {
        log::error!("Tokio ctrl_c signal error: {}", e)
    };
    handle.shutdown().await;
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
        async fn init<S: Supervise<Self>>(&mut self, rt: &mut Self::Context<S>) -> Result<Self::Data, Reason> {
            // build and spawn your apps actors using the rt
            rt.handle().shutdown().await;
            Ok(())
        }
        async fn run<S: Supervise<Self>>(&mut self, rt: &mut Self::Context<S>, _data: Self::Data) -> ActorResult {
            while let Some(event) = rt.inbox_mut().next().await {
                match event {
                    BackstageEvent::Shutdown => {
                        log::info!("backstage got shutdown");
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
        let runtime = Runtime::new(Some("Backstage".into()), backstage).await.expect("Runtime to build");
        runtime.block_on().await.expect("Runtime to run");
    }
    ////////// Custom backstage end /////////
}
