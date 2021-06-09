use super::*;

crate::builder!(LauncherBuilder {});

pub struct Launcher;

use std::collections::HashMap;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

pub enum LauncherEvent {
    Boxed(Box<dyn DynLauncherEvent>),
}

#[derive(Clone)]
/// LauncherEvent handle used to access the launhcer functionality
pub struct LauncherHandle(pub UnboundedSender<LauncherEvent>);
/// Inbox used by LauncherInbox actor to receive events
pub struct LauncherInbox(UnboundedReceiver<LauncherEvent>);
/// Channel implementation
impl Channel for Launcher {
    type Handle = LauncherHandle;
    type Inbox = LauncherInbox;
    fn channel(&mut self) -> Result<(Self::Handle, Self::Inbox), anyhow::Error> {
        let (tx, rx) = unbounded_channel();
        Ok((LauncherHandle(tx), LauncherInbox(rx)))
    }
}

impl ActorHandle for LauncherHandle {
    fn shutdown(self: Box<Self>) {
        // todo
    }
    fn aknshutdown(&self, _service: Service, _r: ActorResult) {
        // todo
    }
    fn service(&self, _service: &Service) {
        // update the service
    }
    fn send(&self, event: Box<dyn std::any::Any>) -> Result<(), Box<dyn std::any::Any>> {
        let my_event: LauncherEvent = *event.downcast()?;
        self.0.send(my_event).ok();
        Ok(())
    }
}

/// The event type of registry
pub trait DynLauncherEvent: Send {
    /// implement how launcher supposed to handle the boxed event
    fn handle(self: Box<Self>, launhcer: &mut Launcher, backstage: &mut Backstage<Launcher, Box<dyn ActorHandle>>);
}

/// Launcher Actor implementation
#[async_trait::async_trait]
impl Actor<Backstage<Self, Box<dyn ActorHandle>>> for Launcher
where
    Backstage<Self, Box<dyn ActorHandle>>: Essential<Actor = Self>,
{
    async fn run(mut self, context: &mut Backstage<Self, Box<dyn ActorHandle>>) -> ActorResult {
        /*while let Some(event) = context.inbox().0.recv().await {
            match event {
                LauncherEvent::Boxed(launcher_event) => {
                    launcher_event.handle(&mut self, context);
                }
            }
        }*/
        Ok(())
    }
}

/// Add T event by name
pub struct Add<T> {
    name: String,
    _marker: std::marker::PhantomData<T>,
    response_handle: tokio::sync::oneshot::Sender<Option<T>>,
}

// impl<T> Lookup<T> {
// Create new lookup T struct
// pub fn new(name: String, response_handle: tokio::sync::oneshot::Sender<Option<T>>) -> Self {
// Self { name, _marker: std::marker::PhantomData::<T>, response_handle}
// }
// }
//
// impl<T: 'static + Sync + Send + Clone> RegistryEvent for Lookup<T> {
// fn handle(self: Box<Self>, registry: &mut GlobalRegistry) {
// if let Some(hash_map) = registry.handles.get::<Mapped<T>>() {
// if let Some(requested_t) = hash_map.values.get(&self.name) {
// self.response_handle.send(Some(requested_t.clone())).ok();
// return ();
// }
// };
// nothing found
// self.response_handle.send(None).ok();
// }
// }
