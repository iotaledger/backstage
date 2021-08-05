use crate::actor::{ActorPool, Channel, EventDriven, Sender, ShutdownHandle, System};
use futures::future::AbortHandle;
use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
};

use super::ScopeId;

/// Wrapper for data types
pub trait DataWrapper<T> {
    /// Get the wrapper value, consuming the wrapper
    fn into_inner(self) -> T;
}

/// A shared resource
#[derive(Clone)]
pub struct Res<R>(pub R);

impl<R: Deref> Deref for Res<R> {
    type Target = R::Target;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl<R: DerefMut> DerefMut for Res<R> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.deref_mut()
    }
}

impl<R> DataWrapper<R> for Res<R> {
    fn into_inner(self) -> R {
        self.0
    }
}

/// A shared system reference
pub struct Sys<S: System> {
    /// The actor handle
    pub actor: Act<S>,
    /// The shared state of the system
    pub state: Res<S::State>,
}

impl<S: System> Clone for Sys<S> {
    fn clone(&self) -> Self {
        Self {
            actor: self.actor.clone(),
            state: self.state.clone(),
        }
    }
}

/// An actor handle, used to send events
pub struct Act<A: EventDriven> {
    pub(crate) scope_id: ScopeId,
    pub(crate) sender: <A::Channel as Channel<A, A::Event>>::Sender,
    pub(crate) shutdown_handle: ShutdownHandle,
    pub(crate) abort_handle: AbortHandle,
}

impl<A: EventDriven> Act<A> {
    pub(crate) fn new(
        scope_id: ScopeId,
        sender: <A::Channel as Channel<A, A::Event>>::Sender,
        shutdown_handle: ShutdownHandle,
        abort_handle: AbortHandle,
    ) -> Self {
        Self {
            scope_id,
            sender,
            shutdown_handle,
            abort_handle,
        }
    }

    /// Shut down the actor with this handle. Use with care!
    pub fn shutdown(&self) {
        self.shutdown_handle.shutdown();
    }

    /// Abort the actor with this handle. Use with care!
    pub fn abort(&self) {
        self.abort_handle.abort();
    }

    /// Get the scope id of the actor this handle represents
    pub fn scope_id(&self) -> &ScopeId {
        &self.scope_id
    }
}

impl<A: EventDriven> Deref for Act<A> {
    type Target = <A::Channel as Channel<A, A::Event>>::Sender;

    fn deref(&self) -> &Self::Target {
        &self.sender
    }
}

impl<A: EventDriven> DerefMut for Act<A> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.sender
    }
}

impl<A: EventDriven> Clone for Act<A>
where
    <A::Channel as Channel<A, A::Event>>::Sender: Clone,
{
    fn clone(&self) -> Self {
        Self {
            scope_id: self.scope_id,
            sender: self.sender.clone(),
            shutdown_handle: self.shutdown_handle.clone(),
            abort_handle: self.abort_handle.clone(),
        }
    }
}

impl<A: EventDriven> DataWrapper<<A::Channel as Channel<A, A::Event>>::Sender> for Act<A> {
    fn into_inner(self) -> <A::Channel as Channel<A, A::Event>>::Sender {
        self.sender
    }
}

#[async_trait::async_trait]
impl<A: EventDriven> Sender<A::Event> for Act<A> {
    fn send(&self, event: A::Event) -> anyhow::Result<()> {
        self.sender.send(event)
    }

    fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }
}

/// A pool of actors which can be used as a dependency
#[derive(Default)]
pub struct Pool<P: ActorPool>(pub Arc<P>);

impl<P: ActorPool> Clone for Pool<P> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<P: ActorPool> Deref for Pool<P> {
    type Target = Arc<P>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<P: ActorPool> DataWrapper<Arc<P>> for Pool<P> {
    fn into_inner(self) -> Arc<P> {
        self.0
    }
}
