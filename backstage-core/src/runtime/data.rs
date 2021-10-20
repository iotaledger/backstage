// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::ScopeId;
use crate::actor::{Actor, ActorPool, Envelope, EnvelopeSender, Sender, ShutdownHandle, System};
use futures::future::AbortHandle;
use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
};

/// Wrapper for data types
pub trait DataWrapper<T> {
    /// Get the wrapper value, consuming the wrapper
    fn into_inner(self) -> T;
}

/// A shared resource
#[derive(Clone, Debug)]
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

impl<S: System> Clone for Sys<S>
where
    Act<S>: Clone,
{
    fn clone(&self) -> Self {
        Self {
            actor: self.actor.clone(),
            state: self.state.clone(),
        }
    }
}

/// An actor handle, used to send events
pub struct Act<A: Actor> {
    pub(crate) scope_id: ScopeId,
    pub(crate) sender: Box<dyn Sender<Envelope<A>>>,
    pub(crate) shutdown_handle: ShutdownHandle,
    pub(crate) abort_handle: AbortHandle,
}

impl<A: 'static + Actor> Act<A> {
    pub(crate) fn new(
        scope_id: ScopeId,
        sender: Box<dyn Sender<Envelope<A>>>,
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

impl<A: Actor> Deref for Act<A> {
    type Target = Box<dyn Sender<Envelope<A>>>;

    fn deref(&self) -> &Self::Target {
        &self.sender
    }
}

impl<A: Actor> DerefMut for Act<A> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.sender
    }
}

impl<A: 'static + Actor> Clone for Act<A> {
    fn clone(&self) -> Self {
        Self {
            scope_id: self.scope_id,
            sender: self.sender.clone(),
            shutdown_handle: self.shutdown_handle.clone(),
            abort_handle: self.abort_handle.clone(),
        }
    }
}

impl<A: 'static + Actor> std::fmt::Debug for Act<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(&format!("Act<{}>", std::any::type_name::<A>()))
            .field("scope_id", &self.scope_id)
            .finish()
    }
}

impl<A: Actor> DataWrapper<Box<dyn Sender<Envelope<A>>>> for Act<A> {
    fn into_inner(self) -> Box<dyn Sender<Envelope<A>>> {
        self.sender
    }
}

impl<A: 'static + Actor> EnvelopeSender<A> for Act<A> {
    fn send<E: 'static + crate::actor::DynEvent<A> + Send + Sync>(&self, event: E) -> anyhow::Result<()>
    where
        Self: Sized,
    {
        self.sender.send(Box::new(event))
    }

    fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }
}

impl<A: 'static + Actor> EnvelopeSender<A> for Option<&Act<A>> {
    fn send<E: 'static + crate::actor::DynEvent<A> + Send + Sync>(&self, event: E) -> anyhow::Result<()>
    where
        Self: Sized,
    {
        match self {
            Some(s) => s.sender.send(Box::new(event)),
            None => Err(anyhow::anyhow!("Sender is None!")),
        }
    }

    fn is_closed(&self) -> bool {
        match self {
            Some(s) => s.sender.is_closed(),
            None => true,
        }
    }
}

/// A pool of actors which can be used as a dependency
#[derive(Default, Debug)]
pub struct Pool<P: ActorPool>(pub Arc<P>)
where
    Act<P::Actor>: Clone;

impl<P: ActorPool> Clone for Pool<P>
where
    Act<P::Actor>: Clone,
{
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<P: ActorPool> Deref for Pool<P>
where
    Act<P::Actor>: Clone,
{
    type Target = Arc<P>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<P: ActorPool> DataWrapper<Arc<P>> for Pool<P>
where
    Act<P::Actor>: Clone,
{
    fn into_inner(self) -> Arc<P> {
        self.0
    }
}
