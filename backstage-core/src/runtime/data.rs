use crate::actor::{Actor, ActorPool, Channel, EventDriven, IdPool, Sender, System};
use lru::LruCache;
use std::{
    collections::HashMap,
    hash::Hash,
    ops::{Deref, DerefMut},
    sync::Arc,
};
use tokio::sync::RwLock;

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
pub struct Act<A: EventDriven>(pub <A::Channel as Channel<A, A::Event>>::Sender);

impl<A: EventDriven> Deref for Act<A> {
    type Target = <A::Channel as Channel<A, A::Event>>::Sender;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<A: EventDriven> DerefMut for Act<A> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<A: EventDriven> Clone for Act<A>
where
    <A::Channel as Channel<A, A::Event>>::Sender: Clone,
{
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<A: EventDriven> DataWrapper<<A::Channel as Channel<A, A::Event>>::Sender> for Act<A> {
    fn into_inner(self) -> <A::Channel as Channel<A, A::Event>>::Sender {
        self.0
    }
}

#[async_trait::async_trait]
impl<A: EventDriven> Sender<A::Event> for Act<A> {
    async fn send(&mut self, event: A::Event) -> anyhow::Result<()> {
        self.0.send(event).await
    }

    fn is_closed(&self) -> bool {
        self.0.is_closed()
    }
}

/// A pool of actors which can be used as a dependency
#[derive(Default)]
pub struct Pool<P: ActorPool>(pub Arc<RwLock<P>>);

impl<P: ActorPool> Clone for Pool<P> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<P: ActorPool> Deref for Pool<P> {
    type Target = Arc<RwLock<P>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<P: ActorPool> DataWrapper<Arc<RwLock<P>>> for Pool<P> {
    fn into_inner(self) -> Arc<RwLock<P>> {
        self.0
    }
}
