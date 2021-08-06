// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use crate::prelude::DataWrapper;
use async_trait::async_trait;
use futures::Stream;
use std::{fmt::Debug, marker::PhantomData};
use tokio_stream::wrappers::UnboundedReceiverStream;

/// Defines a channel which becomes a sender and receiver half
#[async_trait]
pub trait Channel<C, E: 'static + Send + Sync> {
    /// The sender half of the channel
    type Sender: 'static + Sender<E> + Send + Sync + Clone;
    /// The receiver half of the channel
    type Receiver: Stream<Item = E> + Unpin + Send + Sync;

    /// Create a sender and receiver of the appropriate types
    async fn new(config: &C) -> anyhow::Result<(Self::Sender, Self::Receiver)>;
}

/// Defines half of a channel which sends events
pub trait Sender<E: 'static + Send + Sync> {
    /// Send an event over the channel
    fn send(&self, event: E) -> anyhow::Result<()>;

    /// Check if the channel is closed
    fn is_closed(&self) -> bool;
}

/// A tokio mpsc channel implementation
pub struct UnboundedTokioChannel<E>(PhantomData<fn(E) -> E>);

#[async_trait]
impl<C: 'static + Send + Sync, E: 'static + Send + Sync> Channel<C, E> for UnboundedTokioChannel<E> {
    type Sender = UnboundedTokioSender<E>;
    type Receiver = UnboundedReceiverStream<E>;

    async fn new(_config: &C) -> anyhow::Result<(Self::Sender, Self::Receiver)> {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        Ok((UnboundedTokioSender(sender), UnboundedReceiverStream::new(receiver)))
    }
}

/// A tokio mpsc sender implementation
pub struct UnboundedTokioSender<E>(pub tokio::sync::mpsc::UnboundedSender<E>);

impl<E: 'static + Send + Sync> DataWrapper<tokio::sync::mpsc::UnboundedSender<E>> for UnboundedTokioSender<E> {
    fn into_inner(self) -> tokio::sync::mpsc::UnboundedSender<E> {
        self.0
    }
}

impl<E: 'static + Send + Sync> Clone for UnboundedTokioSender<E> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<E: 'static + Send + Sync + Debug> Debug for UnboundedTokioSender<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl<E: 'static + Send + Sync> Sender<E> for UnboundedTokioSender<E> {
    fn send(&self, event: E) -> anyhow::Result<()> {
        self.0.send(event).map_err(|e| anyhow::anyhow!(e.to_string()))
    }

    fn is_closed(&self) -> bool {
        self.0.is_closed()
    }
}

impl<T, E: 'static + Send + Sync> Sender<E> for Option<T>
where
    T: Sender<E> + Send + Sync,
{
    fn send(&self, event: E) -> anyhow::Result<()> {
        match self {
            Some(s) => s.send(event),
            None => Ok(()),
        }
    }

    fn is_closed(&self) -> bool {
        match self {
            Some(s) => s.is_closed(),
            None => true,
        }
    }
}

#[async_trait]
impl Channel<(), ()> for () {
    type Sender = ();
    type Receiver = tokio_stream::Empty<()>;

    async fn new(_config: &()) -> anyhow::Result<(Self::Sender, Self::Receiver)> {
        Ok(((), tokio_stream::empty()))
    }
}

impl<E: 'static + Send + Sync> Sender<E> for () {
    fn send(&self, _event: E) -> anyhow::Result<()> {
        Ok(())
    }

    fn is_closed(&self) -> bool {
        true
    }
}
