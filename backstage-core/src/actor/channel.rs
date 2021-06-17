use async_trait::async_trait;
use futures::Stream;
use std::{fmt::Debug, marker::PhantomData};

/// Defines a channel which becomes a sender and receiver half
pub trait Channel<E: 'static + Send + Sync> {
    /// The sender half of the channel
    type Sender: 'static + Sender<E> + Send + Sync + Clone;
    /// The receiver half of the channel
    type Receiver: Receiver<E> + Stream<Item = E> + Unpin + Send;

    /// Create a sender and receiver of the appropriate types
    fn new() -> (Self::Sender, Self::Receiver);
}

/// Defines half of a channel which sends events
#[async_trait]
pub trait Sender<E: 'static + Send + Sync> {
    /// Send an event over the channel
    async fn send(&mut self, event: E) -> anyhow::Result<()>;

    /// Check if the channel is closed
    fn is_closed(&self) -> bool;
}

/// Defines half of a channel which receives events
#[async_trait]
pub trait Receiver<E: Send> {
    /// Receive an event from the channel
    async fn recv(&mut self) -> Option<E>;
}

/// A tokio mpsc channel implementation
pub struct TokioChannel<E>(PhantomData<E>);

impl<E: 'static + Send + Debug + Sync> Channel<E> for TokioChannel<E> {
    type Sender = TokioSender<E>;

    type Receiver = TokioReceiver<E>;

    fn new() -> (Self::Sender, Self::Receiver) {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        (TokioSender(sender), TokioReceiver(receiver))
    }
}

/// A tokio mpsc sender implementation
#[derive(Debug)]
pub struct TokioSender<E>(tokio::sync::mpsc::UnboundedSender<E>);

impl<E: 'static + Send + Debug + Sync> Clone for TokioSender<E> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

#[async_trait]
impl<E: 'static + Send + Debug + Sync> Sender<E> for TokioSender<E> {
    async fn send(&mut self, event: E) -> anyhow::Result<()> {
        self.0.send(event).map_err(|e| anyhow::anyhow!(e))
    }

    fn is_closed(&self) -> bool {
        self.0.is_closed()
    }
}

/// A tokio mpsc receiver implementation
pub struct TokioReceiver<E>(tokio::sync::mpsc::UnboundedReceiver<E>);

#[async_trait]
impl<E: Send> Receiver<E> for TokioReceiver<E> {
    async fn recv(&mut self) -> Option<E> {
        self.0.recv().await
    }
}

impl<E: Send> Stream for TokioReceiver<E> {
    type Item = E;

    fn poll_next(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        self.0.poll_recv(cx)
    }
}

#[async_trait]
impl<T, E: 'static + Send + Debug + Sync> Sender<E> for Option<T>
where
    T: Sender<E> + Send + Sync,
{
    async fn send(&mut self, event: E) -> anyhow::Result<()> {
        match self {
            Some(s) => s.send(event).await,
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
impl<T, E: Send> Receiver<E> for Option<T>
where
    T: Receiver<E> + Send + Sync,
{
    async fn recv(&mut self) -> Option<E> {
        match self {
            Some(r) => r.recv().await,
            None => None,
        }
    }
}

#[async_trait]
impl<E: 'static + Send + Debug + Sync> Sender<E> for () {
    async fn send(&mut self, _event: E) -> anyhow::Result<()> {
        Ok(())
    }

    fn is_closed(&self) -> bool {
        true
    }
}

#[async_trait]
impl<E: Send> Receiver<E> for () {
    async fn recv(&mut self) -> Option<E> {
        None
    }
}
