use std::{fmt::Debug, marker::PhantomData};

use futures::Stream;

pub trait Channel<E: Send + Sync> {
    type Sender: Sender<E> + Send + Sync + Clone;
    type Receiver: Receiver<E> + Stream<Item = E> + Unpin + Send;

    fn new() -> (Self::Sender, Self::Receiver);
}

#[async_trait::async_trait]
pub trait Sender<E: Send + Sync> {
    async fn send(&mut self, event: E) -> anyhow::Result<()>;
}

#[async_trait::async_trait]
pub trait Receiver<E: Send> {
    async fn recv(&mut self) -> Option<E>;
}

pub struct TokioChannel<E>(PhantomData<E>);

impl<E: 'static + Send + Debug + Sync> Channel<E> for TokioChannel<E> {
    type Sender = TokioSender<E>;

    type Receiver = TokioReceiver<E>;

    fn new() -> (Self::Sender, Self::Receiver) {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        (TokioSender(sender), TokioReceiver(receiver))
    }
}

#[derive(Debug)]
pub struct TokioSender<E>(tokio::sync::mpsc::UnboundedSender<E>);

impl<E: 'static + Send + Debug + Sync> Clone for TokioSender<E> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

#[async_trait::async_trait]
impl<E: 'static + Send + Debug + Sync> Sender<E> for TokioSender<E> {
    async fn send(&mut self, event: E) -> anyhow::Result<()> {
        self.0.send(event).map_err(|e| anyhow::anyhow!(e))
    }
}

pub struct TokioReceiver<E>(tokio::sync::mpsc::UnboundedReceiver<E>);

#[async_trait::async_trait]
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
