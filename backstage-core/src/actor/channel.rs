use std::{fmt::Debug, marker::PhantomData};

pub trait Channel<E: Send + Clone> {
    type Sender: Sender<E> + Send + Sync + Clone;
    type Receiver: Receiver<E> + Send;

    fn new() -> (Self::Sender, Self::Receiver);
}

#[async_trait::async_trait]
pub trait Sender<E: Send + Clone>: Clone {
    async fn send(&mut self, event: E) -> anyhow::Result<()>;
}

#[async_trait::async_trait]
pub trait Receiver<E: Send> {
    async fn recv(&mut self) -> Option<E>;
}

pub struct TokioChannel<E>(PhantomData<E>);

impl<E: 'static + Send + Clone + Debug + Sync> Channel<E> for TokioChannel<E> {
    type Sender = TokioSender<E>;

    type Receiver = TokioReceiver<E>;

    fn new() -> (Self::Sender, Self::Receiver) {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        (TokioSender(sender), TokioReceiver(receiver))
    }
}

#[derive(Clone)]
pub struct TokioSender<E>(tokio::sync::mpsc::UnboundedSender<E>);

#[async_trait::async_trait]
impl<E: 'static + Send + Clone + Debug + Sync> Sender<E> for TokioSender<E> {
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
