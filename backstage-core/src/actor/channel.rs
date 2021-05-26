use std::marker::PhantomData;

pub trait Channel<E: Send> {
    type Sender: Send + Sync + Clone;
    type Receiver: Receiver<E> + Send;

    fn new() -> (Self::Sender, Self::Receiver);
}

#[async_trait::async_trait]
pub trait Receiver<E: Send> {
    async fn recv(&mut self) -> Option<E>;
}

pub struct TokioChannel<E>(PhantomData<E>);

impl<E: Send> Channel<E> for TokioChannel<E> {
    type Sender = tokio::sync::mpsc::UnboundedSender<E>;

    type Receiver = TokioReceiver<E>;

    fn new() -> (Self::Sender, Self::Receiver) {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        (sender, TokioReceiver(receiver))
    }
}

pub struct TokioReceiver<E>(tokio::sync::mpsc::UnboundedReceiver<E>);

#[async_trait::async_trait]
impl<E: Send> Receiver<E> for TokioReceiver<E> {
    async fn recv(&mut self) -> Option<E> {
        self.0.recv().await
    }
}
