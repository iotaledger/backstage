use std::marker::PhantomData;

pub trait Channel<E: Send> {
    type Sender: Send + Sync + Clone;
    type Receiver: Send;

    fn new() -> (Self::Sender, Self::Receiver);
}

pub struct TokioChannel<E>(PhantomData<E>);

impl<E: Send> Channel<E> for TokioChannel<E> {
    type Sender = tokio::sync::mpsc::UnboundedSender<E>;

    type Receiver = tokio::sync::mpsc::UnboundedReceiver<E>;

    fn new() -> (Self::Sender, Self::Receiver) {
        tokio::sync::mpsc::unbounded_channel()
    }
}
