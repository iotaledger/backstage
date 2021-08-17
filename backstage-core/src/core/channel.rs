use super::*;

use async_trait::async_trait;
use core::pin::Pin;
use futures::{
    future::Aborted,
    task::{AtomicWaker, Context, Poll},
};
use pin_project_lite::pin_project;
use prometheus::core::Collector;
use std::sync::atomic::{AtomicBool, Ordering};
pub use tokio::net::TcpListener;
use tokio::sync::mpsc::{error::TrySendError, Receiver, Sender, UnboundedReceiver, UnboundedSender};
use tokio_stream::wrappers::IntervalStream;
pub use tokio_stream::wrappers::TcpListenerStream;
// Inner type storing the waker to awaken and a bool indicating that it
// should be cancelled.
#[derive(Debug)]
struct AbortInner {
    waker: AtomicWaker,
    aborted: AtomicBool,
}

#[derive(Debug, Clone)]
pub struct AbortHandle {
    inner: std::sync::Arc<AbortInner>,
}

impl AbortHandle {
    pub fn abort(&self) {
        self.inner.aborted.store(true, Ordering::Relaxed);
        self.inner.waker.wake();
    }
    pub fn new_pair() -> (Self, AbortRegistration) {
        let inner = std::sync::Arc::new(AbortInner {
            waker: AtomicWaker::new(),
            aborted: AtomicBool::new(false),
        });

        (AbortHandle { inner: inner.clone() }, AbortRegistration { inner })
    }
}

#[derive(Debug, Clone)]
pub struct AbortRegistration {
    inner: std::sync::Arc<AbortInner>,
}
pin_project! {
    /// A future/stream which can be remotely short-circuited using an `AbortHandle`.
    #[derive(Debug, Clone)]
    #[must_use = "futures/streams do nothing unless you poll them"]
    pub struct Abortable<T> {
        #[pin]
        task: T,
        inner: std::sync::Arc<AbortInner>,
    }
}
impl<T> std::convert::AsMut<T> for Abortable<T> {
    fn as_mut(&mut self) -> &mut T {
        &mut self.task
    }
}
/// NOTE: forked from rust futures
impl<T> Abortable<T> {
    /// Create new abortable future/stream
    pub fn new(task: T, reg: AbortRegistration) -> Self {
        Self { task, inner: reg.inner }
    }
    pub fn is_aborted(&self) -> bool {
        self.inner.aborted.load(Ordering::Relaxed)
    }
    fn try_poll<I>(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        poll: impl Fn(Pin<&mut T>, &mut Context<'_>) -> Poll<I>,
    ) -> Poll<Result<I, Aborted>> {
        // Check if the task has been aborted
        if self.is_aborted() {
            return Poll::Ready(Err(Aborted));
        }

        // attempt to complete the task
        if let Poll::Ready(x) = poll(self.as_mut().project().task, cx) {
            return Poll::Ready(Ok(x));
        }

        // Register to receive a wakeup if the task is aborted in the future
        self.inner.waker.register(cx.waker());

        // Check to see if the task was aborted between the first check and
        // registration.
        // Checking with `is_aborted` which uses `Relaxed` is sufficient because
        // `register` introduces an `AcqRel` barrier.
        if self.is_aborted() {
            return Poll::Ready(Err(Aborted));
        }

        Poll::Pending
    }
}

impl<Fut> futures::future::Future for Abortable<Fut>
where
    Fut: futures::future::Future,
{
    type Output = Result<Fut::Output, Aborted>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.try_poll(cx, |fut, cx| fut.poll(cx))
    }
}

impl<St> futures::stream::Stream for Abortable<St>
where
    St: futures::stream::Stream,
{
    type Item = St::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.try_poll(cx, |stream, cx| stream.poll_next(cx))
            .map(Result::ok)
            .map(Option::flatten)
    }
}

/// Defines a channel which becomes a sender and receiver half
#[async_trait]
pub trait Channel: Send + Sized {
    type Event: Send;
    /// The sender half of the channel
    type Handle: Send + Clone + super::Shutdown;
    /// The receiver half of the channel
    type Inbox: Send + Sync;
    /// Metric Collector
    type Metric: Collector + Clone = prometheus::IntGauge;
    /// Create a sender and receiver of the appropriate types
    fn channel<T: Actor>(self, scope_id: ScopeId) -> (Self::Handle, Self::Inbox, AbortRegistration, Option<Self::Metric>);
    /// Get this channel's name
    fn type_name() -> std::borrow::Cow<'static, str> {
        std::any::type_name::<Self>().into()
    }
}

#[async_trait::async_trait]
pub trait ChannelBuilder<C: Channel> {
    async fn build_channel(&mut self) -> Result<C, Reason>
    where
        Self: Actor<Channel = C>;
}
use dyn_clone::DynClone;

#[async_trait::async_trait]
pub trait Route<M>: Send + Sync + DynClone {
    async fn try_send_msg(&self, message: M) -> anyhow::Result<Option<M>>;
    /// if the Route<M> is behind lock, drop the lock before invoking this method.
    async fn send_msg(self: Box<Self>, message: M) -> anyhow::Result<()>;
}

dyn_clone::clone_trait_object!(<M> Route<M>);

/// A tokio unbounded mpsc channel implementation
pub struct UnboundedChannel<E> {
    abort_handle: AbortHandle,
    abort_registration: AbortRegistration,
    tx: UnboundedSender<E>,
    rx: UnboundedReceiver<E>,
}

#[derive(Debug)]
pub struct UnboundedInbox<T> {
    metric: prometheus::IntGauge,
    inner: UnboundedReceiver<T>,
}

impl<T> UnboundedInbox<T> {
    /// Create a new `UnboundedReceiver`.
    pub fn new(recv: UnboundedReceiver<T>, gauge: prometheus::IntGauge) -> Self {
        Self {
            inner: recv,
            metric: gauge,
        }
    }
    /// Closes the receiving half of a channel without dropping it.
    pub fn close(&mut self) {
        self.inner.close()
    }
}

impl<T> tokio_stream::Stream for UnboundedInbox<T> {
    type Item = T;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let r = self.inner.poll_recv(cx);
        if let &Poll::Ready(Some(_)) = &r {
            self.metric.dec();
        }
        r
    }
}

#[derive(Debug)]
pub struct UnboundedHandle<T> {
    scope_id: ScopeId,
    abort_handle: AbortHandle,
    metric: prometheus::IntGauge,
    inner: UnboundedSender<T>,
}

impl<T> Clone for UnboundedHandle<T> {
    fn clone(&self) -> Self {
        Self {
            scope_id: self.scope_id,
            abort_handle: self.abort_handle.clone(),
            metric: self.metric.clone(),
            inner: self.inner.clone(),
        }
    }
}

impl<T> UnboundedHandle<T> {
    pub fn new(sender: UnboundedSender<T>, gauge: prometheus::IntGauge, abort_handle: AbortHandle, scope_id: ScopeId) -> Self {
        Self {
            scope_id,
            abort_handle,
            metric: gauge,
            inner: sender,
        }
    }
    pub fn send(&self, message: T) -> Result<(), tokio::sync::mpsc::error::SendError<T>> {
        let r = self.inner.send(message);
        if r.is_ok() {
            self.metric.inc()
        }
        r
    }
    pub async fn closed(&self) {
        self.inner.closed().await
    }
    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }
    pub fn same_channel(&self, other: &Self) -> bool {
        self.inner.same_channel(&other.inner)
    }
}

#[async_trait::async_trait]
impl<A, T: EolEvent<A> + ReportEvent<A, Service>> Supervise<A> for UnboundedHandle<T>
where
    A: Actor,
{
    async fn eol(self, scope_id: super::ScopeId, service: Service, actor: A, r: super::ActorResult) -> Option<()> {
        self.send(T::eol_event(scope_id, service, actor, r)).ok()
    }
}

#[async_trait::async_trait]
impl<A, T: ReportEvent<A, D>, D: Send + 'static> Report<A, D> for UnboundedHandle<T>
where
    A: Actor,
{
    async fn report(&self, scope_id: ScopeId, data: D) -> Option<()> {
        self.send(T::report_event(scope_id, data)).ok()
    }
}

#[async_trait::async_trait]
impl<E: ShutdownEvent + 'static, T> ChannelBuilder<UnboundedChannel<E>> for T
where
    T: Actor<Channel = UnboundedChannel<E>>,
{
    async fn build_channel(&mut self) -> Result<UnboundedChannel<E>, Reason> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<E>();
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        Ok(UnboundedChannel {
            abort_handle,
            abort_registration,
            tx,
            rx,
        })
    }
}

impl<E: ShutdownEvent + 'static> Channel for UnboundedChannel<E> {
    type Event = E;
    type Handle = UnboundedHandle<E>;
    type Inbox = UnboundedInbox<E>;
    fn channel<T: Actor>(self, scope_id: ScopeId) -> (Self::Handle, Self::Inbox, AbortRegistration, Option<Self::Metric>) {
        let metric_fq_name = format!("ScopeId:{}", scope_id);
        let metric_helper_name = format!("ScopeId: {}, Actor: {}, Channel: {}", scope_id, T::type_name(), Self::type_name());
        let gauge = prometheus::core::GenericGauge::new(metric_fq_name, metric_helper_name).expect("channel gauge can be created");
        let sender = self.tx;
        let recv = self.rx;
        let abort_handle = self.abort_handle;
        let abort_registration = self.abort_registration;
        let unbounded_handle = UnboundedHandle::new(sender, gauge.clone(), abort_handle, scope_id);
        let unbounded_inbox = UnboundedInbox::new(recv, gauge.clone());
        (unbounded_handle, unbounded_inbox, abort_registration, Some(gauge))
    }
}

#[async_trait::async_trait]
impl<E: 'static + ShutdownEvent> super::Shutdown for UnboundedHandle<E> {
    async fn shutdown(&self) {
        self.abort_handle.abort();
        self.send(E::shutdown_event()).ok();
    }
    fn scope_id(&self) -> super::ScopeId {
        self.scope_id
    }
}

#[async_trait::async_trait]
impl<M: 'static + Send, E: Send> Route<M> for UnboundedHandle<E>
where
    E: std::convert::TryFrom<M>,
{
    async fn try_send_msg(&self, message: M) -> anyhow::Result<Option<M>> {
        if let Ok(event) = E::try_from(message) {
            if let Err(error) = self.send(event) {
                anyhow::bail!("SendError: {}", error)
            };
        } else {
            anyhow::bail!("Unable to convert the provided message into channel event")
        };
        Ok(None)
    }
    async fn send_msg(self: Box<Self>, message: M) -> anyhow::Result<()> {
        if let Ok(event) = E::try_from(message) {
            if let Err(error) = self.send(event) {
                anyhow::bail!("{}", error)
            };
            Ok(())
        } else {
            anyhow::bail!("Unable to convert the provided message into channel event")
        }
    }
}

/// Abortable tokio unbounded mpsc channel implementation
pub struct AbortableUnboundedChannel<E> {
    abort_handle: AbortHandle,
    abort_registration: AbortRegistration,
    tx: UnboundedSender<E>,
    rx: UnboundedReceiver<E>,
}

#[async_trait::async_trait]
impl<E: Send + 'static, T> ChannelBuilder<AbortableUnboundedChannel<E>> for T
where
    T: Actor<Channel = AbortableUnboundedChannel<E>>,
{
    async fn build_channel(&mut self) -> Result<AbortableUnboundedChannel<E>, Reason> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<E>();
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        Ok(AbortableUnboundedChannel {
            abort_handle,
            abort_registration,
            tx,
            rx,
        })
    }
}
#[derive(Debug)]
pub struct AbortableUnboundedInbox<T> {
    inner: Abortable<UnboundedInbox<T>>,
}
impl<T> AbortableUnboundedInbox<T> {
    /// Create a new `AbortableUnboundedReceiver`.
    pub fn new(inbox: UnboundedInbox<T>, abort_registration: AbortRegistration) -> Self {
        let abortable = Abortable::new(inbox, abort_registration);
        Self { inner: abortable }
    }
    /// Closes the receiving half of a channel without dropping it.
    pub fn close(&mut self) {
        self.inner.as_mut().close()
    }
}

#[derive(Debug)]
pub struct AbortableUnboundedHandle<T> {
    inner: UnboundedHandle<T>,
}

impl<T> Clone for AbortableUnboundedHandle<T> {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}

impl<T> AbortableUnboundedHandle<T> {
    pub fn new(sender: UnboundedHandle<T>) -> Self {
        Self { inner: sender }
    }
    pub fn send(&self, message: T) -> Result<(), tokio::sync::mpsc::error::SendError<T>> {
        self.inner.send(message)
    }
    pub async fn closed(&self) {
        self.inner.closed().await
    }
    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }
    pub fn same_channel(&self, other: &Self) -> bool {
        self.inner.same_channel(&other.inner)
    }
}

impl<E: Send + 'static> Channel for AbortableUnboundedChannel<E> {
    type Event = E;
    type Handle = AbortableUnboundedHandle<E>;
    type Inbox = AbortableUnboundedInbox<E>;
    fn channel<T: Actor>(self, scope_id: ScopeId) -> (Self::Handle, Self::Inbox, AbortRegistration, Option<prometheus::IntGauge>) {
        let metric_fq_name = format!("ScopeId:{}", scope_id);
        let metric_helper_name = format!("ScopeId: {}, Actor: {}, Channel: {}", scope_id, T::type_name(), Self::type_name());
        let gauge = prometheus::core::GenericGauge::new(metric_fq_name, metric_helper_name).expect("channel gauge can be created");
        let sender = self.tx;
        let recv = self.rx;
        let abort_handle = self.abort_handle;
        let abort_registration = self.abort_registration;
        let unbounded_handle = UnboundedHandle::new(sender, gauge.clone(), abort_handle, scope_id);
        let unbounded_inbox = UnboundedInbox::new(recv, gauge.clone());
        let abortable_unbounded_handle = AbortableUnboundedHandle::new(unbounded_handle);
        let abortable_unbounded_inbox = AbortableUnboundedInbox::new(unbounded_inbox, abort_registration.clone());
        (
            abortable_unbounded_handle,
            abortable_unbounded_inbox,
            abort_registration,
            Some(gauge),
        )
    }
}
#[async_trait::async_trait]
impl<E: Send + 'static> super::Shutdown for AbortableUnboundedHandle<E> {
    async fn shutdown(&self) {
        self.inner.abort_handle.abort();
    }
    fn scope_id(&self) -> ScopeId {
        self.inner.scope_id
    }
}

#[async_trait::async_trait]
impl<M: 'static + Send, E: Send> Route<M> for AbortableUnboundedHandle<E>
where
    E: std::convert::TryFrom<M>,
{
    async fn try_send_msg(&self, message: M) -> anyhow::Result<Option<M>> {
        self.inner.try_send_msg(message).await
    }
    async fn send_msg(self: Box<Self>, message: M) -> anyhow::Result<()> {
        if let Ok(event) = E::try_from(message) {
            if let Err(error) = self.send(event) {
                anyhow::bail!("{}", error)
            };
            Ok(())
        } else {
            anyhow::bail!("Unable to convert the provided message into channel event")
        }
    }
}

////////////// BoundedChannel
/// A tokio bounded mpsc channel implementation
pub struct BoundedChannel<E, const C: usize> {
    abort_handle: AbortHandle,
    abort_registration: AbortRegistration,
    tx: Sender<E>,
    rx: Receiver<E>,
}

#[derive(Debug)]
pub struct BoundedInbox<T> {
    metric: prometheus::IntGauge,
    inner: Receiver<T>,
}

impl<T> BoundedInbox<T> {
    /// Create a new `BoundedInbox`.
    pub fn new(recv: Receiver<T>, gauge: prometheus::IntGauge) -> Self {
        Self {
            inner: recv,
            metric: gauge,
        }
    }
    /// Closes the receiving half of a channel without dropping it.
    pub fn close(&mut self) {
        self.inner.close()
    }
}

impl<T> tokio_stream::Stream for BoundedInbox<T> {
    type Item = T;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let r = self.inner.poll_recv(cx);
        if let &Poll::Ready(Some(_)) = &r {
            self.metric.dec();
        }
        r
    }
}

#[derive(Debug)]
pub struct BoundedHandle<T> {
    scope_id: ScopeId,
    abort_handle: AbortHandle,
    metric: prometheus::IntGauge,
    inner: Sender<T>,
}

impl<T> Clone for BoundedHandle<T> {
    fn clone(&self) -> Self {
        Self {
            scope_id: self.scope_id.clone(),
            abort_handle: self.abort_handle.clone(),
            metric: self.metric.clone(),
            inner: self.inner.clone(),
        }
    }
}

impl<T> BoundedHandle<T> {
    pub fn new(sender: Sender<T>, gauge: prometheus::IntGauge, abort_handle: AbortHandle, scope_id: ScopeId) -> Self {
        Self {
            scope_id,
            abort_handle,
            metric: gauge,
            inner: sender,
        }
    }
    pub async fn send(&self, message: T) -> Result<(), tokio::sync::mpsc::error::SendError<T>> {
        let r = self.inner.send(message).await;
        if r.is_ok() {
            self.metric.inc()
        }
        r
    }
    pub async fn closed(&self) {
        self.inner.closed().await
    }
    pub fn try_send(&self, message: T) -> Result<(), tokio::sync::mpsc::error::TrySendError<T>> {
        let r = self.inner.try_send(message);
        if r.is_ok() {
            self.metric.inc();
        }
        r
    }
    #[cfg(feature = "time")]
    #[cfg_attr(docsrs, doc(cfg(feature = "time")))]
    pub async fn send_timeout(&self, value: T, timeout: Duration) -> Result<(), tokio::sync::mpsc::error::SendTimeoutError<T>> {
        let r = self.inner.send_timeout(message).await;
        if r.is_ok() {
            self.metric.inc();
        }
        r
    }
    #[cfg(feature = "sync")]
    pub fn blocking_send(&self, value: T) -> Result<(), tokio::sync::mpsc::error::SendError<T>> {
        let r = self.inner.blocking_send(message);
        if r.is_ok() {
            self.metric.inc();
        }
        r
    }
    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }

    pub fn same_channel(&self, other: &Self) -> bool {
        self.inner.same_channel(&other.inner)
    }
    // todo support permits
}

#[async_trait::async_trait]
impl<E: ShutdownEvent + 'static, T, const C: usize> ChannelBuilder<BoundedChannel<E, C>> for T
where
    T: Actor<Channel = UnboundedChannel<E>>,
{
    async fn build_channel(&mut self) -> Result<BoundedChannel<E, C>, Reason> {
        let (tx, rx) = tokio::sync::mpsc::channel::<E>(C);
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        Ok(BoundedChannel {
            abort_handle,
            abort_registration,
            tx,
            rx,
        })
    }
}

impl<E: ShutdownEvent + 'static, const C: usize> Channel for BoundedChannel<E, C> {
    type Event = E;
    type Handle = BoundedHandle<E>;
    type Inbox = BoundedInbox<E>;
    fn channel<T: Actor>(self, scope_id: ScopeId) -> (Self::Handle, Self::Inbox, AbortRegistration, Option<Self::Metric>) {
        let metric_fq_name = format!("ScopeId:{}", scope_id);
        let metric_helper_name = format!("ScopeId: {}, Actor: {}, Channel: {}", scope_id, T::type_name(), Self::type_name());
        let gauge = prometheus::core::GenericGauge::new(metric_fq_name, metric_helper_name).expect("channel gauge can be created");
        let sender = self.tx;
        let recv = self.rx;
        let abort_handle = self.abort_handle;
        let abort_registration = self.abort_registration;
        let unbounded_handle = BoundedHandle::new(sender, gauge.clone(), abort_handle, scope_id);
        let unbounded_inbox = BoundedInbox::new(recv, gauge.clone());
        (unbounded_handle, unbounded_inbox, abort_registration, Some(gauge))
    }
}

#[async_trait::async_trait]
impl<E: 'static + ShutdownEvent> super::Shutdown for BoundedHandle<E> {
    async fn shutdown(&self) {
        self.abort_handle.abort();
        self.send(E::shutdown_event()).await;
    }
    fn scope_id(&self) -> ScopeId {
        self.scope_id
    }
}

#[async_trait::async_trait]
impl<M: 'static + Send, E: Send> Route<M> for BoundedHandle<E>
where
    E: std::convert::TryFrom<M>,
    E::Error: Send,
{
    async fn try_send_msg(&self, message: M) -> anyhow::Result<Option<M>> {
        // try to reserve permit
        match self.inner.try_reserve() {
            Ok(permit) => {
                // it's safe to send the message without causing deadlock
                if let Ok(event) = E::try_from(message) {
                    self.metric.inc();
                    permit.send(event);
                    Ok(None)
                } else {
                    anyhow::bail!("Unabled to convert the provided message into event type");
                }
            }
            Err(err) => {
                if let TrySendError::Full(()) = err {
                    Ok(Some(message))
                } else {
                    anyhow::bail!("Closed channel")
                }
            }
        }
    }
    async fn send_msg(self: Box<Self>, message: M) -> anyhow::Result<()> {
        // it's safe to send the message without causing deadlock
        if let Ok(event) = E::try_from(message) {
            self.send(event).await.map_err(|e| anyhow::Error::msg(format!("{}", e)))
        } else {
            anyhow::bail!("Unabled to convert the provided message into event type")
        }
    }
}

////////////// bounded channel end
/// Abortable tokio bounded mpsc channel implementation
pub struct AbortableBoundedChannel<E, const C: usize> {
    abort_handle: AbortHandle,
    abort_registration: AbortRegistration,
    tx: Sender<E>,
    rx: Receiver<E>,
}
#[derive(Debug)]
pub struct AbortableBoundedInbox<T> {
    inner: Abortable<BoundedInbox<T>>,
}

#[async_trait::async_trait]
impl<E: Send + 'static, T, const C: usize> ChannelBuilder<AbortableBoundedChannel<E, C>> for T
where
    T: Actor<Channel = AbortableUnboundedChannel<E>>,
{
    async fn build_channel(&mut self) -> Result<AbortableBoundedChannel<E, C>, Reason> {
        let (tx, rx) = tokio::sync::mpsc::channel::<E>(C);
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        Ok(AbortableBoundedChannel {
            abort_handle,
            abort_registration,
            tx,
            rx,
        })
    }
}

impl<T> AbortableBoundedInbox<T> {
    /// Create a new `AbortableBoundedReceiver`.
    pub fn new(inbox: BoundedInbox<T>, abort_registration: AbortRegistration) -> Self {
        let abortable = Abortable::new(inbox, abort_registration);
        Self { inner: abortable }
    }
    /// Closes the receiving half of a channel without dropping it.
    pub fn close(&mut self) {
        self.inner.as_mut().close()
    }
}

#[derive(Debug)]
pub struct AbortableBoundedHandle<T> {
    inner: BoundedHandle<T>,
}

impl<T> Clone for AbortableBoundedHandle<T> {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}

impl<T> AbortableBoundedHandle<T> {
    pub fn new(sender: BoundedHandle<T>) -> Self {
        Self { inner: sender }
    }
    pub async fn send(&self, message: T) -> Result<(), tokio::sync::mpsc::error::SendError<T>> {
        self.inner.send(message).await
    }
    pub async fn closed(&self) {
        self.inner.closed().await
    }
    pub fn try_send(&self, message: T) -> Result<(), tokio::sync::mpsc::error::TrySendError<T>> {
        self.inner.try_send(message)
    }
    #[cfg(feature = "time")]
    #[cfg_attr(docsrs, doc(cfg(feature = "time")))]
    pub async fn send_timeout(&self, value: T, timeout: Duration) -> Result<(), tokio::sync::mpsc::error::SendTimeoutError<T>> {
        self.inner.send_timeout(message).await
    }
    #[cfg(feature = "sync")]
    pub fn blocking_send(&self, value: T) -> Result<(), tokio::sync::mpsc::error::SendError<T>> {
        self.inner.blocking_send(message)
    }
    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }

    pub fn same_channel(&self, other: &Self) -> bool {
        self.inner.same_channel(&other.inner)
    }
}

impl<E: Send + 'static, const C: usize> Channel for AbortableBoundedChannel<E, C> {
    type Event = E;
    type Handle = AbortableBoundedHandle<E>;
    type Inbox = AbortableBoundedInbox<E>;
    fn channel<T: Actor>(self, scope_id: ScopeId) -> (Self::Handle, Self::Inbox, AbortRegistration, Option<prometheus::IntGauge>) {
        let metric_fq_name = format!("ScopeId:{}", scope_id);
        let metric_helper_name = format!("ScopeId: {}, Actor: {}, Channel: {}", scope_id, T::type_name(), Self::type_name());
        let gauge = prometheus::core::GenericGauge::new(metric_fq_name, metric_helper_name).expect("channel gauge can be created");
        let sender = self.tx;
        let recv = self.rx;
        let abort_handle = self.abort_handle;
        let abort_registration = self.abort_registration;
        let unbounded_handle = BoundedHandle::new(sender, gauge.clone(), abort_handle, scope_id);
        let unbounded_inbox = BoundedInbox::new(recv, gauge.clone());
        let abortable_unbounded_handle = AbortableBoundedHandle::new(unbounded_handle);
        let abortable_unbounded_inbox = AbortableBoundedInbox::new(unbounded_inbox, abort_registration.clone());
        (
            abortable_unbounded_handle,
            abortable_unbounded_inbox,
            abort_registration,
            Some(gauge),
        )
    }
}

#[async_trait::async_trait]
impl<E: Send + 'static> super::Shutdown for AbortableBoundedHandle<E> {
    async fn shutdown(&self) {
        self.inner.abort_handle.abort();
    }
    fn scope_id(&self) -> ScopeId {
        self.inner.scope_id
    }
}

#[async_trait::async_trait]
impl<M: 'static + Send, E: Send> Route<M> for AbortableBoundedHandle<E>
where
    E: std::convert::TryFrom<M>,
    E::Error: Send,
{
    async fn try_send_msg(&self, message: M) -> anyhow::Result<Option<M>> {
        self.inner.try_send_msg(message).await
    }
    async fn send_msg(self: Box<Self>, message: M) -> anyhow::Result<()> {
        // it's safe to send the message without causing deadlock
        if let Ok(event) = E::try_from(message) {
            self.send(event).await.map_err(|e| anyhow::Error::msg(format!("{}", e)))
        } else {
            anyhow::bail!("Unabled to convert the provided message into event type")
        }
    }
}
#[derive(Clone)]
pub struct TcpListenerHandle(AbortHandle, ScopeId);
#[async_trait::async_trait]
impl super::Shutdown for TcpListenerHandle {
    async fn shutdown(&self) {
        self.0.abort();
    }
    fn scope_id(&self) -> ScopeId {
        self.1
    }
}

impl Channel for TcpListenerStream {
    type Event = ();
    type Handle = TcpListenerHandle;
    type Inbox = Abortable<TcpListenerStream>;
    fn channel<T: Actor>(self, scope_id: ScopeId) -> (Self::Handle, Self::Inbox, AbortRegistration, Option<prometheus::IntGauge>) {
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        let abortable_inbox = Abortable::new(self, abort_registration.clone());
        let abortable_handle = TcpListenerHandle(abort_handle, scope_id);
        (abortable_handle, abortable_inbox, abort_registration, None)
    }
}

/////////////////////

/// A tokio IntervalStream channel implementation, which emit Instants
pub struct IntervalChannel<const I: u64>;

#[derive(Clone)]
pub struct IntervalHandle {
    scope_id: ScopeId,
    abort_handle: AbortHandle,
}

impl IntervalHandle {
    pub fn new(abort_handle: AbortHandle, scope_id: ScopeId) -> Self {
        Self { scope_id, abort_handle }
    }
}
#[async_trait::async_trait]
impl<T, const I: u64> ChannelBuilder<IntervalChannel<I>> for T
where
    T: Actor<Channel = IntervalChannel<I>>,
{
    async fn build_channel(&mut self) -> Result<IntervalChannel<I>, Reason> {
        Ok(IntervalChannel::<I>)
    }
}

impl<const I: u64> Channel for IntervalChannel<I> {
    type Event = std::time::Instant;
    type Handle = IntervalHandle;
    type Inbox = Abortable<IntervalStream>;
    fn channel<T: Actor>(self, scope_id: ScopeId) -> (Self::Handle, Self::Inbox, AbortRegistration, Option<prometheus::IntGauge>) {
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        let interval = tokio::time::interval(std::time::Duration::from_millis(I));
        let interval_handle = IntervalHandle::new(abort_handle, scope_id);
        let abortable_inbox = Abortable::new(IntervalStream::new(interval), abort_registration.clone());
        (interval_handle, abortable_inbox, abort_registration, None)
    }
}

#[async_trait::async_trait]
impl super::Shutdown for IntervalHandle {
    async fn shutdown(&self) {
        self.abort_handle.abort();
    }
    fn scope_id(&self) -> ScopeId {
        self.scope_id
    }
}
