// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::{ActorError, Channel, Dependencies, Report, Sender, StatusChange};
use crate::{
    actor::{ActorRequest, ServiceStatus, ShutdownStream},
    prelude::{Act, ActorContext},
    runtime::{RegistryAccess, RuntimeScope},
};
use async_trait::async_trait;
use futures::{
    future::{AbortHandle, Abortable},
    FutureExt,
};
use std::{any::Any, borrow::Cow, fmt::Debug, panic::AssertUnwindSafe, pin::Pin};
/// The all-important Actor trait. This defines an Actor and what it do.
#[async_trait]
pub trait Actor: Debug + Send + Sync + Sized {
    type Data: Debug + Send + Sync;
    type Context: ActorContext<Self>;
    /// Used to initialize the actor. Any children spawned here will be initialized
    /// before this actor's run method is called so they are guaranteed to be
    /// ready to use depending on their requirements. Dependencies are not
    /// handled until after this method is called, so they are not guaranteed
    /// to exist yet. If a dependency must exist to complete initialization,
    /// it can be linked with the runtime, but BEWARE that any dependencies which are
    /// spawned by the parents of this actor will not be available if they are
    /// spawned after this actor and linking them will therefore deadlock the thread.
    async fn init(&mut self, cx: &mut Self::Context) -> Result<Self::Data, ActorError>
    where
        Self: 'static + Sized + Send + Sync;

    async fn shutdown(&mut self, cx: &mut Self::Context, data: &mut Self::Data) -> Result<(), ActorError>
    where
        Self: 'static + Sized + Send + Sync,
    {
        log::debug!("{} shutting down!", self.name());
        cx.update_status(ServiceStatus::Stopped).await.ok();
        Ok(())
    }

    /// Get this actor's name
    fn name(&self) -> Cow<'static, str> {
        std::any::type_name::<Self>().into()
    }

    // /// Start with this actor as the root scope
    // async fn start_as_root<Reg: RegistryAccess>(mut self) -> anyhow::Result<()>
    // where
    //    Self: Sized,
    //{
    //    let (abort_handle, abort_registration) = AbortHandle::new_pair();
    //    let (sender, receiver) = <Self::Channel as Channel<Self, Self::Event>>::new(&self).await?;
    //    let (receiver, shutdown_handle) = ShutdownStream::new(receiver);
    //    let mut scope = Reg::instantiate(self.name(), Some(shutdown_handle.clone()),
    // Some(abort_handle.clone())).await;    scope
    //        .add_data(Act::<Self>::new(
    //            scope.scope_id,
    //            sender.clone(),
    //            shutdown_handle.clone(),
    //            abort_handle.clone(),
    //        ))
    //        .await;
    //    let mut actor_rt = ActorContext {
    //        scope,
    //        handle: sender,
    //        receiver,
    //        shutdown_handle,
    //        abort_handle,
    //        supervisor_handle: None,
    //    };
    //    let deps = Self::Dependencies::instantiate(&mut actor_rt.scope)
    //        .await
    //        .map_err(|e| anyhow::anyhow!("Cannot spawn actor {}: {}", std::any::type_name::<Self>(), e))
    //        .unwrap();
    //    let res = AssertUnwindSafe(self.init(&mut actor_rt)).catch_unwind().await;
    //    RuntimeScope::handle_init_res::<_, ()>(res, &mut actor_rt).await?;
    //    let res = Abortable::new(
    //        AssertUnwindSafe(self.run(&mut actor_rt, deps)).catch_unwind(),
    //        abort_registration,
    //    )
    //    .await;
    //    RuntimeScope::handle_run_res::<_, ()>(res, &mut actor_rt, None, self).await
    //}
}

pub trait DependsOn {
    type Dependencies: Dependencies;
}

#[async_trait]
pub trait HandleEvent<E: Send + Debug>: Actor + Sized {
    async fn handle_event(&mut self, cx: &mut Self::Context, event: E, data: &mut Self::Data)
        -> Result<(), ActorError>;
}

pub trait DynEvent<A: Actor>: Debug {
    fn handle<'c, 'a>(
        self: Box<Self>,
        cx: &'c mut A::Context,
        act: &'c mut A,
        data: &'c mut A::Data,
    ) -> Pin<Box<dyn core::future::Future<Output = Result<(), ActorError>> + Send + 'a>>
    where
        Self: 'a,
        'c: 'a;
}

impl<A, E: Send + Debug> DynEvent<A> for E
where
    A: HandleEvent<E>,
{
    fn handle<'c, 'a>(
        self: Box<Self>,
        cx: &'c mut A::Context,
        act: &'c mut A,
        data: &'c mut A::Data,
    ) -> Pin<Box<dyn core::future::Future<Output = Result<(), ActorError>> + Send + 'a>>
    where
        Self: 'a,
        'c: 'a,
    {
        act.handle_event(cx, *self, data)
    }
}

pub type Envelope<A> = Box<dyn DynEvent<A> + Send + Sync>;

pub trait EnvelopeSender<A>: Send + Sync
where
    A: 'static + Actor,
{
    fn send<E: 'static + DynEvent<A> + Send + Sync>(&self, event: E) -> anyhow::Result<()>
    where
        Self: Sized;

    fn is_closed(&self) -> bool;
}
impl<S, A> EnvelopeSender<A> for S
where
    S: Sender<Envelope<A>>,
    A: 'static + Actor,
{
    fn send<E: 'static + DynEvent<A> + Send + Sync>(&self, event: E) -> anyhow::Result<()> {
        self.send(Box::new(event))
    }

    fn is_closed(&self) -> bool {
        self.is_closed()
    }
}
