// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::{ActorError, Channel, Dependencies, Sender};
use crate::{
    actor::ShutdownStream,
    prelude::{Act, ActorContext},
    runtime::{RegistryAccess, RuntimeScope},
};
use async_trait::async_trait;
use futures::{
    future::{AbortHandle, Abortable},
    FutureExt,
};
use std::{any::Any, borrow::Cow, panic::AssertUnwindSafe, pin::Pin};
/// The all-important Actor trait. This defines an Actor and what it do.
#[async_trait]
pub trait Actor {
    /// Used to initialize the actor. Any children spawned here will be initialized
    /// before this actor's run method is called so they are guaranteed to be
    /// ready to use depending on their requirements. Dependencies are not
    /// handled until after this method is called, so they are not guaranteed
    /// to exist yet. If a dependency must exist to complete initialization,
    /// it can be linked with the runtime, but BEWARE that any dependencies which are
    /// spawned by the parents of this actor will not be available if they are
    /// spawned after this actor and linking them will therefore deadlock the thread.
    async fn init<H: Send>(&mut self, _rt: &mut ActorContext<Self, H>) -> Result<(), ActorError>
    where
        Self: 'static + Sized + Send + Sync,
    {
        Ok(())
    }

    async fn shutdown<H: Send>(&mut self, _rt: &mut ActorContext<Self, H>) -> Result<(), ActorError>
    where
        Self: 'static + Sized + Send + Sync,
    {
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

impl Actor for () {}
impl<A: Actor + ?Sized> Actor for Box<A> {}

pub trait DependsOn {
    type Dependencies: Dependencies;
}

#[async_trait]
pub trait HandleEvent<E: Send>: Actor + Sized {
    async fn handle_event<H: Send>(&mut self, rt: &mut ActorContext<Self, H>, event: Box<E>) -> Result<(), ActorError>;
}

pub trait Context {
    type Actor: Actor;
    fn handle<'a, E: Send>(
        &'a mut self,
        event: Box<E>,
    ) -> Pin<Box<dyn core::future::Future<Output = Result<(), ActorError>> + Send + 'a>>
    where
        Self: Sized,
        Self::Actor: HandleEvent<E>,
    {
        Box::pin(async { Ok(()) })
    }
}

impl<T: Context + ?Sized> Context for &T {
    type Actor = T::Actor;
}

impl<A, H> Context for (&mut ActorContext<A, H>, &mut A)
where
    A: Actor + Send,
    H: Send,
{
    type Actor = A;
    fn handle<'a, E: Send>(
        &'a mut self,
        event: Box<E>,
    ) -> Pin<Box<dyn core::future::Future<Output = Result<(), ActorError>> + Send + 'a>>
    where
        Self: Sized,
        Self::Actor: HandleEvent<E>,
    {
        let (cx, act) = self;
        act.handle_event(*cx, event)
    }
}

pub trait DynEvent<A> {
    fn handle<'a, C: 'a + Context<Actor = A>>(
        self: Box<Self>,
        cx: C,
    ) -> Pin<Box<dyn core::future::Future<Output = Result<(), ActorError>> + Send + 'a>>
    where
        A: 'a;
}

pub trait ErasedDynEvent<A> {
    fn erased_handle<'a>(
        self: Box<Self>,
        cx: &'a dyn Context<Actor = A>,
    ) -> Pin<Box<dyn core::future::Future<Output = Result<(), ActorError>> + Send + 'a>>;
}

impl<A> DynEvent<A> for dyn ErasedDynEvent<A> + Send + Sync {
    fn handle<'a, C: 'a + Context<Actor = A>>(
        self: Box<Self>,
        cx: C,
    ) -> Pin<Box<dyn core::future::Future<Output = Result<(), ActorError>> + Send + 'a>>
    where
        A: 'a,
    {
        self.erased_handle(&cx)
    }
}

impl<A, E> ErasedDynEvent<A> for E
where
    E: DynEvent<A> + Send,
    A: HandleEvent<E>,
{
    fn erased_handle<'a>(
        self: Box<Self>,
        cx: &'a dyn Context<Actor = A>,
    ) -> Pin<Box<dyn core::future::Future<Output = Result<(), ActorError>> + Send + 'a>> {
        self.handle(cx)
    }
}

impl<A, E: Send> DynEvent<A> for E
where
    A: HandleEvent<E>,
{
    fn handle<'a, C: 'a + Context<Actor = A>>(
        self: Box<Self>,
        mut cx: C,
    ) -> Pin<Box<dyn core::future::Future<Output = Result<(), ActorError>> + Send + 'a>> {
        cx.handle(self)
    }
}

pub type Envelope<A> = Box<dyn ErasedDynEvent<A> + Send + Sync>;
