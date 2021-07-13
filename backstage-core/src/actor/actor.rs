use super::{ActorError, Channel, Dependencies, SupervisorEvent};
use crate::{
    actor::ShutdownStream,
    runtime::{ActorScopedRuntime, RegistryAccess, RuntimeScope},
};
use async_trait::async_trait;
use futures::{
    future::{AbortHandle, Abortable},
    FutureExt,
};
use std::{borrow::Cow, marker::PhantomData, panic::AssertUnwindSafe};
/// The all-important Actor trait. This defines an Actor and what it do.
#[async_trait]
pub trait Actor
where
    Self: Sized + Send + Sync,
{
    /// Allows specifying an actor's startup dependencies. Ex. (Act<OtherActor>, Res<MyResource>)
    type Dependencies: Dependencies + Send + Sync;
    /// The type of event this actor will receive
    type Event: 'static + Send + Sync;
    /// The type of channel this actor will use to receive events
    type Channel: Channel<Self, Self::Event> + Send;

    /// Used to initialize the actor. Any children spawned here will be initialized
    /// before this actor's run method is called so they are guaranteed to be
    /// ready to use depending on their requirements. Dependencies are not
    /// handled until after this method is called, so they are not guaranteed
    /// to exist yet. If a dependency must exist to complete initialization,
    /// it can be linked with the runtime, but BEWARE that any dependencies which are
    /// spawned by the parents of this actor will not be available if they are
    /// spawned after this actor and linking them will therefore deadlock the thread.
    async fn init<Reg: RegistryAccess + Send + Sync, Sup: EventDriven>(
        &mut self,
        rt: &mut ActorScopedRuntime<Self, Reg, Sup>,
    ) -> Result<(), ActorError>
    where
        Self: Sized,
        Sup::Event: SupervisorEvent,
        <Sup::Event as SupervisorEvent>::Children: From<PhantomData<Self>>;

    /// The main function for the actor
    async fn run<Reg: RegistryAccess + Send + Sync, Sup: EventDriven>(
        &mut self,
        rt: &mut ActorScopedRuntime<Self, Reg, Sup>,
        deps: Self::Dependencies,
    ) -> Result<(), ActorError>
    where
        Self: Sized,
        Sup::Event: SupervisorEvent,
        <Sup::Event as SupervisorEvent>::Children: From<PhantomData<Self>>;

    /// Get this actor's name
    fn name() -> Cow<'static, str> {
        std::any::type_name::<Self>().into()
    }

    /// Start with this actor as the root scope
    async fn start_as_root<Reg: 'static + RegistryAccess + Send + Sync>(mut self) -> anyhow::Result<()>
    where
        Self: Sized,
    {
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        let (sender, receiver) = <Self::Channel as Channel<Self, Self::Event>>::new(&self).await?;
        let (receiver, shutdown_handle) = ShutdownStream::new(receiver);
        let mut scope = Reg::instantiate(Self::name(), Some(shutdown_handle.clone()), Some(abort_handle)).await;
        scope.add_data(sender.clone()).await;
        let mut actor_rt = ActorScopedRuntime::<_, _, ()> {
            scope,
            handle: sender,
            receiver,
            shutdown_handle,
            supervisor_handle: None,
        };
        let deps = Self::Dependencies::instantiate(&mut actor_rt.lock().await)
            .await
            .map_err(|e| anyhow::anyhow!("Cannot spawn actor {}: {}", std::any::type_name::<Self>(), e))
            .unwrap();
        let res = AssertUnwindSafe(self.init(&mut actor_rt)).catch_unwind().await;
        let mut actor = RuntimeScope::handle_init_res::<_, ()>(res, &mut actor_rt, self).await?;
        let res = Abortable::new(AssertUnwindSafe(actor.run(&mut actor_rt, deps)).catch_unwind(), abort_registration).await;
        RuntimeScope::handle_run_res::<_, ()>(res, &mut actor_rt, None, actor).await
    }
}

/// Anything that is event driven
pub trait EventDriven
where
    Self: Sized,
{
    /// The type of event this implementor will receive
    type Event: 'static + Send + Sync;
    /// The type of channel this implementor will use to receive events
    type Channel: Channel<Self, Self::Event> + Send;
}

impl<T> EventDriven for T
where
    T: Actor,
{
    type Event = T::Event;
    type Channel = T::Channel;
}
