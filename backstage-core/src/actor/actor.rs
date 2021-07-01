use super::{ActorError, Channel, Dependencies, Supervisor};
use crate::{
    prelude::RegistryAccess,
    runtime::{ActorScopedRuntime, RuntimeScope},
};
use async_trait::async_trait;
use futures::{
    future::{AbortHandle, Abortable},
    FutureExt,
};
use std::{borrow::Cow, marker::PhantomData, panic::AssertUnwindSafe};
use tokio::sync::oneshot;
/// The all-important Actor trait. This defines an Actor and what it do.
#[async_trait]
pub trait Actor {
    /// Allows specifying an actor's startup dependencies. Ex. (Act<OtherActor>, Res<MyResource>)
    type Dependencies: Dependencies + Send + Sync;
    /// The type of event this actor will receive
    type Event: 'static + Send + Sync;
    /// The type of channel this actor will use to receive events
    type Channel: Channel<Self::Event> + Send;

    /// The main function for the actor
    async fn run<'a, Reg: RegistryAccess + Send + Sync, Sup: EventDriven + Supervisor>(
        &mut self,
        rt: &mut ActorScopedRuntime<'a, Self, Reg, Sup>,
        deps: Self::Dependencies,
    ) -> Result<(), ActorError>
    where
        Self: Sized,
        Sup::Children: From<PhantomData<Self>>;

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
        let (oneshot_send, oneshot_recv) = oneshot::channel::<()>();
        let mut scope = Reg::instantiate(Self::name(), Some(oneshot_send), Some(abort_handle)).await;
        let (sender, receiver) = <Self::Channel as Channel<Self::Event>>::new();
        scope.add_data(sender.clone()).await;
        let deps = Self::Dependencies::instantiate(&mut scope)
            .await
            .map_err(|e| anyhow::anyhow!("Cannot spawn actor {}: {}", std::any::type_name::<Self>(), e))
            .unwrap();
        let res = {
            let mut actor_rt = ActorScopedRuntime::<'_, _, _, ()>::new(&mut scope, receiver, oneshot_recv, None);
            Abortable::new(AssertUnwindSafe(self.run(&mut actor_rt, deps)).catch_unwind(), abort_registration).await
        };
        RuntimeScope::handle_res::<_, ()>(res, &mut scope, None, self).await
    }
}

/// Anything that is event driven
pub trait EventDriven {
    /// The type of event this implementor will receive
    type Event: 'static + Send + Sync;
    /// The type of channel this implementor will use to receive events
    type Channel: Channel<Self::Event> + Send;
}

impl<T> EventDriven for T
where
    T: Actor,
{
    type Event = T::Event;
    type Channel = T::Channel;
}
