use tokio::sync::broadcast;

use crate::{Act, Actor, BaseRuntime, Res, ResourceRuntime, Sys, System, SystemRuntime};
use std::marker::PhantomData;

pub enum DepStatus<T> {
    Ready(T),
    Waiting(broadcast::Receiver<PhantomData<T>>),
}

/// Defines dependencies that an actor or system can check for
pub trait Dependencies<Rt: BaseRuntime> {
    /// Request a notification when a specific resource is ready
    fn request(rt: &mut Rt) -> DepStatus<Self>
    where
        Self: 'static + Send + Sync + Sized,
    {
        match Self::instantiate(rt) {
            Ok(dep) => DepStatus::Ready(dep),
            Err(_) => DepStatus::Waiting(
                if let Some(sender) = rt.dependency_channels().get::<broadcast::Sender<PhantomData<Self>>>() {
                    sender.subscribe()
                } else {
                    let (sender, receiver) = broadcast::channel::<PhantomData<Self>>(8);
                    rt.dependency_channels_mut().insert(sender);
                    receiver
                },
            ),
        }
    }

    /// Instantiate instances of some dependencies
    fn instantiate(rt: &Rt) -> anyhow::Result<Self>
    where
        Self: Sized;
}

impl<Rt: SystemRuntime, S: 'static + System + Send + Sync> Dependencies<Rt> for Sys<S> {
    fn instantiate(rt: &Rt) -> anyhow::Result<Self> {
        rt.system()
            .ok_or_else(|| anyhow::anyhow!("Missing system dependency: {}", std::any::type_name::<S>()))
    }
}

impl<Rt: BaseRuntime, A: Actor + Send + Sync> Dependencies<Rt> for Act<A> {
    fn instantiate(rt: &Rt) -> anyhow::Result<Self> {
        rt.actor_event_handle()
            .ok_or_else(|| anyhow::anyhow!("Missing actor dependency: {}", std::any::type_name::<A>()))
    }
}

impl<Rt: ResourceRuntime, R: 'static + Send + Sync + Clone> Dependencies<Rt> for Res<R> {
    fn instantiate(rt: &Rt) -> anyhow::Result<Self> {
        rt.resource()
            .ok_or_else(|| anyhow::anyhow!("Missing resource dependency: {}", std::any::type_name::<R>()))
    }
}

impl<Rt: BaseRuntime> Dependencies<Rt> for () {
    fn instantiate(_rt: &Rt) -> anyhow::Result<Self> {
        Ok(())
    }
}

macro_rules! impl_dependencies {
    ($($gen:ident),+) => {
        impl<Rt: BaseRuntime, $($gen),+> Dependencies<Rt> for ($($gen),+,)
        where $($gen: Dependencies<Rt> + Send + Sync),+
        {
            fn request(scope: &mut Rt) -> DepStatus<Self>
            where
                Self: 'static + Send + Sync,
            {
                let mut receivers = anymap::Map::<dyn anymap::any::Any + Send + Sync>::new();
                let mut total = 0;
                let mut ready = 0;
                $(
                    total += 1;
                    let status = $gen::request(scope);
                    match status {
                        DepStatus::Ready(dep) => {
                            ready += 1;
                        }
                        DepStatus::Waiting(receiver) => {
                            receivers.insert(receiver);
                        }
                    }
                )+
                if total == ready {
                    DepStatus::Ready(($(receivers.remove::<$gen>().unwrap()),+,))
                } else {
                    let (sender, receiver) = broadcast::channel::<PhantomData<Self>>(8);
                    tokio::task::spawn(async move {
                        $(
                            let mut receiver = receivers.remove::<broadcast::Receiver<PhantomData<$gen>>>().unwrap();
                            if let Err(_) = receiver.recv().await {
                                return;
                            }

                        )+
                        sender.send(PhantomData).ok();
                    });
                    DepStatus::Waiting(receiver)
                }
            }

            fn instantiate(rt: &Rt) -> anyhow::Result<Self>
            {
                Ok(($($gen::instantiate(rt)?),+,))
            }
        }
    };
}

impl_dependencies!(A);
impl_dependencies!(A, B);
impl_dependencies!(A, B, C);
impl_dependencies!(A, B, C, D);
impl_dependencies!(A, B, C, D, E);
impl_dependencies!(A, B, C, D, E, F);
impl_dependencies!(A, B, C, D, E, F, G);
impl_dependencies!(A, B, C, D, E, F, G, H);
