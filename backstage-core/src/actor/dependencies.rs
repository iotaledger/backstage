use super::{Actor, Channel, System};
use crate::{
    prelude::RegistryAccess,
    runtime::{Act, Res, RuntimeScope, Sys},
};
use async_trait::async_trait;
use std::{marker::PhantomData, sync::Arc};
use tokio::sync::{broadcast, RwLock};

/// A dependency's status
pub enum DepStatus<T> {
    /// The dependency is ready to be used
    Ready(T),
    /// The dependency is not ready, here is a channel to await
    Waiting(broadcast::Receiver<PhantomData<T>>),
}

/// Defines dependencies that an actor or system can check for
#[async_trait]
pub trait Dependencies {
    /// Request a notification when a specific resource is ready
    async fn request<R: 'static + RegistryAccess + Send + Sync>(scope: &mut RuntimeScope<R>) -> DepStatus<Self>
    where
        Self: 'static + Send + Sync + Sized,
    {
        match Self::instantiate(scope).await {
            Ok(dep) => DepStatus::Ready(dep),
            Err(_) => DepStatus::Waiting(
                if let Some(sender) = scope.get_data::<broadcast::Sender<PhantomData<Self>>>().await {
                    sender.subscribe()
                } else {
                    let (sender, receiver) = broadcast::channel::<PhantomData<Self>>(8);
                    scope.add_data_to_parent(sender).await;
                    receiver
                },
            ),
        }
    }

    /// Instantiate instances of some dependencies
    async fn instantiate<R: 'static + RegistryAccess + Send + Sync>(scope: &mut RuntimeScope<R>) -> anyhow::Result<Self>
    where
        Self: Sized;

    /// Link the dependencies so that removing them will shut down the dependent
    async fn link<R: 'static + RegistryAccess + Send + Sync>(scope: &mut RuntimeScope<R>);
}

#[async_trait]
impl<S: 'static + System + Send + Sync> Dependencies for Sys<S> {
    async fn instantiate<R: 'static + RegistryAccess + Send + Sync>(scope: &mut RuntimeScope<R>) -> anyhow::Result<Self> {
        scope
            .system()
            .await
            .ok_or_else(|| anyhow::anyhow!("Missing system dependency: {}", std::any::type_name::<S>()))
    }

    async fn link<R: 'static + RegistryAccess + Send + Sync>(scope: &mut RuntimeScope<R>) {
        scope.depend_on::<Arc<RwLock<S>>>().await;
    }
}

#[async_trait]
impl<A: Actor + Send + Sync> Dependencies for Act<A> {
    async fn instantiate<R: 'static + RegistryAccess + Send + Sync>(scope: &mut RuntimeScope<R>) -> anyhow::Result<Self> {
        scope
            .actor_event_handle()
            .await
            .ok_or_else(|| anyhow::anyhow!("Missing actor dependency: {}", std::any::type_name::<A>()))
    }

    async fn link<R: 'static + RegistryAccess + Send + Sync>(scope: &mut RuntimeScope<R>) {
        scope.depend_on::<<A::Channel as Channel<A::Event>>::Sender>().await;
    }
}

#[async_trait]
impl<R: 'static + Send + Sync + Clone> Dependencies for Res<R> {
    async fn instantiate<Reg: 'static + RegistryAccess + Send + Sync>(scope: &mut RuntimeScope<Reg>) -> anyhow::Result<Self> {
        scope
            .resource()
            .await
            .ok_or_else(|| anyhow::anyhow!("Missing resource dependency: {}", std::any::type_name::<R>()))
    }

    async fn link<Reg: 'static + RegistryAccess + Send + Sync>(scope: &mut RuntimeScope<Reg>) {
        scope.depend_on::<R>().await;
    }
}

#[async_trait]
impl Dependencies for () {
    async fn instantiate<R: 'static + RegistryAccess + Send + Sync>(_scope: &mut RuntimeScope<R>) -> anyhow::Result<Self> {
        Ok(())
    }

    async fn link<Reg: 'static + RegistryAccess + Send + Sync>(_scope: &mut RuntimeScope<Reg>) {}
}

macro_rules! impl_dependencies {
    ($($gen:ident),+) => {
        #[async_trait]
        impl<$($gen),+> Dependencies for ($($gen),+,)
        where $($gen: Dependencies + Send + Sync),+
        {
            async fn request<Reg: 'static + RegistryAccess + Send + Sync>(scope: &mut RuntimeScope<Reg>) -> DepStatus<Self>
            where
                Self: 'static + Send + Sync,
            {
                let mut receivers = anymap::Map::<dyn anymap::any::Any + Send + Sync>::new();
                let mut total = 0;
                let mut ready = 0;
                $(
                    total += 1;
                    let status = $gen::request(scope).await;
                    match status {
                        DepStatus::Ready(_) => {
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
                            if let Some(mut receiver) = receivers.remove::<broadcast::Receiver<PhantomData<$gen>>>() {
                                if let Err(_) = receiver.recv().await {
                                    return;
                                }
                            }
                        )+
                        sender.send(PhantomData).ok();
                    });
                    DepStatus::Waiting(receiver)
                }
            }

            async fn instantiate<Reg: 'static + RegistryAccess + Send + Sync>(scope: &mut RuntimeScope<Reg>) -> anyhow::Result<Self>
            {
                Ok(($($gen::instantiate(scope).await?),+,))
            }

            async fn link<Reg: 'static + RegistryAccess + Send + Sync>(scope: &mut RuntimeScope<Reg>) {
                $($gen::link(scope).await);+
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
