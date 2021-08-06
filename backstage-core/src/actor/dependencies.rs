// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::{Actor, ActorPool, System};
use crate::{
    prelude::{Pool, RegistryAccess},
    runtime::{Act, Res, RuntimeScope, Sys},
};
use async_trait::async_trait;

/// Defines dependencies that an actor or system can check for
#[async_trait]
pub trait Dependencies {
    /// Request a notification when a specific resource is ready
    async fn request<R: 'static + RegistryAccess + Send + Sync>(scope: &mut RuntimeScope<R>) -> anyhow::Result<Self>
    where
        Self: 'static + Clone + Send + Sync,
    {
        scope.get_data().await.get().await
    }

    /// Instantiate instances of some dependencies
    async fn instantiate<R: 'static + RegistryAccess + Send + Sync>(scope: &mut RuntimeScope<R>) -> anyhow::Result<Self>
    where
        Self: 'static + Clone + Send + Sync,
    {
        scope
            .get_data_opt()
            .await
            .ok_or_else(|| anyhow::anyhow!("Missing dependency: {}", std::any::type_name::<Self>()))
    }

    /// Link the dependencies so that removing them will shut down the dependent
    async fn link<R: 'static + RegistryAccess + Send + Sync>(scope: &mut RuntimeScope<R>) -> anyhow::Result<Self>
    where
        Self: 'static + Clone + Send + Sync,
    {
        scope.depend_on().await.get().await
    }
}

#[async_trait]
impl<S: 'static + System + Send + Sync> Dependencies for Sys<S> {
    async fn request<R: 'static + RegistryAccess + Send + Sync>(scope: &mut RuntimeScope<R>) -> anyhow::Result<Self> {
        Ok(Sys {
            actor: Act::request(scope).await?,
            state: Res::request(scope).await?,
        })
    }

    async fn instantiate<Reg: 'static + RegistryAccess + Send + Sync>(
        scope: &mut RuntimeScope<Reg>,
    ) -> anyhow::Result<Self> {
        scope
            .system()
            .await
            .ok_or_else(|| anyhow::anyhow!("Missing system dependency: {}", std::any::type_name::<S>()))
    }

    async fn link<Reg: 'static + RegistryAccess + Send + Sync>(scope: &mut RuntimeScope<Reg>) -> anyhow::Result<Self> {
        Ok(Sys {
            actor: Act::link(scope).await?,
            state: Res::link(scope).await?,
        })
    }
}

#[async_trait]
impl<A: 'static + Actor + Send + Sync> Dependencies for Act<A> {}

#[async_trait]
impl<R: 'static + Send + Sync + Clone> Dependencies for Res<R> {}

#[async_trait]
impl<P: 'static + ActorPool + Send + Sync> Dependencies for Pool<P> {
    async fn request<R: 'static + RegistryAccess + Send + Sync>(scope: &mut RuntimeScope<R>) -> anyhow::Result<Self> {
        scope.get_data().await.get().await
    }

    async fn instantiate<R: 'static + RegistryAccess + Send + Sync>(
        scope: &mut RuntimeScope<R>,
    ) -> anyhow::Result<Self> {
        scope
            .pool()
            .await
            .ok_or_else(|| anyhow::anyhow!("Missing actor pool dependency: {}", std::any::type_name::<Pool<P>>()))
    }

    async fn link<R: 'static + RegistryAccess + Send + Sync>(scope: &mut RuntimeScope<R>) -> anyhow::Result<Self> {
        scope.depend_on().await.get().await
    }
}

#[async_trait]
impl<D: 'static + Dependencies + Clone + Send + Sync> Dependencies for Option<D> {
    async fn request<R: 'static + RegistryAccess + Send + Sync>(scope: &mut RuntimeScope<R>) -> anyhow::Result<Self> {
        Ok(scope.get_data::<D>().await.get_opt())
    }

    async fn instantiate<R: 'static + RegistryAccess + Send + Sync>(
        scope: &mut RuntimeScope<R>,
    ) -> anyhow::Result<Self> {
        Ok(D::instantiate(scope).await.ok())
    }

    async fn link<R: 'static + RegistryAccess + Send + Sync>(scope: &mut RuntimeScope<R>) -> anyhow::Result<Self> {
        Ok(scope.get_data::<D>().await.get_opt())
    }
}

#[async_trait]
impl Dependencies for () {
    async fn request<R: 'static + RegistryAccess + Send + Sync>(_scope: &mut RuntimeScope<R>) -> anyhow::Result<Self> {
        Ok(())
    }

    async fn instantiate<R: 'static + RegistryAccess + Send + Sync>(
        _scope: &mut RuntimeScope<R>,
    ) -> anyhow::Result<Self> {
        Ok(())
    }

    async fn link<R: 'static + RegistryAccess + Send + Sync>(_scope: &mut RuntimeScope<R>) -> anyhow::Result<Self> {
        Ok(())
    }
}

macro_rules! impl_dependencies {
    ($($gen:ident),+) => {
        #[async_trait]
        impl<$($gen),+> Dependencies for ($($gen),+,)
        where $($gen: 'static + Dependencies + Clone + Send + Sync),+
        {
            async fn request<Reg: 'static + RegistryAccess + Send + Sync>(scope: &mut RuntimeScope<Reg>) -> anyhow::Result<Self>
            {
                Ok(($($gen::request(scope).await?),+,))
            }

            async fn instantiate<Reg: 'static + RegistryAccess + Send + Sync>(scope: &mut RuntimeScope<Reg>) -> anyhow::Result<Self>
            {
                Ok(($($gen::instantiate(scope).await?),+,))
            }

            async fn link<Reg: 'static + RegistryAccess + Send + Sync>(scope: &mut RuntimeScope<Reg>) -> anyhow::Result<Self> {
                Ok(($($gen::link(scope).await?),+,))
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
