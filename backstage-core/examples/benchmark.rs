// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use async_trait::async_trait;
use backstage::prelude::*;
use futures::FutureExt;
use log::info;
use std::{
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    time::SystemTime,
};

const MAX_DEPTH: u32 = 6;

#[derive(Clone, Debug)]
enum SpawnerEvent {
    Spawn,
}

#[derive(Debug)]
struct Spawner {
    num: u32,
    depth: u32,
}

#[async_trait]
impl Actor for Spawner {
    type Data = ();
    type Context = SupervisedContext<Self, Launcher, Act<Launcher>>;

    async fn init(&mut self, cx: &mut Self::Context) -> Result<Self::Data, ActorError>
    where
        Self: 'static + Sized + Send + Sync,
    {
        // info!("{}-{} starting!", self.depth, self.num);
        if let Some(i) = cx.resource::<Arc<AtomicU32>>().await {
            i.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }
}

#[async_trait]
impl HandleEvent<SpawnerEvent> for Spawner {
    async fn handle_event(
        &mut self,
        cx: &mut Self::Context,
        event: SpawnerEvent,
        data: &mut Self::Data,
    ) -> Result<(), ActorError> {
        match event {
            SpawnerEvent::Spawn => {
                if self.depth < MAX_DEPTH {
                    cx.spawn_actor(Launcher { depth: self.depth }).await?;
                } else {
                    cx.handle().shutdown();
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl HandleEvent<StatusChange<Launcher>> for Spawner {
    async fn handle_event(
        &mut self,
        cx: &mut Self::Context,
        event: StatusChange<Launcher>,
        data: &mut Self::Data,
    ) -> Result<(), ActorError> {
        Ok(())
    }
}

#[async_trait]
impl HandleEvent<Report<Launcher>> for Spawner {
    async fn handle_event(
        &mut self,
        cx: &mut Self::Context,
        event: Report<Launcher>,
        data: &mut Self::Data,
    ) -> Result<(), ActorError> {
        cx.handle().shutdown();
        Ok(())
    }
}

#[derive(Default, Clone, Debug)]
struct Launcher {
    depth: u32,
}

#[derive(Debug)]
enum LauncherEvent {
    Shutdown { using_ctrl_c: bool },
}

#[async_trait]
impl Actor for Launcher {
    type Data = ();
    type Context = AnyContext<Self, Spawner, Act<Spawner>>;

    async fn init(&mut self, cx: &mut Self::Context) -> Result<Self::Data, ActorError>
    where
        Self: 'static + Sized + Send + Sync,
    {
        cx.update_status(ServiceStatus::Initializing).await.ok();
        if let Some(i) = cx.resource::<Arc<AtomicU32>>().await {
            i.fetch_add(1, Ordering::Relaxed);
        }
        // Spawn the pool
        let mut pool = cx.new_pool::<MapPool<Spawner, i32>>().await;
        // Init all the actors into it
        for i in 0..10 {
            pool.init_keyed(
                i as i32,
                Spawner {
                    num: i,
                    depth: self.depth + 1,
                },
            )
            .await?;
        }
        // Finalize the pool
        pool.spawn_all().await;
        if let Some(pool) = cx.pool::<MapPool<Spawner, i32>>().await {
            pool.send_all(SpawnerEvent::Spawn)
                .await
                .expect("Failed to pass along message!");
        }
        Ok(())
    }
}

#[async_trait]
impl HandleEvent<LauncherEvent> for Launcher {
    async fn handle_event(
        &mut self,
        cx: &mut Self::Context,
        event: LauncherEvent,
        data: &mut Self::Data,
    ) -> Result<(), ActorError> {
        match event {
            LauncherEvent::Shutdown { using_ctrl_c: _ } => {
                cx.shutdown_scope(&ROOT_SCOPE).await.ok();
            }
        }
        Ok(())
    }
}

#[async_trait]
impl HandleEvent<StatusChange<Spawner>> for Launcher {
    async fn handle_event(
        &mut self,
        cx: &mut Self::Context,
        event: StatusChange<Spawner>,
        data: &mut Self::Data,
    ) -> Result<(), ActorError> {
        Ok(())
    }
}

#[async_trait]
impl HandleEvent<Report<Spawner>> for Launcher {
    async fn handle_event(
        &mut self,
        cx: &mut Self::Context,
        event: Report<Spawner>,
        data: &mut Self::Data,
    ) -> Result<(), ActorError> {
        if cx.pool::<MapPool<Spawner, i32>>().await.is_none() {
            // info!("\n{}", cx.root_service_tree().await.unwrap());
            cx.handle().shutdown();
        }
        Ok(())
    }
}

async fn ctrl_c(sender: Act<Launcher>) {
    tokio::signal::ctrl_c().await.unwrap();
    sender.shutdown();
}

#[tokio::main]
async fn main() {
    std::env::set_var("RUST_LOG", "info");
    env_logger::init();

    startup().await.unwrap();
}

async fn startup() -> anyhow::Result<()> {
    std::panic::set_hook(Box::new(|info| {
        log::error!("{}", info);
    }));
    let start = SystemTime::now();
    let counter = Arc::new(AtomicU32::default());
    RuntimeScope::launch::<ActorRegistry, _, _>(|scope| {
        let c = counter.clone();
        async move {
            scope.add_resource(c).await;
            let launcher_handle = scope.spawn_actor_unsupervised(Launcher::default()).await?;
            tokio::task::spawn(ctrl_c(launcher_handle));
            Ok(())
        }
        .boxed()
    })
    .await?;
    info!("Total actors spawned: {}", counter.load(Ordering::Relaxed));
    info!("Total time: {} ms", start.elapsed().unwrap().as_millis());
    Ok(())
}
