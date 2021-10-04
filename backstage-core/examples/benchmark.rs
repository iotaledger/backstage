// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use async_trait::async_trait;
use backstage::prelude::*;
use backstage_macros::supervise;
use futures::FutureExt;
use log::info;
use std::{
    marker::PhantomData,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    time::SystemTime,
};

const MAX_DEPTH: u32 = 6;

#[supervise(Launcher)]
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
    type Dependencies = ();
    type Event = SpawnerEvent;
    type Channel = UnboundedTokioChannel<Self::Event>;

    async fn init<Reg: 'static + RegistryAccess + Send + Sync, Sup: EventDriven>(
        &mut self,
        rt: &mut ActorScopedRuntime<Self, Reg, Sup>,
    ) -> Result<(), ActorError> {
        // info!("{}-{} starting!", self.depth, self.num);
        if let Some(i) = rt.resource::<Arc<AtomicU32>>().await {
            i.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    async fn run<Reg: 'static + RegistryAccess + Send + Sync, Sup: EventDriven>(
        &mut self,
        rt: &mut ActorScopedRuntime<Self, Reg, Sup>,
        _deps: Self::Dependencies,
    ) -> Result<(), ActorError>
    where
        Self: Sized,
        Sup::Event: SupervisorEvent,
        <Sup::Event as SupervisorEvent>::Children: From<PhantomData<Self>>,
    {
        rt.update_status(ServiceStatus::Running).await.ok();
        while let Some(evt) = rt.next_event().await {
            match evt {
                SpawnerEvent::Spawn => {
                    if self.depth < MAX_DEPTH {
                        rt.spawn_actor(Launcher { depth: self.depth }).await?;
                    } else {
                        break;
                    }
                }
                SpawnerEvent::ReportExit(_) => {
                    break;
                }
                SpawnerEvent::StatusChange(_) => {}
            }
        }
        rt.update_status(ServiceStatus::Stopped).await.ok();
        Ok(())
    }
}

#[derive(Default, Clone, Debug)]
struct Launcher {
    depth: u32,
}

#[supervise(Spawner)]
#[derive(Debug)]
enum LauncherEvent {
    Shutdown { using_ctrl_c: bool },
}

#[async_trait]
impl Actor for Launcher {
    type Dependencies = ();
    type Event = LauncherEvent;
    type Channel = UnboundedTokioChannel<Self::Event>;

    async fn init<Reg: RegistryAccess + Send + Sync, Sup: EventDriven>(
        &mut self,
        rt: &mut ActorScopedRuntime<Self, Reg, Sup>,
    ) -> Result<(), ActorError>
    where
        Self: Sized,
        Sup::Event: SupervisorEvent,
        <Sup::Event as SupervisorEvent>::Children: From<PhantomData<Self>>,
    {
        rt.update_status(ServiceStatus::Initializing).await.ok();
        if let Some(i) = rt.resource::<Arc<AtomicU32>>().await {
            i.fetch_add(1, Ordering::Relaxed);
        }
        // Spawn the pool
        let mut pool = rt.new_pool::<MapPool<Spawner, i32>>().await;
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
        if let Some(pool) = rt.pool::<MapPool<Spawner, i32>>().await {
            pool.send_all(SpawnerEvent::Spawn)
                .await
                .expect("Failed to pass along message!");
        }
        Ok(())
    }

    async fn run<Reg: RegistryAccess + Send + Sync, Sup: EventDriven>(
        &mut self,
        rt: &mut ActorScopedRuntime<Self, Reg, Sup>,
        _deps: Self::Dependencies,
    ) -> Result<(), ActorError>
    where
        Self: Sized,
        Sup::Event: SupervisorEvent,
        <Sup::Event as SupervisorEvent>::Children: From<PhantomData<Self>>,
    {
        rt.update_status(ServiceStatus::Running).await.ok();
        while let Some(evt) = rt.next_event().await {
            match evt {
                LauncherEvent::Shutdown { using_ctrl_c: _ } => {
                    rt.shutdown_scope(&ROOT_SCOPE).await.ok();
                }
                LauncherEvent::ReportExit(_) => {
                    if rt.pool::<MapPool<Spawner, i32>>().await.is_none() {
                        // info!("\n{}", rt.root_service_tree().await.unwrap());
                        break;
                    }
                }
                LauncherEvent::StatusChange(_) => {}
            }
        }
        rt.update_status(ServiceStatus::Stopped).await.ok();
        Ok(())
    }
}

async fn ctrl_c(sender: UnboundedTokioSender<LauncherEvent>) {
    tokio::signal::ctrl_c().await.unwrap();
    let exit_program_event = LauncherEvent::Shutdown { using_ctrl_c: true };
    sender.send(exit_program_event).ok();
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
    RuntimeScope::<ActorRegistry>::launch(|scope| {
        let c = counter.clone();
        async move {
            scope.add_resource(c).await;
            let launcher_handle = scope.spawn_actor_unsupervised(Launcher::default()).await?;
            tokio::task::spawn(ctrl_c(launcher_handle.into_inner()));
            Ok(())
        }
        .boxed()
    })
    .await?;
    info!("Total actors spawned: {}", counter.load(Ordering::Relaxed));
    info!("Total time: {} ms", start.elapsed().unwrap().as_millis());
    Ok(())
}
