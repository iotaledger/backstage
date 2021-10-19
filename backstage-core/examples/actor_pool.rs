// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use async_trait::async_trait;
use backstage::prelude::*;
use futures::FutureExt;
use log::{debug, info};
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};
use tokio::sync::RwLock;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum HelloWorldEvent {
    Print(String),
}

#[derive(Debug)]
pub struct HelloWorld {
    name: String,
    num: u32,
}

#[build]
#[derive(Debug, Clone)]
pub fn build_hello_world(name: String, num: Option<u32>) -> HelloWorld {
    HelloWorld {
        name,
        num: num.unwrap_or_default(),
    }
}

#[async_trait]
impl Actor for HelloWorld {
    async fn init<Sup>(&mut self, rt: &mut ActorContext<Self, Sup>) -> Result<(), ActorError>
    where
        Self: 'static + Sized + Send + Sync,
    {
        rt.update_status(format!("Running {}", self.num)).await.ok();
        Ok(())
    }

    async fn shutdown<Sup>(&mut self, rt: &mut ActorContext<Self, Sup>) -> Result<(), ActorError>
    where
        Self: 'static + Sized + Send + Sync,
    {
        rt.update_status(ServiceStatus::Stopped).await.ok();
        Ok(())
    }
}

#[async_trait]
impl HandleEvent<HelloWorldEvent> for HelloWorld {
    async fn handle_event<Sup>(
        &mut self,
        rt: &mut ActorContext<Self, Sup>,
        event: HelloWorldEvent,
    ) -> Result<(), ActorError>
    where
        Self: 'static + Sized + Send + Sync,
    {
        match event {
            HelloWorldEvent::Print(s) => {
                info!("HelloWorld {} printing: {}", self.num, s);
                if rand::random() {
                    panic!("Random panic attack!")
                }
            }
        }
        Ok(())
    }
}

#[derive(Default)]
struct Launcher {
    count: u8,
}
struct LauncherAPI;

impl LauncherAPI {
    pub async fn send_to_hello_world<Reg: 'static + RegistryAccess + Send + Sync>(
        &self,
        event: HelloWorldEvent,
        rt: &mut RuntimeScope,
    ) -> anyhow::Result<()> {
        rt.send_actor_event::<Launcher, _>(LauncherEvents::HelloWorld(event))
            .await
    }
}

#[derive(Debug)]
pub enum LauncherEvents {
    HelloWorld(HelloWorldEvent),
    Shutdown { using_ctrl_c: bool },
}

#[async_trait]
impl Actor for Launcher {
    async fn init<Sup>(&mut self, rt: &mut ActorContext<Self, Sup>) -> Result<(), ActorError>
    where
        Self: 'static + Sized + Send + Sync,
    {
        rt.update_status(ServiceStatus::Initializing).await.ok();
        let builder = HelloWorldBuilder::new().name("Hello World".to_string());
        let my_handle = rt.handle();
        // Start by initializing all the actors in the pool
        let mut initialized = Vec::new();
        for i in 0..5 {
            initialized.push(
                rt.init_into_pool_keyed::<MapPool<HelloWorld, i32>>(i as i32, builder.clone().num(i).build())
                    .await?,
            );
        }
        // Next, spawn them all at once
        for init in initialized {
            init.spawn(rt).await;
        }
        // An alternate way to do the same thing
        // Spawn the pool
        let mut pool = rt.spawn_pool::<MapPool<HelloWorld, i32>>().await;
        // Init all the actors into it
        for i in 5..10 {
            pool.init_keyed(i as i32, builder.clone().num(i).build()).await?;
        }
        // Finalize the pool
        pool.spawn_all().await;
        tokio::task::spawn(ctrl_c(my_handle.into_inner()));
        rt.update_status(ServiceStatus::Running).await.ok();
        Ok(())
    }

    async fn shutdown<Sup>(&mut self, rt: &mut ActorContext<Self, Sup>) -> Result<(), ActorError>
    where
        Self: 'static + Sized + Send + Sync,
    {
        rt.update_status(ServiceStatus::Stopped).await.ok();
    }

    fn name(&self) -> std::borrow::Cow<'static, str> {
        std::any::type_name::<Self>().into()
    }
}

#[async_trait]
impl HandleEvent<LauncherEvents> for Launcher {
    async fn handle_event<Sup>(
        &mut self,
        rt: &mut ActorContext<Self, Sup>,
        event: Box<LauncherEvents>,
    ) -> Result<(), ActorError>
    where
        Self: 'static + Sized + Send + Sync,
    {
        match event {
            LauncherEvents::HelloWorld(event) => {
                info!("Received event for HelloWorld");
                if let Some(pool) = rt.pool::<MapPool<HelloWorld, i32>>().await {
                    pool.send(&self.count, event)
                        .await
                        .expect("Failed to pass along message!");
                    self.count += 1;
                }
            }
            LauncherEvents::Shutdown { using_ctrl_c: _ } => {
                rt.shutdown_scope(&ROOT_SCOPE).await.ok();
            }
        }
    }
}

#[async_trait]
impl HandleEvent<StatusChange<HelloWorld>> for Launcher {
    async fn handle_event<Sup>(
        &mut self,
        rt: &mut ActorContext<Self, Sup>,
        s: StatusChange<HelloWorld>,
    ) -> Result<(), ActorError>
    where
        Self: 'static + Sized + Send + Sync,
    {
        info!(
            "{} status changed ({} -> {})!",
            s.service.name(),
            s.prev_status,
            s.service.status()
        );
        debug!("\n{}", rt.root_service_tree().await.unwrap());
    }
}

#[async_trait]
impl HandleEvent<Result<SuccessReport<HelloWorld>, ErrorReport<HelloWorld>>> for Launcher {
    async fn handle_event<Sup>(
        &mut self,
        rt: &mut ActorContext<Self, Sup>,
        res: Result<SuccessReport<HelloWorld>, ErrorReport<HelloWorld>>,
    ) -> Result<(), ActorError>
    where
        Self: 'static + Sized + Send + Sync,
    {
        match res {
            Ok(s) => {
                info!("{} {} has shutdown!", s.state.name, s.state.num);
            }
            Err(e) => {
                info!("{} {} has shutdown unexpectedly!", e.state.name, e.state.num);
                match e.error.request() {
                    ActorRequest::Restart(_) => {
                        info!("Restarting {} {}", e.state.name, e.state.num);
                        let i = e.state.num as i32;
                        rt.spawn_into_pool_keyed::<MapPool<HelloWorld, i32>>(i, e.state).await?;
                    }
                    ActorRequest::Finish => (),
                    ActorRequest::Panic => panic!("Received request to panic....so I did"),
                }
            }
        }
    }
}

impl System for Launcher {
    type State = Arc<RwLock<LauncherAPI>>;
}

pub async fn ctrl_c(sender: UnboundedTokioSender<LauncherEvents>) {
    tokio::signal::ctrl_c().await.unwrap();
    let exit_program_event = LauncherEvents::Shutdown { using_ctrl_c: true };
    sender.send(exit_program_event).ok();
}

#[tokio::main]
async fn main() {
    std::env::set_var("RUST_LOG", "debug");
    env_logger::init();

    startup().await.unwrap();
}

async fn startup() -> anyhow::Result<()> {
    std::panic::set_hook(Box::new(|info| {
        log::error!("{}", info);
    }));
    RuntimeScope::<ActorRegistry>::launch(|scope| {
        async move {
            scope
                .spawn_system_unsupervised(Launcher::default(), Arc::new(RwLock::new(LauncherAPI)))
                .await?;
            scope
                .spawn_task(|rt| {
                    async move {
                        for i in 0..10 {
                            rt.system::<Launcher>()
                                .await
                                .unwrap()
                                .state
                                .read()
                                .await
                                .send_to_hello_world(HelloWorldEvent::Print(format!("foo {}", i)), rt)
                                .await
                                .unwrap();
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                        Ok(())
                    }
                    .boxed()
                })
                .await;
            Ok(())
        }
        .boxed()
    })
    .await?;
    Ok(())
}
