// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use async_trait::async_trait;
use backstage::prelude::*;
use backstage_macros::supervise;
use futures::FutureExt;
use log::{debug, info};
use serde::{Deserialize, Serialize};
use std::{marker::PhantomData, sync::Arc, time::Duration};
use thiserror::Error;
use tokio::sync::RwLock;

#[derive(Error, Debug)]
pub enum HelloWorldError {
    #[error("Something went wrong")]
    SomeError,
}

impl Into<ActorError> for HelloWorldError {
    fn into(self) -> ActorError {
        ActorError::RuntimeError(ActorRequest::Finish)
    }
}

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
    type Dependencies = ();
    type Event = HelloWorldEvent;
    type Channel = UnboundedTokioChannel<Self::Event>;

    async fn init<Reg: 'static + RegistryAccess + Send + Sync, Sup: EventDriven>(
        &mut self,
        _rt: &mut ActorScopedRuntime<Self, Reg, Sup>,
    ) -> Result<(), ActorError> {
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
        rt.update_status(format!("Running {}", self.num)).await.ok();
        while let Some(evt) = rt.next_event().await {
            match evt {
                HelloWorldEvent::Print(s) => {
                    info!("HelloWorld {} printing: {}", self.num, s);
                    if rand::random() {
                        panic!("Random panic attack!")
                    }
                }
            }
        }
        rt.update_status(ServiceStatus::Stopped).await.ok();
        Ok(())
    }
}

struct Launcher;
struct LauncherAPI;

impl LauncherAPI {
    pub async fn send_to_hello_world<Reg: 'static + RegistryAccess + Send + Sync>(
        &self,
        event: HelloWorldEvent,
        rt: &mut RuntimeScope<Reg>,
    ) -> anyhow::Result<()> {
        rt.send_actor_event::<Launcher>(LauncherEvents::HelloWorld(event)).await
    }
}

#[supervise(HelloWorld)]
#[derive(Debug)]
pub enum LauncherEvents {
    HelloWorld(HelloWorldEvent),
    Shutdown { using_ctrl_c: bool },
}

#[async_trait]
impl Actor for Launcher {
    type Dependencies = ();
    type Event = LauncherEvents;
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
        let mut i = 0;
        while let Some(evt) = rt.next_event().await {
            match evt {
                LauncherEvents::HelloWorld(event) => {
                    info!("Received event for HelloWorld");
                    if let Some(pool) = rt.pool::<MapPool<HelloWorld, i32>>().await {
                        pool.send(&i, event).await.expect("Failed to pass along message!");
                        i += 1;
                    }
                }
                LauncherEvents::Shutdown { using_ctrl_c: _ } => {
                    rt.shutdown_scope(&ROOT_SCOPE).await.ok();
                }
                LauncherEvents::ReportExit(res) => match res {
                    Ok(s) => {
                        info!("{} {} has shutdown!", s.state.name, s.state.num);
                    }
                    Err(e) => {
                        info!("{} {} has shutdown unexpectedly!", e.state.name, e.state.num);
                        match e.error.request() {
                            ActorRequest::Restart => {
                                info!("Restarting {} {}", e.state.name, e.state.num);
                                let i = e.state.num as i32;
                                rt.spawn_into_pool_keyed::<MapPool<HelloWorld, i32>>(i, e.state).await?;
                            }
                            ActorRequest::Reschedule(_) => todo!(),
                            ActorRequest::Finish => (),
                            ActorRequest::Panic => panic!("Received request to panic....so I did"),
                        }
                    }
                },
                LauncherEvents::StatusChange(s) => {
                    info!(
                        "{} status changed ({} -> {})!",
                        s.service.name(),
                        s.prev_status,
                        s.service.status()
                    );
                    debug!("\n{}", rt.root_service_tree().await.unwrap());
                }
            }
        }
        rt.update_status(ServiceStatus::Stopped).await.ok();
        Ok(())
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
                .spawn_system_unsupervised(Launcher, Arc::new(RwLock::new(LauncherAPI)))
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
