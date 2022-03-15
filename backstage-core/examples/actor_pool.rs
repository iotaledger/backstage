// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use async_trait::async_trait;
use backstage::prelude::*;
use futures::FutureExt;
use log::{debug, info};
use serde::{Deserialize, Serialize};
use std::{marker::PhantomData, sync::Arc, time::Duration};
use tokio::sync::RwLock;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum HelloWorldEvent {
    Print(String),
}

#[derive(Debug)]
pub struct HelloWorld<Sup> {
    name: String,
    num: u32,
    _sup: PhantomData<Sup>,
}

#[build]
#[derive(Debug)]
pub fn build_hello_world<Sup>(name: String, num: Option<u32>) -> HelloWorld<Sup> {
    HelloWorld {
        name,
        num: num.unwrap_or_default(),
        _sup: PhantomData,
    }
}

impl<Sup> Clone for HelloWorldBuilder<Sup> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            num: self.num.clone(),
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<Sup: 'static + Actor + Supervisor<Self>> Actor for HelloWorld<Sup> {
    const PATH: &'static str = "hello_world";
    type Data = ();
    type Context = SupervisedContext<Self, Sup, Act<Sup>>;

    async fn init(&mut self, cx: &mut Self::Context) -> Result<Self::Data, ActorError>
    where
        Self: 'static + Sized + Send + Sync,
    {
        cx.update_status(format!("Running {}", self.num)).await.ok();
        Ok(())
    }
}

#[async_trait]
impl<Sup: 'static + Actor + Supervisor<Self>> HandleEvent<HelloWorldEvent> for HelloWorld<Sup> {
    async fn handle_event(
        &mut self,
        _cx: &mut Self::Context,
        event: HelloWorldEvent,
        _data: &mut Self::Data,
    ) -> Result<(), ActorError> {
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

#[derive(Default, Debug)]
struct Launcher {
    count: i32,
}
struct LauncherAPI;

impl LauncherAPI {
    pub async fn send_to_hello_world(&self, event: HelloWorldEvent, rt: &mut RuntimeScope) -> anyhow::Result<()> {
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
    const PATH: &'static str = "launcher";
    type Data = ();
    type Context = UnsupervisedContext<Self>;

    async fn init(&mut self, cx: &mut Self::Context) -> Result<Self::Data, ActorError>
    where
        Self: 'static + Sized + Send + Sync,
    {
        cx.update_status(ServiceStatus::Initializing).await.ok();
        let builder = HelloWorldBuilder::new().name("Hello World".to_string());
        // Start by initializing all the actors in the pool
        let mut initialized = Vec::new();
        for i in 0..5 {
            initialized.push(
                cx.init_into_pool_keyed::<MapPool<HelloWorld<Self>, i32>>(i as i32, builder.clone().num(i).build())
                    .await?,
            );
        }
        // Next, spawn them all at once
        for init in initialized {
            init.spawn(cx).await;
        }
        // An alternate way to do the same thing
        // Spawn the pool
        let mut pool = cx.spawn_pool::<MapPool<HelloWorld<Self>, i32>>().await;
        // Init all the actors into it
        for i in 5..10 {
            pool.init_keyed(i as i32, builder.clone().num(i).build()).await?;
        }
        // Finalize the pool
        pool.spawn_all().await;
        tokio::task::spawn(ctrl_c(cx.handle().clone()));
        cx.update_status(ServiceStatus::Running).await.ok();
        Ok(())
    }
}

#[async_trait]
impl HandleEvent<LauncherEvents> for Launcher {
    async fn handle_event(
        &mut self,
        cx: &mut Self::Context,
        event: LauncherEvents,
        _data: &mut Self::Data,
    ) -> Result<(), ActorError> {
        match event {
            LauncherEvents::HelloWorld(event) => {
                info!("Received event for HelloWorld");
                if let Some(pool) = cx.pool::<MapPool<HelloWorld<Self>, i32>>().await {
                    pool.send(&self.count, event)
                        .await
                        .expect("Failed to pass along message!");
                    self.count += 1;
                }
            }
            LauncherEvents::Shutdown { using_ctrl_c: _ } => {
                cx.shutdown_scope(&ROOT_SCOPE).await.ok();
            }
        }
        Ok(())
    }
}

#[async_trait]
impl HandleEvent<StatusChange<HelloWorld<Self>>> for Launcher {
    async fn handle_event(
        &mut self,
        cx: &mut Self::Context,
        event: StatusChange<HelloWorld<Self>>,
        _data: &mut Self::Data,
    ) -> Result<(), ActorError> {
        info!(
            "{} status changed ({} -> {})!",
            event.service.name(),
            event.prev_status,
            event.service.status()
        );
        debug!("\n{}", cx.root_service_tree().await.unwrap());
        Ok(())
    }
}

#[async_trait]
impl HandleEvent<Report<HelloWorld<Self>>> for Launcher {
    async fn handle_event(
        &mut self,
        cx: &mut Self::Context,
        event: Report<HelloWorld<Self>>,
        _data: &mut Self::Data,
    ) -> Result<(), ActorError> {
        match event {
            Ok(s) => {
                info!("{} {} has shutdown!", s.state.name, s.state.num);
            }
            Err(e) => {
                info!("{} {} has shutdown unexpectedly!", e.state.name, e.state.num);
                if let Some(req) = e.error.request() {
                    match req {
                        ActorRequest::Restart(_) => {
                            info!("Restarting {} {}", e.state.name, e.state.num);
                            let i = e.state.num as i32;
                            cx.spawn_into_pool_keyed::<MapPool<HelloWorld<Self>, i32>>(i, e.state)
                                .await?;
                        }
                        ActorRequest::Finish => (),
                        ActorRequest::Panic => panic!("Received request to panic....so I did"),
                    }
                }
            }
        }
        Ok(())
    }
}

impl System for Launcher {
    type State = Arc<RwLock<LauncherAPI>>;
}

async fn ctrl_c(sender: Act<Launcher>) {
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
    RuntimeScope::launch::<ActorRegistry, _, _>(|scope| {
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
