// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use async_trait::async_trait;
use backstage::{prefabs::websocket::*, prelude::*};
use futures::{FutureExt, SinkExt, StreamExt};
use log::{debug, info};
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, sync::Arc, time::Duration};
use tokio::sync::RwLock;
use tokio_tungstenite::{connect_async, tungstenite::Message};

//////////////////////////////// HelloWorld Actor ////////////////////////////////////////////

// The HelloWorld actor's event type
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum HelloWorldEvent {
    Print(String),
}

// This is an example of a manual builder implementation.
// See the `build_howdy` fn below for a proc_macro implementation.
#[derive(Debug, Default, Clone)]
struct HelloWorldBuilder {
    name: String,
    num: u32,
}

impl HelloWorldBuilder {
    pub fn new(name: String, num: u32) -> Self {
        Self { name, num }
    }
}

impl Builder for HelloWorldBuilder {
    type Built = HelloWorld;

    fn build(self) -> HelloWorld {
        HelloWorld {
            name_num: format!("{}-{}", self.name, self.num),
        }
    }
}

// The HelloWorld actor's state, which holds
// data created by a builder when the actor
// is spawned.
#[derive(Debug)]
struct HelloWorld {
    name_num: String,
}

#[async_trait]
impl Actor for HelloWorld {
    const PATH: &'static str = "hello_world";
    type Data = u8;
    type Context = SupervisedContext<Self, Launcher, Act<Launcher>>;

    async fn init(&mut self, _cx: &mut Self::Context) -> Result<Self::Data, ActorError>
    where
        Self: 'static + Sized + Send + Sync,
    {
        Ok(0)
    }
}

#[async_trait]
impl HandleEvent<HelloWorldEvent> for HelloWorld {
    async fn handle_event(
        &mut self,
        cx: &mut Self::Context,
        event: HelloWorldEvent,
        count: &mut Self::Data,
    ) -> Result<(), ActorError> {
        match event {
            HelloWorldEvent::Print(s) => {
                info!("HelloWorld printing: {}", s);
                *count += 1;
                if *count == 3 {
                    debug!("\n{}", cx.root().service_tree().await);
                    panic!("I counted to 3!");
                }
            }
        }
        Ok(())
    }
}

//////////////////////////////// Howdy Actor ////////////////////////////////////////////

// Below is another actor type, which is identical is most ways to HelloWorld.
// However, it uses the proc_macro `build` to define the HowdyBuilder and it will
// intentionally timeout while shutting down.

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum HowdyEvent {
    Print(String),
}

#[build]
#[derive(Debug, Clone)]
fn build_howdy() -> Howdy {
    Howdy
}

#[derive(Debug)]
struct Howdy;

#[async_trait]
impl Actor for Howdy {
    const PATH: &'static str = "howdy";
    type Data = Res<Arc<RwLock<NecessaryResource>>>;
    type Context = SupervisedContext<Self, Launcher, Act<Launcher>>;

    async fn init(&mut self, cx: &mut Self::Context) -> Result<Self::Data, ActorError>
    where
        Self: 'static + Sized + Send + Sync,
    {
        Ok(cx.request_data::<Res<Arc<RwLock<NecessaryResource>>>>().await?)
    }

    async fn shutdown(&mut self, cx: &mut Self::Context, _data: &mut Self::Data) -> Result<(), ActorError>
    where
        Self: 'static + Sized + Send + Sync,
    {
        cx.update_status(ServiceStatus::Stopping).await;
        for s in 0..4 {
            debug!("Shutting down Howdy. {} secs remaining...", 4 - s);
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        cx.update_status(ServiceStatus::Stopped).await;
        Ok(())
    }
}

#[async_trait]
impl HandleEvent<HowdyEvent> for Howdy {
    async fn handle_event(
        &mut self,
        cx: &mut Self::Context,
        event: HowdyEvent,
        counter: &mut Self::Data,
    ) -> Result<(), ActorError> {
        match event {
            HowdyEvent::Print(s) => {
                info!("Howdy printing: {}", s);
                counter.write().await.counter += 1;
                info!("Printed {} times", counter.read().await.counter);
                cx.send_actor_event::<HelloWorld, _>(HelloWorldEvent::Print(s))
                    .await
                    .expect("Failed to pass along message!");
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct NecessaryResource {
    counter: usize,
}

#[derive(Debug)]
struct Launcher;
pub struct LauncherAPI;

impl LauncherAPI {
    pub async fn send_to_hello_world(&self, event: HelloWorldEvent, scope: &mut RuntimeScope) -> anyhow::Result<()> {
        scope
            .send_actor_event::<Launcher, _>(LauncherEvents::HelloWorld(event))
            .await
    }

    pub async fn send_to_howdy(&self, event: HowdyEvent, scope: &mut RuntimeScope) -> anyhow::Result<()> {
        scope
            .send_actor_event::<Launcher, _>(LauncherEvents::Howdy(event))
            .await
    }
}

#[derive(Debug)]
enum LauncherEvents {
    HelloWorld(HelloWorldEvent),
    Howdy(HowdyEvent),
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
        cx.update_status(ServiceStatus::Initializing).await;
        let hello_world_builder = HelloWorldBuilder::new("Hello World".to_string(), 1);
        let howdy_builder = HowdyBuilder::new();
        cx.add_resource(Arc::new(RwLock::new(NecessaryResource { counter: 0 })))
            .await;
        if let Err(InitError(howdy, e)) = cx.spawn_actor(howdy_builder.build()).await {
            log::error!("Failed to init Howdy actor: {}", e);
            cx.spawn_actor(howdy).await?;
        }
        cx.spawn_actor(hello_world_builder.build()).await?;
        cx.spawn_actor(Websocket::new(([127, 0, 0, 1], 8000).into())).await?;
        tokio::task::spawn(ctrl_c(cx.handle().clone()));
        cx.update_status("Launched").await;
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
                // info!("Received hello world message: {:?}", event);
                cx.send_actor_event::<HelloWorld, _>(event).await?;
            }
            LauncherEvents::Howdy(event) => {
                // info!("Received howdy message: {:?}", event);
                cx.send_actor_event::<Howdy, _>(event).await?;
            }
        }
        Ok(())
    }
}

#[async_trait]
impl HandleEvent<(SocketAddr, Message)> for Launcher {
    async fn handle_event(
        &mut self,
        cx: &mut Self::Context,
        (peer, msg): (SocketAddr, Message),
        _data: &mut Self::Data,
    ) -> Result<(), ActorError> {
        info!("Received websocket message: {:?}", msg);
        cx.send_actor_event::<Websocket<Self>, _>(WebsocketChildren::Response(peer, "bonjour".into()))
            .await?;
        Ok(())
    }
}

#[async_trait]
impl<A: 'static + Debug> HandleEvent<StatusChange<A>> for Launcher {
    async fn handle_event(
        &mut self,
        _cx: &mut Self::Context,
        _event: StatusChange<A>,
        _data: &mut Self::Data,
    ) -> Result<(), ActorError> {
        Ok(())
    }
}

#[async_trait]
impl<A: 'static + Debug + Send> HandleEvent<Report<A>> for Launcher {
    async fn handle_event(
        &mut self,
        _cx: &mut Self::Context,
        _event: Report<A>,
        _data: &mut Self::Data,
    ) -> Result<(), ActorError> {
        Ok(())
    }
}

impl System for Launcher {
    type State = Arc<RwLock<LauncherAPI>>;
}

async fn ctrl_c<A: 'static + Actor>(shutdown_handle: Act<A>) {
    tokio::signal::ctrl_c().await.unwrap();
    shutdown_handle.shutdown().await;
}

#[tokio::main]
async fn main() {
    std::env::set_var("RUST_LOG", "debug, backstage=trace");
    env_logger::init();

    startup().await.unwrap();
}

async fn startup() -> anyhow::Result<()> {
    std::panic::set_hook(Box::new(|info| {
        log::error!("{}", info);
    }));
    RuntimeScope::launch(|scope| {
        async move {
            scope
                .spawn_system_unsupervised(Launcher, Arc::new(RwLock::new(LauncherAPI)))
                .await?;
            scope
                .spawn_task(|rt| {
                    async move {
                        let launcher_handle = rt.link_data::<Act<Launcher>>().await?;
                        for _ in 0..3 {
                            launcher_handle
                                .send(LauncherEvents::Howdy(HowdyEvent::Print("echo".to_owned())))
                                .unwrap();
                        }
                        let (mut stream, _) = connect_async(url::Url::parse("ws://127.0.0.1:8000/").unwrap())
                            .await
                            .unwrap();
                        stream.send(Message::text("Hello there")).await.unwrap();
                        if let Some(Ok(msg)) = stream.next().await {
                            info!("Response from websocket: {}", msg);
                        }
                        Ok(())
                    }
                    .boxed()
                })
                .await;
            tokio::time::sleep(Duration::from_secs(1)).await;
            // scope.print_root().await;
            log::info!("Service Tree:\n{}", scope.service_tree().await);
            Ok(())
        }
        .boxed()
    })
    .await?;
    Ok(())
}
