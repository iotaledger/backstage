use async_trait::async_trait;
use backstage::{prefabs::websocket::*, prelude::*};
use backstage_macros::supervise;
use futures::{FutureExt, SinkExt, StreamExt};
use log::{debug, info};
use serde::{Deserialize, Serialize};
use std::{convert::TryFrom, marker::PhantomData, sync::Arc, time::Duration};
use thiserror::Error;
use tokio::sync::RwLock;
use tokio_tungstenite::{connect_async, tungstenite::Message};

//////////////////////////////// HelloWorld Actor ////////////////////////////////////////////

// The HelloWorld actor's event type
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum HelloWorldEvent {
    Print(String),
}

// The possible errors that a HelloWorld actor can have
#[derive(Error, Debug)]
pub enum HelloWorldError {
    #[error("Something went wrong")]
    SomeError,
}

// In order for an actor to make use of a custom error type,
// it should be convertable to an `ActorError` with an
// associated `ActorRequest` specifying how the supervisor
// should handle the error.
impl Into<ActorError> for HelloWorldError {
    fn into(self) -> ActorError {
        ActorError::RuntimeError(ActorRequest::Finish)
    }
}

// This is an example of a manual builder implementation.
// See the `build_howdy` fn below for a proc_macro implementation.
#[derive(Debug, Default, Clone)]
pub struct HelloWorldBuilder {
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
pub struct HelloWorld {
    name_num: String,
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
        _deps: (),
    ) -> Result<(), ActorError>
    where
        Self: Sized,
        Sup::Event: SupervisorEvent,
        <Sup::Event as SupervisorEvent>::Children: From<PhantomData<Self>>,
    {
        rt.update_status(ServiceStatus::Running).await.ok();
        let mut count = 0;
        while let Some(evt) = rt.next_event().await {
            match evt {
                HelloWorldEvent::Print(s) => {
                    info!("HelloWorld printing: {}", s);
                    count += 1;
                    if count == 3 {
                        rt.print_root().await;
                        panic!("I counted to 3!");
                    }
                }
            }
        }
        rt.update_status(ServiceStatus::Stopped).await.ok();
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
#[derive(Error, Debug)]
pub enum HowdyError {
    #[error("Something went wrong")]
    SomeError,
}

impl Into<ActorError> for HowdyError {
    fn into(self) -> ActorError {
        ActorError::RuntimeError(ActorRequest::Finish)
    }
}

#[build]
#[derive(Debug, Clone)]
pub fn build_howdy() -> Howdy {
    Howdy
}

#[derive(Debug)]
pub struct Howdy;

#[async_trait]
impl Actor for Howdy {
    type Dependencies = ();
    type Event = HowdyEvent;
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
        _: Self::Dependencies,
    ) -> Result<(), ActorError>
    where
        Self: Sized,
        Sup::Event: SupervisorEvent,
        <Sup::Event as SupervisorEvent>::Children: From<PhantomData<Self>>,
    {
        let (counter, hello_world) = rt.link_data::<(Res<Arc<RwLock<NecessaryResource>>>, Act<HelloWorld>)>().await?;
        rt.update_status(ServiceStatus::Running).await.ok();
        while let Some(evt) = rt.next_event().await {
            match evt {
                HowdyEvent::Print(s) => {
                    info!("Howdy printing: {}", s);
                    counter.write().await.counter += 1;
                    info!("Printed {} times", counter.read().await.counter);
                    hello_world.send(HelloWorldEvent::Print(s)).expect("Failed to pass along message!");
                }
            }
        }
        rt.update_status(ServiceStatus::Stopping).await.ok();
        for s in 0..4 {
            debug!("Shutting down Howdy. {} secs remaining...", 4 - s);
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        rt.update_status(ServiceStatus::Stopped).await.ok();
        Ok(())
    }
}

pub struct NecessaryResource {
    counter: usize,
}

struct Launcher;
pub struct LauncherAPI;

impl LauncherAPI {
    pub async fn send_to_hello_world<Reg: 'static + RegistryAccess + Send + Sync>(
        &self,
        event: HelloWorldEvent,
        rt: &mut RuntimeScope<Reg>,
    ) -> anyhow::Result<()> {
        rt.send_actor_event::<Launcher>(LauncherEvents::HelloWorld(event)).await
    }

    pub async fn send_to_howdy<Reg: 'static + RegistryAccess + Send + Sync>(
        &self,
        event: HowdyEvent,
        rt: &mut RuntimeScope<Reg>,
    ) -> anyhow::Result<()> {
        rt.send_actor_event::<Launcher>(LauncherEvents::Howdy(event)).await
    }
}

#[supervise(HelloWorld, Howdy, Websocket<Launcher>)]
enum LauncherEvents {
    HelloWorld(HelloWorldEvent),
    Howdy(HowdyEvent),
    WebsocketMsg(SocketAddr, String),
}

impl TryFrom<(SocketAddr, Message)> for LauncherEvents {
    type Error = anyhow::Error;

    fn try_from((addr, msg): (SocketAddr, Message)) -> Result<Self, Self::Error> {
        match msg {
            Message::Text(t) => Ok(LauncherEvents::WebsocketMsg(addr, t)),
            _ => anyhow::bail!("Invalid message!"),
        }
    }
}

#[async_trait]
impl Actor for Launcher {
    type Event = LauncherEvents;
    type Dependencies = ();
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
        let hello_world_builder = HelloWorldBuilder::new("Hello World".to_string(), 1);
        let howdy_builder = HowdyBuilder::new();
        rt.spawn_actor(howdy_builder.build()).await?;
        rt.spawn_actor(hello_world_builder.build()).await?;
        rt.add_resource(Arc::new(RwLock::new(NecessaryResource { counter: 0 }))).await;
        rt.spawn_actor(
            WebsocketBuilder::new()
                .listen_address(([127, 0, 0, 1], 8000).into())
                .supervisor_handle(rt.handle())
                .build(),
        )
        .await?;
        tokio::task::spawn(ctrl_c(rt.shutdown_handle()));
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
        rt.update_status("Launched").await.ok();
        let websocket_handle = rt
            .actor_event_handle::<Websocket<Self>>()
            .await
            .ok_or_else(|| anyhow::anyhow!("No websocket!"))?;
        while let Some(evt) = rt.next_event().await {
            match evt {
                LauncherEvents::HelloWorld(event) => {
                    // info!("Received hello world message: {:?}", event);
                    rt.send_actor_event::<HelloWorld>(event).await?;
                }
                LauncherEvents::Howdy(event) => {
                    // info!("Received howdy message: {:?}", event);
                    rt.send_actor_event::<Howdy>(event).await?;
                }
                LauncherEvents::WebsocketMsg(peer, msg) => {
                    info!("Received websocket message: {:?}", msg);
                    websocket_handle.send(WebsocketChildren::Response(peer, "bonjour".into()))?;
                }
                LauncherEvents::ReportExit(res) => match res {
                    Ok(s) => {}
                    Err(e) => match e.state {
                        ChildStates::HelloWorld(h) => {}
                        ChildStates::Howdy(h) => {}
                        ChildStates::Websocket(w) => {}
                    },
                },
                LauncherEvents::StatusChange(s) => match s.actor_type {
                    Children::HelloWorld => {}
                    Children::Howdy => {}
                    Children::Websocket => {}
                },
            }
        }
        debug!("Exiting launcher");
        rt.update_status(ServiceStatus::Stopped).await.ok();
        Ok(())
    }
}

impl System for Launcher {
    type State = Arc<RwLock<LauncherAPI>>;
}

async fn ctrl_c(shutdown_handle: ShutdownHandle) {
    tokio::signal::ctrl_c().await.unwrap();
    shutdown_handle.shutdown();
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
                        let launcher_handle = rt.link_data::<Act<Launcher>>().await?;
                        for _ in 0..3 {
                            launcher_handle
                                .send(LauncherEvents::Howdy(HowdyEvent::Print("echo".to_owned())))
                                .unwrap();
                        }
                        let (mut stream, _) = connect_async(url::Url::parse("ws://127.0.0.1:8000/").unwrap()).await.unwrap();
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
