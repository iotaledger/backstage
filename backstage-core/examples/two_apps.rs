use async_trait::async_trait;
use backstage::{prefabs::websocket::*, *};
use futures::{FutureExt, SinkExt, StreamExt};
use log::{debug, info};
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};
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
    type Channel = TokioChannel<Self::Event>;
    type Rt = BasicRuntime;
    type SupervisorEvent = ();

    async fn run<'a>(&mut self, rt: &mut ActorScopedRuntime<'a, Self>, _deps: ()) -> Result<(), ActorError>
    where
        Self: Sized,
    {
        while let Some(evt) = rt.next_event().await {
            match evt {
                HelloWorldEvent::Print(s) => {
                    info!("HelloWorld printing: {}", s);
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
    type Dependencies = (Res<Arc<RwLock<NecessaryResource>>>, Act<HelloWorld>);
    type Event = HowdyEvent;
    type Channel = TokioChannel<Self::Event>;
    type Rt = FullRuntime;
    type SupervisorEvent = ();

    async fn run<'a>(
        &mut self,
        rt: &mut ActorScopedRuntime<'a, Self>,
        (counter, mut hello_world): Self::Dependencies,
    ) -> Result<(), ActorError>
    where
        Self: Sized,
    {
        while let Some(evt) = rt.next_event().await {
            match evt {
                HowdyEvent::Print(s) => {
                    info!("Howdy printing: {}", s);
                    counter.write().await.counter += 1;
                    info!("Printed {} times", counter.read().await.counter);
                    hello_world.send(HelloWorldEvent::Print(s)).await;
                }
            }
        }
        for s in 0..4 {
            debug!("Shutting down Howdy. {} secs remaining...", 4 - s);
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        Ok(())
    }
}

pub struct NecessaryResource {
    counter: usize,
}

struct Launcher;

impl Launcher {
    pub async fn send_to_hello_world(&self, event: HelloWorldEvent, rt: &mut RuntimeScope<'_, FullRuntime>) -> anyhow::Result<()> {
        rt.send_system_event::<Self>(LauncherEvents::HelloWorld(event)).await
    }

    pub async fn send_to_howdy(&self, event: HowdyEvent, rt: &mut RuntimeScope<'_, FullRuntime>) -> anyhow::Result<()> {
        rt.send_system_event::<Self>(LauncherEvents::Howdy(event)).await
    }
}

#[derive(Debug)]
pub enum LauncherEvents {
    HelloWorld(HelloWorldEvent),
    Howdy(HowdyEvent),
    WebsocketMsg((SocketAddr, Message)),
    Status(Service),
    Report(Result<SuccessReport<LauncherChildren>, ErrorReport<LauncherChildren>>),
    Shutdown { using_ctrl_c: bool },
}

#[derive(Debug)]
pub enum LauncherChildren {
    HelloWorld(HelloWorld),
    Howdy(Howdy),
    Websocket(Arc<RwLock<Websocket>>),
}

impl From<HelloWorld> for LauncherChildren {
    fn from(h: HelloWorld) -> Self {
        Self::HelloWorld(h)
    }
}

impl From<Howdy> for LauncherChildren {
    fn from(h: Howdy) -> Self {
        Self::Howdy(h)
    }
}

impl From<Arc<RwLock<Websocket>>> for LauncherChildren {
    fn from(websocket: Arc<RwLock<Websocket>>) -> Self {
        Self::Websocket(websocket)
    }
}

impl<T: Into<LauncherChildren>> SupervisorEvent<T> for LauncherEvents {
    fn report(res: Result<SuccessReport<T>, ErrorReport<T>>) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self::Report(
            res.map(|s| SuccessReport::new(s.state.into(), s.service))
                .map_err(|e| ErrorReport::new(e.state.into(), e.service, e.error)),
        ))
    }

    fn status(service: Service) -> Self {
        Self::Status(service)
    }
}

impl From<(SocketAddr, Message)> for LauncherEvents {
    fn from(msg: (SocketAddr, Message)) -> Self {
        Self::WebsocketMsg(msg)
    }
}

impl From<()> for LauncherEvents {
    fn from(_: ()) -> Self {
        panic!()
    }
}

#[async_trait]
impl System for Launcher {
    type ChildEvents = LauncherEvents;
    type Dependencies = ();
    type Channel = TokioChannel<Self::ChildEvents>;
    type Rt = FullRuntime;
    type SupervisorEvent = ();

    async fn run<'a>(
        _this: std::sync::Arc<tokio::sync::RwLock<Self>>,
        rt: &mut SystemScopedRuntime<'a, Self>,
        _deps: (),
    ) -> Result<(), ActorError>
    where
        Self: Sized,
    {
        let my_handle = rt.my_handle();
        let hello_world_builder = HelloWorldBuilder::new("Hello World".to_string(), 1);
        let howdy_builder = HowdyBuilder::new();
        rt.spawn_actor(howdy_builder.build(), my_handle.clone()).await;
        rt.spawn_actor(hello_world_builder.build(), my_handle.clone()).await;
        rt.add_resource(Arc::new(RwLock::new(NecessaryResource { counter: 0 })));
        let (_, mut websocket_handle) = rt
            .spawn_system(
                prefabs::websocket::WebsocketBuilder::new()
                    .listen_address(([127, 0, 0, 1], 8000).into())
                    .build(),
                my_handle.clone(),
            )
            .await;
        tokio::task::spawn(ctrl_c(my_handle));
        while let Some(evt) = rt.next_event().await {
            match evt {
                LauncherEvents::HelloWorld(event) => {
                    rt.send_actor_event::<HelloWorld>(event).await;
                }
                LauncherEvents::Howdy(event) => {
                    rt.send_actor_event::<Howdy>(event).await;
                }
                LauncherEvents::Shutdown { using_ctrl_c } => {
                    debug!("Exiting launcher");
                    break;
                }
                LauncherEvents::WebsocketMsg((peer, msg)) => {
                    info!("Received websocket message: {:?}", msg);
                    websocket_handle.send(WebsocketChildren::Response((peer, "bonjour".into()))).await;
                }
                LauncherEvents::Report(res) => match res {
                    Ok(s) => {
                        info!("{} has shutdown!", s.service.name);
                        rt.service_mut().update_microservice(s.service);
                    }
                    Err(e) => {
                        info!("{} has shutdown unexpectedly!", e.service.name);
                        rt.service_mut().update_microservice(e.service);
                    }
                },
                LauncherEvents::Status(s) => {
                    rt.service_mut().update_microservice(s);
                }
            }
        }

        Ok(())
    }
}

pub async fn ctrl_c(mut sender: TokioSender<LauncherEvents>) {
    tokio::signal::ctrl_c().await.unwrap();
    let exit_program_event = LauncherEvents::Shutdown { using_ctrl_c: true };
    sender.send(exit_program_event).await.ok();
}

#[tokio::main]
async fn main() {
    std::env::set_var("RUST_LOG", "debug");
    env_logger::init();

    startup().await.unwrap();
}

async fn startup() -> anyhow::Result<()> {
    FullRuntime::default()
        .scope(|scope| {
            async move {
                scope.spawn_system_unsupervised(Launcher).await;
                scope.spawn_task(|mut rt| {
                    async move {
                        for _ in 0..3 {
                            rt.send_system_event::<Launcher>(LauncherEvents::Howdy(HowdyEvent::Print("echo".to_owned())))
                                .await
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
                });
            }
            .boxed()
        })
        .await?;
    Ok(())
}
