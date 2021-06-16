use async_trait::async_trait;
use backstage::*;
use futures::{FutureExt, SinkExt};
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
impl Into<ActorErrorKind> for HelloWorldError {
    fn into(self) -> ActorErrorKind {
        ActorErrorKind::RuntimeError(ActorRequest::Finish)
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

    fn build(self, service: Service) -> HelloWorld {
        HelloWorld {
            service,
            name_num: format!("{}-{}", self.name, self.num),
        }
    }
}

// The HelloWorld actor's state, which holds
// data created by a builder when the actor
// is spawned.
#[derive(Debug)]
pub struct HelloWorld {
    service: Service,
    name_num: String,
}

#[async_trait]
impl<H, E> Actor<H, E> for HelloWorld
where
    H: 'static + Sender<E> + Clone + Send + Sync,
    E: 'static + SupervisorEvent + Send + Sync,
{
    type Dependencies = ();
    type Event = HelloWorldEvent;
    type Channel = TokioChannel<Self::Event>;
    type Rt = BasicRuntime;

    async fn run<'a>(self, mut rt: ActorScopedRuntime<'a, Self, H, E>, deps: ()) -> Result<Service, ActorError>
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
        Ok(self.service)
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

impl Into<ActorErrorKind> for HowdyError {
    fn into(self) -> ActorErrorKind {
        ActorErrorKind::RuntimeError(ActorRequest::Finish)
    }
}

#[build]
#[derive(Debug, Clone)]
pub fn build_howdy(service: Service) -> Howdy {
    Howdy { service }
}

#[derive(Debug)]
pub struct Howdy {
    service: Service,
}

#[async_trait]
impl<H, E> Actor<H, E> for Howdy
where
    H: 'static + Sender<E> + Clone + Send + Sync,
    E: 'static + SupervisorEvent + Send + Sync,
{
    type Dependencies = (Res<Arc<RwLock<NecessaryResource>>>, Act<HelloWorld, H, E>);
    type Event = HowdyEvent;
    type Channel = TokioChannel<Self::Event>;
    type Rt = FullRuntime;

    async fn run<'a>(
        self,
        mut rt: ActorScopedRuntime<'a, Self, H, E>,
        (counter, mut hello_world): Self::Dependencies,
    ) -> Result<Service, ActorError>
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
        Ok(self.service)
    }
}

pub struct NecessaryResource {
    counter: usize,
}

struct Launcher {
    service: Service,
}

impl Launcher {
    pub async fn send_to_hello_world<H, E>(&self, event: HelloWorldEvent, rt: &mut RuntimeScope<'_, FullRuntime>) -> anyhow::Result<()>
    where
        H: 'static + Sender<E> + Clone + Send + Sync,
        E: 'static + SupervisorEvent + Send + Sync,
    {
        rt.send_system_event::<Self, H, E>(LauncherChildren::HelloWorld(event)).await
    }

    pub async fn send_to_howdy<H, E>(&self, event: HowdyEvent, rt: &mut RuntimeScope<'_, FullRuntime>) -> anyhow::Result<()>
    where
        H: 'static + Sender<E> + Clone + Send + Sync,
        E: 'static + SupervisorEvent + Send + Sync,
    {
        rt.send_system_event::<Self, H, E>(LauncherChildren::Howdy(event)).await
    }
}

#[derive(Debug)]
pub enum LauncherChildren {
    HelloWorld(HelloWorldEvent),
    Howdy(HowdyEvent),
    WebsocketMsg(Message),
    Status(Service),
    Report(Result<Service, ActorError>),
    Shutdown { using_ctrl_c: bool },
}

impl SupervisorEvent for LauncherChildren {
    fn report(res: Result<Service, ActorError>) -> Self {
        Self::Report(res)
    }

    fn status(service: Service) -> Self {
        Self::Status(service)
    }
}

impl From<Message> for LauncherChildren {
    fn from(msg: Message) -> Self {
        Self::WebsocketMsg(msg)
    }
}

#[async_trait]
impl<H, E> System<H, E> for Launcher
where
    H: 'static + Sender<E> + Clone + Send + Sync,
    E: 'static + SupervisorEvent + Send + Sync,
{
    type ChildEvents = LauncherChildren;
    type Dependencies = ();
    type Channel = TokioChannel<Self::ChildEvents>;
    type Rt = FullRuntime;

    async fn run<'a>(
        this: std::sync::Arc<tokio::sync::RwLock<Self>>,
        mut rt: SystemScopedRuntime<'a, Self, H, E>,
        deps: (),
    ) -> Result<Service, ActorError>
    where
        Self: Sized,
    {
        let my_handle = rt.my_handle();
        let hello_world_builder = HelloWorldBuilder::new("Hello World".to_string(), 1);
        let howdy_builder = HowdyBuilder::new();
        rt.spawn_actor(
            hello_world_builder.build(this.write().await.service.spawn("Hello World")),
            my_handle.clone(),
        );
        rt.add_resource(Arc::new(RwLock::new(NecessaryResource { counter: 0 })));
        rt.spawn_actor(howdy_builder.build(this.write().await.service.spawn("Howdy")), my_handle.clone());
        rt.spawn_system(
            prefabs::websocket::WebsocketBuilder::new()
                .listen_address(([127, 0, 0, 1], 8000).into())
                .build(this.write().await.service.spawn("Websocket")),
            my_handle.clone(),
        );
        tokio::task::spawn(ctrl_c(my_handle));
        while let Some(evt) = rt.next_event().await {
            match evt {
                LauncherChildren::HelloWorld(event) => {
                    rt.children().send_actor_event::<HelloWorld>(event).await;
                }
                LauncherChildren::Howdy(event) => {
                    rt.children().send_actor_event::<Howdy>(event).await;
                }
                LauncherChildren::Shutdown { using_ctrl_c } => {
                    debug!("Exiting launcher");
                    break;
                }
                LauncherChildren::WebsocketMsg(msg) => {
                    info!("Received websocket message: {:?}", msg);
                }
                LauncherChildren::Report(res) => match res {
                    Ok(s) => {
                        info!("{} has shutdown!", s.name);
                        this.write().await.service.update_microservice(s);
                    }
                    Err(e) => {
                        info!("{} has shutdown unexpectedly!", e.service.name);
                        this.write().await.service.update_microservice(e.service);
                    }
                },
                LauncherChildren::Status(s) => {
                    this.write().await.service.update_microservice(s);
                }
            }
        }

        Ok(this.read().await.service.clone())
    }
}

pub async fn ctrl_c(mut sender: TokioSender<LauncherChildren>) {
    tokio::signal::ctrl_c().await.unwrap();
    let exit_program_event = LauncherChildren::Shutdown { using_ctrl_c: true };
    sender.send(exit_program_event).await.ok();
}

#[tokio::main]
async fn main() {
    std::env::set_var("RUST_LOG", "debug");
    env_logger::init();

    startup().await.unwrap();
}

async fn startup() -> anyhow::Result<()> {
    FullRuntime::new()
        .scope(|scope| {
            let service = Service::new("Launcher");
            let launcher = Launcher { service };
            scope.spawn_system_unsupervised(launcher);
            scope.spawn_task(|mut rt| {
                async move {
                    for _ in 0..3 {
                        rt.send_system_event::<Launcher, (), ()>(LauncherChildren::Howdy(HowdyEvent::Print("echo".to_owned())))
                            .await
                            .unwrap();
                    }
                    let (mut stream, _) = connect_async(url::Url::parse("ws://127.0.0.1:8000/").unwrap()).await.unwrap();
                    stream.send(Message::text("Hello there")).await.unwrap();
                    Ok(())
                }
                .boxed()
            });
        })
        .await?;
    Ok(())
}
