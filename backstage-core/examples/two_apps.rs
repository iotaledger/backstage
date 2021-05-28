use anyhow::anyhow;
use async_trait::async_trait;
use backstage::*;
use futures::FutureExt;
use log::{debug, info};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use thiserror::Error;

//////////////////////////////// HelloWorld Actor ////////////////////////////////////////////

// The HelloWorld actor's event type
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum HelloWorldEvent {
    Print(String),
    Shutdown,
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

impl ActorBuilder for HelloWorldBuilder {
    type BuiltActor = HelloWorld;

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
impl Actor for HelloWorld {
    type Dependencies = ();
    type Event = HelloWorldEvent;
    type Channel = TokioChannel<Self::Event>;

    async fn run<'a>(self, mut rt: ActorRuntime<'a, Self>) -> Result<(), ActorError>
    where
        Self: Sized,
    {
        while let Some(evt) = rt.next_event().await {
            match evt {
                HelloWorldEvent::Shutdown => break,
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
    Shutdown,
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
pub fn build_howdy<LauncherEvent, LauncherSender>(service: Service) -> Howdy {
    Howdy { service }
}

#[derive(Debug)]
pub struct Howdy {
    service: Service,
}

#[async_trait]
impl Actor for Howdy {
    type Dependencies = ();
    type Event = HowdyEvent;
    type Channel = TokioChannel<Self::Event>;

    async fn run<'a>(self, mut rt: ActorRuntime<'a, Self>) -> Result<(), ActorError>
    where
        Self: Sized,
    {
        while let Some(evt) = rt.next_event().await {
            match evt {
                HowdyEvent::Shutdown => {
                    for s in 0..4 {
                        debug!("Shutting down Howdy. {} secs remaining...", 4 - s);
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                    break;
                }
                HowdyEvent::Print(s) => {
                    info!("Howdy printing: {}", s);
                }
            }
        }
        Ok(())
    }
}

struct Launcher {
    service: Service,
}

impl Launcher {
    pub async fn send_to_hello_world(&self, event: HelloWorldEvent, rt: &mut RuntimeScope<'_>) -> anyhow::Result<()> {
        rt.send_system_event::<Self>(LauncherChildren::HelloWorld(event)).await
    }

    pub async fn send_to_howdy(&self, event: HowdyEvent, rt: &mut RuntimeScope<'_>) -> anyhow::Result<()> {
        rt.send_system_event::<Self>(LauncherChildren::Howdy(event)).await
    }
}

#[derive(Clone, Debug)]
pub enum LauncherChildren {
    HelloWorld(HelloWorldEvent),
    Howdy(HowdyEvent),
    Shutdown { using_ctrl_c: bool },
}

#[async_trait]
impl System for Launcher {
    type ChildEvents = LauncherChildren;

    type Dependencies = ();

    type Channel = TokioChannel<Self::ChildEvents>;

    async fn run<'a>(this: std::sync::Arc<tokio::sync::RwLock<Self>>, mut rt: SystemRuntime<'a, Self>) -> Result<(), ActorError>
    where
        Self: Sized,
    {
        let hello_world_builder = HelloWorldBuilder::new("Hello World".to_string(), 1);
        let hello_world_service = this.write().await.service.spawn("Hello World");
        let howdy_builder = HowdyBuilder::new();
        let howdy_world_service = this.write().await.service.spawn("Howdy");
        rt.spawn_actor(hello_world_builder.build(hello_world_service));
        rt.spawn_actor(howdy_builder.build(howdy_world_service));
        tokio::task::spawn(ctrl_c(rt.my_handle().await));
        while let Some(evt) = rt.next_event().await {
            match evt {
                LauncherChildren::HelloWorld(event) => {
                    rt.send_actor_event::<HelloWorld>(event).await;
                }
                LauncherChildren::Howdy(event) => {
                    rt.send_actor_event::<Howdy>(event).await;
                }
                LauncherChildren::Shutdown { using_ctrl_c } => {
                    debug!("Exiting launcher");
                    rt.send_actor_event::<HelloWorld>(HelloWorldEvent::Shutdown).await;
                    rt.send_actor_event::<Howdy>(HowdyEvent::Shutdown).await;
                    break;
                }
            }
        }

        Ok(())
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
    BackstageRuntime::new()
        .scope(|scope| {
            let service = Service::new("Launcher");
            let launcher = Launcher { service };
            scope.spawn_system(launcher);
            scope.spawn_task(|mut rt| {
                async move {
                    rt.system::<Launcher>()
                        .unwrap()
                        .read()
                        .await
                        .send_to_hello_world(HelloWorldEvent::Print("foo".to_owned()), &mut rt)
                        .await
                        .unwrap();
                    rt.send_system_event::<Launcher>(LauncherChildren::Howdy(HowdyEvent::Print("bar".to_owned())))
                        .await
                        .unwrap();
                    Ok(())
                }
                .boxed()
            });
        })
        .await?;
    Ok(())
}
