use async_trait::async_trait;
use backstage::*;
use log::debug;
use serde::{Deserialize, Serialize};
use thiserror::Error;

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
    Shutdown,
}

#[derive(Debug)]
pub struct HelloWorld {
    service: Service,
    name: String,
    num: u32,
}

#[build]
#[derive(Debug, Clone)]
pub fn build_hello_world(service: Service, name: String, num: Option<u32>) -> HelloWorld {
    HelloWorld {
        service,
        name,
        num: num.unwrap_or_default(),
    }
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
            }
        }
        Ok(())
    }
}

struct Launcher {
    service: Service,
}

impl Launcher {
    pub async fn send_to_hello_world(event: HelloWorldEvent) -> anyhow::Result<()> {
        Self::route(LauncherChildren::HelloWorld(event)).await
    }
}

#[derive(Clone, Debug)]
pub enum LauncherChildren {
    HelloWorld(HelloWorldEvent),
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
        let builder = HelloWorldBuilder::new().name("Hello World".to_string());
        let service = this.write().await.service.spawn("Hello World");
        rt.spawn_actor(builder.build(service));
        tokio::task::spawn(ctrl_c(rt.my_handle().await));
        while let Some(evt) = rt.next_event().await {
            if let LauncherChildren::Shutdown { using_ctrl_c } = evt {
                debug!("Exiting launcher");
                rt.send_event::<HelloWorld>(HelloWorldEvent::Shutdown).await;
                break;
            }
            Launcher::route(evt).await.ok();
        }

        Ok(())
    }

    async fn route(event: Self::ChildEvents) -> anyhow::Result<()>
    where
        Self: Sized,
    {
        // match event {
        //    LauncherChildren::HelloWorld(event) => rt.send_event::<HelloWorld>(event).await,
        //}
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
        })
        .await?;
    Ok(())
}
