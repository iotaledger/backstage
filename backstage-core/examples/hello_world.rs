use async_trait::async_trait;
use backstage::*;
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

    async fn run(self, mut rt: ActorRuntime<Self>) -> Result<(), ActorError>
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
    pub async fn send_to_hello_world(event: HelloWorldEvent, rt: &mut SystemRuntime<Self>) -> anyhow::Result<()> {
        Self::route(LauncherChildren::HelloWorld(event), rt).await
    }

    async fn init(&mut self, scope: &mut RuntimeScope<'_>, inbox: &mut TokioReceiver<LauncherChildren>) -> Result<(), ActorError> {
        let builder = HelloWorldBuilder::new().name("Hello World".to_string());
        scope.spawn_actor(builder.build(self.service.spawn("Hello World")));
        while let Some(evt) = inbox.recv().await {
            Self::route(evt, scope.0).await.ok();
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
enum LauncherChildren {
    HelloWorld(HelloWorldEvent),
}

#[async_trait]
impl System for Launcher {
    type ChildEvents = LauncherChildren;

    type Dependencies = ();

    type Channel = TokioChannel<Self::ChildEvents>;

    async fn run(this: std::sync::Arc<tokio::sync::RwLock<Self>>, mut rt: SystemRuntime<Self>) -> Result<(), ActorError>
    where
        Self: Sized,
    {
        rt.system_scope(|mut scope, inbox| async move {
            let builder = HelloWorldBuilder::new().name("Hello World".to_string());
            scope.spawn_actor(builder.build(this.write().await.service.spawn("Hello World")));
            while let Some(evt) = inbox.recv().await {
                Self::route(evt, scope.0).await.ok();
            }
        })
        .await;
        Ok(())
    }

    async fn route(event: Self::ChildEvents, rt: &mut BackstageRuntime) -> anyhow::Result<()>
    where
        Self: Sized,
    {
        match event {
            LauncherChildren::HelloWorld(event) => rt.send_event::<HelloWorld>(event).await,
        }
    }
}

#[tokio::main]
async fn main() {
    std::env::set_var("RUST_LOG", "info");
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
