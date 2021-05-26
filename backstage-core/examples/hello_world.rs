use anyhow::anyhow;
use async_trait::async_trait;
use backstage::{launcher, launcher::*, *};
use log::info;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

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

#[build]
#[derive(Debug, Clone)]
pub fn build_hello_world(service: Service, name: String, num: Option<u32>) -> HelloWorld {
    let (sender, inbox) = tokio::sync::mpsc::unbounded_channel::<HelloWorldEvent>();
    HelloWorld {
        inbox,
        sender: HelloWorldSender(sender),
        service,
        name,
        num: num.unwrap_or_default(),
    }
}

#[derive(Debug, Clone)]
pub struct HelloWorldSender(UnboundedSender<HelloWorldEvent>);

impl EventHandle<HelloWorldEvent> for HelloWorldSender {
    fn send(&mut self, message: HelloWorldEvent) -> anyhow::Result<()> {
        self.0.send(message).map_err(|e| anyhow!(e.to_string()))
    }

    fn shutdown(&mut self) -> anyhow::Result<()> {
        self.send(HelloWorldEvent::Shutdown)
    }

    fn update_status(&mut self, _service: Service) -> anyhow::Result<()> {
        todo!()
    }
}

#[derive(Debug)]
pub struct HelloWorld {
    sender: HelloWorldSender,
    inbox: UnboundedReceiver<HelloWorldEvent>,
    service: Service,
    name: String,
    num: u32,
}

#[async_trait]
impl ActorTypes for HelloWorld {
    type Error = HelloWorldError;

    fn service(&mut self) -> &mut Service {
        &mut self.service
    }
}

impl<E, S> EventActor<E, S> for HelloWorld
where
    S: 'static + Send + EventHandle<E>,
{
    type Event = HelloWorldEvent;

    type Handle = HelloWorldSender;

    fn handle(&self) -> HelloWorldSender {
        self.sender.clone()
    }
}

#[async_trait]
impl<E, S> Init<E, S> for HelloWorld
where
    S: 'static + Send + EventHandle<E>,
{
    async fn init(&mut self, _supervisor: &mut S, rt: &mut BackstageRuntime) -> Result<(), Self::Error> {
        info!("Initializing {}!", self.service.name);
        Ok(())
    }
}

#[async_trait]
impl<E, S> Run<E, S> for HelloWorld
where
    S: 'static + Send + EventHandle<E>,
{
    async fn run(&mut self, _supervisor: &mut S, rt: &mut BackstageRuntime) -> Result<(), Self::Error> {
        info!("Running {}!", self.service.name);
        while let Some(evt) = self.inbox.recv().await {
            match evt {
                HelloWorldEvent::Shutdown => {
                    break;
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl<E, S> Shutdown<E, S> for HelloWorld
where
    S: 'static + Send + EventHandle<E>,
{
    async fn shutdown(
        &mut self,
        status: Result<(), Self::Error>,
        _supervisor: &mut S,
        rt: &mut BackstageRuntime,
    ) -> Result<ActorRequest, ActorError> {
        info!("Shutting down {}!", self.service.name);
        match status {
            std::result::Result::Ok(_) => Ok(ActorRequest::Finish),
            std::result::Result::Err(e) => Err(e.into()),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub enum HelloWorldEvent {
    Shutdown,
}

#[tokio::main]
async fn main() {
    std::env::set_var("RUST_LOG", "info");
    env_logger::init();

    startup().await.unwrap();
}

async fn startup() -> anyhow::Result<()> {
    let builder = HelloWorldBuilder::new();
    launcher!(HelloWorldBuilder)
        .add(
            "HelloWorld",
            builder.clone().name("HelloWorld".to_owned()).num(1),
            &["HelloWorld 3"],
            None,
        )?
        .name("HelloWorld 2")
        .builder(builder.clone().name("HelloWorld 2".to_owned()).num(Some(2)))?
        .name("HelloWorld 3")
        .dep("HelloWorld 2")
        .builder(builder.name("HelloWorld 3".to_owned()))?
        .execute(|_launcher| {
            info!("Executing with launcher");
        })
        .execute_async(|launcher| async {
            info!("Executing async with launcher");
            launcher
        })
        .await
        .launch()
        .await?;
    Ok(())
}
