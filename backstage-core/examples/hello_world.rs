use anyhow::anyhow;
use async_trait::async_trait;
use backstage::{launcher::*, *};
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

#[build(HelloWorld)]
pub fn build_hello_world(service: Service, name: String, num: u32) {
    let (sender, inbox) = tokio::sync::mpsc::unbounded_channel::<HelloWorldEvent>();
    HelloWorld {
        inbox,
        sender: HelloWorldSender(sender),
        service,
        name,
        num,
    }
}

#[derive(Debug, Clone)]
pub struct HelloWorldSender(UnboundedSender<HelloWorldEvent>);

impl EventHandle<HelloWorldEvent> for HelloWorldSender {
    fn send(&mut self, message: HelloWorldEvent) -> anyhow::Result<()> {
        self.0.send(message).map_err(|e| anyhow!(e.to_string()))
    }

    fn shutdown(mut self) -> Option<Self> {
        if let Ok(()) = self.send(HelloWorldEvent::Shutdown) {
            None
        } else {
            Some(self)
        }
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
impl Actor for HelloWorld {
    type Error = HelloWorldError;
    type Event = HelloWorldEvent;
    type Handle = HelloWorldSender;

    fn handle(&mut self) -> &mut Self::Handle {
        &mut self.sender
    }

    fn service(&mut self) -> &mut Service {
        &mut self.service
    }

    async fn init<E, S>(&mut self, _supervisor: &mut S) -> Result<(), Self::Error>
    where
        S: 'static + Send + EventHandle<E>,
    {
        info!("Initializing {}!", self.service.name);
        Ok(())
    }

    async fn run<E, S>(&mut self, _supervisor: &mut S) -> Result<(), Self::Error>
    where
        S: 'static + Send + EventHandle<E>,
    {
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

    async fn shutdown<E, S>(&mut self, status: Result<(), Self::Error>, _supervisor: &mut S) -> Result<ActorRequest, ActorError>
    where
        S: 'static + Send + EventHandle<E>,
    {
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

#[launcher]
pub struct Apps {
    #[HelloWorld]
    hello_world: HelloWorldBuilder,
    #[HelloWorld("HelloWorld 2", depends_on(hello_world))]
    hello_world2: HelloWorldBuilder,
    #[HelloWorld(name = "Hello World 3", depends_on(hello_world, hello_world2))]
    hello_world3: HelloWorldBuilder,
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let builder = HelloWorldBuilder::new();
    Apps::new(
        builder.clone().name(Apps::hello_world_name()).num(1),
        builder.clone().name(Apps::hello_world2_name()).num(2),
        builder.name(Apps::hello_world3_name()).num(3),
    )
    .execute(|_launcher| {
        info!("Executing with launcher");
    })
    .execute_async(|launcher| async {
        info!("Executing async with launcher");
        launcher
    })
    .await
    .launch()
    .await
    .unwrap();
}
