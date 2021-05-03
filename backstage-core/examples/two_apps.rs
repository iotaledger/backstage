use anyhow::anyhow;
use async_trait::async_trait;
use backstage::newactor::{launcher::*, *};
use backstage_macros::{build, launcher};
use log::info;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

//////////////////////////////// HelloWorld App ////////////////////////////////////////////
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

    fn update_status<E, S>(&mut self, status: ServiceStatus, supervisor: &mut S)
    where
        S: 'static + Send + EventHandle<E>,
    {
        self.service.update_status(status);
        supervisor.update_status(self.service.clone()).ok();
    }

    async fn init<E, S>(&mut self, supervisor: &mut S) -> Result<(), Self::Error>
    where
        S: 'static + Send + EventHandle<E>,
    {
        info!("Initializing {}!", self.service.name);
        Ok(())
    }

    async fn run<E, S>(&mut self, supervisor: &mut S) -> Result<(), Self::Error>
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

    async fn shutdown<E, S>(&mut self, status: Result<(), Self::Error>, supervisor: &mut S) -> Result<ActorRequest, ActorError>
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

//////////////////////////////// Howdy App ////////////////////////////////////////////

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

#[build(Howdy)]
pub fn build_howdy(service: Service) {
    let (sender, inbox) = tokio::sync::mpsc::unbounded_channel::<HowdyEvent>();
    Howdy {
        inbox,
        sender: HowdySender(sender),
        service,
    }
}

#[derive(Debug, Clone)]
pub struct HowdySender(UnboundedSender<HowdyEvent>);

impl EventHandle<HowdyEvent> for HowdySender {
    fn send(&mut self, message: HowdyEvent) -> anyhow::Result<()> {
        self.0.send(message).map_err(|e| anyhow!(e.to_string()))
    }

    fn shutdown(mut self) -> Option<Self> {
        if let Ok(()) = self.send(HowdyEvent::Shutdown) {
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
pub struct Howdy {
    sender: HowdySender,
    inbox: UnboundedReceiver<HowdyEvent>,
    service: Service,
}

#[async_trait]
impl Actor for Howdy {
    type Error = HowdyError;
    type Event = HowdyEvent;
    type Handle = HowdySender;

    fn handle(&mut self) -> &mut Self::Handle {
        &mut self.sender
    }

    fn update_status<E, S>(&mut self, status: ServiceStatus, supervisor: &mut S)
    where
        S: 'static + Send + EventHandle<E>,
    {
        self.service.update_status(status);
        supervisor.update_status(self.service.clone()).ok();
    }

    async fn init<E, S>(&mut self, supervisor: &mut S) -> Result<(), Self::Error>
    where
        S: 'static + Send + EventHandle<E>,
    {
        info!("Initializing {}!", self.service.name);
        Ok(())
    }

    async fn run<E, S>(&mut self, supervisor: &mut S) -> Result<(), Self::Error>
    where
        S: 'static + Send + EventHandle<E>,
    {
        info!("Running {}!", self.service.name);
        while let Some(evt) = self.inbox.recv().await {
            match evt {
                HowdyEvent::Shutdown => {
                    break;
                }
            }
        }
        Ok(())
    }

    async fn shutdown<E, S>(&mut self, status: Result<(), Self::Error>, supervisor: &mut S) -> Result<ActorRequest, ActorError>
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
pub enum HowdyEvent {
    Shutdown,
}

#[launcher]
pub struct Apps {
    #[HelloWorld]
    hello_world: HelloWorldBuilder,
    #[Howdy("Howdy App", depends_on(hello_world))]
    howdy: HowdyBuilder,
}

#[tokio::main]
async fn main() {
    env_logger::init();

    Apps::new(HelloWorldBuilder::new().name(Apps::hello_world_name()).num(1), HowdyBuilder::new())
        .launch()
        .await
        .unwrap();
}
