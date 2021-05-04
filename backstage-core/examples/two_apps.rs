use anyhow::anyhow;
use async_trait::async_trait;
use backstage::{launcher::*, *};
use log::info;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

//////////////////////////////// HelloWorld App ////////////////////////////////////////////

// The HelloWorld actor's event type
#[derive(Serialize, Deserialize)]
pub enum HelloWorldEvent {
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

impl ActorBuilder<HelloWorld> for HelloWorldBuilder {
    fn build(self, service: Service) -> HelloWorld {
        let (name, num) = (self.name, self.num);
        let (sender, inbox) = tokio::sync::mpsc::unbounded_channel::<HelloWorldEvent>();
        HelloWorld {
            inbox,
            sender: HelloWorldSender(sender),
            service,
            name_num: format!("{}-{}", name, num),
        }
    }
}

// A wrapper type for a simple tokio channel which is used to pass
// events to the actor. This implements EventHandle so it can be used
// by other actors without knowing details about how this actor
// implements event handling.
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

// The HelloWorld actor's state, which holds
// data created by a builder when the actor
// is spawned.
#[derive(Debug)]
pub struct HelloWorld {
    sender: HelloWorldSender,
    inbox: UnboundedReceiver<HelloWorldEvent>,
    service: Service,
    name_num: String,
}

// The Actor implementation, which defines how this actor will
// behave.
#[async_trait]
impl Actor for HelloWorld {
    type Error = HelloWorldError;
    type Event = HelloWorldEvent;
    type Handle = HelloWorldSender;

    fn handle(&mut self) -> &mut Self::Handle {
        &mut self.sender
    }

    // The actor must define how it updates its service and sends it to
    // its supervisor. This will be called before each lifetime fn.
    async fn update_status<E, S>(&mut self, status: ServiceStatus, supervisor: &mut S)
    where
        S: 'static + Send + EventHandle<E>,
    {
        self.service.update_status(status);
        supervisor.update_status(self.service.clone()).ok();
    }

    async fn init<E, S>(&mut self, _supervisor: &mut S) -> Result<(), Self::Error>
    where
        S: 'static + Send + EventHandle<E>,
    {
        info!("Initializing {}!", self.service.name);
        Ok(())
    }

    // This actor simply waits for a shutdown signal and then exits
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

//////////////////////////////// Howdy App ////////////////////////////////////////////

// Below is another actor type, which is identical is most ways to HelloWorld.
// However, it uses the proc_macro `build` to define the HowdyBuilder.

#[derive(Serialize, Deserialize)]
pub enum HowdyEvent {
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

    async fn update_status<E, S>(&mut self, status: ServiceStatus, supervisor: &mut S)
    where
        S: 'static + Send + EventHandle<E>,
    {
        self.service.update_status(status);
        supervisor.update_status(self.service.clone()).ok();
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
                HowdyEvent::Shutdown => {
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

/// The launcher actor, defined using the `launcher` proc_macro.
/// This will construct an actor whose sole purpose is to launch
/// and oversee a set of actors.
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

    Apps::new(HelloWorldBuilder::new(Apps::hello_world_name(), 1), HowdyBuilder::new())
        .launch()
        .await
        .unwrap();
}
