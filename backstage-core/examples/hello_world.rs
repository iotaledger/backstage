use async_trait::async_trait;
use backstage::*;
use futures::FutureExt;
use log::{debug, info};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum HelloWorldError {
    #[error("Something went wrong")]
    SomeError,
}

impl Into<ActorErrorKind> for HelloWorldError {
    fn into(self) -> ActorErrorKind {
        ActorErrorKind::RuntimeError(ActorRequest::Finish)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum HelloWorldEvent {
    Print(String),
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
                    info!("HelloWorld {} printing: {}", self.num, s);
                    break;
                }
            }
        }
        Ok(self.service)
    }
}

struct Launcher {
    service: Service,
}

impl Launcher {
    pub async fn send_to_hello_world(&self, event: HelloWorldEvent, rt: &mut RuntimeScope<'_, FullRuntime>) -> anyhow::Result<()> {
        rt.send_system_event::<Self, (), ()>(LauncherChildren::HelloWorld(event)).await
    }
}

#[derive(Debug)]
pub enum LauncherChildren {
    HelloWorld(HelloWorldEvent),
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

#[async_trait]
impl System<(), ()> for Launcher {
    type ChildEvents = LauncherChildren;
    type Dependencies = ();
    type Channel = TokioChannel<Self::ChildEvents>;
    type Rt = FullRuntime;

    async fn run<'a>(
        this: std::sync::Arc<tokio::sync::RwLock<Self>>,
        mut rt: SystemScopedRuntime<'a, Self, (), ()>,
        deps: (),
    ) -> Result<Service, ActorError>
    where
        Self: Sized,
    {
        let builder = HelloWorldBuilder::new().name("Hello World".to_string());
        {
            let service = &mut this.write().await.service;
            let my_handle = rt.my_handle();
            rt.spawn_pool(my_handle, |pool| {
                for i in 0..10 {
                    let service = service.spawn(format!("Hello World {}", i));
                    let (abort, handle) = pool.spawn(builder.clone().num(i).build(service));
                }
            });
        }
        tokio::task::spawn(ctrl_c(rt.my_handle()));
        while let Some(evt) = rt.next_event().await {
            match evt {
                LauncherChildren::HelloWorld(event) => {
                    info!("Received event for HelloWorld");
                    if let Some(pool) = rt.children().pool::<HelloWorld>() {
                        pool.write().await.send_all(event).await;
                    }
                }
                LauncherChildren::Shutdown { using_ctrl_c } => {
                    debug!("Exiting launcher");
                    break;
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
            scope.spawn_system(launcher, None);
            scope.spawn_task(|mut rt| {
                async move {
                    rt.system::<Launcher, _, _>()
                        .unwrap()
                        .read()
                        .await
                        .send_to_hello_world(HelloWorldEvent::Print("foo".to_owned()), &mut rt)
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
