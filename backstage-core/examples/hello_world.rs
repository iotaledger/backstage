use std::time::Duration;

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

impl Into<ActorError> for HelloWorldError {
    fn into(self) -> ActorError {
        ActorError::RuntimeError(ActorRequest::Finish)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum HelloWorldEvent {
    Print(String),
}

#[derive(Debug)]
pub struct HelloWorld {
    name: String,
    num: u32,
}

#[build]
#[derive(Debug, Clone)]
pub fn build_hello_world(name: String, num: Option<u32>) -> HelloWorld {
    HelloWorld {
        name,
        num: num.unwrap_or_default(),
    }
}

#[async_trait]
impl Actor for HelloWorld {
    type Dependencies = ();
    type Event = HelloWorldEvent;
    type Channel = TokioChannel<Self::Event>;
    type Rt = BasicRuntime;
    type SupervisorEvent = ();

    async fn run<'a>(&mut self, rt: &mut ActorScopedRuntime<'a, Self>, _deps: Self::Dependencies) -> Result<(), ActorError>
    where
        Self: Sized,
    {
        while let Some(evt) = rt.next_event().await {
            match evt {
                HelloWorldEvent::Print(s) => {
                    info!("HelloWorld {} printing: {}", self.num, s);
                    if rand::random() {
                        panic!("Random panic attack!")
                    }
                }
            }
        }
        Ok(())
    }
}

struct Launcher;

impl Launcher {
    pub async fn send_to_hello_world(&self, event: HelloWorldEvent, rt: &mut RuntimeScope<'_, FullRuntime>) -> anyhow::Result<()> {
        rt.send_system_event::<Self>(LauncherEvents::HelloWorld(event)).await
    }
}

#[derive(Debug)]
pub enum LauncherEvents {
    HelloWorld(HelloWorldEvent),
    Status(Service),
    Report(Result<SuccessReport<LauncherChildren>, ErrorReport<LauncherChildren>>),
    Shutdown { using_ctrl_c: bool },
}

#[derive(Debug)]
pub enum LauncherChildren {
    HelloWorld(HelloWorld),
}

impl SupervisorEvent<HelloWorld> for LauncherEvents {
    fn report(res: Result<SuccessReport<HelloWorld>, ErrorReport<HelloWorld>>) -> anyhow::Result<Self> {
        Ok(Self::Report(
            res.map(|s| SuccessReport::new(LauncherChildren::HelloWorld(s.state), s.service))
                .map_err(|e| ErrorReport::new(LauncherChildren::HelloWorld(e.state), e.service, e.error)),
        ))
    }

    fn status(service: Service) -> Self {
        Self::Status(service)
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
        this: std::sync::Arc<tokio::sync::RwLock<Self>>,
        rt: &mut SystemScopedRuntime<'a, Self>,
        _deps: (),
    ) -> Result<(), ActorError>
    where
        Self: Sized,
    {
        let builder = HelloWorldBuilder::new().name("Hello World".to_string());
        {
            let my_handle = rt.my_handle();
            rt.spawn_pool(my_handle, |pool| {
                for i in 0..10 {
                    let (_abort, _handle) = pool.spawn(builder.clone().num(i).build());
                }
            });
        }
        tokio::task::spawn(ctrl_c(rt.my_handle()));
        while let Some(evt) = rt.next_event().await {
            match evt {
                LauncherEvents::HelloWorld(event) => {
                    info!("Received event for HelloWorld");
                    if let Some(pool) = rt.pool::<HelloWorld>().await {
                        pool.write().await.send_all(event).await;
                    }
                }
                LauncherEvents::Shutdown { using_ctrl_c } => {
                    debug!("Exiting launcher");
                    break;
                }
                LauncherEvents::Report(res) => match res {
                    Ok(s) => {
                        match s.state {
                            LauncherChildren::HelloWorld(h) => {
                                info!("{} {} has shutdown!", h.name, h.num);
                            }
                        }
                        rt.service_mut().update_microservice(s.service).ok();
                    }
                    Err(e) => {
                        match e.state {
                            LauncherChildren::HelloWorld(ref h) => {
                                info!("{} {} has shutdown unexpectedly!", h.name, h.num);
                            }
                        }
                        rt.service_mut().update_microservice(e.service.clone()).ok();
                        match e.error.request() {
                            ActorRequest::Restart => {
                                if let Some(_) = rt.service_mut().remove_microservice(&e.service.id) {
                                    let num = match e.state {
                                        LauncherChildren::HelloWorld(ref h) => {
                                            info!("Restarting {} {}", h.name, h.num);
                                            h.num
                                        }
                                    };
                                    let my_handle = rt.my_handle();
                                    rt.spawn_into_pool(builder.clone().num(num).build(), my_handle).await;
                                }
                            }
                            ActorRequest::Reschedule(_) => todo!(),
                            ActorRequest::Finish => (),
                            ActorRequest::Panic => panic!("Received request to panic....so I did"),
                        }
                    }
                },
                LauncherEvents::Status(s) => {
                    rt.service_mut().update_microservice(s).ok();
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
    std::panic::set_hook(Box::new(|info| {
        log::error!("{}", info);
    }));
    FullRuntime::new()
        .scope(|scope| {
            scope.spawn_system_unsupervised(Launcher);
            scope.spawn_task(|mut rt| {
                async move {
                    for _ in 0..10 {
                        rt.system::<Launcher>()
                            .unwrap()
                            .read()
                            .await
                            .send_to_hello_world(HelloWorldEvent::Print("foo".to_owned()), &mut rt)
                            .await
                            .unwrap();
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                    Ok(())
                }
                .boxed()
            });
        })
        .await?;
    Ok(())
}
