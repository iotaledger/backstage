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
    pub async fn send_to_hello_world(&self, event: HelloWorldEvent, rt: &RuntimeScope) -> anyhow::Result<()> {
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
    type SupervisorEvent = ();

    async fn run<'a>(
        _this: std::sync::Arc<tokio::sync::RwLock<Self>>,
        rt: &mut SystemScopedRuntime<'a, Self>,
        _deps: (),
    ) -> Result<(), ActorError>
    where
        Self: Sized,
    {
        let builder = HelloWorldBuilder::new().name("Hello World".to_string());
        let my_handle = rt.my_handle().await;
        rt.spawn_pool(my_handle.clone(), |pool| {
            async move {
                for i in 0..10 {
                    let (_abort, _handle) = pool.spawn(builder.clone().num(i).build()).await;
                }
                Ok(())
            }
            .boxed()
        })
        .await
        .expect("Failed to create actor pool!");

        tokio::task::spawn(ctrl_c(my_handle.clone()));
        while let Some(evt) = rt.next_event().await {
            match evt {
                LauncherEvents::HelloWorld(event) => {
                    info!("Received event for HelloWorld");
                    if let Some(pool) = rt.pool::<HelloWorld>().await {
                        pool.write().await.send_all(event).await.expect("Failed to pass along message!");
                    }
                }
                LauncherEvents::Shutdown { using_ctrl_c: _ } => {
                    debug!("Exiting launcher");
                    log::debug!("Tree:\n{}", rt);
                    break;
                }
                LauncherEvents::Report(res) => match res {
                    Ok(s) => match s.state {
                        LauncherChildren::HelloWorld(h) => {
                            info!("{} {} has shutdown!", h.name, h.num);
                        }
                    },
                    Err(e) => {
                        match e.state {
                            LauncherChildren::HelloWorld(ref h) => {
                                info!("{} {} has shutdown unexpectedly!", h.name, h.num);
                            }
                        }
                        //log::debug!("Tree:\n{}", rt);
                        match e.error.request() {
                            ActorRequest::Restart => {
                                match e.state {
                                    LauncherChildren::HelloWorld(h) => {
                                        info!("Restarting {} {}", h.name, h.num);
                                        rt.spawn_into_pool(h, my_handle.clone()).await;
                                    }
                                };
                            }
                            ActorRequest::Reschedule(_) => todo!(),
                            ActorRequest::Finish => (),
                            ActorRequest::Panic => panic!("Received request to panic....so I did"),
                        }
                    }
                },
                LauncherEvents::Status(_) => {}
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
    RuntimeScope::launch(|scope| {
        async move {
            scope.spawn_system_unsupervised(Launcher).await;
            scope
                .spawn_task(|rt| {
                    async move {
                        for i in 0..10 {
                            rt.system::<Launcher>()
                                .await
                                .unwrap()
                                .read()
                                .await
                                .send_to_hello_world(HelloWorldEvent::Print(format!("foo {}", i)), &rt)
                                .await
                                .unwrap();
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                        Ok(())
                    }
                    .boxed()
                })
                .await;
        }
        .boxed()
    })
    .await?;
    Ok(())
}
