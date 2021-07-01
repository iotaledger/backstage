use async_trait::async_trait;
use backstage::prelude::*;
use futures::FutureExt;
use log::{debug, info};
use serde::{Deserialize, Serialize};
use std::{marker::PhantomData, sync::Arc, time::Duration};
use thiserror::Error;
use tokio::sync::RwLock;

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

    async fn run<'a, Reg: 'static + RegistryAccess + Send + Sync, Sup: EventDriven + Supervisor>(
        &mut self,
        rt: &mut ActorScopedRuntime<'a, Self, Reg, Sup>,
        _deps: Self::Dependencies,
    ) -> Result<(), ActorError>
    where
        Self: Sized,
        Sup::Children: From<PhantomData<Self>>,
    {
        rt.update_status(ServiceStatus::Running).await;
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
        rt.update_status(ServiceStatus::Stopped).await;
        Ok(())
    }
}

struct Launcher;
struct LauncherAPI;

impl LauncherAPI {
    pub async fn send_to_hello_world<Reg: 'static + RegistryAccess + Send + Sync>(
        &self,
        event: HelloWorldEvent,
        rt: &mut RuntimeScope<Reg>,
    ) -> anyhow::Result<()> {
        rt.send_actor_event::<Launcher>(LauncherEvents::HelloWorld(event)).await
    }
}

#[derive(Debug)]
pub enum LauncherEvents {
    HelloWorld(HelloWorldEvent),
    Status(StatusChange<LauncherChildren>),
    Report(Result<SuccessReport<LauncherChildrenStates>, ErrorReport<LauncherChildrenStates>>),
    Shutdown { using_ctrl_c: bool },
}

#[derive(Debug)]
pub enum LauncherChildrenStates {
    HelloWorld(HelloWorld),
}

// This will become part of a proc macro later
impl From<HelloWorld> for LauncherChildrenStates {
    fn from(h: HelloWorld) -> Self {
        LauncherChildrenStates::HelloWorld(h)
    }
}

#[derive(Debug, Copy, Clone)]
pub enum LauncherChildren {
    HelloWorld,
}

// This will become part of a proc macro later
impl From<PhantomData<HelloWorld>> for LauncherChildren {
    fn from(_: PhantomData<HelloWorld>) -> Self {
        LauncherChildren::HelloWorld
    }
}

// This might become part of a proc macro later?
impl Supervisor for Launcher {
    type ChildStates = LauncherChildrenStates;
    type Children = LauncherChildren;

    fn report(res: Result<SuccessReport<Self::ChildStates>, ErrorReport<Self::ChildStates>>) -> anyhow::Result<Self::Event>
    where
        Self: Sized,
    {
        Ok(LauncherEvents::Report(res))
    }

    fn status_change(status_change: StatusChange<Self::Children>) -> anyhow::Result<Self::Event>
    where
        Self: Sized,
    {
        Ok(LauncherEvents::Status(status_change))
    }
}

#[async_trait]
impl Actor for Launcher {
    type Dependencies = ();
    type Event = LauncherEvents;
    type Channel = TokioChannel<Self::Event>;

    async fn run<'a, Reg: RegistryAccess + Send + Sync, Sup: EventDriven + Supervisor>(
        &mut self,
        rt: &mut ActorScopedRuntime<'a, Self, Reg, Sup>,
        _deps: Self::Dependencies,
    ) -> Result<(), ActorError>
    where
        Self: Sized,
        Sup::Children: From<PhantomData<Self>>,
    {
        rt.update_status(ServiceStatus::Initializing).await;
        let builder = HelloWorldBuilder::new().name("Hello World".to_string());
        let my_handle = rt.my_handle().await;
        rt.spawn_pool(my_handle.clone(), |pool| {
            async move {
                for i in 0..10 {
                    let (_abort, _handle) = pool.spawn_with_metric(builder.clone().num(i).build(), i as i32).await;
                }
                Ok(())
            }
            .boxed()
        })
        .await
        .expect("Failed to create actor pool!");

        tokio::task::spawn(ctrl_c(my_handle.clone().into_inner()));

        rt.update_status(ServiceStatus::Running).await;
        let mut i = 0;
        while let Some(evt) = rt.next_event().await {
            match evt {
                LauncherEvents::HelloWorld(event) => {
                    info!("Received event for HelloWorld");
                    if let Some(pool) = rt.pool_with_metric::<HelloWorld, i32>().await {
                        //pool.write().await.send_all(event).await.expect("Failed to pass along message!");
                        pool.write()
                            .await
                            .send_by_metric(&i, event)
                            .await
                            .expect("Failed to pass along message!");
                        i += 1;
                    }
                }
                LauncherEvents::Shutdown { using_ctrl_c: _ } => {
                    debug!("Exiting launcher");
                    break;
                }
                LauncherEvents::Report(res) => match res {
                    Ok(s) => match s.state {
                        LauncherChildrenStates::HelloWorld(h) => {
                            info!("{} {} has shutdown!", h.name, h.num);
                        }
                    },
                    Err(e) => {
                        match e.state {
                            LauncherChildrenStates::HelloWorld(ref h) => {
                                info!("{} {} has shutdown unexpectedly!", h.name, h.num);
                            }
                        }
                        match e.error.request() {
                            ActorRequest::Restart => {
                                match e.state {
                                    LauncherChildrenStates::HelloWorld(h) => {
                                        info!("Restarting {} {}", h.name, h.num);
                                        let i = h.num;
                                        rt.spawn_into_pool_with_metric(h, i, my_handle.clone()).await;
                                    }
                                };
                            }
                            ActorRequest::Reschedule(_) => todo!(),
                            ActorRequest::Finish => (),
                            ActorRequest::Panic => panic!("Received request to panic....so I did"),
                        }
                    }
                },
                LauncherEvents::Status(s) => match s.actor_type {
                    LauncherChildren::HelloWorld => {
                        info!("{} status changed ({} -> {})!", s.service.name, s.prev_status, s.service.status);
                        debug!("\n{}", rt.service_tree().await);
                    }
                },
            }
        }
        rt.update_status(ServiceStatus::Stopped).await;
        Ok(())
    }
}

impl System for Launcher {
    type State = Arc<RwLock<LauncherAPI>>;
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
    RuntimeScope::<ArcedRegistry>::launch(|scope| {
        async move {
            scope.spawn_system_unsupervised(Launcher, Arc::new(RwLock::new(LauncherAPI))).await;
            scope
                .spawn_task(|rt| {
                    async move {
                        for i in 0..10 {
                            rt.system::<Launcher>()
                                .await
                                .unwrap()
                                .state
                                .read()
                                .await
                                .send_to_hello_world(HelloWorldEvent::Print(format!("foo {}", i)), rt)
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
