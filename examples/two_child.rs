use backstage::{core::*, runtime::BackstageRuntime};

/////////////////// First child START /////////////////////
#[derive(Clone)]
/// The FirstChild's State
struct FirstChild;
#[derive(Clone, Debug)]
/// The FirstChildHandle
struct FirstChildHandle;
/// The FirstChildInbox
struct FirstChildInbox;

impl ActorHandle for FirstChildHandle {
    fn service(&self, _service: &Service) {}
    fn shutdown(self: Box<Self>) {}
    fn aknshutdown(&self, _service: Service, _r: ActorResult) {}
    fn send(&self, _event: Box<dyn std::any::Any>) -> Result<(), Box<dyn std::any::Any>> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl<C> Actor<C> for FirstChild
where
    C: Registry<Actor = Self>,
{
    async fn run(self, context: &mut C) -> ActorResult {
        let _second_child_handle: SecondChildHandle = context.depends_on::<SecondChildHandle>("SecondChild".into()).await.map_err(|e| {
            log::error!("{} unable to get SecondChildHandle {}", context.service().name(), e);
            Reason::Exit
        })?;
        log::info!("{} acquired SecondChildHandle", context.service().name());
        Ok(())
    }
}

impl Channel for FirstChild {
    type Handle = FirstChildHandle;
    type Inbox = FirstChildInbox;
    fn channel(&mut self) -> Result<(Self::Handle, Self::Inbox), anyhow::Error> {
        Ok((FirstChildHandle, FirstChildInbox))
    }
}

/////////////////// First child END /////////////////////

/////////////////// Second child START /////////////////////
#[derive(Clone)]
/// The SecondChild's State
struct SecondChild;
#[derive(Clone)]
/// The SecondChildHandle
struct SecondChildHandle;
/// The SecondChildInbox
struct SecondChildInbox;

impl ActorHandle for SecondChildHandle {
    fn service(&self, _service: &Service) {}
    fn shutdown(self: Box<Self>) {}
    fn aknshutdown(&self, _service: Service, _r: ActorResult) {}
    fn send(&self, _event: Box<dyn std::any::Any>) -> Result<(), Box<dyn std::any::Any>> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl<C> Actor<C> for SecondChild
where
    C: Registry<Actor = Self>,
{
    async fn run(self, context: &mut C) -> ActorResult {
        let _first_child_handle: FirstChildHandle = context.depends_on::<FirstChildHandle>("FirstChild".into()).await.map_err(|e| {
            log::error!("{} unable to get FirstChildHandle {}", context.service().name(), e);
            Reason::Exit
        })?;
        log::info!("{} acquired FirstChildHandle", context.service().name());
        Ok(())
    }
}

impl Channel for SecondChild {
    type Handle = SecondChildHandle;
    type Inbox = SecondChildInbox;
    fn channel(&mut self) -> Result<(Self::Handle, Self::Inbox), anyhow::Error> {
        Ok((SecondChildHandle, SecondChildInbox))
    }
}

/////////////////// Second child END /////////////////////
#[tokio::main]
async fn main() {
    env_logger::init();
    #[derive(Clone)]
    pub struct NoBounds;
    let mut runtime = BackstageRuntime::new("two-child-example", NoBounds).expect("runtime to get created");
    runtime.add("FirstChild", FirstChild).await.expect("FirstChild to get spawned");
    runtime.add("SecondChild", SecondChild).await.expect("SecondChild to get spawned");
    runtime.block_on().await;
}
