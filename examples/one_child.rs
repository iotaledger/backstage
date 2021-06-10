use backstage::{core::*, runtime::BackstageRuntime};

#[derive(Clone)]
/// The OneChild's State
struct OneChild;
#[derive(Clone)]
/// The OneChildHandle
struct OneChildHandle;
/// The OneChildInbox
struct OneChildInbox;

impl ActorHandle for OneChildHandle {
    fn service(&self, _service: &Service) {}
    fn shutdown(self: Box<Self>) {}
    fn aknshutdown(&self, _service: Service, _r: ActorResult) {}
    fn send(&self, _event: Box<dyn std::any::Any>) -> Result<(), Box<dyn std::any::Any>> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl<C> Actor<C> for OneChild
where
    C: Essential<Actor = Self>,
{
    async fn run(self, context: &mut C) -> ActorResult {
        context.service().update_status(ServiceStatus::Running);
        let name = context.service().name();
        log::info!("{} is running", name);
        Ok(())
    }
}

impl Channel for OneChild {
    type Handle = OneChildHandle;
    type Inbox = OneChildInbox;
    fn channel(&mut self) -> Result<(Self::Handle, Self::Inbox), anyhow::Error> {
        Ok((OneChildHandle, OneChildInbox))
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let mut runtime = BackstageRuntime::new("one-child-example").expect("runtime to get created");
    runtime.add("OneChild", OneChild).await.expect("OneChild to get spawned");
    runtime.block_on().await;
}
