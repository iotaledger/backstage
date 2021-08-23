use backstage::core::*;

struct Backstage;
#[derive(Debug)]
enum BackstageEvent {
    // #[backstage_macros::shutdown] todo impl macro
    Shutdown,
}
impl ShutdownEvent for BackstageEvent {
    fn shutdown_event() -> Self {
        BackstageEvent::Shutdown
    }
}

#[async_trait::async_trait]
impl Actor for Backstage {
    type Channel = UnboundedChannel<BackstageEvent>;
    async fn init<S: Supervise<Self>>(&mut self, rt: &mut Self::Context<S>) -> Result<Self::Deps, Reason> {
        // build and spawn your apps actors using the rt
        
        Ok(())
    }
    async fn run<S: Supervise<Self>>(&mut self, rt: &mut Self::Context<S>, _deps: Self::Deps) -> ActorResult {
        while let Some(event) = rt.inbox_mut().next().await {
            match event {
                BackstageEvent::Shutdown => {
                    rt.stop().await;
                    log::info!("{} got shutdown", rt.name());
                    if rt.microservices_stopped() {
                        break;
                    }
                }
            }
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let backstage = Backstage;
    let websocket_server_addr = "127.0.0.1:9000".parse::<std::net::SocketAddr>().expect("parsable socket addr");
    let runtime = Runtime::new("Custom", backstage)
        .await
        .expect("Runtime to build")
        .websocket_server(websocket_server_addr, None)
        .await
        .expect("Websocket server to run");
    runtime.block_on().await.expect("Runtime to shutdown gracefully");
}
