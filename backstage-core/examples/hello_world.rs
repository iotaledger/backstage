use backstage::core::*;

struct HelloWorld;

#[async_trait::async_trait]
impl Actor for HelloWorld {
    type Channel = AbortableUnboundedChannel<String>;
    async fn init<S: Supervise<Self>>(&mut self, rt: &mut Self::Context<S>) -> Result<Self::Deps, Reason> {
        Ok(())
    }
    async fn run<S: Supervise<Self>>(&mut self, rt: &mut Self::Context<S>, _deps: Self::Deps) -> ActorResult {
        while let Some(event) = rt.inbox_mut().next().await {
            log::info!("HelloWorld: {}", event);
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let hello_world = HelloWorld;
    let websocket_server_addr = "127.0.0.1:9000".parse::<std::net::SocketAddr>().expect("parsable socket addr");
    let runtime = Runtime::new("HelloWorld", hello_world)
        .await
        .expect("Runtime to build")
        .websocket_server(websocket_server_addr, None)
        .await
        .expect("Websocket server to run");
    runtime.block_on().await.expect("Runtime to shutdown gracefully");
}
