use backstage::{core::*, prefab::websocket::RouteMessage};

struct HelloWorld;

#[async_trait::async_trait]
impl<S> Actor<S> for HelloWorld
where
    S: SupHandle<Self>,
{
    type Data = ();
    type Channel = AbortableUnboundedChannel<String>;
    async fn init(&mut self, rt: &mut Rt<Self, S>) -> Result<Self::Data, Reason> {
        rt.add_route::<RouteMessage>().await.ok();
        log::info!("HelloWorld: {}", rt.service().status());
        Ok(())
    }
    async fn run(&mut self, rt: &mut Rt<Self, S>, _data: Self::Data) -> ActorResult {
        log::info!("HelloWorld: {}", rt.service().status());
        while let Some(event) = rt.inbox_mut().next().await {
            log::info!("HelloWorld: Received {}", event);
        }
        rt.stop().await;
        log::info!("HelloWorld: {}", rt.service().status());
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let hello_world = HelloWorld;
    let websocket_server_addr = "127.0.0.1:9000".parse::<std::net::SocketAddr>().expect("parsable socket addr");
    let runtime = Runtime::new(Some("HelloWorld".into()), hello_world)
        .await
        .expect("Runtime to run")
        .websocket_server(websocket_server_addr, None)
        .await
        .expect("Websocket server to run");
    tokio::spawn(ws_client());
    runtime.block_on().await.expect("Runtime to shutdown gracefully");
}

async fn ws_client() {
    use backstage::prefab::websocket::*;
    use futures::SinkExt;
    let (mut stream, _) = tokio_tungstenite::connect_async(url::Url::parse("ws://127.0.0.1:9000/").unwrap())
        .await
        .unwrap();
    let actor_path = ActorPath::new();
    let request = Interface::new(actor_path.clone(), Event::cast("Print this".into()));
    stream.send(request.to_message()).await.unwrap();
    while let Some(Ok(msg)) = stream.next().await {
        log::info!("Response from websocket: {}", msg);
        let request = Interface::new(ActorPath::new(), Event::shutdown());
        stream.send(request.to_message()).await.ok();
    }
}
