use super::ResponseResult;
use crate::core::*;
use futures::{stream::SplitSink, SinkExt};
use tokio::net::TcpStream;
use tokio_tungstenite::{tungstenite::Message, WebSocketStream};

pub struct WebsocketSender {
    split_sink: SplitSink<WebSocketStream<TcpStream>, Message>,
}

impl WebsocketSender {
    pub(crate) fn new(split_sink: SplitSink<WebSocketStream<TcpStream>, Message>) -> Self {
        Self { split_sink }
    }
}
pub enum WebsocketSenderEvent {
    Shutdown,
    Result(ResponseResult),
}
impl ShutdownEvent for WebsocketSenderEvent {
    fn shutdown_event() -> Self {
        Self::Shutdown
    }
}

#[async_trait::async_trait]
impl Actor for WebsocketSender {
    type Channel = UnboundedChannel<WebsocketSenderEvent>;
    async fn init<S: Supervise<Self>>(&mut self, _rt: &mut Self::Context<S>) -> Result<Self::Deps, Reason> {
        Ok(())
    }
    async fn run<S: Supervise<Self>>(&mut self, rt: &mut Self::Context<S>, _deps: Self::Deps) -> ActorResult {
        while let Some(event) = rt.inbox_mut().next().await {
            match event {
                WebsocketSenderEvent::Shutdown => break,
                WebsocketSenderEvent::Result(r) => match r {
                    Ok(response) => {
                        let json = serde_json::to_string(&response).expect("Serializable response");
                        let message = Message::from(json);
                        self.split_sink.send(message).await.ok();
                    }
                    Err(error) => {
                        let json = serde_json::to_string(&error).expect("Serializable response");
                        let message = Message::from(json);
                        self.split_sink.send(message).await.ok();
                    }
                },
            }
        }
        Ok(())
    }
}
