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
impl<S> Actor<S> for WebsocketSender
where
    S: SupHandle<Self>,
{
    type Data = ();
    type Channel = UnboundedChannel<WebsocketSenderEvent>;
    async fn init(&mut self, _rt: &mut Rt<Self, S>) -> Result<Self::Data, Reason> {
        Ok(())
    }
    async fn run(&mut self, rt: &mut Rt<Self, S>, _data: Self::Data) -> ActorResult {
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
