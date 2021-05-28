use futures::{FutureExt, SinkExt, StreamExt};
use futures_util::stream::SplitSink;
use std::{collections::HashMap, net::SocketAddr};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{accept_async, tungstenite::Message, WebSocketStream};

use super::*;
pub struct Websocket {
    service: Service,
    listen_address: SocketAddr,
    connections: HashMap<SocketAddr, TokioSender<Message>>,
}

#[build]
#[derive(Debug, Clone)]
pub fn build(service: Service, listen_address: SocketAddr) -> Websocket {
    Websocket {
        service,
        listen_address,
        connections: Default::default(),
    }
}

#[derive(Clone, Debug)]
pub enum WebsocketChildren {
    Responder(Message),
    Connection(Connection),
}

#[derive(Clone, Debug)]
pub struct Connection {
    peer: SocketAddr,
    sender: TokioSender<Message>,
}

#[async_trait]
impl System for Websocket {
    type ChildEvents = WebsocketChildren;

    type Dependencies = ();

    type Channel = TokioChannel<Self::ChildEvents>;

    async fn run<'a>(this: std::sync::Arc<tokio::sync::RwLock<Self>>, mut rt: SystemRuntime<'a, Self>) -> Result<(), ActorError>
    where
        Self: Sized,
    {
        let tcp_listener = TcpListener::bind(this.read().await.listen_address)
            .await
            .map_err(|_| anyhow::anyhow!("Unable to bind to dashboard listen address"))?;
        rt.spawn_actor(Connector { tcp_listener });
        while let Some(evt) = rt.next_event().await {
            match evt {
                WebsocketChildren::Responder(msg) => {
                    // Pass the message to the correct responder somehow
                }
                WebsocketChildren::Connection(conn) => {
                    // Store this connection
                    this.write().await.connections.insert(conn.peer, conn.sender);
                }
            }
        }
        Ok(())
    }
}

struct Connector {
    tcp_listener: TcpListener,
}

#[derive(Clone, Debug)]
struct ConnectorShutdown;

#[async_trait]
impl Actor for Connector {
    type Dependencies = ();

    type Event = ConnectorShutdown;

    type Channel = TokioChannel<Self::Event>;

    async fn run<'a>(self, mut rt: ActorRuntime<'a, Self>) -> Result<(), ActorError>
    where
        Self: Sized,
    {
        loop {
            if let Ok((socket, peer)) = self.tcp_listener.accept().await {
                let peer = socket.peer_addr().unwrap_or(peer);
                if let Ok(stream) = accept_async(socket).await {
                    let (sender, mut receiver) = stream.split();
                    let responder_handle = rt.spawn_actor(Responder { sender });
                    rt.spawn_task(|_| {
                        async move {
                            while let Some(Ok(msg)) = receiver.next().await {
                                match msg {
                                    Message::Text(_) => {
                                        // forward this up
                                    }
                                    Message::Close(_) => {
                                        break;
                                    }
                                    _ => (),
                                }
                            }
                            Ok(())
                        }
                        .boxed()
                    });
                    rt.send_system_event::<Websocket>(WebsocketChildren::Connection(Connection {
                        peer,
                        sender: responder_handle,
                    }))
                    .await?;
                }
            }
        }
    }
}

struct Responder {
    sender: SplitSink<WebSocketStream<TcpStream>, Message>,
}

#[async_trait]
impl Actor for Responder {
    type Dependencies = ();

    type Event = Message;

    type Channel = TokioChannel<Self::Event>;

    async fn run<'a>(mut self, mut rt: ActorRuntime<'a, Self>) -> Result<(), ActorError>
    where
        Self: Sized,
    {
        while let Some(msg) = rt.next_event().await {
            self.sender.send(msg).await.map_err(|e| anyhow::anyhow!(e))?;
        }
        Ok(())
    }
}
