use super::*;
use futures::{FutureExt, SinkExt, StreamExt};
use futures_util::stream::SplitSink;
use std::collections::HashMap;
pub use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{accept_async, tungstenite::Message, WebSocketStream};

/// A websocket which awaits connections on a specified listen
/// address and forwards messages to its supervisor
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

/// The websocket system's events
#[derive(Clone, Debug)]
pub enum WebsocketChildren {
    /// A response to send across the websocket to the client
    Response((SocketAddr, Message)),
    /// A message received from the client, to be sent to the supervisor
    Received((SocketAddr, Message)),
    /// A new connection
    Connection(Connection),
    /// A closed connection
    Close(SocketAddr),
}

/// A websocket connection
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
    type Rt = SystemsRuntime;
    type SupervisorEvent = (SocketAddr, Message);

    async fn run_supervised<'a, H, E>(
        this: std::sync::Arc<tokio::sync::RwLock<Self>>,
        mut rt: SupervisedSystemScopedRuntime<'a, Self, H, E>,
        _deps: (),
    ) -> Result<Service, ActorError>
    where
        Self: Send + Sync + Sized,
        H: 'static + Sender<E> + Clone + Send + Sync,
        E: 'static + SupervisorEvent + Send + Sync + From<Self::SupervisorEvent>,
    {
        let tcp_listener = {
            let service = this.read().await.service.clone();
            TcpListener::bind(this.read().await.listen_address)
                .await
                .map_err(|_| ActorError::new(anyhow::anyhow!("Unable to bind to dashboard listen address").into(), service))?
        };
        let mut listener_service = this.write().await.service.spawn("Listener");
        let connector_abort = rt.spawn_task(|mut rt| {
            async move {
                loop {
                    if let Ok((socket, peer)) = tcp_listener.accept().await {
                        let peer = socket.peer_addr().unwrap_or(peer);
                        if let Ok(stream) = accept_async(socket).await {
                            let (sender, mut receiver) = stream.split();
                            let (responder_abort, responder_handle) = rt.spawn_actor_unsupervised(Responder {
                                service: listener_service.spawn("Responder"),
                                sender,
                            });
                            rt.spawn_task(move |mut rt| {
                                async move {
                                    while let Some(Ok(msg)) = receiver.next().await {
                                        match msg {
                                            Message::Close(_) => {
                                                responder_abort.abort();
                                                rt.send_system_event::<Websocket>(WebsocketChildren::Close(peer)).await?;
                                                break;
                                            }
                                            msg => {
                                                rt.send_system_event::<Websocket>(WebsocketChildren::Received((peer, msg))).await?;
                                            }
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
            .boxed()
        });
        while let Some(evt) = rt.next_event().await {
            match evt {
                WebsocketChildren::Response((peer, msg)) => {
                    if let Some(conn) = this.write().await.connections.get_mut(&peer) {
                        if let Err(_) = conn.send(msg).await {
                            this.write().await.connections.remove(&peer);
                        }
                    }
                }
                WebsocketChildren::Connection(conn) => {
                    // Store this connection
                    this.write().await.connections.insert(conn.peer, conn.sender);
                }
                WebsocketChildren::Received(msg) => {
                    if let Err(_) = rt.supervisor_handle().send(msg.into()).await {
                        break;
                    }
                }
                WebsocketChildren::Close(peer) => {
                    this.write().await.connections.remove(&peer);
                }
            }
        }
        connector_abort.abort();
        Ok(this.read().await.service.clone())
    }

    async fn run<'a>(
        this: std::sync::Arc<tokio::sync::RwLock<Self>>,
        _rt: SystemScopedRuntime<'a, Self>,
        _deps: (),
    ) -> Result<Service, ActorError> {
        Ok(this.read().await.service.clone())
    }
}

struct Responder {
    service: Service,
    sender: SplitSink<WebSocketStream<TcpStream>, Message>,
}

#[async_trait]
impl Actor for Responder {
    type Dependencies = ();
    type Event = Message;
    type Channel = TokioChannel<Self::Event>;
    type Rt = BasicRuntime;
    type SupervisorEvent = ();

    async fn run<'a>(mut self, mut rt: ActorScopedRuntime<'a, Self>, _deps: ()) -> Result<Service, ActorError>
    where
        Self: Sized,
    {
        while let Some(msg) = rt.next_event().await {
            self.sender
                .send(msg)
                .await
                .map_err(|e| ActorError::new(anyhow::anyhow!(e).into(), self.service.clone()))?;
        }
        Ok(self.service)
    }
}
