use super::*;
use crate::{
    actor::{build, Actor, ActorError, Builder, Sender, SupervisorEvent, System, TokioChannel, TokioSender},
    prelude::RegistryAccess,
    runtime::{ActorScopedRuntime, SupervisedSystemScopedRuntime, SystemScopedRuntime},
};
use futures::{FutureExt, SinkExt, StreamExt};
use futures_util::stream::SplitSink;
pub use std::net::SocketAddr;
use std::{collections::HashMap, sync::Arc};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::RwLock,
};
use tokio_tungstenite::{accept_async, tungstenite::Message, WebSocketStream};

/// A websocket which awaits connections on a specified listen
/// address and forwards messages to its supervisor
#[derive(Debug)]
pub struct Websocket {
    listen_address: SocketAddr,
    connections: HashMap<SocketAddr, TokioSender<Message>>,
}

#[build]
#[derive(Debug, Clone)]
pub fn build(listen_address: SocketAddr) -> Websocket {
    Websocket {
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
    type SupervisorEvent = (SocketAddr, Message);

    async fn run_supervised<'a, Reg: RegistryAccess + Send + Sync, H, E>(
        this: Arc<RwLock<Self>>,
        rt: &mut SupervisedSystemScopedRuntime<'a, Self, Reg, H, E>,
        _deps: (),
    ) -> Result<(), ActorError>
    where
        Self: Send + Sync + Sized,
        H: 'static + Sender<E> + Clone + Send + Sync,
        E: 'static + SupervisorEvent<Arc<RwLock<Self>>> + Send + Sync + From<Self::SupervisorEvent>,
    {
        let tcp_listener = {
            TcpListener::bind(this.read().await.listen_address)
                .await
                .map_err(|_| ActorError::from(anyhow::anyhow!("Unable to bind to dashboard listen address")))?
        };
        let connector_abort = rt
            .spawn_task(|rt| {
                async move {
                    loop {
                        if let Ok((socket, peer)) = tcp_listener.accept().await {
                            let peer = socket.peer_addr().unwrap_or(peer);
                            if let Ok(stream) = accept_async(socket).await {
                                let (sender, mut receiver) = stream.split();
                                let (responder_abort, responder_handle) = rt.spawn_actor_unsupervised(Responder { sender }).await;
                                rt.spawn_task(move |rt| {
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
                                })
                                .await;
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
            })
            .await;
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
        Ok(())
    }

    async fn run<'a, Reg: RegistryAccess + Send + Sync>(
        _this: Arc<RwLock<Self>>,
        _rt: &mut SystemScopedRuntime<'a, Self, Reg>,
        _deps: (),
    ) -> Result<(), ActorError> {
        Ok(())
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
    type SupervisorEvent = ();

    async fn run<'a, Reg: RegistryAccess + Send + Sync>(
        &mut self,
        rt: &mut ActorScopedRuntime<'a, Self, Reg>,
        _deps: (),
    ) -> Result<(), ActorError>
    where
        Self: Sized,
    {
        while let Some(msg) = rt.next_event().await {
            self.sender.send(msg).await.map_err(|e| ActorError::from(anyhow::anyhow!(e)))?;
        }
        Ok(())
    }
}
