use super::*;
use crate::{
    actor::{build, Actor, ActorError, Builder, Sender, TokioChannel, TokioSender},
    prelude::RegistryAccess,
    runtime::ActorScopedRuntime,
};
use futures::{FutureExt, SinkExt, StreamExt};
use futures_util::stream::SplitSink;
pub use std::net::SocketAddr;
use std::{
    collections::HashMap,
    convert::{TryFrom, TryInto},
    marker::PhantomData,
};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{accept_async, tungstenite::Message, WebSocketStream};

/// A websocket which awaits connections on a specified listen
/// address and forwards messages to its supervisor
#[derive(Debug)]
pub struct Websocket<H, E>
where
    H: 'static + Sender<E> + Clone + Send + Sync,
    E: 'static + Send + Sync + TryFrom<(SocketAddr, Message)>,
    E::Error: Send,
{
    listen_address: SocketAddr,
    connections: HashMap<SocketAddr, TokioSender<Message>>,
    supervisor_handle: H,
    _data: PhantomData<E>,
}

#[build]
#[derive(Debug, Clone)]
pub fn build<H, E>(listen_address: SocketAddr, supervisor_handle: H) -> Websocket<H, E>
where
    H: 'static + Sender<E> + Clone + Send + Sync,
    E: 'static + Send + Sync + TryFrom<(SocketAddr, Message)>,
    E::Error: Send,
{
    Websocket {
        listen_address,
        connections: Default::default(),
        supervisor_handle,
        _data: PhantomData,
    }
}

/// The websocket system's events
#[derive(Clone, Debug)]
pub enum WebsocketChildren {
    /// A response to send across the websocket to the client
    Response(SocketAddr, Message),
    /// A message received from the client, to be sent to the supervisor
    Received(SocketAddr, Message),
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
impl<SH, SE> Actor for Websocket<SH, SE>
where
    SH: 'static + Sender<SE> + Clone + Send + Sync,
    SE: 'static + Send + Sync + TryFrom<(SocketAddr, Message)>,
    SE::Error: Send,
{
    type Event = WebsocketChildren;
    type Dependencies = ();
    type Channel = TokioChannel<Self::Event>;

    async fn run<'a, Reg: RegistryAccess + Send + Sync>(
        &mut self,
        rt: &mut ActorScopedRuntime<'a, Self, Reg>,
        _deps: Self::Dependencies,
    ) -> Result<(), ActorError>
    where
        Self: Sized,
    {
        let tcp_listener = {
            TcpListener::bind(self.listen_address)
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
                                                    rt.send_actor_event::<Websocket<SH, SE>>(WebsocketChildren::Close(peer)).await?;
                                                    break;
                                                }
                                                msg => {
                                                    rt.send_actor_event::<Websocket<SH, SE>>(WebsocketChildren::Received(peer, msg))
                                                        .await?;
                                                }
                                            }
                                        }
                                        Ok(())
                                    }
                                    .boxed()
                                })
                                .await;
                                rt.send_actor_event::<Websocket<SH, SE>>(WebsocketChildren::Connection(Connection {
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
                WebsocketChildren::Response(peer, msg) => {
                    if let Some(conn) = self.connections.get_mut(&peer) {
                        if let Err(_) = conn.send(msg).await {
                            self.connections.remove(&peer);
                        }
                    }
                }
                WebsocketChildren::Connection(conn) => {
                    // Store this connection
                    self.connections.insert(conn.peer, conn.sender);
                }
                WebsocketChildren::Received(addr, msg) => {
                    if let Ok(msg) = (addr, msg).try_into() {
                        if let Err(_) = self.supervisor_handle.send(msg).await {
                            break;
                        }
                    }
                }
                WebsocketChildren::Close(peer) => {
                    self.connections.remove(&peer);
                }
            }
        }
        connector_abort.abort();
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
