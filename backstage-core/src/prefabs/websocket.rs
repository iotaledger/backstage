// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use crate::{
    actor::{Actor, ActorError, EnvelopeSender, HandleEvent, ServiceStatus},
    prelude::{Act, ActorContext, SupervisedContext, Supervisor, UnsupervisedContext},
};
use async_trait::async_trait;
use futures::{FutureExt, SinkExt, StreamExt};
use futures_util::stream::SplitSink;
pub use std::net::SocketAddr;
use std::{collections::HashMap, marker::PhantomData};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{accept_async, tungstenite::Message, WebSocketStream};

/// A websocket which awaits connections on a specified listen
/// address and forwards messages to its supervisor
#[derive(Debug)]
pub struct Websocket<Sup> {
    listen_address: SocketAddr,
    connections: HashMap<SocketAddr, Act<Responder>>,
    // supervisor_handle: Act<Sup>,
    _sup: PhantomData<fn(Sup) -> Sup>,
}

impl<Sup> Websocket<Sup> {
    /// Create a new websocket with a listen address
    pub fn new(listen_address: SocketAddr) -> Self {
        Websocket {
            listen_address,
            connections: Default::default(),
            _sup: PhantomData,
        }
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
    sender: Act<Responder>,
}

#[async_trait]
impl<Sup: 'static + Supervisor<Self>> Actor for Websocket<Sup>
where
    Sup: HandleEvent<(SocketAddr, Message)>,
{
    const PATH: &'static str = "websocket";
    type Data = ();
    type Context = SupervisedContext<Self, Sup, Act<Sup>>;

    async fn init(&mut self, cx: &mut Self::Context) -> Result<Self::Data, ActorError>
    where
        Self: 'static + Sized + Send + Sync,
    {
        cx.update_status(ServiceStatus::Initializing).await;
        let tcp_listener = {
            TcpListener::bind(self.listen_address)
                .await
                .map_err(|_| ActorError::from(anyhow::anyhow!("Unable to bind to dashboard listen address")))?
        };
        cx.spawn_task(|scope| {
            async move {
                scope.update_status(ServiceStatus::Running).await;
                loop {
                    if let Ok((socket, peer)) = tcp_listener.accept().await {
                        let peer = socket.peer_addr().unwrap_or(peer);
                        if let Ok(stream) = accept_async(socket).await {
                            let (sender, mut receiver) = stream.split();
                            let responder_handle = scope.spawn_actor_unsupervised(Responder { sender, peer }).await?;
                            scope
                                .send_actor_event::<Websocket<Sup>, _>(WebsocketChildren::Connection(Connection {
                                    peer,
                                    sender: responder_handle.clone(),
                                }))
                                .await?;
                            scope
                                .spawn_task(move |scope| {
                                    async move {
                                        scope.update_status(ServiceStatus::Running).await;
                                        while let Some(Ok(msg)) = receiver.next().await {
                                            match msg {
                                                Message::Close(_) => {
                                                    break;
                                                }
                                                msg => {
                                                    scope
                                                        .send_actor_event::<Websocket<Sup>, _>(
                                                            WebsocketChildren::Received(peer, msg),
                                                        )
                                                        .await?;
                                                }
                                            }
                                        }
                                        responder_handle.shutdown().await;
                                        scope
                                            .send_actor_event::<Websocket<Sup>, _>(WebsocketChildren::Close(peer))
                                            .await?;
                                        scope.update_status(ServiceStatus::Stopped).await;
                                        Ok(())
                                    }
                                    .boxed()
                                })
                                .await;
                        }
                    }
                }
            }
            .boxed()
        })
        .await;
        Ok(())
    }
}

#[async_trait]
impl<Sup: 'static + Supervisor<Self>> HandleEvent<WebsocketChildren> for Websocket<Sup>
where
    Sup: HandleEvent<(SocketAddr, Message)>,
{
    async fn handle_event(
        &mut self,
        cx: &mut Self::Context,
        event: WebsocketChildren,
        _data: &mut Self::Data,
    ) -> Result<(), ActorError> {
        match event {
            WebsocketChildren::Response(peer, msg) => {
                log::trace!("Received response for peer {}: {}", peer, msg);
                if let Some(conn) = self.connections.get(&peer) {
                    log::trace!("Sending response to peer");
                    if let Err(_) = conn.send(msg) {
                        log::trace!("Peer was not found!");
                        self.connections.remove(&peer);
                    }
                }
            }
            WebsocketChildren::Connection(conn) => {
                // Store this connection
                log::trace!("Received new connection for peer {}", conn.peer);
                self.connections.insert(conn.peer, conn.sender);
            }
            WebsocketChildren::Received(addr, msg) => {
                log::trace!("Received message from peer {}: {}", addr, msg);
                if let Err(_) = cx.supervisor_handle().send((addr, msg)) {
                    log::trace!("Failed to pass message to supervisor!");
                    cx.shutdown().await;
                }
            }
            WebsocketChildren::Close(peer) => {
                log::trace!("Received close command for peer {}", peer);
                self.connections.remove(&peer);
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
struct Responder {
    sender: SplitSink<WebSocketStream<TcpStream>, Message>,
    peer: SocketAddr,
}

#[async_trait]
impl Actor for Responder {
    const PATH: &'static str = "websocket-responder";
    type Data = ();
    type Context = UnsupervisedContext<Self>;

    async fn init(&mut self, _cx: &mut Self::Context) -> Result<Self::Data, ActorError>
    where
        Self: 'static + Sized + Send + Sync,
    {
        todo!()
    }

    fn name(&self) -> std::borrow::Cow<'static, str> {
        format!("Responder ({})", self.peer).into()
    }
}

#[async_trait]
impl HandleEvent<Message> for Responder {
    async fn handle_event(
        &mut self,
        _cx: &mut Self::Context,
        event: Message,
        _data: &mut Self::Data,
    ) -> Result<(), ActorError> {
        self.sender
            .send(event)
            .await
            .map_err(|e| ActorError::from(anyhow::anyhow!(e)))
    }
}
