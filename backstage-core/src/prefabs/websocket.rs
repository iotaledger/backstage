// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::{
    actor::{
        build, Actor, ActorError, Builder, EventDriven, Sender, ServiceStatus, SupervisorEvent, UnboundedTokioChannel,
        UnboundedTokioSender,
    },
    prelude::{Act, ActorScopedRuntime, DataWrapper, RegistryAccess},
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
pub struct Websocket<Sup>
where
    Sup: EventDriven,
    Sup::Event: TryFrom<(SocketAddr, Message)>,
    <Sup::Event as TryFrom<(SocketAddr, Message)>>::Error: Send,
{
    listen_address: SocketAddr,
    connections: HashMap<SocketAddr, UnboundedTokioSender<Message>>,
    supervisor_handle: Act<Sup>,
}

#[build]
#[derive(Clone)]
pub fn build<Sup>(listen_address: SocketAddr, supervisor_handle: Act<Sup>) -> Websocket<Sup>
where
    Sup: EventDriven,
    Sup::Event: TryFrom<(SocketAddr, Message)>,
    <Sup::Event as TryFrom<(SocketAddr, Message)>>::Error: Send,
{
    Websocket {
        listen_address,
        connections: Default::default(),
        supervisor_handle,
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
    sender: UnboundedTokioSender<Message>,
}

#[async_trait]
impl<Sup> Actor for Websocket<Sup>
where
    Sup: 'static + EventDriven,
    Sup::Event: TryFrom<(SocketAddr, Message)>,
    <Sup::Event as TryFrom<(SocketAddr, Message)>>::Error: Send,
{
    type Dependencies = ();
    type Event = WebsocketChildren;
    type Channel = UnboundedTokioChannel<Self::Event>;

    async fn init<Reg: RegistryAccess + Send + Sync, Sup2: EventDriven>(
        &mut self,
        rt: &mut ActorScopedRuntime<Self, Reg, Sup2>,
    ) -> Result<(), ActorError>
    where
        Self: Sized,
        Sup2::Event: SupervisorEvent,
        <Sup2::Event as SupervisorEvent>::Children: From<PhantomData<Self>>,
    {
        rt.update_status(ServiceStatus::Initializing).await.ok();
        let tcp_listener = {
            TcpListener::bind(self.listen_address)
                .await
                .map_err(|_| ActorError::from(anyhow::anyhow!("Unable to bind to dashboard listen address")))?
        };
        rt.spawn_task(|rt| {
            async move {
                rt.update_status(ServiceStatus::Running).await.ok();
                loop {
                    if let Ok((socket, peer)) = tcp_listener.accept().await {
                        let peer = socket.peer_addr().unwrap_or(peer);
                        if let Ok(stream) = accept_async(socket).await {
                            let (sender, mut receiver) = stream.split();
                            let responder_handle = rt.spawn_actor_unsupervised(Responder { sender, peer }).await?;
                            rt.send_actor_event::<Websocket<Sup>>(WebsocketChildren::Connection(Connection {
                                peer,
                                sender: responder_handle.clone().into_inner(),
                            }))
                            .await?;
                            rt.spawn_task(move |rt| {
                                async move {
                                    rt.update_status(ServiceStatus::Running).await.ok();
                                    while let Some(Ok(msg)) = receiver.next().await {
                                        match msg {
                                            Message::Close(_) => {
                                                break;
                                            }
                                            msg => {
                                                rt.send_actor_event::<Websocket<Sup>>(WebsocketChildren::Received(
                                                    peer, msg,
                                                ))
                                                .await?;
                                            }
                                        }
                                    }
                                    responder_handle.shutdown();
                                    rt.send_actor_event::<Websocket<Sup>>(WebsocketChildren::Close(peer))
                                        .await?;
                                    rt.update_status(ServiceStatus::Stopped).await.ok();
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

    async fn run<Reg: RegistryAccess + Send + Sync, Sup2: EventDriven>(
        &mut self,
        rt: &mut ActorScopedRuntime<Self, Reg, Sup2>,
        _deps: Self::Dependencies,
    ) -> Result<(), ActorError>
    where
        Self: Sized,
        Sup2::Event: SupervisorEvent,
        <Sup2::Event as SupervisorEvent>::Children: From<PhantomData<Self>>,
    {
        rt.update_status(ServiceStatus::Running).await.ok();
        while let Some(evt) = rt.next_event().await {
            match evt {
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
                    if let Ok(msg) = (addr, msg).try_into() {
                        if let Err(_) = self.supervisor_handle.send(msg) {
                            log::trace!("Failed to pass message to supervisor!");
                            break;
                        }
                    }
                }
                WebsocketChildren::Close(peer) => {
                    log::trace!("Received close command for peer {}", peer);
                    self.connections.remove(&peer);
                }
            }
        }
        rt.update_status(ServiceStatus::Stopped).await.ok();
        Ok(())
    }
}

struct Responder {
    sender: SplitSink<WebSocketStream<TcpStream>, Message>,
    peer: SocketAddr,
}

#[async_trait]
impl Actor for Responder {
    type Dependencies = ();
    type Event = Message;
    type Channel = UnboundedTokioChannel<Self::Event>;

    async fn init<Reg: RegistryAccess + Send + Sync, Sup: EventDriven>(
        &mut self,
        _rt: &mut ActorScopedRuntime<Self, Reg, Sup>,
    ) -> Result<(), ActorError> {
        Ok(())
    }

    async fn run<Reg: RegistryAccess + Send + Sync, Sup: EventDriven>(
        &mut self,
        rt: &mut ActorScopedRuntime<Self, Reg, Sup>,
        _deps: (),
    ) -> Result<(), ActorError>
    where
        Self: Sized,
        Sup::Event: SupervisorEvent,
        <Sup::Event as SupervisorEvent>::Children: From<PhantomData<Self>>,
    {
        rt.update_status(ServiceStatus::Running).await.ok();
        while let Some(msg) = rt.next_event().await {
            self.sender
                .send(msg)
                .await
                .map_err(|e| ActorError::from(anyhow::anyhow!(e)))?;
        }
        Ok(())
    }

    fn name(&self) -> std::borrow::Cow<'static, str> {
        format!("Responder ({})", self.peer).into()
    }
}
