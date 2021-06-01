use super::*;
use futures::{FutureExt, SinkExt, StreamExt};
use futures_util::stream::SplitSink;
use std::{collections::HashMap, marker::PhantomData, net::SocketAddr};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{accept_async, tungstenite::Message, WebSocketStream};

pub struct Websocket<E: From<Message>, H: WebsocketHandle<E>> {
    service: Service,
    listen_address: SocketAddr,
    connections: HashMap<SocketAddr, TokioSender<Message>>,
    handle: H,
    _phantom: PhantomData<E>,
}

#[async_trait]
pub trait WebsocketHandle<E: From<Message>> {
    async fn send_websocket_msg(&mut self, msg: Message);
}

#[async_trait]
impl<T, E> WebsocketHandle<E> for T
where
    T: Sender<E> + Send,
    E: From<Message> + Send + Clone,
{
    async fn send_websocket_msg(&mut self, msg: Message) {
        self.send(E::from(msg)).await.ok();
    }
}

#[build]
#[derive(Debug, Clone)]
pub fn build<E: From<Message>, H: WebsocketHandle<E>>(service: Service, listen_address: SocketAddr, handle: H) -> Websocket<E, H> {
    Websocket {
        service,
        listen_address,
        connections: Default::default(),
        handle,
        _phantom: PhantomData::<E>,
    }
}

#[derive(Clone, Debug)]
pub enum WebsocketChildren {
    Response(Message),
    Received(Message),
    Connection(Connection),
    Shutdown,
}

#[derive(Clone, Debug)]
pub struct Connection {
    peer: SocketAddr,
    sender: TokioSender<Message>,
}

#[async_trait]
impl<E: 'static + From<Message> + Send + Sync, H: 'static + WebsocketHandle<E> + Send + Sync> System for Websocket<E, H> {
    type ChildEvents = WebsocketChildren;

    type Dependencies = ();

    type Channel = TokioChannel<Self::ChildEvents>;

    async fn run<'a>(this: std::sync::Arc<tokio::sync::RwLock<Self>>, mut rt: SystemRuntime<'a, Self>, _deps: ()) -> Result<(), ActorError>
    where
        Self: Sized,
    {
        let tcp_listener = TcpListener::bind(this.read().await.listen_address)
            .await
            .map_err(|_| anyhow::anyhow!("Unable to bind to dashboard listen address"))?;

        let connector_abort = rt.spawn_task(|mut rt| {
            async move {
                loop {
                    if let Ok((socket, peer)) = tcp_listener.accept().await {
                        let peer = socket.peer_addr().unwrap_or(peer);
                        if let Ok(stream) = accept_async(socket).await {
                            let (sender, mut receiver) = stream.split();
                            let (responder_abort, responder_handle) = rt.spawn_actor(Responder { sender });
                            rt.spawn_task(|mut rt| {
                                async move {
                                    while let Some(Ok(msg)) = receiver.next().await {
                                        match msg {
                                            Message::Close(_) => {
                                                responder_abort.abort();
                                                break;
                                            }
                                            msg => {
                                                rt.send_system_event::<Websocket<E, H>>(WebsocketChildren::Received(msg)).await?;
                                            }
                                        }
                                    }
                                    Ok(())
                                }
                                .boxed()
                            });
                            rt.send_system_event::<Websocket<E, H>>(WebsocketChildren::Connection(Connection {
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
                WebsocketChildren::Response(msg) => {}
                WebsocketChildren::Connection(conn) => {
                    // Store this connection
                    this.write().await.connections.insert(conn.peer, conn.sender);
                }
                WebsocketChildren::Received(msg) => {
                    this.write().await.handle.send_websocket_msg(msg).await;
                }
                WebsocketChildren::Shutdown => {
                    connector_abort.abort();
                    break;
                }
            }
        }
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

    async fn run<'a>(mut self, mut rt: ActorRuntime<'a, Self>, _deps: ()) -> Result<(), ActorError>
    where
        Self: Sized,
    {
        while let Some(msg) = rt.next_event().await {
            self.sender.send(msg).await.map_err(|e| anyhow::anyhow!(e))?;
        }
        Ok(())
    }
}
