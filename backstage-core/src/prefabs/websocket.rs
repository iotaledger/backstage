use super::*;
use futures::{FutureExt, SinkExt, StreamExt};
use futures_util::stream::SplitSink;
use std::{collections::HashMap, marker::PhantomData, net::SocketAddr};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{accept_async, tungstenite::Message, WebSocketStream};

pub struct Websocket<E> {
    service: Service,
    listen_address: SocketAddr,
    connections: HashMap<SocketAddr, TokioSender<Message>>,
    _evt: PhantomData<E>,
}

#[build]
#[derive(Debug, Clone)]
pub fn build<E: From<Message>>(service: Service, listen_address: SocketAddr) -> Websocket<E> {
    Websocket {
        service,
        listen_address,
        connections: Default::default(),
        _evt: PhantomData,
    }
}

#[derive(Clone, Debug)]
pub enum WebsocketChildren {
    Response(Message),
    Received(Message),
    Connection(Connection),
}

#[derive(Clone, Debug)]
pub struct Connection {
    peer: SocketAddr,
    sender: TokioSender<Message>,
}

#[async_trait]
impl<Rt, H, E> System<Rt, H, E> for Websocket<E>
where
    Rt: 'static + SystemRuntime + Into<BasicRuntime> + Into<SystemsRuntime>,
    H: 'static + Sender<E> + Clone + Send + Sync,
    E: 'static + SupervisorEvent + Send + Sync,
    E: From<Message>,
{
    type ChildEvents = WebsocketChildren;
    type Dependencies = ();
    type Channel = TokioChannel<Self::ChildEvents>;

    async fn run<'a>(
        this: std::sync::Arc<tokio::sync::RwLock<Self>>,
        mut rt: SystemScopedRuntime<'a, Self, Rt, H, E>,
        _deps: (),
    ) -> Result<Service, ActorError>
    where
        Self: Sized,
    {
        let tcp_listener = {
            let service = this.read().await.service.clone();
            TcpListener::bind(this.read().await.listen_address)
                .await
                .map_err(|_| ActorError::new(anyhow::anyhow!("Unable to bind to dashboard listen address").into(), service))?
        };
        let mut listener_service = this.write().await.service.spawn("Listener");
        let connector_abort = rt.with_runtime::<SystemsRuntime>().spawn_task(|mut rt| {
            async move {
                loop {
                    if let Ok((socket, peer)) = tcp_listener.accept().await {
                        let peer = socket.peer_addr().unwrap_or(peer);
                        if let Ok(stream) = accept_async(socket).await {
                            let (sender, mut receiver) = stream.split();
                            let (responder_abort, responder_handle) = rt.with_runtime::<BasicRuntime>().spawn_actor::<_, H, E, _>(
                                Responder {
                                    service: listener_service.spawn("Responder"),
                                    sender,
                                },
                                None,
                            );
                            rt.spawn_task(|mut rt| {
                                async move {
                                    while let Some(Ok(msg)) = receiver.next().await {
                                        match msg {
                                            Message::Close(_) => {
                                                responder_abort.abort();
                                                break;
                                            }
                                            msg => {
                                                rt.send_system_event::<Websocket<E>, H, E>(WebsocketChildren::Received(msg)).await?;
                                            }
                                        }
                                    }
                                    Ok(())
                                }
                                .boxed()
                            });
                            rt.send_system_event::<Websocket<E>, H, E>(WebsocketChildren::Connection(Connection {
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
                    if let Some(supervisor) = rt.supervisor_handle() {
                        supervisor.send(msg.into()).await;
                    }
                }
            }
        }
        connector_abort.abort();
        Ok(this.read().await.service.clone())
    }
}

struct Responder {
    service: Service,
    sender: SplitSink<WebSocketStream<TcpStream>, Message>,
}

#[async_trait]
impl<Rt, H, E> Actor<Rt, H, E> for Responder
where
    Rt: BaseRuntime,
    H: 'static + Sender<E> + Clone + Send + Sync,
    E: 'static + SupervisorEvent + Send + Sync,
{
    type Dependencies = ();
    type Event = Message;
    type Channel = TokioChannel<Self::Event>;

    async fn run<'a>(mut self, mut rt: ActorScopedRuntime<'a, Self, Rt, H, E>, _deps: ()) -> Result<Service, ActorError>
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
