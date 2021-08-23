/// Websocket listener
mod websocket_listener;
/// WebSocket receiver (incomings from client)
mod websocket_receiver;
/// WebSocket Sender (outcomings to client)
mod websocket_sender;

use websocket_listener::WebsocketListener;
use websocket_receiver::WebsocketReceiver;
use websocket_sender::{WebsocketSender, WebsocketSenderEvent};

#[derive(serde::Deserialize, serde::Serialize, Clone)]
pub struct RouteMessage(String);
// Deserializable event
#[derive(serde::Deserialize)]
pub enum Event {
    /// shutdown the actor
    Shutdown,
    /// Cast event T using the pid's router, without responder.
    Cast(RouteMessage),
    /// Send event T using the pid's router.
    Call(RouteMessage),
    /// Request the service tree
    RequestServiceTree,
}

// Serializable response
#[derive(serde::Serialize)]
pub enum Response {
    /// Success shutdown signal
    Shutdown(ScopeId),
    /// Successful cast
    Sent(RouteMessage),
    /// Successful Response from a call
    Response(RouteMessage),
    /// Requested service tree
    ServiceTree(Service),
}

#[derive(serde::Serialize, serde::Deserialize)]
pub enum Error {
    Shutdown(ScopeId, String),
    Cast(ScopeId, RouteMessage, String),
    Call(ScopeId, RouteMessage, String),
    ServiceTree(String),
}

pub type ResponseResult = Result<Response, Error>;

#[derive(serde::Deserialize)]
pub struct Interface {
    pub(crate) scope_id: Option<ScopeId>,
    pub(crate) event: Event,
}

use crate::core::*;
// Websocket supervisor

pub struct Websocket {
    addr: std::net::SocketAddr,
    ttl: Option<u32>,
    link_to: Option<Box<dyn Shutdown>>,
}

impl Websocket {
    /// Create new Websocket struct
    pub fn new(addr: std::net::SocketAddr) -> Self {
        Self {
            addr,
            ttl: None,
            link_to: None,
        }
    }
    /// Set Time to live
    pub fn set_ttl(mut self, ttl: u32) -> Self {
        self.ttl.replace(ttl);
        self
    }
    /// Link handle to the websocket
    pub fn link_to(mut self, handle: Box<dyn Shutdown>) -> Self {
        self.link_to.replace(handle);
        self
    }
}

pub enum WebsocketEvent {
    Shutdown,
    Microservice(ScopeId, Service, Option<ActorResult>),
    /// New TcpStream from listener
    TcpStream(tokio::net::TcpStream),
}

impl<T: Actor> ReportEvent<T, Service> for WebsocketEvent {
    fn report_event(scope_id: ScopeId, service: Service) -> Self {
        Self::Microservice(scope_id, service, None)
    }
}

impl<T: Actor> EolEvent<T> for WebsocketEvent {
    fn eol_event(scope_id: ScopeId, service: Service, _actor: T, r: ActorResult) -> Self {
        Self::Microservice(scope_id, service, Some(r))
    }
}

impl ShutdownEvent for WebsocketEvent {
    fn shutdown_event() -> Self {
        Self::Shutdown
    }
}

#[async_trait::async_trait]
impl Actor for Websocket {
    type Channel = UnboundedChannel<WebsocketEvent>;
    async fn init<S: Supervise<Self>>(&mut self, rt: &mut Self::Context<S>) -> Result<Self::Deps, Reason> {
        let listener = WebsocketListener::new(self.addr, self.ttl);
        rt.spawn("Listener", listener).await?;
        Ok(())
    }
    async fn run<S: Supervise<Self>>(&mut self, rt: &mut Self::Context<S>, _deps: Self::Deps) -> ActorResult {
        while let Some(event) = rt.inbox_mut().next().await {
            match event {
                WebsocketEvent::Shutdown => {
                    rt.stop().await;
                    if rt.microservices_stopped() {
                        break;
                    }
                }
                WebsocketEvent::TcpStream(tcp_stream) => {
                    if let Ok(peer) = tcp_stream.peer_addr() {
                        if let Ok(ws_stream) = tokio_tungstenite::accept_async(tcp_stream).await {
                            let peer: String = peer.to_string();
                            let sender_name = format!("Sender@{}", peer);
                            let receiver_name = format!("Receiver@{}", peer);
                            let (split_sink, split_stream) = ws_stream.split();
                            let sender = WebsocketSender::new(split_sink);
                            if let Ok((sender_handle, _)) = rt.spawn(sender_name, sender).await {
                                let receiver = WebsocketReceiver::new(split_stream, sender_handle);
                                rt.spawn(receiver_name, receiver).await.ok();
                            }
                        }
                    }
                }
                WebsocketEvent::Microservice(scope_id, service, _r_opt) => {
                    if service.is_stopped() {
                        rt.remove_microservice(scope_id);
                    } else {
                        rt.upsert_microservice(scope_id, service);
                    }
                    if rt.microservices_stopped() {
                        break;
                    }
                }
            }
        }
        Ok(())
    }
}
