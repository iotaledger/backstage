use super::WebsocketEvent;
use crate::core::*;

pub struct WebsocketListener {
    addr: std::net::SocketAddr,
    ttl: Option<u32>,
}

impl WebsocketListener {
    /// Create new WebsocketListener struct
    pub(crate) fn new(addr: std::net::SocketAddr, ttl: Option<u32>) -> Self {
        Self { addr, ttl }
    }
}

#[async_trait::async_trait]
impl ChannelBuilder<TcpListenerStream> for WebsocketListener {
    async fn build_channel(&mut self) -> Result<TcpListenerStream, Reason>
    where
        Self: Actor<Channel = TcpListenerStream>,
    {
        let listener = TcpListener::bind(self.addr).await.map_err(|e| {
            log::error!("{}", e);
            Reason::Exit
        })?;
        if let Some(ttl) = self.ttl.as_ref() {
            listener.set_ttl(*ttl).map_err(|e| {
                log::error!("{}", e);
                Reason::Exit
            })?;
        }
        Ok(TcpListenerStream::new(listener))
    }
}

#[async_trait::async_trait]
impl Actor for WebsocketListener {
    type Context<S: Supervise<Self>> = Rt<Self, UnboundedHandle<WebsocketEvent>>;
    type Channel = TcpListenerStream;
    async fn init<S: Supervise<Self>>(&mut self, _rt: &mut Self::Context<S>) -> Result<Self::Deps, Reason> {
        Ok(())
    }
    async fn run<S: Supervise<Self>>(&mut self, rt: &mut Self::Context<S>, _deps: Self::Deps) -> ActorResult {
        while let Some(r) = rt.inbox_mut().next().await {
            if let Ok(stream) = r {
                rt.supervisor_handle().send(WebsocketEvent::TcpStream(stream)).ok();
            }
        }
        // this will link the supervisor to the listener
        rt.supervisor_handle().shutdown().await;
        Ok(())
    }
}
