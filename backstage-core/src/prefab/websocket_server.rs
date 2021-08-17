use crate::core::*;

pub struct WebsocketServer {
    addr: std::net::SocketAddr,
    ttl: Option<u32>,
}

impl WebsocketServer {
    pub fn new(addr: std::net::SocketAddr) -> Self {
        Self { addr, ttl: None }
    }
    pub fn set_ttl(mut self, ttl: u32) -> Self {
        self.ttl.replace(ttl);
        self
    }
}

#[async_trait::async_trait]
impl ChannelBuilder<TcpListenerStream> for WebsocketServer {
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
            })?
        }
        Ok(TcpListenerStream::new(listener))
    }
}

#[async_trait::async_trait]
impl Actor for WebsocketServer {
    type Channel = TcpListenerStream;
    async fn init<S: Supervise<Self>>(&mut self, rt: &mut Self::Context<S>) -> Result<Self::Deps, Reason> {
        Ok(())
    }
    async fn run<S: Supervise<Self>>(&mut self, rt: &mut Self::Context<S>, _deps: Self::Deps) -> ActorResult {
        while let Some(r) = rt.inbox_mut().next().await {
            if let Ok(stream) = r {
                if let Ok(peer) = stream.peer_addr() {
                    if let Ok(ws_stream) = tokio_tungstenite::accept_async(stream).await {
                        // todo spawn generic websocket actor ..
                    }
                }
            }
        }
        Ok(())
    }
}
