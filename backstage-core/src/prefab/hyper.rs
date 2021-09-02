/// Our hyper example STARTS from here
use crate::core::*;

pub struct Hyper<T> {
    addr: std::net::SocketAddr,
    make_svc: Option<T>,
}

impl<T> Hyper<T> {
    /// Create new hyper server
    pub fn new(addr: std::net::SocketAddr, make_svc: T) -> Self {
        Self {
            addr,
            make_svc: Some(make_svc),
        }
    }
}

#[async_trait::async_trait]
impl<T, E, F, R> ChannelBuilder<HyperChannel<T>> for Hyper<T>
where
    for<'a> T:
        Send + Sync + 'static + hyper::service::Service<&'a hyper::server::conn::AddrStream, Error = E, Response = R, Future = F> + Send,
    E: std::error::Error + Send + Sync + 'static,
    F: Send + std::future::Future<Output = Result<R, E>> + 'static,
    R: Send + hyper::service::Service<hyper::Request<hyper::Body>, Response = hyper::Response<hyper::Body>> + 'static,
    R::Error: std::error::Error + Send + Sync,
    R::Future: Send,
{
    async fn build_channel(&mut self) -> Result<HyperChannel<T>, Reason> {
        if let Some(make_svc) = self.make_svc.take() {
            let server = hyper::Server::try_bind(&self.addr)
                .map_err(|e| {
                    log::error!("{}", e);
                    Reason::Exit
                })?
                .serve(make_svc);
            Ok(HyperChannel::new(server))
        } else {
            log::error!("No provided make svc to serve");
            return Err(Reason::Exit);
        }
    }
}

#[async_trait::async_trait]
impl<T, E, F, R> Actor for Hyper<T>
where
    for<'a> T:
        Send + Sync + 'static + hyper::service::Service<&'a hyper::server::conn::AddrStream, Error = E, Response = R, Future = F> + Send,
    E: std::error::Error + Send + Sync + 'static,
    F: Send + std::future::Future<Output = Result<R, E>> + 'static,
    R: Send + hyper::service::Service<hyper::Request<hyper::Body>, Response = hyper::Response<hyper::Body>> + 'static,
    R::Error: std::error::Error + Send + Sync,
    R::Future: Send,
{
    type Channel = HyperChannel<T>;
    async fn init<S: Supervise<Self>>(&mut self, rt: &mut Self::Context<S>) -> Result<Self::Data, Reason> {
        log::info!("Hyper: {}", rt.service().status());
        Ok(())
    }
    async fn run<S: Supervise<Self>>(&mut self, rt: &mut Self::Context<S>, _deps: Self::Data) -> ActorResult {
        log::info!("Hyper: {}", rt.service().status());
        if let Err(err) = rt.inbox_mut().ignite().await {
            log::error!("Hyper: {}", err);
            return Err(Reason::Aborted);
        }
        Ok(())
    }
}
