use backstage::core::*;

////////////////// First ///////////

struct First;

#[async_trait::async_trait]
impl Actor for First {
    type Channel = AbortableUnboundedChannel<(ScopeId, String)>;
    async fn init<S: Supervise<Self>>(&mut self, _rt: &mut Self::Context<S>) -> Result<Self::Data, Reason> {
        Ok(())
    }
    async fn run<S: Supervise<Self>>(&mut self, rt: &mut Self::Context<S>, _data: Self::Data) -> ActorResult {
        while let Some(event) = rt.inbox_mut().next().await {
            log::info!("First received from: ScopeId: {}, message: {}", event.0, event.1);
            rt.send(event.0, "Hey Second".to_string()).await.ok();
        }
        Ok(())
    }
}
//////////////// Second ////////////

struct Second;

#[async_trait::async_trait]
impl Actor for Second {
    type Data = ScopeId;
    type Channel = AbortableUnboundedChannel<String>;
    async fn init<S: Supervise<Self>>(&mut self, rt: &mut Self::Context<S>) -> Result<Self::Data, Reason> {
        if let Some(first_scope_id) = rt.sibling("first").scope_id().await {
            return Ok(first_scope_id);
        };
        Err(Reason::Exit)
    }
    async fn run<S: Supervise<Self>>(&mut self, rt: &mut Self::Context<S>, first_scope_id: Self::Data) -> ActorResult {
        let my_scope_id = rt.scope_id();
        rt.send(first_scope_id, (my_scope_id, "Hey first".to_string())).await.ok();
        while let Some(event) = rt.inbox_mut().next().await {
            log::info!("Second received: {}", event);
        }
        Ok(())
    }
}
// The root custome actor, equivalent to a launcher;
struct Backstage;
enum BackstageEvent {
    Shutdown,
    Microservice(ScopeId, Service),
}

///// Alll of these should be implemented using proc_macro or some macro. start //////
impl ShutdownEvent for BackstageEvent {
    fn shutdown_event() -> Self {
        Self::Shutdown
    }
}
impl<T: Actor> ReportEvent<T, Service> for BackstageEvent {
    fn report_event(scope_id: ScopeId, service: Service) -> Self {
        Self::Microservice(scope_id, service)
    }
}
impl<T: Actor> EolEvent<T> for BackstageEvent {
    fn eol_event(scope_id: ScopeId, service: Service, _actor: T, _r: ActorResult) -> Self {
        Self::Microservice(scope_id, service)
    }
}
///// All of these should be implemented using proc_macro or some macro end ///////

#[async_trait::async_trait]
impl Actor for Backstage {
    type Channel = UnboundedChannel<BackstageEvent>;
    async fn init<S: Supervise<Self>>(&mut self, rt: &mut Self::Context<S>) -> Result<Self::Data, Reason> {
        log::info!("Backstage: {}", rt.service().status());
        // build and spawn your apps actors using the rt
        // - build First
        let first = First;
        // start first
        rt.start(Some("first".into()), first).await?;
        //
        // - build Second
        let second = Second;
        // start second
        rt.start(Some("second".into()), second).await?;
        Ok(())
    }
    async fn run<S: Supervise<Self>>(&mut self, rt: &mut Self::Context<S>, _deps: Self::Data) -> ActorResult {
        log::info!("Backstage: {}", rt.service().status());
        while let Some(event) = rt.inbox_mut().next().await {
            match event {
                BackstageEvent::Shutdown => {
                    rt.stop().await;
                    log::info!("backstage got shutdown signal");
                    if rt.microservices_stopped() {
                        rt.inbox_mut().close();
                    }
                }
                BackstageEvent::Microservice(scope_id, service) => {
                    log::info!(
                        "Microservice: {}, dir: {:?}, status: {}",
                        service.actor_type_name(),
                        service.directory(),
                        service.status()
                    );
                    rt.upsert_microservice(scope_id, service);
                    if rt.microservices_stopped() {
                        rt.inbox_mut().close();
                    }
                }
            }
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let backstage = Backstage;
    let websocket_server_addr = "127.0.0.1:9000".parse::<std::net::SocketAddr>().expect("parsable socket addr");
    let runtime = Runtime::new(Some("backstage".into()), backstage)
        .await
        .expect("Runtime to build")
        .websocket_server(websocket_server_addr, None)
        .await
        .expect("Websocket server to run");
    runtime.block_on().await.expect("Runtime to shutdown gracefully");
}
