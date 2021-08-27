use backstage::core::*;

////////////////// Incrementer ///////////
use std::sync::{
    atomic::{AtomicIsize, Ordering},
    Arc,
};

struct Incrementer;

#[async_trait::async_trait]
impl Actor for Incrementer {
    type Data = Arc<AtomicIsize>;
    type Channel = IntervalChannel<1000>;
    async fn init<S: Supervise<Self>>(&mut self, rt: &mut Self::Context<S>) -> Result<Self::Data, Reason> {
        log::info!(
            "scope_id: {}, {} is {}",
            rt.scope_id(),
            rt.service().actor_type_name(),
            rt.service().service_status()
        );
        // create atomic resource, and publish it
        let counter = Arc::new(AtomicIsize::new(0));
        rt.add_resource(counter.clone()).await;
        Ok(counter)
    }
    async fn run<S: Supervise<Self>>(&mut self, rt: &mut Self::Context<S>, counter: Self::Data) -> ActorResult {
        while let Some(_instant) = rt.inbox_mut().next().await {
            // increment the counter
            let old_counter = counter.fetch_add(1, Ordering::Relaxed);
            log::info!("Counter in Incrementer is {}", old_counter);
        }
        Ok(())
    }
}
//////////////// Decrementer ////////////

struct Decrementer;

#[async_trait::async_trait]
impl Actor for Decrementer {
    type Data = Arc<AtomicIsize>;
    type Channel = IntervalChannel<1000>;
    async fn init<S: Supervise<Self>>(&mut self, rt: &mut Self::Context<S>) -> Result<Self::Data, Reason> {
        log::info!(
            "scope_id: {}, {} is {}",
            rt.scope_id(),
            rt.service().actor_type_name(),
            rt.service().service_status()
        );
        // link to the atomic resource under the following scope_id
        if let Some(resource_scope_id) = rt.highest_scope_id::<Self::Data>().await {
            let counter = rt.link::<Self::Data>(resource_scope_id).await.map_err(|e| {
                log::error!("{:?}", e);
                Reason::Exit
            })?;
            Ok(counter)
        } else {
            Err(Reason::Exit)
        }
    }
    async fn run<S: Supervise<Self>>(&mut self, rt: &mut Self::Context<S>, counter: Self::Data) -> ActorResult {
        while let Some(_instant) = rt.inbox_mut().next().await {
            // decrement the counter
            let old_counter = counter.fetch_sub(1, Ordering::Relaxed);
            log::info!("Counter in Decrementer is {}", old_counter);
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
        log::info!("Backstage: {}", rt.service().service_status());
        // build and spawn your apps actors using the rt
        // - build Incrementer
        let incrementer = Incrementer;
        // spawn incrementer
        let (_h, i) = rt.spawn(Some("incrementer".into()), incrementer).await.map_err(|e| {
            log::error!("{:?}", e);
            Reason::Exit
        })?;
        // await incrementer till it gets initialized
        i.initialized().await?;
        //
        // - build Decrementer
        let decrementer = Decrementer;
        // spawn decrementer
        rt.spawn(Some("decrementer".into()), decrementer).await.map_err(|e| {
            log::error!("{:?}", e);
            Reason::Exit
        })?;
        Ok(())
    }
    async fn run<S: Supervise<Self>>(&mut self, rt: &mut Self::Context<S>, _deps: Self::Data) -> ActorResult {
        log::info!("Backstage: {}", rt.service().service_status());
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
                        service.service_status()
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
