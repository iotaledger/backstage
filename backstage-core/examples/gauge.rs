// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use backstage::core::*;

////////////////// Incrementer ///////////

struct Incrementer;

#[async_trait::async_trait]
impl<S> Actor<S> for Incrementer
where
    S: SupHandle<Self>,
{
    type Data = prometheus::IntGauge;
    type Channel = IntervalChannel<10>;
    async fn init(&mut self, rt: &mut Rt<Self, S>) -> Result<Self::Data, Reason> {
        log::info!(
            "scope_id: {}, {} is {}",
            rt.scope_id(),
            rt.service().actor_type_name(),
            rt.service().status(),
        );
        let gauge: prometheus::IntGauge =
            prometheus::core::GenericGauge::new("magnitude", "Decrementer and Incrementer gauge resource").unwrap();
        // register the gauge
        rt.register(gauge.clone()).ok();
        // add it as resource
        rt.add_resource(gauge.clone()).await;
        Ok(gauge)
    }
    async fn run(&mut self, rt: &mut Rt<Self, S>, counter: Self::Data) -> ActorResult {
        while let Some(_instant) = rt.inbox_mut().next().await {
            // increment the counter
            counter.inc();
        }
        Ok(())
    }
}
//////////////// Decrementer ////////////

struct Decrementer;

#[async_trait::async_trait]
impl<S> Actor<S> for Decrementer
where
    S: SupHandle<Self>,
{
    type Data = prometheus::IntGauge;
    type Channel = IntervalChannel<10>;
    async fn init(&mut self, rt: &mut Rt<Self, S>) -> Result<Self::Data, Reason> {
        log::info!(
            "scope_id: {}, {} is {}",
            rt.scope_id(),
            rt.service().actor_type_name(),
            rt.service().status()
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
    async fn run(&mut self, rt: &mut Rt<Self, S>, counter: Self::Data) -> ActorResult {
        while let Some(_instant) = rt.inbox_mut().next().await {
            // decrement the counter
            counter.dec();
        }
        Ok(())
    }
}

// The root custom actor, equivalent to a launcher;
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
impl<T> ReportEvent<T> for BackstageEvent {
    fn report_event(scope_id: ScopeId, service: Service) -> Self {
        Self::Microservice(scope_id, service)
    }
}
impl<T> EolEvent<T> for BackstageEvent {
    fn eol_event(scope_id: ScopeId, service: Service, _actor: T, _r: ActorResult) -> Self {
        Self::Microservice(scope_id, service)
    }
}
///// All of these should be implemented using proc_macro or some macro end ///////

#[async_trait::async_trait]
impl<S> Actor<S> for Backstage
where
    S: SupHandle<Self>,
{
    type Data = ();
    type Channel = UnboundedChannel<BackstageEvent>;
    async fn init(&mut self, rt: &mut Rt<Self, S>) -> Result<Self::Data, Reason> {
        log::info!("Backstage: {}", rt.service().status());
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
    async fn run(&mut self, rt: &mut Rt<Self, S>, _deps: Self::Data) -> ActorResult {
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
    #[cfg(not(feature = "console"))]
    {
        let env = env_logger::Env::new().filter_or("RUST_LOG", "info");
        env_logger::Builder::from_env(env).init();
    }
    let backstage = Backstage;
    let backserver_addr = "127.0.0.1:9000"
        .parse::<std::net::SocketAddr>()
        .expect("parsable socket addr");
    let runtime = Runtime::new(Some("backstage".into()), backstage)
        .await
        .expect("Runtime to build")
        .backserver(backserver_addr)
        .await
        .expect("Websocket server to run");
    runtime.block_on().await.expect("Runtime to shutdown gracefully");
}