// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use backstage::core::*;

////////////////// First ///////////

struct First;

#[async_trait::async_trait]
impl<S> Actor<S> for First
where
    S: SupHandle<Self>,
{
    type Data = ();
    type Channel = AbortableUnboundedChannel<String>;
    async fn init(&mut self, _rt: &mut Rt<Self, S>) -> Result<Self::Data, Reason> {
        Ok(())
    }
    async fn run(&mut self, rt: &mut Rt<Self, S>, _data: Self::Data) -> ActorResult {
        while let Some(event) = rt.inbox_mut().next().await {
            log::info!("First received: {}", event);
            if let Some(second_scope_id) = rt.sibling("second").scope_id().await {
                rt.send(second_scope_id, "Hey second".to_string()).await.ok();
            }
        }
        Ok(())
    }
}
//////////////// Second ////////////

struct Second;

#[async_trait::async_trait]
impl<S> Actor<S> for Second
where
    S: SupHandle<Self>,
{
    type Data = ScopeId;
    type Channel = AbortableUnboundedChannel<String>;
    async fn init(&mut self, rt: &mut Rt<Self, S>) -> Result<Self::Data, Reason> {
        if let Some(first_scope_id) = rt.sibling("first").scope_id().await {
            return Ok(first_scope_id);
        };
        Err(Reason::Exit)
    }
    async fn run(&mut self, rt: &mut Rt<Self, S>, first_scope_id: Self::Data) -> ActorResult {
        rt.send(first_scope_id, "Hey first".to_string()).await.ok();
        while let Some(event) = rt.inbox_mut().next().await {
            log::info!("Second received: {}", event);
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
    env_logger::init();
    let backstage = Backstage;
    let websocket_server_addr = "127.0.0.1:9000"
        .parse::<std::net::SocketAddr>()
        .expect("parsable socket addr");
    let runtime = Runtime::new(Some("backstage".into()), backstage)
        .await
        .expect("Runtime to build")
        .websocket_server(websocket_server_addr, None)
        .await
        .expect("Websocket server to run");
    runtime.block_on().await.expect("Runtime to shutdown gracefully");
}
