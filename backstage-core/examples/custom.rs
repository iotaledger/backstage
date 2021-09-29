// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use backstage::{
    core::*,
    prefab::rocket::*,
};
use futures::TryFutureExt;
use rocket::get;

////////////////// Incrementer ///////////
use std::sync::{
    atomic::{
        AtomicIsize,
        Ordering,
    },
    Arc,
};

struct Incrementer;

#[async_trait::async_trait]
impl<S> Actor<S> for Incrementer
where
    S: SupHandle<Self>,
{
    type Data = Arc<AtomicIsize>;
    type Channel = IntervalChannel<1000>;
    async fn init(&mut self, rt: &mut Rt<Self, S>) -> ActorResult<Self::Data> {
        log::info!(
            "scope_id: {}, {} is {}",
            rt.scope_id(),
            rt.service().actor_type_name(),
            rt.service().status(),
        );
        // create atomic resource, and publish it
        let counter = Arc::new(AtomicIsize::new(0));
        rt.add_resource(counter.clone()).await;
        Ok(counter)
    }
    async fn run(&mut self, rt: &mut Rt<Self, S>, counter: Self::Data) -> ActorResult<()> {
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
impl<S> Actor<S> for Decrementer
where
    S: SupHandle<Self>,
{
    type Data = Arc<AtomicIsize>;
    type Channel = IntervalChannel<1000>;
    async fn init(&mut self, rt: &mut Rt<Self, S>) -> ActorResult<Self::Data> {
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
                ActorError::exit_msg(e)
            })?;
            Ok(counter)
        } else {
            Err(ActorError::exit_msg("Unable to find scope for Arc<AtomicIsize> data"))
        }
    }
    async fn run(&mut self, rt: &mut Rt<Self, S>, counter: Self::Data) -> ActorResult<()> {
        while let Some(_instant) = rt.inbox_mut().next().await {
            // decrement the counter
            let old_counter = counter.fetch_sub(1, Ordering::Relaxed);
            log::info!("Counter in Decrementer is {}", old_counter);
        }
        Ok(())
    }
}

// The root custom actor, equivalent to a launcher;
struct Backstage;

#[supervise]
enum BackstageEvent {
    #[shutdown]
    Shutdown,
    #[report]
    Microservice(ScopeId, Service),
}

fn construct_rocket(
    rt: &mut Rt<RocketServer<UnboundedHandle<BackstageEvent>>, UnboundedHandle<BackstageEvent>>,
) -> impl futures::Future<Output = anyhow::Result<rocket::Rocket<rocket::Ignite>>> {
    rocket::build()
        .mount("/api", rocket::routes![info])
        .attach(CORS)
        .attach(RequestTimer::default())
        .register("/", rocket::catchers![internal_error, not_found])
        .ignite()
        .map_err(|e| anyhow::anyhow!(e))
}

async fn construct_rocket_async(
    rt: &mut Rt<RocketServer<UnboundedHandle<BackstageEvent>>, UnboundedHandle<BackstageEvent>>,
) -> anyhow::Result<rocket::Rocket<rocket::Ignite>> {
    rocket::build()
        .mount("/api", rocket::routes![info])
        .attach(CORS)
        .attach(RequestTimer::default())
        .register("/", rocket::catchers![internal_error, not_found])
        .ignite()
        .await
        .map_err(|e| anyhow::anyhow!(e))
}

#[get("/info")]
async fn info() -> &'static str {
    "Got info endpoint!"
}

#[async_trait::async_trait]
impl<S: SupHandle<Self>> Actor<S> for Backstage {
    type Data = ();
    type Channel = UnboundedChannel<BackstageEvent>;
    async fn init(&mut self, rt: &mut Rt<Self, S>) -> ActorResult<Self::Data> {
        log::info!("Backstage: {}", rt.service().status());
        // build and spawn your apps actors using the rt
        // - build Incrementer
        let incrementer = Incrementer;
        // spawn incrementer
        let (_h, i) = rt.spawn(Some("incrementer".into()), incrementer).await.map_err(|e| {
            log::error!("{:?}", e);
            ActorError::exit_msg(format!("{:?}", e))
        })?;
        // await incrementer till it gets initialized
        i.initialized().await?;
        // 
        // - build Decrementer
        let decrementer = Decrementer;
        // spawn decrementer
        rt.spawn(Some("decrementer".into()), decrementer).await.map_err(|e| {
            log::error!("{:?}", e);
            ActorError::exit_msg(format!("{:?}", e))
        })?;
        rt.spawn(Some("rocket".into()), RocketServer::new(construct_rocket_async))
            .await
            .map_err(|e| {
                log::error!("{:?}", e);
                ActorError::exit_msg(format!("{:?}", e))
            })?;
        Ok(())
    }
    async fn run(&mut self, rt: &mut Rt<Self, S>, _deps: Self::Data) -> ActorResult<()> {
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
