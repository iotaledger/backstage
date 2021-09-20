// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use backstage::core::{
    AbortableUnboundedChannel,
    Actor,
    ActorResult,
    EolEvent,
    NullChannel,
    ReportEvent,
    Rt,
    Runtime,
    ScopeId,
    Service,
    StreamExt,
    SupHandle,
};

// First actor
struct First;

#[async_trait::async_trait]
impl<S> Actor<S> for First
where
    S: SupHandle<Self>,
{
    type Data = ();
    type Channel = NullChannel;
    async fn init(&mut self, rt: &mut Rt<Self, S>) -> ActorResult<()> {
        assert!(rt.service().is_initializing());
        Ok(())
    }
    async fn run(&mut self, rt: &mut Rt<Self, S>, _data: Self::Data) -> ActorResult<()> {
        assert!(rt.service().is_running());
        Ok(())
    }
}

struct Second;

#[async_trait::async_trait]
impl<S> Actor<S> for Second
where
    S: SupHandle<Self>,
{
    type Data = ();
    type Channel = NullChannel;
    async fn init(&mut self, rt: &mut Rt<Self, S>) -> ActorResult<()> {
        assert!(rt.service().is_initializing());
        Ok(())
    }
    async fn run(&mut self, rt: &mut Rt<Self, S>, _data: Self::Data) -> ActorResult<()> {
        assert!(rt.service().is_running());
        Ok(())
    }
}

//////// Our root runtime actor ////////
struct RuntimeActor;

enum RuntimeEvent {
    Microservice(ScopeId, Service),
}

impl<T> EolEvent<T> for RuntimeEvent {
    fn eol_event(scope_id: ScopeId, service: Service, _actor: T, _r: ActorResult<()>) -> Self {
        Self::Microservice(scope_id, service)
    }
}

impl<T> ReportEvent<T> for RuntimeEvent {
    fn report_event(scope_id: ScopeId, service: Service) -> Self {
        Self::Microservice(scope_id, service)
    }
}

#[async_trait::async_trait]
impl<S> Actor<S> for RuntimeActor
where
    S: SupHandle<Self>,
{
    type Data = ();
    type Channel = AbortableUnboundedChannel<RuntimeEvent>;
    async fn init(&mut self, rt: &mut Rt<Self, S>) -> ActorResult<()> {
        // start short lived 2000 actors
        for n in 0..1000 {
            rt.spawn(format!("f{}", n), First).await?;
            rt.spawn(format!("s{}", n), Second).await?;
        }
        Ok(())
    }
    async fn run(&mut self, rt: &mut Rt<Self, S>, _data: Self::Data) -> ActorResult<()> {
        assert!(rt.service().is_running());
        while let Some(event) = rt.inbox_mut().next().await {
            match event {
                RuntimeEvent::Microservice(scope_id, service) => {
                    assert!(service.is_type::<First>() || service.is_type::<Second>());
                    if service.is_stopped() {
                        rt.remove_microservice(scope_id);
                    } else {
                        rt.upsert_microservice(scope_id, service);
                    }
                    // stop the runtime test if all children are offline
                    if rt.microservices_stopped() {
                        rt.inbox_mut().close(); // or break
                    }
                }
            }
        }
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn actors() {
    let runtime = Runtime::new(None, RuntimeActor).await.expect("Runtime to run");
    runtime.block_on().await.expect("Runtime to gracefully shutdown");
}
