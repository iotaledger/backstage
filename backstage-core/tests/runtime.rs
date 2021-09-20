// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use backstage::core::{
    Actor,
    ActorResult,
    NullChannel,
    Rt,
    Runtime,
    SupHandle,
};

struct RuntimeActor;

#[async_trait::async_trait]
impl<S> Actor<S> for RuntimeActor
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

#[tokio::test]
async fn runtime() {
    let runtime = Runtime::new(None, RuntimeActor).await.expect("Runtime to run");
    runtime.block_on().await.expect("Runtime to gracefully shutdown");
}
