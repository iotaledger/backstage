use crate::{Act, Actor, BackstageRuntime, Channel, Res, Sys, System};

pub trait Dependencies {
    fn instantiate(rt: &BackstageRuntime) -> anyhow::Result<Self>
    where
        Self: Sized;
}

impl<S: 'static + System + Send + Sync> Dependencies for Sys<S> {
    fn instantiate(rt: &BackstageRuntime) -> anyhow::Result<Self> {
        rt.system()
            .ok_or_else(|| anyhow::anyhow!("Missing system dependency: {}", std::any::type_name::<S>()))
    }
}

impl<A: Actor> Dependencies for Act<A>
where
    <A::Channel as Channel<A::Event>>::Sender: 'static,
{
    fn instantiate(rt: &BackstageRuntime) -> anyhow::Result<Self> {
        rt.actor_event_handle()
            .ok_or_else(|| anyhow::anyhow!("Missing actor dependency: {}", std::any::type_name::<A>()))
    }
}

impl<R: 'static + Send + Sync> Dependencies for Res<R> {
    fn instantiate(rt: &BackstageRuntime) -> anyhow::Result<Self> {
        rt.resource()
            .ok_or_else(|| anyhow::anyhow!("Missing resource dependency: {}", std::any::type_name::<R>()))
    }
}

impl Dependencies for () {
    fn instantiate(_rt: &BackstageRuntime) -> anyhow::Result<Self> {
        Ok(())
    }
}

macro_rules! impl_dependencies {
    ($($gen:ident),+) => {
        impl<$($gen),+> Dependencies for ($($gen),+,)
        where $($gen: Dependencies),+
        {
            fn instantiate(rt: &BackstageRuntime) -> anyhow::Result<Self>
            {
                Ok(($($gen::instantiate(rt)?),+,))
            }
        }
    };
}

impl_dependencies!(A);
impl_dependencies!(A, B);
impl_dependencies!(A, B, C);
impl_dependencies!(A, B, C, D);
impl_dependencies!(A, B, C, D, E);
impl_dependencies!(A, B, C, D, E, F);
impl_dependencies!(A, B, C, D, E, F, G);
impl_dependencies!(A, B, C, D, E, F, G, H);
