use crate::{Act, Actor, BaseRuntime, Channel, Res, ResourceRuntime, Sender, SupervisorEvent, Sys, System, SystemRuntime};

pub trait Dependencies<Rt: BaseRuntime> {
    fn instantiate(rt: &Rt) -> anyhow::Result<Self>
    where
        Self: Sized;
}

impl<Rt: SystemRuntime, S: 'static + System<H, E> + Send + Sync, H, E> Dependencies<Rt> for Sys<S, H, E>
where
    H: 'static + Sender<E> + Send + Clone + Send + Sync,
    E: 'static + SupervisorEvent + Send + Sync,
{
    fn instantiate(rt: &Rt) -> anyhow::Result<Self> {
        rt.system()
            .ok_or_else(|| anyhow::anyhow!("Missing system dependency: {}", std::any::type_name::<S>()))
    }
}

impl<Rt: BaseRuntime, A: Actor<H, E> + Send + Sync, H, E> Dependencies<Rt> for Act<A, H, E>
where
    H: 'static + Sender<E> + Send + Clone + Send + Sync,
    E: 'static + SupervisorEvent + Send + Sync,
{
    fn instantiate(rt: &Rt) -> anyhow::Result<Self> {
        rt.actor_event_handle()
            .ok_or_else(|| anyhow::anyhow!("Missing actor dependency: {}", std::any::type_name::<A>()))
    }
}

impl<Rt: ResourceRuntime, R: 'static + Send + Sync + Clone> Dependencies<Rt> for Res<R> {
    fn instantiate(rt: &Rt) -> anyhow::Result<Self> {
        rt.resource()
            .ok_or_else(|| anyhow::anyhow!("Missing resource dependency: {}", std::any::type_name::<R>()))
    }
}

impl<Rt: BaseRuntime> Dependencies<Rt> for () {
    fn instantiate(_rt: &Rt) -> anyhow::Result<Self> {
        Ok(())
    }
}

macro_rules! impl_dependencies {
    ($($gen:ident),+) => {
        impl<Rt: BaseRuntime, $($gen),+> Dependencies<Rt> for ($($gen),+,)
        where $($gen: Dependencies<Rt>),+
        {
            fn instantiate(rt: &Rt) -> anyhow::Result<Self>
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
