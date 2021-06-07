use crate::BaseRuntime;

pub struct NullRuntime;

impl<T: BaseRuntime> From<T> for NullRuntime {
    fn from(_: T) -> Self {
        Self
    }
}
