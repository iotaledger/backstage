#![warn(missing_docs)]

pub mod core;
pub mod prefab;

/// Spawn a task with a provided name, if tokio console tracing is enabled
pub fn spawn_task<T>(name: &str, future: T) -> tokio::task::JoinHandle<T::Output>
where
    T: futures::Future + Send + 'static,
    T::Output: Send + 'static,
{
    #[cfg(all(tokio_unstable, feature = "console"))]
    return tokio::task::Builder::new().name(name).spawn(future);

    #[cfg(not(all(tokio_unstable, feature = "console")))]
    return tokio::spawn(future);
}
