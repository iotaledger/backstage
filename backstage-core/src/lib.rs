// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! Backstage actor framework

#![warn(missing_docs)]
/// Backstage core functionality
pub mod core;
#[cfg(feature = "prefabs")]
/// Backstage core functionality
pub mod prefab;

#[cfg(feature = "config")]
pub mod config;

/// Spawn a task with a provided name, if tokio console tracing is enabled
pub fn spawn_task<T>(_name: &str, future: T) -> tokio::task::JoinHandle<T::Output>
where
    T: futures::Future + Send + 'static,
    T::Output: Send + 'static,
{
    #[cfg(all(tokio_unstable, feature = "console"))]
    return tokio::task::Builder::new().name(_name).spawn(future);

    #[cfg(not(all(tokio_unstable, feature = "console")))]
    return tokio::spawn(future);
}
