// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

/// Specifies that the implementor should be able to persist itself
pub trait Persist {
    /// Persist this value
    fn persist(&self) -> anyhow::Result<()>;
}

/// A handle which will persist when dropped
pub struct PersistHandle<C: 'static + Config + Persist + Ord> {
    guard: tokio::sync::RwLockWriteGuard<'static, History<HistoricalConfig<C>>>,
}

impl<C: Config + Persist + Ord> From<tokio::sync::RwLockWriteGuard<'static, History<HistoricalConfig<C>>>>
    for PersistHandle<C>
{
    fn from(guard: tokio::sync::RwLockWriteGuard<'static, History<HistoricalConfig<C>>>) -> Self {
        Self { guard }
    }
}

impl<C: Config + Persist + Ord> Deref for PersistHandle<C> {
    type Target = tokio::sync::RwLockWriteGuard<'static, History<HistoricalConfig<C>>>;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl<C: Config + Persist + Ord> DerefMut for PersistHandle<C> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}

impl<C: Config + Persist + Ord> std::ops::Drop for PersistHandle<C> {
    fn drop(&mut self) {
        if let Err(e) = self.persist() {
            error!("{}", e);
        }
    }
}
