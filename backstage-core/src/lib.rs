// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

#![feature(generic_associated_types)]
#![warn(missing_docs)]
pub mod actor;
#[cfg(feature = "prefabs")]
pub mod prefabs;
pub mod runtime;

pub mod prelude {
    pub use crate::{actor::*, runtime::*};
}
