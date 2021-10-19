// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::Actor;
/// A system is an actor with some specified shared state
pub trait System: Actor {
    /// The shared state of this actor, which more-or-less
    /// defines a public API
    type State: Clone + Send + Sync;
}
