// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;
/// The returned result by the actor
pub type ActorResult = std::result::Result<(), Reason>;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// Actor shutdown error reason.
pub enum Reason {
    /// The actor got Aborted while working on something critical.
    Aborted,
    /// The actor exit, as it cannot do anything further.
    // example maybe the disk is full or a bind address is in-use
    Exit,
    /// The actor is asking for restart, if possible.
    Restart(Option<Duration>),
}
