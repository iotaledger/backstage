// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

/// An actor/system builder, which defines how you take some
/// input data and create an `Actor` or `System` with it.
pub trait Builder {
    /// The type this builder will create
    type Built;

    /// Create the built type with the builder data and a service
    /// provided by the supervisor.
    fn build(self) -> Self::Built;
}
