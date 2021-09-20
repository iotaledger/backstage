// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::{
    bail,
    file::*,
    LoadableConfig,
    SerializableConfig,
};
use serde::{
    de::DeserializeOwned,
    Deserialize,
    Deserializer,
    Serialize,
};
use std::{
    convert::{
        TryFrom,
        TryInto,
    },
    marker::PhantomData,
};

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct VersionedConfig<C, const V: usize> {
    version: usize,
    config: C,
}

impl<C: FileSystemConfig + DeserializeOwned, const V: usize> VersionedConfig<C, V> {
    pub fn load<CT, Val>() -> anyhow::Result<Self>
    where
        CT: ConfigFileType,
        Val: 'static,
        for<'de> Val: Deserializer<'de>,
        for<'de> <Val as Deserializer<'de>>::Error: Into<anyhow::Error>,
        VersionedValue<CT, Val, V>: DeserializeOwned,
    {
        VersionedValue::<CT, Val, V>::load()?.verify_version()?.try_into()
    }

    pub fn load_or_save_default<CT, Val>() -> anyhow::Result<Self>
    where
        Self: Default + SerializableConfig,
        CT: ConfigFileType,
        Val: 'static,
        for<'de> Val: Deserializer<'de>,
        for<'de> <Val as Deserializer<'de>>::Error: Into<anyhow::Error>,
        VersionedValue<CT, Val, V>: DeserializeOwned,
    {
        match VersionedValue::<CT, Val, V>::load() {
            Ok(v) => v.verify_version()?.try_into(),
            Err(e) => {
                Self::default().save()?;
                bail!(
                        "Config file was not found! Saving a default config file. Please edit it and restart the application! (Error: {})", e,
                    );
            }
        }
    }
}

impl<C: Default, const V: usize> Default for VersionedConfig<C, V> {
    fn default() -> Self {
        Self {
            version: V,
            config: Default::default(),
        }
    }
}

// Could be made into proc macro
impl<C: FileSystemConfig, const V: usize> FileSystemConfig for VersionedConfig<C, V> {
    type ConfigType = <C as FileSystemConfig>::ConfigType;
}
impl<C, const V: usize> DefaultFileSave for VersionedConfig<C, V> {}

impl<C: FileSystemConfig + DeserializeOwned, CT, Val, const V: usize> TryFrom<VersionedValue<CT, Val, V>>
    for VersionedConfig<C, V>
where
    Val: 'static,
    for<'de> Val: Deserializer<'de>,
    for<'de> <Val as Deserializer<'de>>::Error: Into<anyhow::Error>,
{
    type Error = anyhow::Error;

    fn try_from(
        VersionedValue {
            version,
            config,
            _config_type,
        }: VersionedValue<CT, Val, V>,
    ) -> anyhow::Result<VersionedConfig<C, V>> {
        Ok(Self {
            version,
            config: C::deserialize(config).map_err(|e| anyhow::anyhow!(e))?,
        })
    }
}

/// A variant of `VersionedConfig` which is used to deserialize
/// a config file independent of its inner structure so the version can
/// be validated.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct VersionedValue<CT, Val, const V: usize> {
    version: usize,
    config: Val,
    #[serde(skip)]
    _config_type: PhantomData<fn(CT) -> CT>,
}

impl<CT, Val, const V: usize> VersionedValue<CT, Val, V> {
    fn verify_version(self) -> anyhow::Result<Self> {
        anyhow::ensure!(
            self.version == V,
            "Config file version mismatch! Expected: {}, Actual: {}",
            V,
            self.version
        );
        Ok(self)
    }
}

impl<CT: ConfigFileType, Val, const V: usize> FileSystemConfig for VersionedValue<CT, Val, V> {
    type ConfigType = CT;
}
impl<CT, Val, const V: usize> DefaultFileLoad for VersionedValue<CT, Val, V> {}
