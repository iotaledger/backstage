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
use std::convert::{
    TryFrom,
    TryInto,
};

/// A versioned configuration
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct VersionedConfig<C, const V: usize> {
    version: usize,
    config: C,
}

impl<C: FileSystemConfig + DeserializeOwned, const V: usize> VersionedConfig<C, V>
where
    VersionedValue<<C as FileSystemConfig>::ConfigType, V>: DeserializeOwned,
    <C as FileSystemConfig>::ConfigType: ValueType,
    for<'de> <<C as FileSystemConfig>::ConfigType as ValueType>::Value: Deserializer<'de>,
    for<'de> <<<C as FileSystemConfig>::ConfigType as ValueType>::Value as Deserializer<'de>>::Error:
        Into<anyhow::Error>,
{
    /// Load a versioned configuration from a file. Will fail if the file does not exist or the actual version does not
    /// match the requested one.
    pub fn load() -> anyhow::Result<Self> {
        VersionedValue::load()?.verify_version()?.try_into()
    }

    /// Load a versioned configuration from a file. Will fail if the file does not exist or the actual version does not
    /// match the requested one. If the file does not exist, a default config will be saved to the disk.
    pub fn load_or_save_default() -> anyhow::Result<Self>
    where
        Self: Default + SerializableConfig,
    {
        match VersionedValue::load() {
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

impl<C: FileSystemConfig + DeserializeOwned, CT: ValueType, const V: usize> TryFrom<VersionedValue<CT, V>>
    for VersionedConfig<C, V>
where
    for<'de> CT::Value: Deserializer<'de>,
    for<'de> <CT::Value as Deserializer<'de>>::Error: Into<anyhow::Error>,
{
    type Error = anyhow::Error;

    fn try_from(VersionedValue { version, config }: VersionedValue<CT, V>) -> anyhow::Result<VersionedConfig<C, V>> {
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
pub struct VersionedValue<CT: ValueType, const V: usize> {
    version: usize,
    config: CT::Value,
}

impl<CT: ValueType, const V: usize> VersionedValue<CT, V> {
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

impl<CT: ConfigFileType + ValueType, const V: usize> FileSystemConfig for VersionedValue<CT, V> {
    type ConfigType = CT;
}
impl<CT: ValueType, const V: usize> DefaultFileLoad for VersionedValue<CT, V> {}
