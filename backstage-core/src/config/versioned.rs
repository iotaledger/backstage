// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::{
    bail,
    file::*,
    persist::Persist,
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
    ops::{
        Deref,
        DerefMut,
    },
};

/// A versioned configuration
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct VersionedConfig<C, const V: usize> {
    version: usize,
    config: C,
}

impl<C: FileSystemConfig + DeserializeOwned, const V: usize> LoadableConfig for VersionedConfig<C, V>
where
    VersionedValue<C, V>: DeserializeOwned,
    <C as FileSystemConfig>::ConfigType: ValueType,
    <<C as FileSystemConfig>::ConfigType as ValueType>::Value:
        std::fmt::Debug + Serialize + DeserializeOwned + Clone + PartialEq,
    for<'de> <<C as FileSystemConfig>::ConfigType as ValueType>::Value: Deserializer<'de>,
    for<'de> <<<C as FileSystemConfig>::ConfigType as ValueType>::Value as Deserializer<'de>>::Error:
        Into<anyhow::Error>,
{
    /// Load a versioned configuration from a file. Will fail if the file does not exist or the actual version does not
    /// match the requested one.
    fn load() -> anyhow::Result<Self> {
        VersionedValue::load()?.verify_version()?.try_into()
    }

    /// Load a versioned configuration from a file. Will fail if the file does not exist or the actual version does not
    /// match the requested one. If the file does not exist, a default config will be saved to the disk.
    fn load_or_save_default() -> anyhow::Result<Self>
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

impl<C, const V: usize> Deref for VersionedConfig<C, V> {
    type Target = C;

    fn deref(&self) -> &Self::Target {
        &self.config
    }
}

impl<C, const V: usize> DerefMut for VersionedConfig<C, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.config
    }
}

impl<C: FileSystemConfig + Serialize, const V: usize> Persist for VersionedConfig<C, V> {
    fn persist(&self) -> anyhow::Result<()> {
        self.save()
    }
}

// Could be made into proc macro
impl<C: FileSystemConfig, const V: usize> FileSystemConfig for VersionedConfig<C, V> {
    type ConfigType = <C as FileSystemConfig>::ConfigType;
    const CONFIG_DIR: &'static str = C::CONFIG_DIR;
    const FILENAME: &'static str = C::FILENAME;
}
impl<C, const V: usize> DefaultFileSave for VersionedConfig<C, V> {}

impl<C: FileSystemConfig + DeserializeOwned, const V: usize> TryFrom<VersionedValue<C, V>> for VersionedConfig<C, V>
where
    <C as FileSystemConfig>::ConfigType: ValueType,
    <<C as FileSystemConfig>::ConfigType as ValueType>::Value:
        std::fmt::Debug + Serialize + DeserializeOwned + Clone + PartialEq,
    for<'de> <<C as FileSystemConfig>::ConfigType as ValueType>::Value: Deserializer<'de>,
    for<'de> <<<C as FileSystemConfig>::ConfigType as ValueType>::Value as Deserializer<'de>>::Error:
        Into<anyhow::Error>,
{
    type Error = anyhow::Error;

    fn try_from(VersionedValue { version, config }: VersionedValue<C, V>) -> anyhow::Result<VersionedConfig<C, V>> {
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
pub struct VersionedValue<C, const V: usize>
where
    C: FileSystemConfig,
    <C as FileSystemConfig>::ConfigType: ValueType,
    <<C as FileSystemConfig>::ConfigType as ValueType>::Value:
        std::fmt::Debug + Serialize + DeserializeOwned + Clone + PartialEq,
{
    version: usize,
    config: <<C as FileSystemConfig>::ConfigType as ValueType>::Value,
}

impl<C, const V: usize> VersionedValue<C, V>
where
    C: FileSystemConfig,
    <C as FileSystemConfig>::ConfigType: ValueType,
    <<C as FileSystemConfig>::ConfigType as ValueType>::Value:
        std::fmt::Debug + Serialize + DeserializeOwned + Clone + PartialEq,
{
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

impl<C, const V: usize> FileSystemConfig for VersionedValue<C, V>
where
    C: FileSystemConfig,
    <C as FileSystemConfig>::ConfigType: ValueType,
    <<C as FileSystemConfig>::ConfigType as ValueType>::Value:
        std::fmt::Debug + Serialize + DeserializeOwned + Clone + PartialEq,
{
    type ConfigType = <C as FileSystemConfig>::ConfigType;
    const CONFIG_DIR: &'static str = C::CONFIG_DIR;
    const FILENAME: &'static str = C::FILENAME;
}
impl<C, const V: usize> DefaultFileLoad for VersionedValue<C, V>
where
    C: FileSystemConfig,
    <C as FileSystemConfig>::ConfigType: ValueType,
    <<C as FileSystemConfig>::ConfigType as ValueType>::Value:
        std::fmt::Debug + Serialize + DeserializeOwned + Clone + PartialEq,
{
}
