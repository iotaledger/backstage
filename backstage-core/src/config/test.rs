// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::{
    file::*,
    *,
};
use serde::{
    Deserialize,
    Serialize,
};
use std::convert::{
    TryFrom,
    TryInto,
};

#[derive(Default, Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
struct MyConfig {
    pub name: String,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
struct VersionedConfig {
    version: u32,
    config: MyConfig,
}

impl Default for VersionedConfig {
    fn default() -> Self {
        Self {
            version: CURRENT_VERSION,
            config: Default::default(),
        }
    }
}

// Could be made into proc macro
impl FileSystemConfig for VersionedConfig {
    #[cfg(feature = "ron_config")]
    type ConfigType = RONConfig;
    #[cfg(all(feature = "json_config", not(any(feature = "ron_config", feature = "toml_config"))))]
    type ConfigType = JSONConfig;
    #[cfg(all(feature = "toml_config", not(any(feature = "ron_config", feature = "json_config"))))]
    type ConfigType = TOMLConfig;
}
impl DefaultFileSave for VersionedConfig {}

impl LoadableConfig for VersionedConfig {
    fn load() -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        VersionedValue::load()?.verify_version()?.try_into()
    }

    fn load_or_save_default() -> anyhow::Result<Self>
    where
        Self: Sized + Default + SerializableConfig,
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

impl TryFrom<VersionedValue> for VersionedConfig {
    type Error = anyhow::Error;

    fn try_from(VersionedValue { version, config }: VersionedValue) -> anyhow::Result<VersionedConfig> {
        Ok(Self {
            version,
            config: MyConfig::deserialize(config)?,
        })
    }
}

const CURRENT_VERSION: u32 = 3;

/// A variant of `VersionedConfig` which is used to deserialize
/// a config file independent of its inner structure so the version can
/// be validated.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
struct VersionedValue {
    version: u32,
    #[cfg(feature = "ron_config")]
    config: ron::Value,
    #[cfg(all(feature = "json_config", not(any(feature = "ron_config", feature = "toml_config"))))]
    config: serde_json::Value,
    #[cfg(all(feature = "toml_config", not(any(feature = "ron_config", feature = "json_config"))))]
    config: toml::Value,
}

impl VersionedValue {
    fn verify_version(self) -> anyhow::Result<Self> {
        anyhow::ensure!(
            self.version == CURRENT_VERSION,
            "Config file version mismatch! Expected: {}, Actual: {}",
            CURRENT_VERSION,
            self.version
        );
        Ok(self)
    }
}

// Could be made into proc macro
impl FileSystemConfig for VersionedValue {
    type ConfigType = <VersionedConfig as FileSystemConfig>::ConfigType;
}
impl DefaultFileLoad for VersionedValue {}

#[test]
fn test_load() {
    if let Err(_) = VersionedConfig::load_or_save_default() {
        VersionedConfig::load().unwrap();
    }
}
