// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::{
    file::FileSystemConfig,
    versioned::*,
    *,
};
use serde::{
    Deserialize,
    Serialize,
};

const CURRENT_VERSION: usize = 3;

#[derive(Default, Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
struct MyConfig {
    pub name: String,
}

impl FileSystemConfig for MyConfig {
    #[cfg(feature = "ron_config")]
    type ConfigType = RONConfig;
    #[cfg(all(feature = "json_config", not(any(feature = "ron_config", feature = "toml_config"))))]
    type ConfigType = JSONConfig;
    #[cfg(all(feature = "toml_config", not(any(feature = "ron_config", feature = "json_config"))))]
    type ConfigType = TOMLConfig;
}

#[cfg(feature = "ron_config")]
#[test]
fn test_load_ron() {
    if let Err(_) = VersionedConfig::<MyConfig, CURRENT_VERSION>::load_or_save_default::<RONConfig, ron::Value>() {
        VersionedConfig::<MyConfig, CURRENT_VERSION>::load::<RONConfig, ron::Value>().unwrap();
    }
}

#[cfg(feature = "toml_config")]
#[test]
fn test_load_toml() {
    if let Err(_) = VersionedConfig::<MyConfig, CURRENT_VERSION>::load_or_save_default::<TOMLConfig, toml::Value>() {
        VersionedConfig::<MyConfig, CURRENT_VERSION>::load::<TOMLConfig, toml::Value>().unwrap();
    }
}

#[cfg(feature = "json_config")]
#[test]
fn test_load_json() {
    if let Err(_) =
        VersionedConfig::<MyConfig, CURRENT_VERSION>::load_or_save_default::<JSONConfig, serde_json::Value>()
    {
        VersionedConfig::<MyConfig, CURRENT_VERSION>::load::<JSONConfig, serde_json::Value>().unwrap();
    }
}
