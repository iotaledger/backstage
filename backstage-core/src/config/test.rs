// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use std::marker::PhantomData;

use super::{
    file::{
        ConfigFileType,
        FileSystemConfig,
    },
    versioned::*,
    *,
};
use serde::{
    Deserialize,
    Serialize,
};

const CURRENT_VERSION: usize = 3;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct MyConfig<CT> {
    pub name: String,
    #[serde(skip)]
    _config_type: PhantomData<fn(CT) -> CT>,
}

impl<CT> Default for MyConfig<CT> {
    fn default() -> Self {
        Self {
            name: Default::default(),
            _config_type: Default::default(),
        }
    }
}

impl<CT> Clone for MyConfig<CT> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            _config_type: Default::default(),
        }
    }
}

impl<CT: ConfigFileType> FileSystemConfig for MyConfig<CT> {
    type ConfigType = CT;
}

#[cfg(feature = "ron_config")]
#[test]
fn test_load_ron() {
    if let Err(_) = VersionedConfig::<MyConfig<RONConfig>, CURRENT_VERSION>::load_or_save_default() {
        VersionedConfig::<MyConfig<RONConfig>, CURRENT_VERSION>::load().unwrap();
    }
}

#[cfg(feature = "toml_config")]
#[test]
fn test_load_toml() {
    if let Err(_) = VersionedConfig::<MyConfig<TOMLConfig>, CURRENT_VERSION>::load_or_save_default() {
        VersionedConfig::<MyConfig<TOMLConfig>, CURRENT_VERSION>::load().unwrap();
    }
}

#[cfg(feature = "json_config")]
#[test]
fn test_load_json() {
    if let Err(_) = VersionedConfig::<MyConfig<JSONConfig>, CURRENT_VERSION>::load_or_save_default() {
        VersionedConfig::<MyConfig<JSONConfig>, CURRENT_VERSION>::load().unwrap();
    }
}
