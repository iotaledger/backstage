// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

/// Configuration type which defines how configuration files are serialized and deserialized
pub trait ConfigFileType {
    /// The extension of this config type
    fn extension() -> &'static str;

    /// Serialize the config to a writer
    fn serialize<C: Serialize, W: Write>(config: &C, writer: &mut W) -> anyhow::Result<()>;

    /// Deserialize the config from a reader
    fn deserialize<C: DeserializeOwned, R: Read>(reader: &mut R) -> anyhow::Result<C>;
}

/// Marker trait which enables the default file save implementation
pub trait DefaultFileSave {}

/// Marker trait which enables the default file load implementation
pub trait DefaultFileLoad {}

impl<T> SerializableConfig for T
where
    T: FileSystemConfig + DefaultFileSave + Serialize,
{
    fn save(&self) -> anyhow::Result<()> {
        let path = T::dir();
        debug!("Saving config to {}", path.to_string_lossy());
        if let Some(dir) = path.parent() {
            if !dir.exists() {
                std::fs::create_dir_all(dir)?;
            }
        }
        T::write_file()?.write_config(self)
    }
}

impl<T> LoadableConfig for T
where
    T: FileSystemConfig + DefaultFileLoad + DeserializeOwned,
{
    fn load() -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        T::read_file()?.read_config()
    }
}

impl<C: FileSystemConfig + LoadableConfig + DeserializeOwned> ConfigReader<C> for File {
    fn read_config(mut self) -> anyhow::Result<C> {
        C::ConfigType::deserialize(&mut self)
    }
}

impl<C: FileSystemConfig + SerializableConfig + Serialize> ConfigWriter<C> for File {
    fn write_config(&mut self, config: &C) -> anyhow::Result<()> {
        C::ConfigType::serialize(config, self)
    }
}

/// Defines values which can be used to load a config from a serialized file
pub trait FileSystemConfig {
    /// The type of config. Can be anything which defines a `ConfigFileType` implementation.
    type ConfigType: ConfigFileType;
    /// The directory from which to load the config file
    const CONFIG_DIR: &'static str = ".";
    /// The config filename
    const FILENAME: &'static str = DEFAULT_FILENAME;

    /// Get the directory from which to load the config file.
    /// Defaults to checking the `CONFIG_DIR` env variable, then the defined constant.
    fn dir() -> PathBuf {
        std::env::var("CONFIG_DIR")
            .ok()
            .unwrap_or(Self::CONFIG_DIR.to_string())
            .into()
    }

    /// Open the config file for reading
    fn read_file() -> anyhow::Result<File> {
        OpenOptions::new()
            .read(true)
            .open(Self::dir().join(format!("{}.{}", Self::FILENAME, Self::ConfigType::extension())))
            .map_err(|e| anyhow!(e))
    }

    /// Open the config file for writing (or create one)
    fn write_file() -> anyhow::Result<File> {
        OpenOptions::new()
            .create(true)
            .write(true)
            .open(Self::dir().join(format!("{}.{}", Self::FILENAME, Self::ConfigType::extension())))
            .map_err(|e| anyhow!(e))
    }
}
