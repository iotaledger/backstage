// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::{
    file::*,
    persist::*,
    *,
};

/// A historical record
#[derive(Serialize, Deserialize, PartialEq, Eq, Default, Debug)]
pub struct HistoricalConfig<C: Config> {
    #[serde(bound(deserialize = "C: DeserializeOwned"))]
    config: C,
    /// The timestamp representing when this record was created
    pub created: u128,
}

impl<C: Config> HistoricalConfig<C> {
    /// Create a new historical record with the current timestamp
    pub fn new(config: C) -> Self {
        Self {
            config,
            created: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos(),
        }
    }
}

impl<C: Config> Clone for HistoricalConfig<C> {
    fn clone(&self) -> Self {
        Self::new(self.config.clone())
    }
}

impl<C: Config> From<C> for HistoricalConfig<C> {
    fn from(record: C) -> Self {
        Self::new(record)
    }
}

impl<C: Config> From<(C, u128)> for HistoricalConfig<C> {
    fn from((config, created): (C, u128)) -> Self {
        Self { config, created }
    }
}

impl<C: Config> Deref for HistoricalConfig<C> {
    type Target = C;

    fn deref(&self) -> &Self::Target {
        &self.config
    }
}

impl<C: Config> DerefMut for HistoricalConfig<C> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.config
    }
}

impl<C: Config> Wrapper for HistoricalConfig<C> {
    fn into_inner(self) -> Self::Target {
        self.config
    }
}

impl<C: Config + FileSystemConfig> FileSystemConfig for HistoricalConfig<C> {
    type ConfigType = C::ConfigType;
    const CONFIG_DIR: &'static str = "./historical_config";
    const FILENAME: &'static str = C::FILENAME;

    fn dir() -> PathBuf {
        PathBuf::from(Self::CONFIG_DIR)
    }
}

impl<C: Config> Persist for HistoricalConfig<C>
where
    Self: FileSystemConfig,
{
    fn persist(&self) -> anyhow::Result<()> {
        let dir = Self::dir();
        debug!("Persisting historical config to {}", dir.to_string_lossy());
        if !dir.exists() {
            std::fs::create_dir_all(&dir)?;
        }
        OpenOptions::new()
            .create(true)
            .write(true)
            .open(dir.join(format!(
                "{}_{}.{}",
                self.created,
                Self::FILENAME,
                <Self as FileSystemConfig>::ConfigType::extension()
            )))
            .map_err(|e| anyhow!(e))?
            .write_config(self)
    }
}

impl<C: Config> PartialOrd for HistoricalConfig<C> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.created.cmp(&other.created))
    }
}

impl<C: Config> Ord for HistoricalConfig<C> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.created.cmp(&other.created)
    }
}

/// A historical record which maintains `max_records`
#[derive(Deserialize, Serialize)]
pub struct History<R: Ord> {
    records: BinaryHeap<R>,
    max_records: usize,
}

impl<R> History<R>
where
    R: DerefMut + Default + Ord + Persist + Wrapper,
{
    /// Create a new history with `max_records`
    pub fn new(max_records: usize) -> Self {
        Self {
            records: BinaryHeap::new(),
            max_records,
        }
    }

    /// Get the most recent record with its created timestamp
    pub fn last(&self) -> R
    where
        R: Clone,
    {
        self.records.peek().cloned().unwrap_or_default()
    }

    /// Get an immutable reference to the latest record without a timestamp
    pub fn latest(&self) -> R::Target
    where
        R::Target: Clone + Default,
    {
        self.records.peek().map(Deref::deref).cloned().unwrap_or_default()
    }

    /// Update the history with a new record
    pub fn update(&mut self, record: R::Target)
    where
        R: From<<R as Deref>::Target>,
        R::Target: Sized,
    {
        self.records.push(record.into());
        self.truncate();
    }

    /// Add to the history with a new record and a timestamp and return a reference to it.
    /// *This should only be used to deserialize a `History`.*
    fn add(&mut self, record: R::Target, created: u128)
    where
        R: From<(<R as Deref>::Target, u128)>,
        R::Target: Sized,
    {
        self.records.push((record, created).into());
        self.truncate();
    }

    /// Rollback to the previous version and return the removed record
    pub fn rollback(&mut self) -> Option<R::Target>
    where
        R::Target: Sized,
    {
        self.records.pop().map(|r| r.into_inner())
    }

    fn truncate(&mut self) {
        self.records = self.records.drain().take(self.max_records).collect();
    }

    /// Get an interator over the time-ordered history
    pub fn iter(&self) -> std::collections::binary_heap::Iter<R> {
        self.records.iter()
    }
}

impl<C: Config + Persist + FileSystemConfig + DeserializeOwned> History<HistoricalConfig<C>> {
    /// Load the historical config from the file system
    pub fn load<M: Into<Option<usize>>>(max_records: M) -> anyhow::Result<Self> {
        let mut history = max_records
            .into()
            .map(|max_records| Self::new(max_records))
            .unwrap_or_default();
        let latest = C::load_or_save_default()?;
        debug!("Latest Config found! {:?}", latest);
        let historical_config_path = <HistoricalConfig<C> as FileSystemConfig>::CONFIG_DIR;
        history.update(latest);
        glob(&format!(r"{}/\d+_config.ron", historical_config_path))
            .into_iter()
            .flat_map(|v| v.into_iter())
            .filter_map(|path| {
                debug!("historical path: {:?}", path);
                path.map(|ref p| {
                    File::open(p)
                        .map_err(|e| anyhow!(e))
                        .and_then(|f| f.read_config())
                        .ok()
                        .and_then(|c| {
                            p.file_name()
                                .and_then(|s| s.to_string_lossy().split("_").next().map(|s| s.to_owned()))
                                .and_then(|s| s.parse::<u128>().ok())
                                .map(|created| (c, created))
                        })
                })
                .ok()
                .flatten()
            })
            .for_each(|(config, created)| {
                history.add(config, created);
            });
        Ok(history)
    }
}

impl<R> Default for History<R>
where
    R: DerefMut + Ord + Persist,
    R::Target: Persist,
{
    fn default() -> Self {
        Self {
            records: Default::default(),
            max_records: 20,
        }
    }
}

impl<C: Config + Persist + FileSystemConfig> Persist for History<HistoricalConfig<C>>
where
    HistoricalConfig<C>: FileSystemConfig,
{
    fn persist(&self) -> anyhow::Result<()> {
        debug!("Persisting history! {:?}", self.records);
        let mut iter = self.records.clone().into_sorted_vec().into_iter().rev();
        if let Some(latest) = iter.next() {
            debug!("Persisting latest config! {:?}", latest);
            latest.deref().persist()?;
            for v in iter {
                debug!("Persisting historical config! {:?}", v);
                //<HistoricalConfig<C> as Persist>::persist(&v)?;
                v.persist()?;
            }
        }
        Ok(())
    }
}
