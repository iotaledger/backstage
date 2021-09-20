// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::{
    file::*,
    persist::*,
    *,
};

/// A historical record
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Default, Debug)]
pub struct HistoricalConfig<C: Config> {
    #[serde(bound(deserialize = "C: DeserializeOwned"))]
    config: C,
    /// The timestamp representing when this record was created
    pub created: u64,
}

impl<C: Config> HistoricalConfig<C> {
    /// Create a new historical record with the current timestamp
    pub fn new(config: C) -> Self {
        Self {
            config,
            created: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        }
    }
}

impl<C: Config> From<C> for HistoricalConfig<C> {
    fn from(record: C) -> Self {
        Self::new(record)
    }
}

impl<C: Config> From<(C, u64)> for HistoricalConfig<C> {
    fn from((config, created): (C, u64)) -> Self {
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
}

impl<C: Config + FileSystemConfig> Persist for HistoricalConfig<C> {
    fn persist(&self) -> anyhow::Result<()> {
        OpenOptions::new()
            .write(true)
            .open(Self::dir().join(format!(
                "{}_{}.{}",
                self.created,
                Self::FILENAME,
                <Self as FileSystemConfig>::ConfigType::extension()
            )))
            .map_err(|e| anyhow!(e))?
            .write_config(&self.config)
    }
}

impl<C: Config + Ord + PartialOrd> PartialOrd for HistoricalConfig<C> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<C: Config + Ord> Ord for HistoricalConfig<C> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.created.cmp(&other.created)
    }
}

/// A historical record which maintains `max_records`
#[derive(Deserialize, Serialize)]
pub struct History<R: Ord> {
    records: BinaryHeap<R>,
    config_filename: String,
    max_records: usize,
}

impl<R> History<R>
where
    R: DerefMut + Default + Ord + Persist + Wrapper,
{
    /// Create a new history with `max_records`
    pub fn new<S: Into<Option<String>>>(max_records: usize, config_filename: S) -> Self {
        Self {
            records: BinaryHeap::new(),
            config_filename: config_filename.into().unwrap_or_else(|| DEFAULT_FILENAME.to_string()),
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
    pub fn add(&mut self, record: R::Target, created: u64)
    where
        R: From<(<R as Deref>::Target, u64)>,
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

impl<C: Config + Ord + Persist + FileSystemConfig + DeserializeOwned> History<HistoricalConfig<C>> {
    /// Load the historical config from the file system
    pub fn load<M: Into<Option<usize>>, S: Into<Option<String>>>(max_records: M, config_filename: S) -> Self {
        let mut history = max_records
            .into()
            .map(|max_records| Self::new(max_records, config_filename))
            .unwrap_or_default();
        match C::load() {
            Ok(latest) => {
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
                                        .and_then(|s| s.parse::<u64>().ok())
                                        .map(|created| (c, created))
                                })
                        })
                        .ok()
                        .flatten()
                    })
                    .for_each(|(config, created)| {
                        history.add(config, created);
                    });
            }
            Err(e) => {
                panic!("{}", e)
            }
        }
        history
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
            config_filename: DEFAULT_FILENAME.to_string(),
            max_records: 20,
        }
    }
}

impl<C: Config + Ord + Persist> Persist for History<HistoricalConfig<C>> {
    fn persist(&self) -> anyhow::Result<()> {
        debug!("Persisting history!");
        let mut iter = self.records.clone().into_sorted_vec().into_iter().rev();
        if let Some(latest) = iter.next() {
            debug!("Persisting latest config!");
            latest.deref().persist()?;
            for v in iter {
                debug!("Persisting historical config!");
                v.persist()?;
            }
        }
        Ok(())
    }
}
