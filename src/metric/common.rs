use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use fnv::{FnvHashMap, FnvHashSet};

use crate::metric::tags::{PrimaryTag, SecondaryTagsFilter, SecondaryTagsIndex, TagsFilter};
use crate::model::{MetricError, MetricResult, Query, Tags, TIME_SCALE};
use crate::storage::MetricStorage;

pub const DEFAULT_BLOCK_DURATION: f64 = 10.0 * 60.0;

pub const DEFAULT_DATAPOINT_DURATION: f64 = 0.0;
// pub const DEFAULT_DATAPOINT_DURATION: f64 = 0.2;

pub struct PrimaryTagsStorage<TStorage: MetricStorage<E>, E: Copy> {
    base_path: PathBuf,
    tags: FnvHashMap<PrimaryTag, PrimaryTagMetric<TStorage, E>>
}

impl<TStorage: MetricStorage<E>, E: Copy> PrimaryTagsStorage<TStorage, E> {
    pub fn new(base_path: &Path) -> MetricResult<PrimaryTagsStorage<TStorage, E>> {
        if !base_path.exists() {
            std::fs::create_dir_all(base_path).map_err(|err| MetricError::FailedToCreateBaseDir(err))?;
        }

        for entry in std::fs::read_dir(base_path).map_err(|err| MetricError::FailedToCreateMetric(err))? {
            if let Ok(entry) = entry {
                if entry.path().is_dir() {
                    std::fs::remove_dir_all(entry.path()).map_err(|err| MetricError::FailedToCreateMetric(err))?;
                } else {
                    std::fs::remove_file(entry.path()).map_err(|err| MetricError::FailedToCreateMetric(err))?;
                }
            }
        }

        let mut primary_tags_storage = PrimaryTagsStorage {
            base_path: base_path.to_owned(),
            tags: FnvHashMap::default()
        };
        primary_tags_storage.add_primary_tag(PrimaryTag::Default)?;

        Ok(primary_tags_storage)
    }

    pub fn from_existing(base_path: &Path) -> MetricResult<PrimaryTagsStorage<TStorage, E>> {
        Ok(
            PrimaryTagsStorage {
                base_path: base_path.to_owned(),
                tags: PrimaryTagsSerialization::new(base_path).load()?
            }
        )
    }

    pub fn stats(&self) {
        for (tag, primary_tag) in self.tags.iter() {
            println!("Tag: {:?}", tag);
            println!("Num blocks: {}", primary_tag.storage.len());
            let mut num_datapoints = 0;
            let mut max_datapoints_in_block = 0;
            for block_index in 0..primary_tag.storage.len() {
                if let Some(iterator) = primary_tag.storage.block_datapoints(block_index) {
                    for (_, datapoints) in iterator {
                        let block_length = datapoints.len();
                        num_datapoints += block_length;
                        max_datapoints_in_block = max_datapoints_in_block.max(block_length);
                    }
                }
            }
            println!("Num datapoints: {}, max datapoints: {}", num_datapoints, max_datapoints_in_block);
        }
    }

    pub fn iter(&self) -> impl Iterator<Item=(&PrimaryTag, &PrimaryTagMetric<TStorage, E>)> {
        self.tags.iter()
    }

    pub fn iter_for_query<'a>(&'a self, tags_filter: &'a TagsFilter) -> impl Iterator<Item=(&PrimaryTagMetric<TStorage, E>, SecondaryTagsFilter)> + '_ {
        self.tags
            .iter()
            .map(|(primary_tag_key, primary_tag)| (primary_tag, tags_filter.apply(&primary_tag.tags_index, primary_tag_key)))
            .filter(|(_, tags_filter)| tags_filter.is_some())
            .map(|(primary_tag, tags_filter)| (primary_tag, tags_filter.unwrap()))
    }

    pub fn add_primary_tag(&mut self, tag: PrimaryTag) -> MetricResult<()> {
        if !self.tags.contains_key(&tag) {
            let path = match &tag {
                PrimaryTag::Default => self.base_path.join("default"),
                PrimaryTag::Named(tag) => self.base_path.join(&tag)
            };

            let primary_tag = PrimaryTagMetric::new(&path, DEFAULT_BLOCK_DURATION, DEFAULT_DATAPOINT_DURATION)?;
            primary_tag.tags_index.save().map_err(|err| MetricError::FailedToSavePrimaryTag(err))?;
            self.tags.insert(tag, primary_tag);
            PrimaryTagsSerialization::new(&self.base_path).save(&self.tags)?;
        }

        Ok(())
    }

    pub fn insert_tags(&mut self, tags: &mut Vec<String>) -> MetricResult<(PrimaryTag, PrimaryTagMetric<TStorage, E>, Tags)> {
        let (primary_tag_key, mut primary_tag) = self.extract_primary_tag(tags);
        let secondary_tags = match primary_tag.tags_index.try_add_tags(&tags) {
            Ok(secondary_tags) => secondary_tags,
            Err(err) => {
                self.tags.insert(primary_tag_key, primary_tag);
                return Err(err);
            }
        };

        Ok((primary_tag_key, primary_tag, secondary_tags))
    }

    fn extract_primary_tag(&mut self, tags: &mut Vec<String>) -> (PrimaryTag, PrimaryTagMetric<TStorage, E>) {
        for (index, tag) in tags.iter().enumerate() {
            let tag = PrimaryTag::Named((*tag).to_owned());
            if let Some(primary_tag) = self.tags.remove(&tag) {
                tags.remove(index);
                return (tag, primary_tag);
            }
        }

        (PrimaryTag::Default, self.tags.remove(&PrimaryTag::Default).unwrap())
    }

    pub fn return_tags(&mut self, primary_tag_key: PrimaryTag, primary_tag: PrimaryTagMetric<TStorage, E>) {
        self.tags.insert(primary_tag_key, primary_tag);
    }

    pub fn apply_group_by<F: Fn(&TagsFilter) -> T, T>(&self, query: &Query, key: &str, apply: F) -> Vec<(String, T)> {
        let mut groups = self.gather_group_values(&query, key)
            .into_iter()
            .map(|group_value| {
                let tags_filter = query.tags_filter.clone().add_and_clause(vec![format!("{}:{}", key, group_value)]);
                (group_value, apply(&tags_filter))
            })
            .collect::<Vec<_>>();

        groups.sort_by(|a, b| a.0.cmp(&b.0));
        groups
    }

    pub fn gather_group_values(&self, query: &Query, key: &str) -> Vec<String> {
        let mut group_values = FnvHashSet::default();

        for (primary_tag_key, primary_tag) in self.iter() {
            if let Some(tags_filter) = query.tags_filter.apply(&primary_tag.tags_index, primary_tag_key) {
                for pattern in primary_tag.tags_index.all_patterns() {
                    if tags_filter.accept(*pattern) {
                        for index in 0..Tags::BITS {
                            let index_pattern = 1 << index as Tags;
                            if index_pattern & pattern != 0 {
                                if let Some(key_value) = primary_tag.tags_index.tags_pattern_to_string(&index_pattern) {
                                    let parts = key_value.split(":").collect::<Vec<_>>();
                                    if parts.len() == 2 {
                                        if parts[0] == key {
                                            group_values.insert(parts[1].to_owned());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        group_values.into_iter().collect()
    }

    pub fn scheduled(&mut self) {
        for primary_tag in self.tags.values_mut() {
            primary_tag.storage.scheduled();
        }
    }
}

pub struct PrimaryTagMetric<TStorage: MetricStorage<E>, E: Copy> {
    pub storage: TStorage,
    pub tags_index: SecondaryTagsIndex,
    _phantom: PhantomData<E>
}

impl<TStorage: MetricStorage<E>, E: Copy> PrimaryTagMetric<TStorage, E> {
    pub fn new(base_path: &Path, block_duration: f64, datapoint_duration: f64) -> MetricResult<PrimaryTagMetric<TStorage, E>> {
        if !base_path.exists() {
            std::fs::create_dir_all(base_path).map_err(|err| MetricError::FailedToCreateMetric(err))?;
        }

        Ok(
            PrimaryTagMetric {
                storage: TStorage::new(
                    base_path,
                    (block_duration * TIME_SCALE as f64) as u64,
                    (datapoint_duration * TIME_SCALE as f64) as u64
                )?,
                tags_index: SecondaryTagsIndex::new(base_path),
                _phantom: PhantomData::default()
            }
        )
    }

    pub fn from_existing(base_path: &Path) -> MetricResult<PrimaryTagMetric<TStorage, E>> {
        Ok(
            PrimaryTagMetric {
                storage: TStorage::from_existing(base_path)?,
                tags_index: SecondaryTagsIndex::load(&base_path.join("tags.json")).map_err(|err| MetricError::FailedToLoadSecondaryTag(err))?,
                _phantom: PhantomData::default()
            }
        )
    }
}

struct PrimaryTagsSerialization {
    base_path: PathBuf,
    index_path: PathBuf
}

impl PrimaryTagsSerialization {
    pub fn new(base_path: &Path) -> PrimaryTagsSerialization {
        PrimaryTagsSerialization {
            base_path: base_path.to_owned(),
            index_path: base_path.join("primary_tags.json").to_owned()
        }
    }

    pub fn save<TStorage: MetricStorage<E>, E: Copy>(&self, primary_tags: &FnvHashMap<PrimaryTag, PrimaryTagMetric<TStorage, E>>) -> MetricResult<()> {
        let save = || -> std::io::Result<()> {
            let content = serde_json::to_string(&primary_tags.keys().collect::<Vec<_>>())?;
            std::fs::write(&self.index_path, &content)?;
            Ok(())
        };

        save().map_err(|err| MetricError::FailedToSavePrimaryTag(err))?;
        Ok(())
    }

    pub fn load<TStorage: MetricStorage<E>, E: Copy>(&self) -> MetricResult<FnvHashMap<PrimaryTag, PrimaryTagMetric<TStorage, E>>> {
        let mut primary_tags = FnvHashMap::default();

        let load = || -> std::io::Result<Vec<PrimaryTag>> {
            let primary_tag_values_content = std::fs::read_to_string(&self.index_path)?;
            let primary_tag_values: Vec<PrimaryTag> = serde_json::from_str(&primary_tag_values_content)?;
            Ok(primary_tag_values)
        };

        let primary_tag_values = load().map_err(|err| MetricError::FailedToLoadPrimaryTag(err))?;
        for primary_tag_value in primary_tag_values {
            let primary_tag_base_path = match &primary_tag_value {
                PrimaryTag::Default => self.base_path.join("default"),
                PrimaryTag::Named(tag) => self.base_path.join(tag)
            };

            primary_tags.insert(primary_tag_value, PrimaryTagMetric::from_existing(&primary_tag_base_path)?);
        }

        Ok(primary_tags)
    }
}