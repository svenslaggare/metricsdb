use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use fnv::FnvHashMap;

use crate::storage::memory_file::MemoryFileError;
use crate::model::TIME_SCALE;
use crate::storage::MetricStorage;
use crate::tags::{PrimaryTag, SecondaryTagsIndex};

// pub const DEFAULT_BLOCK_DURATION: f64 = 0.0;
// pub const DEFAULT_BLOCK_DURATION: f64 = 1.0;
// pub const DEFAULT_BLOCK_DURATION: f64 = 10.0;
pub const DEFAULT_BLOCK_DURATION: f64 = 10.0 * 60.0;

pub const DEFAULT_DATAPOINT_DURATION: f64 = 0.0;
// pub const DEFAULT_DATAPOINT_DURATION: f64 = 0.5;

pub type MetricResult<T> = Result<T, MetricError>;

#[derive(Debug)]
pub enum MetricError {
    FailedToCreateBaseDir(std::io::Error),
    MemoryFileError(MemoryFileError),
    ExceededSecondaryTags,
    FailedToSavePrimaryTag(std::io::Error),
    FailedToLoadPrimaryTag(std::io::Error),
    FailedToSaveSecondaryTag(std::io::Error),
    FailedToLoadSecondaryTag(std::io::Error),
    FailedToCreateMetric(std::io::Error),
    FailedToAllocateSubBlock
}

impl From<MemoryFileError> for MetricError {
    fn from(err: MemoryFileError) -> Self {
        MetricError::MemoryFileError(err)
    }
}

pub struct PrimaryTagsStorage<TStorage: MetricStorage<E>, E: Copy> {
    base_path: PathBuf,
    pub tags: FnvHashMap<PrimaryTag, PrimaryTagMetric<TStorage, E>>
}

impl<TStorage: MetricStorage<E>, E: Copy> PrimaryTagsStorage<TStorage, E> {
    pub fn new(base_path: &Path) -> MetricResult<PrimaryTagsStorage<TStorage, E>> {
        if !base_path.exists() {
            std::fs::create_dir_all(base_path).map_err(|err| MetricError::FailedToCreateBaseDir(err))?;
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

    pub fn extract_primary_tag<'a>(&'a mut self, tags: &mut Vec<&str>) -> (PrimaryTag, PrimaryTagMetric<TStorage, E>) {
        for (index, tag) in tags.iter().enumerate() {
            let tag = PrimaryTag::Named((*tag).to_owned());
            if let Some(primary_tag) = self.tags.remove(&tag) {
                tags.remove(index);
                return (tag, primary_tag);
            }
        }

        (PrimaryTag::Default, self.tags.remove(&PrimaryTag::Default).unwrap())
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