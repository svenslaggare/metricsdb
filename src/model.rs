use serde::{Serialize, Deserialize, Serializer};
use serde::ser::SerializeSeq;

use crate::metric::expression::{ExpressionValue, FilterExpression, TransformExpression};
use crate::metric::tags::{Tag, TagsFilter};
use crate::storage::memory_file::MemoryFileError;

pub type Time = u64;
pub type Tags = u128;
pub const TIME_SCALE: u64 = 1_000_000;

#[derive(Clone)]
#[repr(C)]
pub struct Datapoint<T: Copy> {
    pub time_offset: u32,
    pub value: T
}

#[derive(Debug, Clone, Copy, Deserialize)]
pub struct TimeRange {
    pub start: f64,
    pub end: f64
}

impl TimeRange {
    pub fn new(start: f64, end: f64) -> TimeRange {
        assert!(end > start);

        TimeRange {
            start,
            end
        }
    }

    pub fn int_range(&self) -> (Time, Time) {
        (
            (self.start * TIME_SCALE as f64).round() as Time,
            (self.end * TIME_SCALE as f64).round() as Time
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GroupKey(pub Vec<String>);

impl GroupKey {
    pub fn from_ref(key: &str) -> GroupKey {
        GroupKey(vec![key.to_owned()])
    }

    pub fn from_multi_ref(keys: &[&str]) -> GroupKey {
        GroupKey(keys.iter().map(|x| (*x).to_owned()).collect())
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GroupValue(pub Vec<String>);

impl GroupValue {
    pub fn from_ref(value: &str) -> GroupValue {
        GroupValue(vec![value.to_owned()])
    }

    pub fn from_tags(tags: &Vec<Tag>) -> GroupValue {
        GroupValue(tags.iter().map(|tag| tag.1.clone()).collect::<Vec<_>>())
    }
}

impl Serialize for GroupValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
        if self.0.len() == 1 {
            serializer.serialize_str(&self.0[0])
        } else {
            let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
            for part in &self.0 {
                seq.serialize_element(part)?;
            }
            seq.end()
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Query {
    pub time_range: TimeRange,
    pub tags_filter: TagsFilter,
    pub input_filter: Option<FilterExpression>,
    pub input_transform: Option<TransformExpression>,
    pub output_filter: Option<FilterExpression>,
    pub output_transform: Option<TransformExpression>,
    pub group_by: Option<GroupKey>,
    pub remove_empty_datapoints: bool
}

impl Query {
    pub fn new(time_range: TimeRange) -> Query {
        Query {
            time_range,
            tags_filter: TagsFilter::None,
            input_filter: None,
            input_transform: None,
            output_filter: None,
            output_transform: None,
            group_by: None,
            remove_empty_datapoints: true
        }
    }

    pub fn placeholder() -> Query {
        Query::new(TimeRange::new(0.0, 1.0))
    }

    pub fn with_tags_filter(self, tags: TagsFilter) -> Query {
        let mut new = self;
        new.tags_filter = tags;
        new
    }

    pub fn with_input_filter(self, filter: FilterExpression) -> Query {
        let mut new = self;
        new.input_filter = Some(filter);
        new
    }

    pub fn with_input_transform(self, transform: TransformExpression) -> Query {
        let mut new = self;
        new.input_transform = Some(transform);
        new
    }

    pub fn with_output_filter(self, filter: FilterExpression) -> Query {
        let mut new = self;
        new.output_filter = Some(filter);
        new
    }

    pub fn with_output_transform(self, transform: TransformExpression) -> Query {
        let mut new = self;
        new.output_transform = Some(transform);
        new
    }

    pub fn with_group_by(self, key: GroupKey) -> Query {
        let mut new = self;
        new.group_by = Some(key);
        new
    }

    pub fn apply_output_transform(&self, value: ExpressionValue) -> Option<f64> {
        if let Some(filter) = &self.output_filter {
            if !filter.evaluate(&value).unwrap_or(false) {
                return None;
            }
        }

        match &self.output_transform {
            Some(operation) => operation.evaluate(&value),
            None => value.float()
        }
    }
}

impl Default for Query {
    fn default() -> Self {
        Query::placeholder()
    }
}

pub type MetricResult<T> = Result<T, MetricError>;

#[derive(Debug)]
pub enum MetricError {
    FailedToCreateBaseDir(std::io::Error),
    FailedToLoadConfig(std::io::Error),
    FailedToSaveConfig(std::io::Error),
    MemoryFileError(MemoryFileError),
    ExceededSecondaryTags,
    FailedToSavePrimaryTag(std::io::Error),
    FailedToLoadPrimaryTag(std::io::Error),
    FailedToSaveSecondaryTag(std::io::Error),
    FailedToLoadSecondaryTag(std::io::Error),
    FailedToCreateMetric(std::io::Error),
    InvalidTimeOrder,
    TooLargeCount
}

impl From<MemoryFileError> for MetricError {
    fn from(err: MemoryFileError) -> Self {
        MetricError::MemoryFileError(err)
    }
}