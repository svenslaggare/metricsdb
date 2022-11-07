use crate::metric::expression::{FilterExpression, TransformExpression};
use crate::metric::tags::TagsFilter;
use crate::storage::memory_file::MemoryFileError;

pub type Time = u64;
pub type Tags = u128;
pub const TIME_SCALE: u64 = 1_000_000;

#[derive(Clone)]
pub struct Datapoint<T: Copy> {
    pub time_offset: u32,
    pub value: T
}

#[derive(Debug, Clone, Copy)]
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

#[derive(Debug, Clone)]
pub struct Query {
    pub time_range: TimeRange,
    pub tags_filter: TagsFilter,
    pub input_filter: Option<FilterExpression>,
    pub input_transform: Option<TransformExpression>,
    pub output_filter: Option<FilterExpression>,
    pub output_transform: Option<TransformExpression>,
    pub group_by: Option<String>
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
            group_by: None
        }
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

    pub fn with_group_by(self, key: String) -> Query {
        let mut new = self;
        new.group_by = Some(key);
        new
    }

    pub fn apply_output_transform(&self, value: f64) -> Option<f64> {
        if let Some(filter) = &self.output_filter {
            if !filter.evaluate(Some(value)).unwrap_or(false) {
                return None;
            }
        }

        match &self.output_transform {
            Some(operation) => operation.evaluate(Some(value)),
            None => Some(value)
        }
    }
}

pub trait MinMax {
    fn min(&self, other: Self) -> Self;
    fn max(&self, other: Self) -> Self;
}

impl MinMax for f64 {
    fn min(&self, other: Self) -> Self {
        f64::min(*self, other)
    }

    fn max(&self, other: Self) -> Self {
        f64::max(*self, other)
    }
}

impl MinMax for f32 {
    fn min(&self, other: Self) -> Self {
        f32::min(*self, other)
    }

    fn max(&self, other: Self) -> Self {
        f32::max(*self, other)
    }
}

impl MinMax for u32 {
    fn min(&self, other: Self) -> Self {
        if self < &other {
            *self
        } else {
            other
        }
    }

    fn max(&self, other: Self) -> Self {
        if self > &other {
            *self
        } else {
            other
        }
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
    FailedToAllocateSubBlock,
    InvalidTimeOrder
}

impl From<MemoryFileError> for MetricError {
    fn from(err: MemoryFileError) -> Self {
        MetricError::MemoryFileError(err)
    }
}