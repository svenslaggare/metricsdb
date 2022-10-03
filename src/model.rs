use crate::{TransformOperation};
use crate::tags::{TagsFilter};

pub type Time = u64;
pub type Tags = u64;
pub const TIME_SCALE: u64 = 1_000_000;

#[derive(Clone)]
pub struct Datapoint<T: Copy> {
    pub time_offset: u32,
    pub value: T
}

#[derive(Debug, Clone, Copy)]
pub struct TimeRange {
    start: f64,
    end: f64
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
    pub input_transform: Option<TransformOperation>,
    pub output_transform: Option<TransformOperation>
}

impl Query {
    pub fn new(time_range: TimeRange) -> Query {
        Query {
            time_range,
            tags_filter: TagsFilter::None,
            input_transform: None,
            output_transform: None
        }
    }

    pub fn with_tags_filter(self, tags: TagsFilter) -> Query {
        let mut new = self;
        new.tags_filter = tags;
        new
    }

    pub fn with_input_transform(self, transform: TransformOperation) -> Query {
        let mut new = self;
        new.input_transform = Some(transform);
        new
    }

    pub fn with_output_transform(self, transform: TransformOperation) -> Query {
        let mut new = self;
        new.output_transform = Some(transform);
        new
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