use crate::TransformOperation;

pub type Time = u64;
pub type Tags = u64;
pub const TIME_SCALE: u64 = 1_000_000;

#[derive(Clone)]
pub struct Datapoint {
    pub time_offset: u32,
    pub value: f32
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TagsFilter {
    None,
    And(Tags),
    Or(Tags)
}

impl TagsFilter {
    pub fn accept(&self, tags: Tags) -> bool {
        match self {
            TagsFilter::None => true,
            TagsFilter::And(pattern) => (tags & pattern) == *pattern,
            TagsFilter::Or(pattern) => (tags & pattern) != 0
        }
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

#[test]
fn test_tags_filter1() {
    let current_tags = 0;
    assert_eq!(false, TagsFilter::And(1).accept(current_tags));
}

#[test]
fn test_tags_filter2() {
    let current_tags = 1;
    assert_eq!(true, TagsFilter::And(1).accept(current_tags));
}

#[test]
fn test_tags_filter3() {
    let current_tags = 1 | (1 << 2);
    assert_eq!(true, TagsFilter::And(1).accept(current_tags));
}

#[test]
fn test_tags_filter4() {
    let current_tags = 1;
    assert_eq!(false, TagsFilter::And(1 | (1 << 2)).accept(current_tags));
}

#[test]
fn test_tags_filter5() {
    let current_tags = 1;
    assert_eq!(true, TagsFilter::Or(1).accept(current_tags));
}

#[test]
fn test_tags_filter6() {
    let current_tags = 1;
    assert_eq!(true, TagsFilter::Or(1 | (1 << 2)).accept(current_tags));
}

#[test]
fn test_tags_filter7() {
    let current_tags = 1 | (1 << 2);
    assert_eq!(true, TagsFilter::Or(1).accept(current_tags));
}

#[test]
fn test_tags_filter8() {
    let current_tags = 2;
    assert_eq!(false, TagsFilter::Or(1).accept(current_tags));
}