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