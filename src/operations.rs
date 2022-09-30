pub trait StreamingOperation<T> {
    fn add(&mut self, value: T);
    fn value(&self) -> Option<T>;
}

pub struct StreamingAverage<T> {
    sum: T,
    count: i32
}

impl<T: Default> StreamingAverage<T> {
    pub fn new() -> StreamingAverage<T> {
        StreamingAverage {
            sum: Default::default(),
            count: 0
        }
    }
}

impl<T: Clone + Default + Clone + std::ops::AddAssign + std::ops::Div<Output=T> + From<i32>> StreamingOperation<T> for StreamingAverage<T> {
    fn add(&mut self, value: T) {
        self.sum += value;
        self.count += 1;
    }

    fn value(&self) -> Option<T> {
        if self.count > 0 {
            Some(self.sum.clone() / self.count.into())
        } else {
            None
        }
    }
}

impl<T: Clone + Default + Clone + std::ops::AddAssign + std::ops::Div<Output=T> + From<i32>> Default for StreamingAverage<T> {
    fn default() -> Self {
        StreamingAverage::new()
    }
}

pub struct StreamingMax {
    max: Option<f64>
}

impl StreamingMax {
    pub fn new() -> StreamingMax {
        StreamingMax {
            max: None
        }
    }
}

impl StreamingOperation<f64> for StreamingMax {
    fn add(&mut self, value: f64) {
        if let Some(max) = self.max.as_mut() {
            *max = max.max(value);
        } else {
            self.max = Some(value);
        }
    }

    fn value(&self) -> Option<f64> {
        self.max
    }
}

impl Default for StreamingMax {
    fn default() -> Self {
        StreamingMax::new()
    }
}

pub struct StreamingHistogram {
    buckets: Vec<usize>,
    total_count: usize,
    min: f64,
    max: f64
}

impl StreamingHistogram {
    pub fn new(min: f64, max: f64, num_buckets: usize) -> StreamingHistogram {
        StreamingHistogram {
            buckets: vec![0; num_buckets],
            total_count: 0,
            min,
            max
        }
    }

    pub fn prints(&self) {
        println!("Histogram:");
        for (bucket_index, count) in self.buckets.iter().enumerate() {
            println!("\t[{:.2}, {:.2}): {}", self.edge_from_index(bucket_index), self.edge_from_index(bucket_index + 1), count);
        }
        println!();
    }

    pub fn percentile(&self, percentile: i32) -> Option<f64> {
        let percentile = percentile as f64 / 100.0;
        let required_count = (percentile * self.total_count as f64).round() as usize;

        let mut accumulated_count = 0;
        for (bucket_index, count) in self.buckets.iter().enumerate() {
            accumulated_count += count;

            if accumulated_count >= required_count {
                let interpolation = (required_count - (accumulated_count - count)) as f64 / *count as f64;
                return Some(self.edge_from_float_index(bucket_index as f64 + interpolation));
            }
        }

        None
    }

    fn edge_from_index(&self, index: usize) -> f64 {
        self.min + (index as f64 / (self.buckets.len()) as f64) * (self.max - self.min)
    }

    fn edge_from_float_index(&self, index: f64) -> f64 {
        self.min + (index / (self.buckets.len()) as f64) * (self.max - self.min)
    }
}

impl StreamingOperation<f64> for StreamingHistogram {
    fn add(&mut self, value: f64) {
        let bucket_float = (value - self.min) / (self.max - self.min);
        let bucket_index = (bucket_float * (self.buckets.len() - 0) as f64).floor() as usize;

        if bucket_index < self.buckets.len() {
            self.total_count += 1;
            self.buckets[bucket_index] += 1;
        }
    }

    fn value(&self) -> Option<f64> {
        None
    }
}

pub struct StreamingApproxPercentile {
    histogram: StreamingHistogram,
    percentile: i32
}

impl StreamingApproxPercentile {
    pub fn new(min: f64, max: f64, num_buckets: usize, percentile: i32) -> StreamingApproxPercentile {
        StreamingApproxPercentile {
            histogram: StreamingHistogram::new(min, max, num_buckets),
            percentile
        }
    }
}

impl StreamingOperation<f64> for StreamingApproxPercentile {
    fn add(&mut self, value: f64) {
        self.histogram.add(value);
    }

    fn value(&self) -> Option<f64> {
        self.histogram.percentile(self.percentile)
    }
}

#[derive(Debug, Clone, Copy)]
pub enum TransformOperation {
    Abs,
    Max(f64),
    Min(f64),
    Round,
    Ceil,
    Floor,
    Sqrt,
    Square,
    Power(f64),
    Exponential,
    LogE,
    LogBase(f64),
    Sin,
    Cos,
    Tan
}

impl TransformOperation {
    pub fn apply(&self, value: f64) -> Option<f64> {
        match self {
            TransformOperation::Abs => Some(value.abs()),
            TransformOperation::Max(other) => Some(value.max(*other)),
            TransformOperation::Min(other) => Some(value.min(*other)),
            TransformOperation::Round => Some(value.round()),
            TransformOperation::Ceil => Some(value.ceil()),
            TransformOperation::Floor => Some(value.floor()),
            TransformOperation::Sqrt if value >= 0.0 => Some(value.sqrt()),
            TransformOperation::Square => Some(value * value),
            TransformOperation::Power(power) => Some(value.powf(*power)),
            TransformOperation::Exponential => Some(value.exp()),
            TransformOperation::LogE if value > 0.0 => Some(value.log2()),
            TransformOperation::LogBase(base) if value > 0.0 => Some(value.log(*base)),
            TransformOperation::Sin => Some(value.sin()),
            TransformOperation::Cos => Some(value.cos()),
            TransformOperation::Tan => Some(value.tan()),
            _ => None
        }
    }
}

pub struct StreamingTransformOperation<T> {
    operation: TransformOperation,
    inner: T
}

impl<T: StreamingOperation<f64>> StreamingTransformOperation<T> {
    pub fn new(operation: TransformOperation, inner: T) -> StreamingTransformOperation<T> {
        StreamingTransformOperation {
            operation,
            inner
        }
    }
}

impl<T: StreamingOperation<f64> + Default> StreamingTransformOperation<T> {
    pub fn from_default(operation: TransformOperation) -> StreamingTransformOperation<T> {
        StreamingTransformOperation {
            operation,
            inner: Default::default()
        }
    }
}

impl<T: StreamingOperation<f64>> StreamingOperation<f64> for StreamingTransformOperation<T> {
    fn add(&mut self, value: f64) {
        if let Some(value) = self.operation.apply(value) {
            self.inner.add(value);
        }
    }

    fn value(&self) -> Option<f64> {
        self.inner.value()
    }
}

#[test]
fn test_streaming_histogram1() {
    let mut streaming = StreamingHistogram::new(1.0, 1001.0, 50);
    let values = (1..1001).collect::<Vec<_>>();
    for value in values {
        streaming.add(value as f64);
    }

    assert_eq!(Some(991.0), streaming.percentile(99));
}

#[test]
fn test_streaming_histogram2() {
    use rand::prelude::SliceRandom;
    use rand::thread_rng;

    let mut streaming = StreamingHistogram::new(1.0, 1001.0, 50);
    let mut values = (1..1001).collect::<Vec<_>>();
    values.shuffle(&mut thread_rng());
    for value in values {
        streaming.add(value as f64);
    }

    assert_eq!(Some(991.0), streaming.percentile(99));
}

#[test]
fn test_streaming_histogram3() {
    let mut streaming = StreamingHistogram::new(1.0, 1001.0, 50);
    let values = (1..1001).collect::<Vec<_>>();
    for value in values {
        streaming.add(value as f64);
        streaming.add(value as f64);
    }

    assert_eq!(Some(991.0), streaming.percentile(99));
}

#[test]
fn test_streaming_approx_percentile1() {
    use rand::prelude::SliceRandom;
    use rand::thread_rng;

    let mut streaming = StreamingApproxPercentile::new(1.0, 1001.0, 50, 99);
    let mut values = (1..1001).collect::<Vec<_>>();
    values.shuffle(&mut thread_rng());
    for value in values {
        streaming.add(value as f64);
    }

    assert_eq!(Some(991.0), streaming.value());
}