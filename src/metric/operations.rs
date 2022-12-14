use std::marker::PhantomData;
use tdigest::TDigest;

use crate::metric::expression::{ExpressionValue, FilterExpression, TransformExpression};
use crate::metric::helpers::TimeRangeStatistics;
use crate::metric::ratio::{Ratio};
use crate::model::TimeRange;
use crate::traits::{MinMax, ToExpressionValue};

pub trait StreamingOperation<TInput, TOutput=TInput> {
    fn add(&mut self, value: TInput);
    fn value(&self) -> Option<TOutput>;

    fn merge(&mut self, other: Self);
}

pub struct StreamingConvert<TInput, TOutput, TInner: StreamingOperation<TInput, TInput>, TConverter: Fn(TInput) -> TOutput> {
    inner: TInner,
    converter: TConverter,
    _phantom1: PhantomData<TInput>,
    _phantom2: PhantomData<TOutput>
}

impl<TInput, TOutput, TInner: StreamingOperation<TInput, TInput>, TConverter: Fn(TInput) -> TOutput> StreamingConvert<TInput, TOutput, TInner, TConverter> {
    pub fn new(inner: TInner, converter: TConverter) -> StreamingConvert<TInput, TOutput, TInner, TConverter> {
        StreamingConvert {
            inner,
            converter,
            _phantom1: Default::default(),
            _phantom2: Default::default()
        }
    }
}

impl<TInput, TOutput, TInner: StreamingOperation<TInput, TInput>, TConverter: Fn(TInput) -> TOutput> StreamingOperation<TInput, TOutput> for StreamingConvert<TInput, TOutput, TInner, TConverter> {
    fn add(&mut self, value: TInput) {
        self.inner.add(value);
    }

    fn value(&self) -> Option<TOutput> {
        self.inner.value().map(|x| (self.converter)(x))
    }

    fn merge(&mut self, other: Self) {
        self.inner.merge(other.inner);
    }
}

pub struct StreamingSum<T> {
    sum: T
}

impl<T: Default> StreamingSum<T> {
    pub fn new() -> StreamingSum<T> {
        StreamingSum {
            sum: Default::default()
        }
    }
}

impl<T: Clone + Default + std::ops::AddAssign> StreamingOperation<T> for StreamingSum<T> {
    fn add(&mut self, value: T) {
        self.sum += value;
    }

    fn value(&self) -> Option<T> {
        Some(self.sum.clone())
    }

    fn merge(&mut self, other: Self) {
        self.sum += other.sum;
    }
}

impl<T: Clone + Default + std::ops::AddAssign> Default for StreamingSum<T> {
    fn default() -> Self {
        StreamingSum::new()
    }
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

impl<T: Clone + Default + std::ops::AddAssign + std::ops::Div<Output=T> + From<i32>> StreamingOperation<T> for StreamingAverage<T> {
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

    fn merge(&mut self, other: Self) {
        self.sum += other.sum;
        self.count += other.count;
    }
}

impl<T: Clone + Default + std::ops::AddAssign + std::ops::Div<Output=T> + From<i32>> Default for StreamingAverage<T> {
    fn default() -> Self {
        StreamingAverage::new()
    }
}

pub struct StreamingTimeAverage<T> {
    sum: T,
    start: f64,
    end: f64
}

impl<T: Default + DivConvert> StreamingTimeAverage<T> {
    pub fn new(time_range: TimeRange) -> StreamingTimeAverage<T> {
        StreamingTimeAverage {
            sum: Default::default(),
            start: time_range.start,
            end: time_range.end
        }
    }
}

impl<T: Clone + Default + std::ops::AddAssign + DivConvert> StreamingOperation<T, f64> for StreamingTimeAverage<T> {
    fn add(&mut self, value: T) {
        self.sum += value;
    }

    fn value(&self) -> Option<f64> {
        Some(self.sum.div_convert(self.end - self.start))
    }

    fn merge(&mut self, other: Self) {
        self.sum += other.sum;
        self.start = self.start.min(other.start);
        self.end = self.end.max(other.end);
    }
}

pub struct StreamingRatioValue<T: StreamingOperation<f64>> {
    inner: T
}

impl<T: StreamingOperation<f64>> StreamingRatioValue<T> {
    pub fn new(inner: T) -> StreamingRatioValue<T> {
        StreamingRatioValue {
            inner
        }
    }
}

impl<T: StreamingOperation<f64>> StreamingOperation<Ratio, ExpressionValue> for StreamingRatioValue<T> {
    fn add(&mut self, value: Ratio) {
        if let Some(value) = value.value() {
            self.inner.add(value);
        }
    }

    fn value(&self) -> Option<ExpressionValue> {
        Some(ExpressionValue::Float(self.inner.value()?))
    }

    fn merge(&mut self, other: Self) {
        self.inner.merge(other.inner)
    }
}

impl<T: StreamingOperation<f64> + Default> StreamingRatioValue<T> {
    pub fn from_default() -> StreamingRatioValue<T> {
        StreamingRatioValue {
            inner: Default::default()
        }
    }
}

pub trait DivConvert {
    fn div_convert(&self, other: f64) -> f64;
}

impl DivConvert for u64 {
    fn div_convert(&self, other: f64) -> f64 {
        (*self as f64) / other
    }
}

pub struct StreamingMax<T> {
    max: Option<T>
}

impl<T> StreamingMax<T> {
    pub fn new() -> StreamingMax<T> {
        StreamingMax {
            max: None
        }
    }
}

impl<T: MinMax + Copy> StreamingOperation<T> for StreamingMax<T> {
    fn add(&mut self, value: T) {
        if let Some(max) = self.max.as_mut() {
            *max = max.max(value);
        } else {
            self.max = Some(value);
        }
    }

    fn value(&self) -> Option<T> {
        self.max
    }

    fn merge(&mut self, other: Self) {
        if let Some(value) = other.max {
            self.add(value);
        }
    }
}

impl<T> Default for StreamingMax<T> {
    fn default() -> Self {
        StreamingMax::new()
    }
}

pub struct StreamingMin<T> {
    min: Option<T>
}

impl<T> StreamingMin<T> {
    pub fn new() -> StreamingMin<T> {
        StreamingMin {
            min: None
        }
    }
}

impl<T: MinMax + Copy> StreamingOperation<T> for StreamingMin<T> {
    fn add(&mut self, value: T) {
        if let Some(min) = self.min.as_mut() {
            *min = min.min(value);
        } else {
            self.min = Some(value);
        }
    }

    fn value(&self) -> Option<T> {
        self.min
    }

    fn merge(&mut self, other: Self) {
        if let Some(value) = other.min {
            self.add(value);
        }
    }
}

impl<T> Default for StreamingMin<T> {
    fn default() -> Self {
        StreamingMin::new()
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

    pub fn print(&self) {
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

    fn auto_num_buckets(count: usize) -> usize {
        (count as f64).sqrt().ceil() as usize
    }

    fn add_with_count(&mut self, value: f64, count: usize) {
        if self.buckets.len() == 0 {
            return;
        }

        let bucket_float = (value - self.min) / (self.max - self.min);
        let bucket_index = (bucket_float * self.buckets.len() as f64).floor() as usize;
        let bucket_index = bucket_index.min(self.buckets.len() - 1);

        self.total_count += count;
        self.buckets[bucket_index] += count;
    }
}

impl StreamingOperation<f64> for StreamingHistogram {
    fn add(&mut self, value: f64) {
        self.add_with_count(value, 1);
    }

    fn value(&self) -> Option<f64> {
        None
    }

    fn merge(&mut self, other: Self) {
        let mut new_histogram = StreamingHistogram::new(
            self.min.min(other.min),
            self.max.max(other.max),
            StreamingHistogram::auto_num_buckets(self.total_count + other.total_count)
        );

        let mut add_histogram = |histogram: &StreamingHistogram| {
            for (window_index, &count) in histogram.buckets.iter().enumerate() {
                let center = histogram.edge_from_float_index(window_index as f64 + 0.5);
                new_histogram.add_with_count(center, count);
            }
        };

        add_histogram(self);
        add_histogram(&other);

        *self = new_histogram;
    }
}

pub struct StreamingApproxPercentileHistogram {
    histogram: StreamingHistogram,
    percentile: i32
}

impl StreamingApproxPercentileHistogram {
    pub fn new(min: f64, max: f64, num_buckets: usize, percentile: i32) -> StreamingApproxPercentileHistogram {
        StreamingApproxPercentileHistogram {
            histogram: StreamingHistogram::new(min, max, num_buckets),
            percentile
        }
    }

    pub fn from_stats(stats: &TimeRangeStatistics<f64>, percentile: i32) -> StreamingApproxPercentileHistogram {
        StreamingApproxPercentileHistogram::new(stats.min(), stats.max(), StreamingHistogram::auto_num_buckets(stats.count), percentile)
    }
}

impl StreamingOperation<f64> for StreamingApproxPercentileHistogram {
    fn add(&mut self, value: f64) {
        self.histogram.add(value);
    }

    fn value(&self) -> Option<f64> {
        self.histogram.percentile(self.percentile)
    }

    fn merge(&mut self, other: Self) {
        assert_eq!(self.percentile, other.percentile);
        self.histogram.merge(other.histogram);
    }
}

pub struct StreamingTDigest {
    digest: TDigest,
    buffer: Vec<f64>,
    max_buffer_before_merge: usize
}

impl StreamingTDigest {
    pub fn new(max_size: usize) -> StreamingTDigest {
        StreamingTDigest {
            digest: TDigest::new_with_size(max_size),
            buffer: Vec::new(),
            max_buffer_before_merge: 512
        }
    }

    fn digest(&self) -> TDigest {
        self.digest.merge_unsorted(self.buffer.clone())
    }
}

impl StreamingOperation<f64> for StreamingTDigest {
    fn add(&mut self, value: f64) {
        self.buffer.push(value);
        if self.buffer.len() >= self.max_buffer_before_merge {
            self.digest = self.digest.merge_unsorted(std::mem::take(&mut self.buffer));
        }
    }

    fn value(&self) -> Option<f64> {
        None
    }

    fn merge(&mut self, other: Self) {
        let other_digest = other.digest.merge_unsorted(other.buffer);
        self.digest = TDigest::merge_digests(vec![std::mem::take(&mut self.digest), other_digest]);
    }
}

pub struct StreamingApproxPercentileTDigest {
    digest: StreamingTDigest,
    percentile: i32
}

impl StreamingApproxPercentileTDigest {
    pub fn new(percentile: i32) -> StreamingApproxPercentileTDigest {
        StreamingApproxPercentileTDigest {
            digest: StreamingTDigest::new(150),
            percentile
        }
    }
}

impl StreamingOperation<f64> for StreamingApproxPercentileTDigest {
    fn add(&mut self, value: f64) {
        self.digest.add(value);
    }

    fn value(&self) -> Option<f64> {
        Some(self.digest.digest().estimate_quantile(self.percentile as f64 / 100.0))
    }

    fn merge(&mut self, other: Self) {
        assert_eq!(self.percentile, other.percentile);
        self.digest.merge(other.digest);
    }
}

pub struct StreamingTransformOperation<T> {
    operation: TransformExpression,
    inner: T
}

impl<T: StreamingOperation<f64>> StreamingTransformOperation<T> {
    pub fn new(operation: TransformExpression, inner: T) -> StreamingTransformOperation<T> {
        StreamingTransformOperation {
            operation,
            inner
        }
    }
}

impl<T: StreamingOperation<f64> + Default> StreamingTransformOperation<T> {
    pub fn from_default(operation: TransformExpression) -> StreamingTransformOperation<T> {
        StreamingTransformOperation {
            operation,
            inner: Default::default()
        }
    }
}

impl<T: StreamingOperation<f64>> StreamingOperation<f64> for StreamingTransformOperation<T> {
    fn add(&mut self, value: f64) {
        if let Some(value) = self.operation.evaluate(&ExpressionValue::Float(value)) {
            self.inner.add(value);
        }
    }

    fn value(&self) -> Option<f64> {
        self.inner.value()
    }

    fn merge(&mut self, other: Self) {
        assert_eq!(self.operation, other.operation);
        self.inner.merge(other.inner);
    }
}

pub struct StreamingFilterOperation<TInput, TOutput, TOp> {
    operation: FilterExpression,
    inner: TOp,
    _phantom1: PhantomData<TInput>,
    _phantom2: PhantomData<TOutput>
}

impl<TInput, TOutput, TOp: StreamingOperation<TInput, TOutput>> StreamingFilterOperation<TInput, TOutput, TOp> {
    pub fn new(operation: FilterExpression, inner: TOp) -> StreamingFilterOperation<TInput, TOutput, TOp> {
        StreamingFilterOperation {
            operation,
            inner,
            _phantom1: Default::default(),
            _phantom2: Default::default(),
        }
    }
}

impl<TInput, TOutput, TOp: StreamingOperation<TInput, TOutput> + Default> StreamingFilterOperation<TInput, TOutput, TOp> {
    pub fn from_default(operation: FilterExpression) -> StreamingFilterOperation<TInput, TOutput, TOp> {
        StreamingFilterOperation {
            operation,
            inner: Default::default(),
            _phantom1: Default::default(),
            _phantom2: Default::default()
        }
    }
}

impl<TInput: ToExpressionValue, TOutput, TOp: StreamingOperation<TInput, TOutput>> StreamingOperation<TInput, TOutput> for StreamingFilterOperation<TInput, TOutput, TOp> {
    fn add(&mut self, value: TInput) {
        if self.operation.evaluate(&value.to_value()).unwrap_or(false) {
            self.inner.add(value);
        }
    }

    fn value(&self) -> Option<TOutput> {
        self.inner.value()
    }

    fn merge(&mut self, other: Self) {
        assert_eq!(self.operation, other.operation);
        self.inner.merge(other.inner);
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
fn test_merge_streaming_histogram3() {
    use approx::assert_abs_diff_eq;

    let mut streaming_full = StreamingHistogram::new(1.0, 2001.0, 120);

    let mut streaming1 = StreamingHistogram::new(1.0, 1001.0, 50);
    let values = (1..1001).collect::<Vec<_>>();
    for value in values {
        streaming1.add(value as f64);
        streaming_full.add(value as f64);
    }

    let mut streaming2 = StreamingHistogram::new(1.0, 2001.0, 70);
    let values = (1..2001).collect::<Vec<_>>();
    for value in values {
        streaming2.add(value as f64);
        streaming_full.add(value as f64);
    }

    streaming1.merge(streaming2);

    assert_abs_diff_eq!(streaming_full.percentile(99).unwrap_or(0.0), streaming1.percentile(99).unwrap_or(100.0), epsilon = 10.0);
}

#[test]
fn test_streaming_approx_percentile1() {
    use rand::prelude::SliceRandom;
    use rand::thread_rng;

    let mut streaming = StreamingApproxPercentileHistogram::new(1.0, 1001.0, 50, 99);
    let mut values = (1..1001).collect::<Vec<_>>();
    values.shuffle(&mut thread_rng());
    for value in values {
        streaming.add(value as f64);
    }

    assert_eq!(Some(991.0), streaming.value());
}

#[test]
fn test_streaming_approx_percentile2() {
    use rand::prelude::SliceRandom;
    use rand::thread_rng;

    let mut streaming = StreamingApproxPercentileTDigest::new(99);
    let mut values = (1..1001).collect::<Vec<_>>();
    values.shuffle(&mut thread_rng());
    for value in values {
        streaming.add(value as f64);
    }

    assert_eq!(Some(990.5), streaming.value());
}
