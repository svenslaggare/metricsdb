use std::cmp::Ordering;
use std::collections::{BinaryHeap};

use float_ord::FloatOrd;

use rand::prelude::SliceRandom;
use rand::thread_rng;

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

    pub fn with_initial(value: T) -> StreamingAverage<T> {
        StreamingAverage {
            sum: value,
            count: 1
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

pub struct StreamingHigherPercentile<T: Ord + Clone> {
    percentile_count: usize,
    values: BinaryHeap<Entry<T>>
}

impl<T: Ord + Clone> StreamingHigherPercentile<T> {
    pub fn new(count: usize, percentile: i32) -> StreamingHigherPercentile<T> {
        StreamingHigherPercentile {
            percentile_count: (count as f64 * ((100 - percentile) as f64 / 100.0) as f64).ceil() as usize,
            values: BinaryHeap::new()
        }
    }

    fn all_values(&self) -> Vec<T> {
        self.values.iter().map(|x| x.value.clone()).collect()
    }
}

impl<T: Ord + Clone> StreamingOperation<T> for StreamingHigherPercentile<T> {
    fn add(&mut self, value: T) {
        if self.values.len() < self.percentile_count {
            self.values.push(Entry::new(value));
        } else if let Some(min) = self.values.peek() {
            if value > min.value {
                self.values.pop();
                self.values.push(Entry::new(value));
            }
        }
    }

    fn value(&self) -> Option<T> {
        self.values.peek().map(|e| e.value.clone())
    }
}

pub struct StreamingHigherPercentileF64 {
    inner: StreamingHigherPercentile<FloatOrd<f64>>
}

impl StreamingHigherPercentileF64 {
    pub fn new(count: usize, percentile: i32) -> StreamingHigherPercentileF64{
        StreamingHigherPercentileF64 {
            inner: StreamingHigherPercentile::new(count, percentile)
        }
    }
}

impl StreamingOperation<f64> for StreamingHigherPercentileF64{
    fn add(&mut self, value: f64) {
        self.inner.add(FloatOrd(value));
    }

    fn value(&self) -> Option<f64> {
        self.inner.value().map(|x| x.0)
    }
}

#[derive(Copy, Clone, Eq, PartialEq)]
struct Entry<T: Ord + Clone> {
    value: T
}

impl<T: Ord + Clone> Entry<T> {
    pub fn new(value: T) -> Entry<T> {
        Entry {
            value
        }
    }
}

impl<T: Ord + Clone> Ord for Entry<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        other.value.cmp(&self.value)
    }
}

impl<T: Ord + Clone> PartialOrd for Entry<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub struct StreamingLowerPercentile<T: Ord + Clone> {
    percentile_count: usize,
    values: BinaryHeap<T>
}

impl<T: Ord + Clone> StreamingLowerPercentile<T> {
    pub fn new(count: usize, percentile: i32) -> StreamingLowerPercentile<T> {
        StreamingLowerPercentile {
            percentile_count: (count as f64 * (percentile as f64 / 100.0) as f64).ceil() as usize,
            values: BinaryHeap::new()
        }
    }

    fn all_values(&self) -> Vec<T> {
        self.values.iter().map(|x| x.clone()).collect()
    }
}

impl<T: Ord + Clone> StreamingOperation<T> for StreamingLowerPercentile<T> {
    fn add(&mut self, value: T) {
        if self.values.len() < self.percentile_count {
            self.values.push(value);
        } else if let Some(max) = self.values.peek() {
            if &value < max {
                self.values.pop();
                self.values.push(value);
            }
        }
    }

    fn value(&self) -> Option<T> {
        self.values.peek().map(|e| e.clone())
    }
}

#[test]
fn test_streaming_higher_percentile1() {
    let mut streaming = StreamingHigherPercentile::new(1000, 99);
    for value in (1..1001).rev() {
        streaming.add(FloatOrd(value as f64));
    }

    println!("{:?}", streaming.all_values());
    println!("{:?}", streaming.value());

    assert_eq!(Some(FloatOrd(991.0)), streaming.value());
}

#[test]
fn test_streaming_higher_percentile2() {
    let mut streaming = StreamingHigherPercentile::new(1000, 99);
    for value in (1..1001).rev() {
        streaming.add(FloatOrd(value as f64));
    }

    println!("{:?}", streaming.all_values());
    println!("{:?}", streaming.value());

    assert_eq!(Some(FloatOrd(991.0)), streaming.value());
}

#[test]
fn test_streaming_higher_percentile3() {
    let mut streaming = StreamingHigherPercentile::new(2000, 99);
    for value in (1..1001).rev() {
        streaming.add(FloatOrd(value as f64));
        streaming.add(FloatOrd((2 * value) as f64));
    }

    println!("{:?}", streaming.all_values());
    println!("{:?}", streaming.value());

    assert_eq!(Some(FloatOrd(1962.0)), streaming.value());
}

#[test]
fn test_streaming_higher_percentile4() {
    let mut streaming = StreamingHigherPercentile::new(1000, 99);
    let mut values = (1..1001).collect::<Vec<_>>();
    values.shuffle(&mut thread_rng());
    for value in values {
        streaming.add(FloatOrd(value as f64));
    }

    println!("{:?}", streaming.all_values());
    println!("{:?}", streaming.value());

    assert_eq!(Some(FloatOrd(991.0)), streaming.value());
}

#[test]
fn test_streaming_higher_percentile5() {
    let mut streaming = StreamingHigherPercentile::new(2000, 65);
    for value in (1..1001).rev() {
        streaming.add(FloatOrd(value as f64));
        streaming.add(FloatOrd((2 * value) as f64));
    }

    println!("{:?}", streaming.all_values());
    println!("{:?}", streaming.value());

    assert_eq!(Some(FloatOrd(868.0)), streaming.value());
}

#[test]
fn test_streaming_higher_percentile6() {
    let mut streaming = StreamingHigherPercentile::new(2000, 5);
    for value in (1..1001).rev() {
        streaming.add(FloatOrd(value as f64));
        streaming.add(FloatOrd((2 * value) as f64));
    }

    println!("{:?}", streaming.all_values());
    println!("{:?}", streaming.value());

    assert_eq!(Some(FloatOrd(68.0)), streaming.value());
}

#[test]
fn test_streaming_lower_percentile1() {
    let mut streaming = StreamingLowerPercentile::new(2000, 5);
    for value in (1..1001).rev() {
        streaming.add(FloatOrd(value as f64));
        streaming.add(FloatOrd((2 * value) as f64));
    }

    println!("{:?}", streaming.all_values());
    println!("{:?}", streaming.value());

    assert_eq!(Some(FloatOrd(67.0)), streaming.value());
}

#[test]
fn test_streaming_lower_percentile2() {
    let mut streaming = StreamingLowerPercentile::new(2000, 10);
    for value in (1..1001).rev() {
        streaming.add(FloatOrd(value as f64));
        streaming.add(FloatOrd((2 * value) as f64));
    }

    println!("{:?}", streaming.all_values());
    println!("{:?}", streaming.value());

    assert_eq!(Some(FloatOrd(134.0)), streaming.value());
}