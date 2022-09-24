use std::cmp::Ordering;
use std::collections::{BinaryHeap};

use float_ord::FloatOrd;

use rand::prelude::SliceRandom;
use rand::thread_rng;

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

    pub fn add(&mut self, value: T) {
        if self.values.len() < self.percentile_count {
            self.values.push(Entry::new(value));
        } else if let Some(min) = self.values.peek() {
            if value > min.value {
                self.values.pop();
                self.values.push(Entry::new(value));
            }
        }
    }

    pub fn value(&self) -> Option<T> {
        self.values.peek().map(|e| e.value.clone())
    }

    fn all_values(&self) -> Vec<T> {
        self.values.iter().map(|x| x.value.clone()).collect()
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

    pub fn add(&mut self, value: T) {
        if self.values.len() < self.percentile_count {
            self.values.push(value);
        } else if let Some(max) = self.values.peek() {
            if &value < max {
                self.values.pop();
                self.values.push(value);
            }
        }
    }

    pub fn value(&self) -> Option<T> {
        self.values.peek().cloned()
    }

    fn all_values(&self) -> Vec<T> {
        self.values.iter().map(|x| x.clone()).collect()
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