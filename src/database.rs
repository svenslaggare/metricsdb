use std::path::Path;
use std::time::Duration;

use crate::operations::{StreamingAverage, StreamingHigherPercentileF64, StreamingMax, StreamingOperation, StreamingTransformOperation, TransformOperation};
use crate::model::{Datapoint, Query, Tags, Time, TIME_SCALE};
use crate::storage::DatabaseStorage;
use crate::storage::file::DatabaseStorageFile;

use crate::{TagsFilter, TimeRange};

// pub const DEFAULT_BLOCK_DURATION: f64 = 0.0;
// pub const DEFAULT_BLOCK_DURATION: f64 = 1.0;
// pub const DEFAULT_BLOCK_DURATION: f64 = 10.0;
pub const DEFAULT_BLOCK_DURATION: f64 = 10.0 * 60.0;

pub const DEFAULT_DATAPOINT_DURATION: f64 = 0.0;
// pub const DEFAULT_DATAPOINT_DURATION: f64 = 0.5;

pub type DefaultDatabase = Database<DatabaseStorageFile>;

pub struct Database<TStorage: DatabaseStorage> {
    storage: TStorage,
    block_duration: u64,
    datapoint_duration: u64
}

impl<TStorage: DatabaseStorage> Database<TStorage> {
    pub fn new(base_path: &Path) -> Database<TStorage> {
        if !base_path.exists() {
            std::fs::create_dir_all(base_path).unwrap();
        }

        Database {
            storage: TStorage::new(base_path),
            block_duration: (DEFAULT_BLOCK_DURATION * TIME_SCALE as f64) as u64,
            datapoint_duration: (DEFAULT_DATAPOINT_DURATION * TIME_SCALE as f64) as u64
        }
    }

    pub fn from_existing(base_path: &Path) -> Database<TStorage> {
        Database {
            storage: TStorage::from_existing(base_path),
            block_duration: (DEFAULT_BLOCK_DURATION * TIME_SCALE as f64) as u64,
            datapoint_duration: (DEFAULT_DATAPOINT_DURATION * TIME_SCALE as f64) as u64
        }
    }

    pub fn stats(&self) {
        println!("Num blocks: {}", self.storage.len());
        let mut num_datapoints = 0;
        let mut max_datapoints_in_block = 0;
        for block_index in 0..self.storage.len() {
            self.storage.visit_datapoints(block_index, |tags, datapoints| {
                let block_length = datapoints.len();
                num_datapoints += block_length;
                max_datapoints_in_block = max_datapoints_in_block.max(block_length);
            });
        }
        println!("Num datapoints: {}, max datapoints: {}", num_datapoints, max_datapoints_in_block);
    }

    pub fn gauge(&mut self, time: f64, value: f64, tags: Tags) {
        let time = (time * TIME_SCALE as f64).round() as Time;
        let value = value as f32;

        let mut datapoint = Datapoint {
            time_offset: 0,
            value
        };

        if let Some(block_start_time) = self.storage.active_block_start_time() {
            assert!(time >= block_start_time, "{}, {}", time, block_start_time);

            if time - block_start_time < self.block_duration {
                let time_offset = time - block_start_time;
                assert!(time_offset < u32::MAX as u64);
                datapoint.time_offset = time_offset as u32;

                let last_datapoint = self.storage.active_block_datapoints_mut(tags)
                    .map(|datapoint| datapoint.last_mut())
                    .flatten();

                if let Some(last_datapoint) = last_datapoint {
                    if (time - (block_start_time + last_datapoint.time_offset as u64)) < self.datapoint_duration {
                        last_datapoint.value = value;
                        return;
                    }
                }

                self.storage.add_datapoint(tags, datapoint);
            } else {
                self.storage.create_block_with_datapoint(time, tags, datapoint);
            }
        } else {
            self.storage.create_block_with_datapoint(time, tags, datapoint);
        }
    }

    pub fn average(&self, query: Query) -> Option<f64> {
        match query.input_transform {
            Some(op) => {
                self.operation(query, |_| StreamingTransformOperation::<StreamingAverage<f64>>::from_default(op), false)
            }
            None => {
                self.operation(query, |_| StreamingAverage::new(), false)
            }
        }
    }

    pub fn max(&self, query: Query) -> Option<f64> {
        match query.input_transform {
            Some(op) => {
                self.operation(query, |_| StreamingTransformOperation::<StreamingMax>::from_default(op), false)
            }
            None => {
                self.operation(query, |_| StreamingMax::new(), false)
            }
        }
    }

    pub fn percentile(&self, query: Query, percentile: i32) -> Option<f64> {
        match query.input_transform {
            Some(op) => {
                self.operation(query, |count| StreamingTransformOperation::new(op, StreamingHigherPercentileF64::new(count.unwrap(), percentile)), true)
            }
            None => {
                self.operation(query, |count| StreamingHigherPercentileF64::new(count.unwrap(), percentile), true)
            }
        }
    }

    fn operation<T: StreamingOperation<f64>, F: Fn(Option<usize>) -> T>(&self,
                                                                        query: Query,
                                                                        create_op: F,
                                                                        require_count: bool) -> Option<f64> {
        let (start_time, end_time) = query.time_range.int_range();
        assert!(end_time > start_time);

        let start_block_index = find_block_index(&self.storage, start_time)?;

        let count = if require_count {
            Some(
                count_datapoints_in_time_range(
                    &self.storage,
                    start_time,
                    end_time,
                    query.tags_filter.clone(),
                    start_block_index
                )
            )
        } else {
            None
        };

        let mut streaming_operation = create_op(count);
        visit_datapoints_in_time_range(
            &self.storage,
            start_time,
            end_time,
            query.tags_filter,
            start_block_index,
            |_, datapoint| {
                streaming_operation.add(datapoint.value as f64);
            }
        );

        match query.output_transform {
            Some(operation) => operation.apply(streaming_operation.value()?),
            None => streaming_operation.value()
        }
    }

    pub fn average_in_window(&self, query: Query, duration: Duration) -> Vec<(f64, f64)> {
        match query.input_transform {
            Some(op) => {
                self.operation_in_window(query, duration, || StreamingTransformOperation::<StreamingAverage<f64>>::from_default(op))
            }
            None => {
                self.operation_in_window(query, duration, || StreamingAverage::new())
            }
        }
    }

    pub fn max_in_window(&self, query: Query, duration: Duration) -> Vec<(f64, f64)> {
        match query.input_transform {
            Some(op) => {
                self.operation_in_window(query, duration, || StreamingTransformOperation::<StreamingMax>::from_default(op))
            }
            None => {
                self.operation_in_window(query, duration, || StreamingMax::new())
            }
        }
    }

    fn operation_in_window<T: StreamingOperation<f64>, F: Fn() -> T>(&self, query: Query, duration: Duration, create_op: F) -> Vec<(f64, f64)> {
        let (start_time, end_time) = query.time_range.int_range();
        assert!(end_time > start_time);

        let duration = (duration.as_secs_f64() * TIME_SCALE as f64) as u64;

        let start_block_index = match find_block_index(&self.storage, start_time) {
            Some(start_block_index) => start_block_index,
            None => { return Vec::new(); }
        };

        let mut windows = Vec::<(Time, T)>::new();
        visit_datapoints_in_time_range(
            &self.storage,
            start_time,
            end_time,
            query.tags_filter,
            start_block_index,
            |block_start_time, datapoint| {
                let datapoint_time = block_start_time + datapoint.time_offset as Time;
                let value = datapoint.value as f64;
                if let Some(instance) = windows.last_mut() {
                    if datapoint_time - instance.0 <= duration {
                        instance.1.add(value);
                    } else {
                        let mut op = create_op();
                        op.add(value);
                        windows.push((datapoint_time, op));
                    }
                } else {
                    let mut op = create_op();
                    op.add(value);
                    windows.push((datapoint_time, op));
                }
            }
        );

        let transform_output = |value: Option<f64>| {
            let value = value?;

            match query.output_transform {
                Some(operation) => operation.apply(value),
                None => Some(value)
            }
        };

        windows
            .iter()
            .map(|(start, operation)| transform_output(operation.value()).map(|value| ((start / TIME_SCALE) as f64, value)))
            .flatten()
            .collect()
    }
}

fn find_block_index<TStorage: DatabaseStorage>(storage: &TStorage, time: Time) -> Option<usize> {
    if storage.len() == 0 {
        return None;
    }

    let mut lower = 0;
    let mut upper = storage.len() - 1;
    while lower <= upper {
        let middle = lower + (upper - lower) / 2;
        println!("{}, {}, {}", lower, upper, middle);
        let (_, middle_time) = storage.block_time_range(middle).unwrap();
        if time > middle_time {
            lower = middle + 1;
        } else if time < middle_time {
            upper = middle - 1;
        } else {
            break;
        }
    }

    Some(lower)
}

fn visit_datapoints_in_time_range<TStorage: DatabaseStorage, F: FnMut(Time, &Datapoint)>(storage: &TStorage,
                                                                                         start_time: Time,
                                                                                         end_time: Time,
                                                                                         tags_filter: TagsFilter,
                                                                                         start_block_index: usize,
                                                                                         mut apply: F) {
    for block_index in start_block_index..storage.len() {
        let (block_start_time, block_end_time) = storage.block_time_range(block_index).unwrap();
        if block_end_time >= start_time {
            let mut outside_time_range = false;
            storage.visit_datapoints(block_index, |tags, datapoints| {
                if tags_filter.accept(tags) {
                    let mut iterator = DatapointIterator::new(
                        start_time,
                        end_time,
                        block_start_time,
                        block_end_time,
                        datapoints.iter()
                    );

                    for datapoint in &mut iterator {
                        apply(block_start_time, datapoint);
                    }

                    if iterator.outside_time_range {
                        outside_time_range = true;
                    }
                }
            });

            if outside_time_range {
                break;
            }
        }
    }
}

fn count_datapoints_in_time_range<TStorage: DatabaseStorage>(storage: &TStorage,
                                                             start_time: Time,
                                                             end_time: Time,
                                                             tags_filter: TagsFilter,
                                                             start_block_index: usize) -> usize {
    let mut count = 0;
    for block_index in start_block_index..storage.len() {
        let (block_start_time, block_end_time) = storage.block_time_range(block_index).unwrap();
        if block_end_time >= start_time {
            let mut outside_time_range = false;
            storage.visit_datapoints(block_index, |tags, datapoints| {
                if tags_filter.accept(tags) {
                    for datapoint in datapoints.iter() {
                        let datapoint_time = block_start_time + datapoint.time_offset as Time;
                        if datapoint_time > end_time {
                            outside_time_range = true;
                            break;
                        }

                        if datapoint_time >= start_time {
                            count += 1;
                        }
                    }
                }
            });

            if outside_time_range {
                break;
            }
        }
    }

    count
}

struct DatapointIterator<'a, T: Iterator<Item=&'a Datapoint>> {
    start_time: Time,
    end_time: Time,
    block_start_time: Time,
    block_end_time: Time,
    iterator: T,
    outside_time_range: bool
}

impl<'a, T: Iterator<Item=&'a Datapoint>> DatapointIterator<'a, T> {
    pub fn new(start_time: Time,
               end_time: Time,
               block_start_time: Time,
               block_end_time: Time,
               iterator: T) -> DatapointIterator<'a, T> {
        DatapointIterator {
            start_time,
            end_time,
            block_start_time,
            block_end_time,
            iterator,
            outside_time_range: false
        }
    }
}

impl<'a, T: Iterator<Item=&'a Datapoint>> Iterator for DatapointIterator<'a, T> {
    type Item = &'a Datapoint;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(datapoint) = self.iterator.next() {
            let datapoint_time = self.block_start_time + datapoint.time_offset as Time;
            if datapoint_time > self.end_time {
                self.outside_time_range = true;
                return None;
            }

            if datapoint_time >= self.start_time {
                return Some(datapoint);
            }
        }

        return None;
    }
}