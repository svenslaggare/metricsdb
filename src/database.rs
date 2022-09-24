use std::path::Path;
use float_ord::FloatOrd;
use crate::algorithms::StreamingHigherPercentile;
use crate::model::{Datapoint, Tags, Time, TIME_SCALE};

use crate::storage::{DatabaseStorage, DatabaseStorageFile, DatabaseStorageVec};
use crate::TimeRange;

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
        for block_index in 0..self.storage.len() {
            num_datapoints += self.storage.datapoints(block_index).unwrap().len();
        }
        println!("Num datapoints: {}", num_datapoints);
    }

    pub fn gauge(&mut self, time: f64, value: f64, tags: Tags) {
        let time = (time * TIME_SCALE as f64).round() as Time;
        let value = value as f32;

        let mut datapoint = Datapoint {
            time_offset: 0,
            tags,
            value
        };

        if let Some(block_start_time) = self.storage.active_block_start_time() {
            assert!(time >= block_start_time, "{}, {}", time, block_start_time);

            if time - block_start_time < self.block_duration {
                let time_offset = time - block_start_time;
                assert!(time_offset < u32::MAX as u64);
                datapoint.time_offset = time_offset as u32;

                let last_datapoint = self.storage.active_block_datapoints_mut()
                    .map(|datapoint| datapoint.last_mut())
                    .flatten();

                if let Some(last_datapoint) = last_datapoint {
                    if (time - (block_start_time + last_datapoint.time_offset as u64)) < self.datapoint_duration {
                        last_datapoint.value = value;
                        return;
                    }
                }

                self.storage.add_datapoint(datapoint);
            } else {
                self.storage.create_block(time, datapoint);
            }
        } else {
            self.storage.create_block(time, datapoint);
        }
    }

    pub fn average(&self, range: TimeRange, binary_search: bool) -> f64 {
        let (start_time, end_time) = range.int_range();
        assert!(end_time > start_time);

        let start_block_index = if binary_search {
            find_block_index(&self.storage, start_time)
        } else {
            Some(0)
        };

        let mut sum = 0.0;
        let mut count = 0;
        if let Some(start_block_index) = start_block_index {
            visit_datapoints_in_time_range(
                &self.storage,
                start_time,
                end_time,
                start_block_index,
                |datapoint| {
                    sum += datapoint.value as f64;
                    count += 1;
                }
            );
        }

        println!("count: {}", count);

        sum / count as f64
    }

    pub fn max(&self, range: TimeRange, binary_search: bool) -> f64 {
        let (start_time, end_time) = range.int_range();
        assert!(end_time > start_time);

        let start_block_index = if binary_search {
            find_block_index(&self.storage, start_time)
        } else {
            Some(0)
        };

        let mut max = f64::NEG_INFINITY;
        if let Some(start_block_index) = start_block_index {
            visit_datapoints_in_time_range(
                &self.storage,
                start_time,
                end_time,
                start_block_index,
                |datapoint| {
                    max = max.max(datapoint.value as f64);
                }
            );
        }

        max
    }

    pub fn percentile(&self, range: TimeRange, binary_search: bool, percentile: i32) -> Option<f64> {
        let (start_time, end_time) = range.int_range();
        assert!(end_time > start_time);

        let start_block_index = if binary_search {
            find_block_index(&self.storage, start_time)
        } else {
            Some(0)
        };

        if let Some(start_block_index) = start_block_index {
            let count = count_datapoints_in_time_range(
                &self.storage,
                start_time,
                end_time,
                start_block_index
            );

            println!("count: {}", count);

            let mut streaming_percentile = StreamingHigherPercentile::new(count, percentile);
            visit_datapoints_in_time_range(
                &self.storage,
                start_time,
                end_time,
                start_block_index,
                |datapoint| {
                    streaming_percentile.add(FloatOrd(datapoint.value as f64));
                }
            );

            streaming_percentile.value().map(|x| x.0)
        } else {
            None
        }
    }
}

fn find_block_index<TStorage: DatabaseStorage>(storage: &TStorage, time: u64) -> Option<usize> {
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

fn visit_datapoints_in_time_range<TStorage: DatabaseStorage, F: FnMut(&Datapoint)>(storage: &TStorage,
                                                                                   start_time: Time,
                                                                                   end_time: Time,
                                                                                   start_block_index: usize,
                                                                                   mut apply: F) {
    for block_index in start_block_index..storage.len() {
        let (block_start_time, block_end_time) = storage.block_time_range(block_index).unwrap();
        if block_end_time >= start_time {
            let mut iterator = DatapointIterator::new(
                start_time,
                end_time,
                block_start_time,
                block_end_time,
                storage.datapoints(block_index).unwrap().iter()
            );

            for datapoint in &mut iterator {
                apply(datapoint);
            }

            if iterator.outside_time_range {
                break;
            }
        }
    }
}

fn count_datapoints_in_time_range<TStorage: DatabaseStorage>(storage: &TStorage,
                                                             start_time: Time,
                                                             end_time: Time,
                                                             start_block_index: usize) -> usize {
    let mut count = 0;
    for block_index in start_block_index..storage.len() {
        let (block_start_time, block_end_time) = storage.block_time_range(block_index).unwrap();
        if block_end_time >= start_time {
            let mut outside_time_range = false;
            for datapoint in storage.datapoints(block_index).unwrap().iter() {
                let datapoint_time = block_start_time + datapoint.time_offset as Time;
                if datapoint_time > end_time {
                    outside_time_range = true;
                    break;
                }

                if datapoint_time >= start_time {
                    count += 1;
                }
            }

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