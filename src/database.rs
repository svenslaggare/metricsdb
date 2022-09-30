use std::path::Path;
use std::time::Duration;

use crate::operations::{StreamingApproxPercentile, StreamingAverage, StreamingMax, StreamingOperation, StreamingTransformOperation};
use crate::model::{Datapoint, Query, Tags, Time, TIME_SCALE};
use crate::storage::DatabaseStorage;
use crate::storage::file::DatabaseStorageFile;
use crate::database_operations;
use crate::database_operations::TimeRangeStatistics;
use crate::tags::TagsIndex;

// pub const DEFAULT_BLOCK_DURATION: f64 = 0.0;
// pub const DEFAULT_BLOCK_DURATION: f64 = 1.0;
// pub const DEFAULT_BLOCK_DURATION: f64 = 10.0;
pub const DEFAULT_BLOCK_DURATION: f64 = 10.0 * 60.0;

pub const DEFAULT_DATAPOINT_DURATION: f64 = 0.0;
// pub const DEFAULT_DATAPOINT_DURATION: f64 = 0.5;

pub type DefaultDatabase = Database<DatabaseStorageFile>;

pub struct Database<TStorage: DatabaseStorage> {
    block_duration: u64,
    datapoint_duration: u64,
    storage: TStorage,
    tags_index: TagsIndex
}

impl<TStorage: DatabaseStorage> Database<TStorage> {
    pub fn new(base_path: &Path) -> Database<TStorage> {
        if !base_path.exists() {
            std::fs::create_dir_all(base_path).unwrap();
        }

        Database {
            block_duration: (DEFAULT_BLOCK_DURATION * TIME_SCALE as f64) as u64,
            datapoint_duration: (DEFAULT_DATAPOINT_DURATION * TIME_SCALE as f64) as u64,
            storage: TStorage::new(base_path),
            tags_index: TagsIndex::new()
        }
    }

    pub fn from_existing(base_path: &Path) -> Database<TStorage> {
        Database {
            block_duration: (DEFAULT_BLOCK_DURATION * TIME_SCALE as f64) as u64,
            datapoint_duration: (DEFAULT_DATAPOINT_DURATION * TIME_SCALE as f64) as u64,
            storage: TStorage::from_existing(base_path),
            tags_index: TagsIndex::new() // TODO: load from disk
        }
    }

    pub fn stats(&self) {
        println!("Num blocks: {}", self.storage.len());
        let mut num_datapoints = 0;
        let mut max_datapoints_in_block = 0;
        for block_index in 0..self.storage.len() {
            self.storage.visit_datapoints(block_index, |_, datapoints| {
                let block_length = datapoints.len();
                num_datapoints += block_length;
                max_datapoints_in_block = max_datapoints_in_block.max(block_length);
            });
        }
        println!("Num datapoints: {}, max datapoints: {}", num_datapoints, max_datapoints_in_block);
    }

    pub fn tags_pattern(&self, tags: &[&str]) -> Option<Tags> {
        self.tags_index.tags_pattern(tags)
    }

    pub fn gauge(&mut self, time: f64, value: f64, tags: &[&str]) {
        let tags = self.try_add_tags(tags).unwrap();

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

    fn try_add_tags(&mut self, tags: &[&str]) -> Option<Tags> {
        for tag in tags {
            self.tags_index.try_add(*tag)?;
        }
        self.tags_index.tags_pattern(tags)
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
        let create = |stats: TimeRangeStatistics, percentile: i32| {
            StreamingApproxPercentile::new(stats.min, stats.max, (stats.count as f64).sqrt().ceil() as usize, percentile)
        };

        match query.input_transform {
            Some(op) => {
                self.operation(query, |stats| StreamingTransformOperation::new(op, create(stats.unwrap(), percentile)), true)
            }
            None => {
                self.operation(query, |stats| create(stats.unwrap(), percentile), true)
            }
        }
    }

    fn operation<T: StreamingOperation<f64>, F: Fn(Option<TimeRangeStatistics>) -> T>(&self,
                                                                                      query: Query,
                                                                                      create_op: F,
                                                                                      require_statistics: bool) -> Option<f64> {
        let (start_time, end_time) = query.time_range.int_range();
        assert!(end_time > start_time);

        let start_block_index = database_operations::find_block_index(&self.storage, start_time)?;

        let stats = if require_statistics {
            Some(
                database_operations::determine_statistics_for_time_range(
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

        let mut streaming_operation = create_op(stats);
        database_operations::visit_datapoints_in_time_range(
            &self.storage,
            start_time,
            end_time,
            query.tags_filter,
            start_block_index,
            false,
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

    fn operation_in_window<T: StreamingOperation<f64>, F: Fn() -> T>(&self,
                                                                     query: Query,
                                                                     duration: Duration,
                                                                     create_op: F) -> Vec<(f64, f64)> {
        let (start_time, end_time) = query.time_range.int_range();
        assert!(end_time > start_time);

        let duration = (duration.as_secs_f64() * TIME_SCALE as f64) as u64;

        let start_block_index = match database_operations::find_block_index(&self.storage, start_time) {
            Some(start_block_index) => start_block_index,
            None => { return Vec::new(); }
        };

        let window_start = start_time / duration;
        let num_windows = (end_time / duration) - window_start;
        let mut windows = (0..num_windows).map(|_| None).collect::<Vec<_>>();

        let get_timestamp = |window_index: usize| {
            (((window_index as u64 + window_start) * duration) / TIME_SCALE) as f64
        };

        database_operations::visit_datapoints_in_time_range(
            &self.storage,
            start_time,
            end_time,
            query.tags_filter,
            start_block_index,
            false,
            |block_start_time, datapoint| {
                let datapoint_time = block_start_time + datapoint.time_offset as Time;
                let value = datapoint.value as f64;
                windows[((datapoint_time / duration) - window_start) as usize]
                    .get_or_insert_with(|| create_op())
                    .add(value);
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
            .filter(|operation| operation.is_some())
            .enumerate()
            .map(|(start, operation)| transform_output(operation.as_ref().unwrap().value()).map(|value| (get_timestamp(start), value)))
            .flatten()
            .collect()
    }
}