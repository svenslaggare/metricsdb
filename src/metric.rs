use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::time::Duration;

use fnv::FnvHashMap;

use crate::operations::{StreamingApproxPercentile, StreamingAverage, StreamingMax, StreamingOperation, StreamingTransformOperation};
use crate::model::{Datapoint, Query, Time, TIME_SCALE};
use crate::storage::MetricStorage;
use crate::storage::file::MetricStorageFile;
use crate::{metric_operations};
use crate::metric_operations::{MetricWindowing, TimeRangeStatistics};
use crate::tags::SecondaryTagsIndex;

// pub const DEFAULT_BLOCK_DURATION: f64 = 0.0;
// pub const DEFAULT_BLOCK_DURATION: f64 = 1.0;
// pub const DEFAULT_BLOCK_DURATION: f64 = 10.0;
pub const DEFAULT_BLOCK_DURATION: f64 = 10.0 * 60.0;

pub const DEFAULT_DATAPOINT_DURATION: f64 = 0.0;
// pub const DEFAULT_DATAPOINT_DURATION: f64 = 0.5;

pub type DefaultMetric = Metric<MetricStorageFile<f32>>;

pub type MetricResult<T> = Result<T, MetricError>;

#[derive(Debug)]
pub enum MetricError {
    ExceededSecondaryTags,
    FailedToSaveTags(std::io::Error)
}

pub struct Metric<TStorage: MetricStorage<f32>> {
    base_path: PathBuf,
    primary_tags: FnvHashMap<Option<String>, PrimaryTagMetric<TStorage, f32>>
}

impl<TStorage: MetricStorage<f32>> Metric<TStorage> {
    pub fn new(base_path: &Path) -> Metric<TStorage> {
        if !base_path.exists() {
            std::fs::create_dir_all(base_path).unwrap();
        }

        let mut primary_tags = FnvHashMap::default();
        primary_tags.insert(None, PrimaryTagMetric::new(&base_path.join("default"), DEFAULT_BLOCK_DURATION, DEFAULT_DATAPOINT_DURATION));

        let metric = Metric {
            base_path: base_path.to_owned(),
            primary_tags
        };

        metric.save_primary_tags().unwrap();

        metric
    }

    pub fn from_existing(base_path: &Path) -> Metric<TStorage> {
        let mut primary_tags = FnvHashMap::default();
        primary_tags.insert(None, PrimaryTagMetric::from_existing(&base_path.join("default")));

        Metric {
            base_path: base_path.to_owned(),
            primary_tags
        }
    }

    pub fn stats(&self) {
        for (tag, primary_tag) in self.primary_tags.iter() {
            println!("Tag: {:?}", tag);
            println!("Num blocks: {}", primary_tag.storage.len());
            let mut num_datapoints = 0;
            let mut max_datapoints_in_block = 0;
            for block_index in 0..primary_tag.storage.len() {
                if let Some(iterator) = primary_tag.storage.block_datapoints(block_index) {
                    for (_, datapoints) in iterator {
                        let block_length = datapoints.len();
                        num_datapoints += block_length;
                        max_datapoints_in_block = max_datapoints_in_block.max(block_length);
                    }
                }
            }
            println!("Num datapoints: {}, max datapoints: {}", num_datapoints, max_datapoints_in_block);
        }
    }

    pub fn add_primary_tag(&mut self, tag: &str) {
        let mut inserted = false;
        self.primary_tags
            .entry(Some(tag.to_owned()))
            .or_insert_with(|| {
                inserted = true;
                PrimaryTagMetric::new(&self.base_path.join(tag), DEFAULT_BLOCK_DURATION, DEFAULT_DATAPOINT_DURATION)
            });

        if inserted {
            self.save_primary_tags().unwrap();
        }
    }

    fn save_primary_tags(&self) -> std::io::Result<()> {
        let content = serde_json::to_string(&self.primary_tags.keys().collect::<Vec<_>>())?;
        std::fs::write(&self.base_path.join("primary_tags.json"), &content)?;
        Ok(())
    }

    pub fn gauge(&mut self, time: f64, value: f64, tags: &[&str]) -> MetricResult<()> {
        let mut tags = tags.into_iter().cloned().collect::<Vec<_>>();

        let (primary_tag_id, mut primary_tag) = self.extract_primary_tag(&mut tags);
        let secondary_tags = match primary_tag.tags_index.try_add_tags(&tags) {
            Ok(secondary_tags) => secondary_tags,
            Err(err) => {
                self.primary_tags.insert(primary_tag_id, primary_tag);
                return Err(err);
            }
        };

        let time = (time * TIME_SCALE as f64).round() as Time;
        let value = value as f32;

        let mut datapoint = Datapoint {
            time_offset: 0,
            value
        };

        if let Some(block_start_time) = primary_tag.storage.active_block_start_time() {
            assert!(time >= block_start_time, "{}, {}", time, block_start_time);

            let time_offset = time - block_start_time;
            if time_offset < primary_tag.storage.block_duration() {
                assert!(time_offset < u32::MAX as u64);
                datapoint.time_offset = time_offset as u32;

                let datapoint_duration = primary_tag.storage.datapoint_duration();
                let last_datapoint = primary_tag.storage.active_block_datapoints_mut(secondary_tags)
                    .map(|datapoint| datapoint.last_mut())
                    .flatten();

                if let Some(last_datapoint) = last_datapoint {
                    if (time - (block_start_time + last_datapoint.time_offset as u64)) < datapoint_duration {
                        last_datapoint.value = value;
                        return Ok(());
                    }
                }

                primary_tag.storage.add_datapoint(secondary_tags, datapoint);
            } else {
                primary_tag.storage.create_block_with_datapoint(time, secondary_tags, datapoint);
            }
        } else {
            primary_tag.storage.create_block_with_datapoint(time, secondary_tags, datapoint);
        }

        self.primary_tags.insert(primary_tag_id, primary_tag);
        Ok(())
    }

    pub fn average(&self, query: Query) -> Option<f64> {
        self.simple_operation::<StreamingAverage<f64>>(query)
    }

    pub fn max(&self, query: Query) -> Option<f64> {
        self.simple_operation::<StreamingMax::<f64>>(query)
    }

    fn simple_operation<T: StreamingOperation<f64> + Default>(&self, query: Query) -> Option<f64> {
        match query.input_transform {
            Some(op) => {
                self.operation(query, |_| StreamingTransformOperation::<T>::from_default(op), false)
            }
            None => {
                self.operation(query, |_| T::default(), false)
            }
        }
    }

    pub fn percentile(&self, query: Query, percentile: i32) -> Option<f64> {
        let create = |stats: &TimeRangeStatistics<f32>, percentile: i32| {
            let stats = TimeRangeStatistics::new(stats.count, stats.min() as f64, stats.max() as f64);
            StreamingApproxPercentile::from_stats(&stats, percentile)
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

    fn operation<T: StreamingOperation<f64>, F: Fn(Option<&TimeRangeStatistics<f32>>) -> T>(&self,
                                                                                            query: Query,
                                                                                            create_op: F,
                                                                                            require_statistics: bool) -> Option<f64> {
        let (start_time, end_time) = query.time_range.int_range();
        assert!(end_time > start_time);

        let mut streaming_operations = Vec::new();
        for (primary_tag_value, primary_tag) in self.primary_tags.iter() {
            if let Some(tags_filter) = query.tags_filter.apply(&primary_tag.tags_index, primary_tag_value) {
                if let Some(start_block_index) = metric_operations::find_block_index(&primary_tag.storage, start_time) {
                    let stats = if require_statistics {
                        Some(
                            metric_operations::determine_statistics_for_time_range(
                                &primary_tag.storage,
                                start_time,
                                end_time,
                                tags_filter,
                                start_block_index
                            )
                        )
                    } else {
                        None
                    };

                    let mut streaming_operation = create_op(stats.as_ref());
                    metric_operations::visit_datapoints_in_time_range(
                        &primary_tag.storage,
                        start_time,
                        end_time,
                        tags_filter,
                        start_block_index,
                        false,
                        |_, datapoint| {
                            streaming_operation.add(datapoint.value as f64);
                        }
                    );

                    streaming_operations.push(streaming_operation);
                }
            }
        }

        if streaming_operations.is_empty() {
            return None;
        }

        let mut streaming_operation = streaming_operations.remove(0);
        for other_operation in streaming_operations.into_iter() {
            streaming_operation.merge(other_operation);
        }

        match query.output_transform {
            Some(operation) => operation.apply(streaming_operation.value()?),
            None => streaming_operation.value()
        }
    }

    pub fn average_in_window(&self, query: Query, duration: Duration) -> Vec<(f64, f64)> {
        self.simple_operation_in_window::<StreamingAverage<f64>>(query, duration)
    }

    pub fn max_in_window(&self, query: Query, duration: Duration) -> Vec<(f64, f64)> {
        self.simple_operation_in_window::<StreamingMax<f64>>(query, duration)
    }

    pub fn simple_operation_in_window<T: StreamingOperation<f64> + Default>(&self, query: Query, duration: Duration) -> Vec<(f64, f64)> {
        match query.input_transform {
            Some(op) => {
                self.operation_in_window(query, duration, |_| StreamingTransformOperation::<T>::from_default(op), false)
            }
            None => {
                self.operation_in_window(query, duration, |_| T::default(), false)
            }
        }
    }

    pub fn percentile_in_window(&self, query: Query, duration: Duration, percentile: i32) -> Vec<(f64, f64)> {
        let create = |stats: &TimeRangeStatistics<f64>, percentile: i32| {
            StreamingApproxPercentile::from_stats(stats, percentile)
        };

        match query.input_transform {
            Some(op) => {
                self.operation_in_window(query, duration, |stats| StreamingTransformOperation::new(op, create(stats.unwrap(), percentile)), true)
            }
            None => {
                self.operation_in_window(query, duration, |stats| create(stats.unwrap(), percentile), true)
            }
        }
    }

    fn operation_in_window<T: StreamingOperation<f64>, F: Fn(Option<&TimeRangeStatistics<f64>>) -> T>(&self,
                                                                                                      query: Query,
                                                                                                      duration: Duration,
                                                                                                      create_op: F,
                                                                                                      require_statistics: bool) -> Vec<(f64, f64)> {
        let (start_time, end_time) = query.time_range.int_range();
        assert!(end_time > start_time);

        let duration = (duration.as_secs_f64() * TIME_SCALE as f64) as u64;

        let mut primary_tag_windows = Vec::new();
        for (primary_tag_value, primary_tag) in self.primary_tags.iter() {
            if let Some(tags_filter) = query.tags_filter.apply(&primary_tag.tags_index, primary_tag_value) {
                if let Some(start_block_index) = metric_operations::find_block_index(&primary_tag.storage, start_time) {
                    let mut windowing = MetricWindowing::new(start_time, end_time, duration);

                    let window_stats = if require_statistics {
                        let mut window_stats = windowing.create_windows(|| None);

                        metric_operations::visit_datapoints_in_time_range(
                            &primary_tag.storage,
                            start_time,
                            end_time,
                            tags_filter,
                            start_block_index,
                            false,
                            |block_start_time, datapoint| {
                                let datapoint_time = block_start_time + datapoint.time_offset as Time;
                                window_stats[windowing.get_window_index(datapoint_time)]
                                    .get_or_insert_with(|| TimeRangeStatistics::default())
                                    .handle(datapoint.value as f64);
                            }
                        );

                        Some(window_stats)
                    } else {
                        None
                    };

                    metric_operations::visit_datapoints_in_time_range(
                        &primary_tag.storage,
                        start_time,
                        end_time,
                        tags_filter,
                        start_block_index,
                        false,
                        |block_start_time, datapoint| {
                            let datapoint_time = block_start_time + datapoint.time_offset as Time;
                            let window_index = windowing.get_window_index(datapoint_time);
                            windowing.get(window_index)
                                .get_or_insert_with(|| {
                                    if require_statistics {
                                        create_op((&window_stats.as_ref().unwrap()[window_index]).as_ref())
                                    } else {
                                        create_op(None)
                                    }
                                })
                                .add(datapoint.value as f64);
                        }
                    );

                    primary_tag_windows.push(windowing);
                }
            }
        }

        if primary_tag_windows.is_empty() {
            return Vec::new();
        }

        let mut windowing = primary_tag_windows.remove(0);
        for current_windowing in primary_tag_windows.into_iter() {
            for (window_index, current_window) in current_windowing.into_windows().into_iter().enumerate() {
                let merged_window = windowing.get(window_index);

                if let Some(merged_window) = merged_window {
                    if let Some(current_window) = current_window {
                        merged_window.merge(current_window);
                    }
                } else {
                    *merged_window = current_window;
                }
            }
        }
        metric_operations::extract_operations_in_windows(
            windowing,
            |value| {
                let value = value?;

                match query.output_transform {
                    Some(operation) => operation.apply(value),
                    None => Some(value)
                }
            }
        )
    }

    fn extract_primary_tag<'a>(&'a mut self, tags: &mut Vec<&str>) -> (Option<String>, PrimaryTagMetric<TStorage, f32>) {
        for (index, tag) in tags.iter().enumerate() {
            let tag = Some((*tag).to_owned());
            if let Some(primary_tag) = self.primary_tags.remove(&tag) {
                tags.remove(index);
                return (tag, primary_tag);
            }
        }

        (None, self.primary_tags.remove(&None).unwrap())
    }
}

struct PrimaryTagMetric<TStorage: MetricStorage<E>, E: Copy> {
    storage: TStorage,
    tags_index: SecondaryTagsIndex,
    _phantom: PhantomData<E>
}

impl<TStorage: MetricStorage<E>, E: Copy> PrimaryTagMetric<TStorage, E> {
    pub fn new(base_path: &Path, block_duration: f64, datapoint_duration: f64) -> PrimaryTagMetric<TStorage, E> {
        if !base_path.exists() {
            std::fs::create_dir_all(base_path).unwrap();
        }

        PrimaryTagMetric {
            storage: TStorage::new(
                base_path,
                (block_duration * TIME_SCALE as f64) as u64,
                (datapoint_duration * TIME_SCALE as f64) as u64
            ),
            tags_index: SecondaryTagsIndex::new(base_path),
            _phantom: PhantomData::default()
        }
    }

    pub fn from_existing(base_path: &Path) -> PrimaryTagMetric<TStorage, E> {
        PrimaryTagMetric {
            storage: TStorage::from_existing(base_path),
            tags_index: SecondaryTagsIndex::load(&base_path.join("tags.json")).unwrap(),
            _phantom: PhantomData::default()
        }
    }
}