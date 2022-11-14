use std::path::Path;
use std::time::Duration;

use crate::metric::common::{CountInput, GenericMetric, PrimaryTagMetric, PrimaryTagsStorage};
use crate::metric::metric_operations::{MetricWindowing};
use crate::metric::operations::{StreamingConvert, StreamingOperation, StreamingSum, StreamingTimeAverage};
use crate::metric::{metric_operations, OperationResult};
use crate::metric::tags::{PrimaryTag, Tag, TagsFilter};
use crate::model::{Datapoint, MetricError, MetricResult, Query, Time, TIME_SCALE, TimeRange};
use crate::storage::file::FileMetricStorage;
use crate::storage::MetricStorage;

pub type DefaultCountMetric = CountMetric<FileMetricStorage<u32>>;

pub struct CountMetric<TStorage: MetricStorage<u32>> {
    primary_tags_storage: PrimaryTagsStorage<TStorage, u32>
}

impl<TStorage: MetricStorage<u32>> CountMetric<TStorage> {
    pub fn new(base_path: &Path) -> MetricResult<CountMetric<TStorage>> {
        Ok(
            CountMetric {
                primary_tags_storage: PrimaryTagsStorage::new(base_path)?
            }
        )
    }

    pub fn from_existing(base_path: &Path) -> MetricResult<CountMetric<TStorage>> {
        Ok(
            CountMetric {
                primary_tags_storage: PrimaryTagsStorage::from_existing(base_path)?
            }
        )
    }

    pub fn primary_tags(&self) -> impl Iterator<Item=&PrimaryTag> {
        self.primary_tags_storage.primary_tags()
    }

    fn operation<T: StreamingOperation<u64, f64>, F: Fn() -> T>(&self, query: Query, create_op: F) -> OperationResult {
        let (start_time, end_time) = query.time_range.int_range();
        assert!(end_time > start_time);

        let apply = |tags_filter: &TagsFilter| {
            let mut streaming_operations = Vec::new();
            for (primary_tag, tags_filter) in self.primary_tags_storage.iter_for_query(tags_filter) {
                if let Some(start_block_index) = metric_operations::find_block_index(&primary_tag.storage, start_time) {
                    let mut streaming_operation = create_op();
                    metric_operations::visit_datapoints_in_time_range(
                        &primary_tag.storage,
                        start_time,
                        end_time,
                        tags_filter,
                        start_block_index,
                        false,
                        |_, _, datapoint| {
                            streaming_operation.add(datapoint.value as u64);
                        }
                    );

                    streaming_operations.push(streaming_operation);
                }
            }

            if streaming_operations.is_empty() {
                return None;
            }

            let streaming_operation = metric_operations::merge_operations(streaming_operations);
            query.apply_output_transform(streaming_operation.value()?)
        };

        match &query.group_by {
            None => {
                OperationResult::Value(apply(&query.tags_filter))
            }
            Some(key) => {
                OperationResult::GroupValues(self.primary_tags_storage.apply_group_by(&query, key, apply))
            }
        }
    }

    fn operation_in_window<T: StreamingOperation<u64, f64>, F: Fn(f64, f64) -> T>(&self,
                                                                                  query: Query,
                                                                                  duration: Duration,
                                                                                  create_op: F) -> OperationResult {
        let (start_time, end_time) = query.time_range.int_range();
        assert!(end_time > start_time);

        let duration = (duration.as_secs_f64() * TIME_SCALE as f64) as u64;

        let apply = |tags_filter: &TagsFilter| {
            let mut primary_tags_windowing = Vec::new();
            for (primary_tag, tags_filter) in self.primary_tags_storage.iter_for_query(tags_filter) {
                if let Some(start_block_index) = metric_operations::find_block_index(&primary_tag.storage, start_time) {
                    let mut windowing = MetricWindowing::new(start_time, end_time, duration);

                    metric_operations::visit_datapoints_in_time_range(
                        &primary_tag.storage,
                        start_time,
                        end_time,
                        tags_filter,
                        start_block_index,
                        false,
                        |_, datapoint_time, datapoint| {
                            let window_index = windowing.get_window_index(datapoint_time);
                            if window_index < windowing.len() {
                                windowing.get(window_index)
                                    .get_or_insert_with(|| {
                                        create_op(
                                            (datapoint_time / TIME_SCALE) as f64,
                                            ((datapoint_time + duration) / TIME_SCALE) as f64
                                        )
                                    })
                                    .add(datapoint.value as u64);
                            }
                        }
                    );

                    primary_tags_windowing.push(windowing);
                }
            }

            if primary_tags_windowing.is_empty() {
                return Vec::new();
            }

            metric_operations::extract_operations_in_windows(
                metric_operations::merge_windowing(primary_tags_windowing),
                |value| query.apply_output_transform(value?),
                query.remove_empty_datapoints
            )
        };

        match &query.group_by {
            None => {
                OperationResult::TimeValues(apply(&query.tags_filter))
            }
            Some(key) => {
                OperationResult::GroupTimeValues(self.primary_tags_storage.apply_group_by(&query, key, apply))
            }
        }
    }
}

impl<TStorage: MetricStorage<u32>> GenericMetric for CountMetric<TStorage> {
    fn stats(&self) {
        self.primary_tags_storage.stats();
    }

    fn add_primary_tag(&mut self, tag: PrimaryTag) -> MetricResult<()> {
        self.primary_tags_storage.add_primary_tag(tag)
    }

    fn add_auto_primary_tag(&mut self, key: &str) -> MetricResult<()> {
        self.primary_tags_storage.add_auto_primary_tag(key)
    }

    type Input = CountInput;
    fn add(&mut self, time: f64, count: CountInput, mut tags: Vec<Tag>) -> MetricResult<()> {
        let (primary_tag_key, mut primary_tag, secondary_tags) = self.primary_tags_storage.insert_tags(&mut tags)?;

        let add = |primary_tag: &mut PrimaryTagMetric<TStorage, u32>| {
            let time = (time * TIME_SCALE as f64).round() as Time;
            let value = count.value()?;

            let mut datapoint = Datapoint {
                time_offset: 0,
                value
            };

            if let Some((block_start_time, block_end_time)) = primary_tag.storage.active_block_time_range() {
                if time < block_end_time {
                    return Err(MetricError::InvalidTimeOrder);
                }

                let time_offset = time - block_start_time;
                if time_offset < primary_tag.storage.block_duration() {
                    assert!(time_offset < u32::MAX as u64);
                    datapoint.time_offset = time_offset as u32;

                    let datapoint_duration = primary_tag.storage.datapoint_duration();
                    if let Some(last_datapoint) = primary_tag.storage.last_datapoint_mut(secondary_tags) {
                        if (time - (block_start_time + last_datapoint.time_offset as u64)) < datapoint_duration {
                            last_datapoint.value += value;
                            return Ok(());
                        }
                    }

                    primary_tag.storage.add_datapoint(secondary_tags, datapoint)?;
                } else {
                    primary_tag.storage.create_block_with_datapoint(time, secondary_tags, datapoint)?;
                }
            } else {
                primary_tag.storage.create_block_with_datapoint(time, secondary_tags, datapoint)?;
            }

            Ok(())
        };

        let result = add(&mut primary_tag);
        self.primary_tags_storage.return_tags(primary_tag_key, primary_tag);
        result
    }

    fn sum(&self, query: Query) -> OperationResult {
        if query.input_filter.is_some() || query.input_transform.is_some() {
            return OperationResult::NotSupported;
        }

        self.operation(query, || StreamingConvert::<u64, f64, _, _>::new(StreamingSum::<u64>::default(), |x| x as f64))
    }

    fn average(&self, query: Query) -> OperationResult {
        if query.input_filter.is_some() || query.input_transform.is_some() {
            return OperationResult::NotSupported;
        }

        self.operation(query.clone(), || StreamingTimeAverage::<u64>::new(query.time_range))
    }

    fn max(&self, _query: Query) -> OperationResult {
        OperationResult::NotSupported
    }

    fn min(&self, _query: Query) -> OperationResult {
        OperationResult::NotSupported
    }

    fn percentile(&self, _query: Query, _percentile: i32) -> OperationResult {
        OperationResult::NotSupported
    }

    fn sum_in_window(&self, query: Query, duration: Duration) -> OperationResult {
        if query.input_filter.is_some() || query.input_transform.is_some() {
            return OperationResult::NotSupported;
        }

        self.operation_in_window(query, duration, |_, _| StreamingConvert::<u64, f64, _, _>::new(StreamingSum::<u64>::default(), |x| x as f64))
    }

    fn average_in_window(&self, query: Query, duration: Duration) -> OperationResult {
        if query.input_filter.is_some() || query.input_transform.is_some() {
            return OperationResult::NotSupported;
        }

        self.operation_in_window(query, duration, |start, end| StreamingTimeAverage::new(TimeRange::new(start, end)))
    }

    fn max_in_window(&self, _query: Query, _duration: Duration) -> OperationResult {
        OperationResult::NotSupported
    }

    fn min_in_window(&self, _query: Query, _duration: Duration) -> OperationResult {
        OperationResult::NotSupported
    }

    fn percentile_in_window(&self, _query: Query, _duration: Duration, _percentile: i32) -> OperationResult {
        OperationResult::NotSupported
    }

    fn scheduled(&mut self) {
        self.primary_tags_storage.scheduled();
    }
}