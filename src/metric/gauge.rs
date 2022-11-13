use std::path::Path;
use std::time::Duration;

use crate::metric::common::{GenericMetric, PrimaryTagMetric, PrimaryTagsStorage};
use crate::metric::metric_operations::{MetricWindowing, TimeRangeStatistics};
use crate::metric::operations::{StreamingApproxPercentile, StreamingAverage, StreamingFilterOperation, StreamingMax, StreamingOperation, StreamingSum, StreamingTransformOperation};
use crate::metric::{metric_operations, OperationResult};
use crate::metric::tags::{PrimaryTag, Tag, TagsFilter};
use crate::model::{Datapoint, MetricError, MetricResult, Query, Time, TIME_SCALE};
use crate::storage::file::FileMetricStorage;
use crate::storage::MetricStorage;

pub type DefaultGaugeMetric = GaugeMetric<FileMetricStorage<f32>>;

pub struct GaugeMetric<TStorage: MetricStorage<f32>> {
    primary_tags_storage: PrimaryTagsStorage<TStorage, f32>
}

macro_rules! apply_operation {
    ($self:expr, $T:ident, $query:expr, $create:expr, $require_stats:expr) => {
        {
           match (&$query.input_filter, &$query.input_transform) {
                (Some(filter), Some(transform)) => {
                    let filter = filter.clone();
                    let transform = transform.clone();
                    $self.operation(
                        $query,
                        |stats| StreamingFilterOperation::<f64, _>::new(filter.clone(), StreamingTransformOperation::<$T>::new(transform.clone(), $create(stats))),
                        $require_stats
                    )
                }
                (Some(filter), None) => {
                    let filter = filter.clone();
                    $self.operation($query, |stats| StreamingFilterOperation::<f64, $T>::new(filter.clone(), $create(stats)), $require_stats)
                }
                (None, Some(transform)) => {
                    let transform = transform.clone();
                    $self.operation($query, |stats| StreamingTransformOperation::<$T>::new(transform.clone(), $create(stats)), $require_stats)
                }
                (None, None) => {
                    $self.operation($query, |stats| $create(stats), $require_stats)
                }
            }
        }
    };
}

macro_rules! apply_operation_in_window {
    ($self:expr, $T:ident, $query:expr, $duration:expr, $create:expr, $require_stats:expr) => {
        {
           match (&$query.input_filter, &$query.input_transform) {
                (Some(filter), Some(transform)) => {
                    let filter = filter.clone();
                    let transform = transform.clone();
                    $self.operation_in_window(
                        $query,
                        $duration,
                        |stats| StreamingFilterOperation::<f64, _>::new(filter.clone(), StreamingTransformOperation::<$T>::new(transform.clone(), $create(stats))),
                        $require_stats
                    )
                }
                (Some(filter), None) => {
                    let filter = filter.clone();
                    $self.operation_in_window($query, $duration, |stats| StreamingFilterOperation::<f64, $T>::new(filter.clone(), $create(stats)), $require_stats)
                }
                (None, Some(transform)) => {
                    let transform = transform.clone();
                    $self.operation_in_window($query, $duration, |stats| StreamingTransformOperation::<$T>::new(transform.clone(), $create(stats)), $require_stats)
                }
                (None, None) => {
                    $self.operation_in_window($query, $duration, |stats| $create(stats), $require_stats)
                }
            }
        }
    };
}

impl<TStorage: MetricStorage<f32>> GaugeMetric<TStorage> {
    pub fn new(base_path: &Path) -> MetricResult<GaugeMetric<TStorage>> {
        Ok(
            GaugeMetric {
                primary_tags_storage: PrimaryTagsStorage::new(base_path)?
            }
        )
    }

    pub fn from_existing(base_path: &Path) -> MetricResult<GaugeMetric<TStorage>> {
        Ok(
            GaugeMetric {
                primary_tags_storage: PrimaryTagsStorage::from_existing(base_path)?
            }
        )
    }

    pub fn primary_tags(&self) -> impl Iterator<Item=&PrimaryTag> {
        self.primary_tags_storage.primary_tags()
    }

    fn simple_operation<T: StreamingOperation<f64> + Default>(&self, query: Query) -> OperationResult {
        apply_operation!(self, T, query, |_| T::default(), false)
    }

    fn operation<T: StreamingOperation<f64>, F: Fn(Option<&TimeRangeStatistics<f32>>) -> T>(&self,
                                                                                            query: Query,
                                                                                            create_op: F,
                                                                                            require_statistics: bool) -> OperationResult {
        let (start_time, end_time) = query.time_range.int_range();
        assert!(end_time > start_time);

        let apply = |tags_filter: &TagsFilter| {
            let mut streaming_operations = Vec::new();
            for (primary_tag, tags_filter) in self.primary_tags_storage.iter_for_query(tags_filter) {
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
                        |_, _, datapoint| {
                            streaming_operation.add(datapoint.value as f64);
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

    pub fn simple_operation_in_window<T: StreamingOperation<f64> + Default>(&self, query: Query, duration: Duration) -> OperationResult {
        apply_operation_in_window!(self, T, query, duration, |_| T::default(), false)
    }

    fn operation_in_window<T: StreamingOperation<f64>, F: Fn(Option<&TimeRangeStatistics<f64>>) -> T>(&self,
                                                                                                      query: Query,
                                                                                                      duration: Duration,
                                                                                                      create_op: F,
                                                                                                      require_statistics: bool) -> OperationResult {
        let (start_time, end_time) = query.time_range.int_range();
        assert!(end_time > start_time);

        let duration = (duration.as_secs_f64() * TIME_SCALE as f64) as u64;

        let apply = |tags_filter: &TagsFilter| {
            let mut primary_tags_windowing = Vec::new();
            for (primary_tag, tags_filter) in self.primary_tags_storage.iter_for_query(tags_filter) {
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
                            |_, datapoint_time, datapoint| {
                                let window_index = windowing.get_window_index(datapoint_time);
                                if window_index < windowing.len() {
                                    window_stats[window_index]
                                        .get_or_insert_with(|| TimeRangeStatistics::default())
                                        .handle(datapoint.value as f64);
                                }
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
                        |_, datapoint_time, datapoint| {
                            let window_index = windowing.get_window_index(datapoint_time);
                            if window_index < windowing.len() {
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

impl<TStorage: MetricStorage<f32>> GenericMetric for GaugeMetric<TStorage> {
    fn stats(&self) {
        self.primary_tags_storage.stats();
    }

    fn add_primary_tag(&mut self, tag: PrimaryTag) -> MetricResult<()> {
        self.primary_tags_storage.add_primary_tag(tag)
    }

    fn add_auto_primary_tag(&mut self, key: &str) -> MetricResult<()> {
        self.primary_tags_storage.add_auto_primary_tag(key)
    }

    type Input = f64;
    fn add(&mut self, time: f64, value: f64, mut tags: Vec<Tag>) -> MetricResult<()> {
        let (primary_tag_key, mut primary_tag, secondary_tags) = self.primary_tags_storage.insert_tags(&mut tags)?;

        let add = |primary_tag: &mut PrimaryTagMetric<TStorage, f32>| {
            let time = (time * TIME_SCALE as f64).round() as Time;
            let value = value as f32;

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
                            last_datapoint.value = value;
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

    fn average(&self, query: Query) -> OperationResult {
        self.simple_operation::<StreamingAverage<f64>>(query)
    }

    fn sum(&self, query: Query) -> OperationResult {
        self.simple_operation::<StreamingSum<f64>>(query)
    }

    fn max(&self, query: Query) -> OperationResult {
        self.simple_operation::<StreamingMax<f64>>(query)
    }

    fn percentile(&self, query: Query, percentile: i32) -> OperationResult {
        let create = |stats: Option<&TimeRangeStatistics<f32>>| {
            let stats = stats.unwrap();
            let stats = TimeRangeStatistics::new(stats.count, stats.min() as f64, stats.max() as f64);
            StreamingApproxPercentile::from_stats(&stats, percentile)
        };

        apply_operation!(self, StreamingApproxPercentile, query, create, true)
    }

    fn average_in_window(&self, query: Query, duration: Duration) -> OperationResult {
        self.simple_operation_in_window::<StreamingAverage<f64>>(query, duration)
    }

    fn sum_in_window(&self, query: Query, duration: Duration) -> OperationResult {
        self.simple_operation_in_window::<StreamingSum<f64>>(query, duration)
    }

    fn max_in_window(&self, query: Query, duration: Duration) -> OperationResult {
        self.simple_operation_in_window::<StreamingMax<f64>>(query, duration)
    }

    fn percentile_in_window(&self, query: Query, duration: Duration, percentile: i32) -> OperationResult {
        let create = |stats: Option<&TimeRangeStatistics<f64>>| {
            let stats = stats.unwrap();
            let stats = TimeRangeStatistics::new(stats.count, stats.min() as f64, stats.max() as f64);
            StreamingApproxPercentile::from_stats(&stats, percentile)
        };

        apply_operation_in_window!(self, StreamingApproxPercentile, query, duration, create, true)
    }

    fn scheduled(&mut self) {
        self.primary_tags_storage.scheduled();
    }
}