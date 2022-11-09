use std::ops::AddAssign;
use std::path::Path;
use std::time::Duration;

use crate::metric::common::{PrimaryTagMetric, PrimaryTagsStorage};
use crate::metric::metric_operations::{MetricWindowing, TimeRangeStatistics};
use crate::metric::operations::{StreamingApproxPercentile, StreamingAverage, StreamingConvert, StreamingMax, StreamingOperation, StreamingRatioValue, StreamingSum, StreamingFilterOperation};
use crate::metric::{metric_operations, OperationResult};
use crate::metric::tags::{PrimaryTag, Tag, TagsFilter};
use crate::model::{Datapoint, MetricError, MetricResult, Query, Time, TIME_SCALE};
use crate::storage::file::FileMetricStorage;
use crate::storage::MetricStorage;
use crate::traits::MinMax;

pub type DefaultRatioMetric = RatioMetric<FileMetricStorage<RatioU32>>;

pub struct RatioMetric<TStorage: MetricStorage<RatioU32>> {
    primary_tags_storage: PrimaryTagsStorage<TStorage, RatioU32>
}

macro_rules! apply_operation {
    ($self:expr, $T:ident, $query:expr, $create:expr, $require_stats:expr) => {
        {
           match &$query.input_filter {
                Some(filter) => {
                    let filter = filter.clone();
                    $self.operation($query, |stats| StreamingFilterOperation::<Ratio, $T>::new(filter.clone(), $create(stats)), $require_stats)
                }
                None => {
                    $self.operation($query, |stats| $create(stats), $require_stats)
                }
            }
        }
    };
}

macro_rules! apply_operation_in_window {
    ($self:expr, $T:ident, $query:expr, $duration:expr, $create:expr, $require_stats:expr) => {
        {
           match &$query.input_filter {
                Some(filter) => {
                    let filter = filter.clone();
                    $self.operation_in_window($query, $duration, |stats| StreamingFilterOperation::<Ratio, $T>::new(filter.clone(), $create(stats)), $require_stats)
                }
                None => {
                    $self.operation_in_window($query, $duration, |stats| $create(stats), $require_stats)
                }
            }
        }
    };
}

fn ratio_sum() -> impl StreamingOperation<Ratio, f64> {
    StreamingConvert::<Ratio, f64, _, _>::new(StreamingSum::<Ratio>::default(), |x| x.value().unwrap())
}

impl<TStorage: MetricStorage<RatioU32>> RatioMetric<TStorage> {
    pub fn new(base_path: &Path) -> MetricResult<RatioMetric<TStorage>> {
        Ok(
            RatioMetric {
                primary_tags_storage: PrimaryTagsStorage::new(base_path)?
            }
        )
    }

    pub fn from_existing(base_path: &Path) -> MetricResult<RatioMetric<TStorage>> {
        Ok(
            RatioMetric {
                primary_tags_storage: PrimaryTagsStorage::from_existing(base_path)?
            }
        )
    }

    pub fn stats(&self) {
        self.primary_tags_storage.stats();
    }

    pub fn primary_tags(&self) -> impl Iterator<Item=&PrimaryTag> {
        self.primary_tags_storage.primary_tags()
    }

    pub fn add_primary_tag(&mut self, tag: PrimaryTag) -> MetricResult<()> {
        self.primary_tags_storage.add_primary_tag(tag)
    }

    pub fn add_auto_primary_tag(&mut self, key: &str) -> MetricResult<()> {
        self.primary_tags_storage.add_auto_primary_tag(key)
    }

    pub fn add(&mut self, time: f64, numerator: u16, denominator: u16, mut tags: Vec<Tag>) -> MetricResult<()> {
        let (primary_tag_key, mut primary_tag, secondary_tags) = self.primary_tags_storage.insert_tags(&mut tags)?;

        let add = |primary_tag: &mut PrimaryTagMetric<TStorage, RatioU32>| {
            let time = (time * TIME_SCALE as f64).round() as Time;
            let value = RatioU32(numerator as u32, denominator as u32);

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

    pub fn average(&self, query: Query) -> OperationResult {
        type Op = StreamingRatioValue<StreamingAverage<f64>>;
        apply_operation!(self, Op, query, |_| Op::from_default(), false)
    }

    pub fn sum(&self, query: Query) -> OperationResult {
        self.operation(query, |_| ratio_sum(), false)
    }

    pub fn max(&self, query: Query) -> OperationResult {
        type Op = StreamingRatioValue<StreamingMax<f64>>;
        apply_operation!(self, Op, query, |_| Op::from_default(), false)
    }

    pub fn percentile(&self, query: Query, percentile: i32) -> OperationResult {
        let create = |stats: Option<&TimeRangeStatistics<RatioU32>>| {
            let stats = stats.unwrap();
            let min = stats.min().value().unwrap_or(0.0);
            let max = stats.max().value().unwrap_or(1.0);
            let stats = TimeRangeStatistics::new(stats.count, min, max);
            StreamingRatioValue::new(StreamingApproxPercentile::from_stats(&stats, percentile))
        };

        type Op = StreamingRatioValue<StreamingApproxPercentile>;
        apply_operation!(self, Op, query, create, true)
    }

    fn operation<T: StreamingOperation<Ratio, f64>, F: Fn(Option<&TimeRangeStatistics<RatioU32>>) -> T>(&self,
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
                            streaming_operation.add(datapoint.value.to_u64());
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

    pub fn average_in_window(&self, query: Query, duration: Duration) -> OperationResult {
        type Op = StreamingRatioValue<StreamingAverage<f64>>;
        apply_operation_in_window!(self, Op, query, duration, |_| Op::from_default(), false)
    }

    pub fn sum_in_window(&self, query: Query, duration: Duration) -> OperationResult {
        self.operation_in_window(query, duration, |_| ratio_sum(), false)
    }

    pub fn max_in_window(&self, query: Query, duration: Duration) -> OperationResult {
        type Op = StreamingRatioValue<StreamingMax<f64>>;
        apply_operation_in_window!(self, Op, query, duration, |_| Op::from_default(), false)
    }

    pub fn percentile_in_window(&self, query: Query, duration: Duration, percentile: i32) -> OperationResult {
        let create = |stats: Option<&TimeRangeStatistics<Ratio>>| {
            let stats = stats.unwrap();
            let min = stats.min().value().unwrap_or(0.0);
            let max = stats.max().value().unwrap_or(1.0);
            let stats = TimeRangeStatistics::new(stats.count, min, max);
            StreamingRatioValue::new(StreamingApproxPercentile::from_stats(&stats, percentile))
        };

        type Op = StreamingRatioValue<StreamingApproxPercentile>;
        apply_operation_in_window!(self, Op, query, duration, create, true)
    }

    fn operation_in_window<T: StreamingOperation<Ratio, f64>, F: Fn(Option<&TimeRangeStatistics<Ratio>>) -> T>(&self,
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
                                        .handle(datapoint.value.to_u64());
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
                                    .add(datapoint.value.to_u64());
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
                |value| query.apply_output_transform(value?)
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

    pub fn scheduled(&mut self) {
        self.primary_tags_storage.scheduled();
    }
}

#[derive(Debug, Copy, Clone, Default)]
pub struct Ratio(u64, u64);

impl Ratio {
    pub fn value(&self) -> Option<f64> {
        if self.1 == 0 {
            return None;
        }

        Some(self.0 as f64 / self.1 as f64)
    }
}

impl AddAssign for Ratio {
    fn add_assign(&mut self, rhs: Self) {
        self.0 += rhs.0;
        self.1 += rhs.1;
    }
}

impl MinMax for Ratio {
    fn min(&self, other: Self) -> Self {
        // Ratio(self.0.min(other.0), self.1.min(other.1))
        if self.value() < other.value() {
            *self
        } else {
            other
        }
    }

    fn max(&self, other: Self) -> Self {
        // Ratio(self.0.max(other.0), self.1.max(other.1))
        if self.value() > other.value() {
            *self
        } else {
            other
        }
    }
}

#[derive(Debug, Copy, Clone, Default)]
pub struct RatioU32(u32, u32);

impl RatioU32 {
    pub fn to_u64(&self) -> Ratio {
        Ratio(self.0 as u64, self.1 as u64)
    }

    pub fn value(&self) -> Option<f64> {
        if self.1 == 0 {
            return None;
        }

        Some(self.0 as f64 / self.1 as f64)
    }
}

impl AddAssign for RatioU32 {
    fn add_assign(&mut self, rhs: Self) {
        self.0 += rhs.0;
        self.1 += rhs.1;
    }
}

impl MinMax for RatioU32 {
    fn min(&self, other: Self) -> Self {
        // RatioU32(self.0.min(other.0), self.1.min(other.1))
        if self.value() < other.value() {
            *self
        } else {
            other
        }
    }

    fn max(&self, other: Self) -> Self {
        // RatioU32(self.0.max(other.0), self.1.max(other.1))
        if self.value() > other.value() {
            *self
        } else {
            other
        }
    }
}