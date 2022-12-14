use std::ops::AddAssign;
use std::path::Path;
use std::time::Duration;

use serde::{Serialize, Deserialize};

use crate::metric::common::{CountInput, GenericMetric, MetricType, PrimaryTagsStorage, MetricConfig};
use crate::metric::helpers::{MetricWindowing, TimeRangeStatistics};
use crate::metric::operations::{StreamingAverage, StreamingConvert, StreamingMax, StreamingOperation, StreamingRatioValue, StreamingSum, StreamingFilterOperation, StreamingMin, StreamingApproxPercentileTDigest};
use crate::metric::{helpers, OperationResult};
use crate::metric::expression::ExpressionValue;
use crate::metric::tags::{PrimaryTag, Tag, TagsFilter};
use crate::model::{MetricResult, Query, Time, TIME_SCALE};
use crate::storage::file::FileMetricStorage;
use crate::storage::MetricStorage;
use crate::traits::{MinMax, ToExpressionValue};

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
                    $self.operation($query, |stats| StreamingFilterOperation::<Ratio, ExpressionValue, $T>::new(filter.clone(), $create(stats)), $require_stats)
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
                    $self.operation_in_window($query, $duration, |stats| StreamingFilterOperation::<Ratio, ExpressionValue, $T>::new(filter.clone(), $create(stats)), $require_stats)
                }
                None => {
                    $self.operation_in_window($query, $duration, |stats| $create(stats), $require_stats)
                }
            }
        }
    };
}

fn ratio_sum() -> impl StreamingOperation<Ratio, ExpressionValue> {
    StreamingConvert::<_, _, _, _>::new(StreamingSum::<Ratio>::default(), |x| ExpressionValue::Ratio(x))
}

impl<TStorage: MetricStorage<RatioU32>> RatioMetric<TStorage> {
    pub fn new(base_path: &Path) -> MetricResult<RatioMetric<TStorage>> {
        Ok(
            RatioMetric {
                primary_tags_storage: PrimaryTagsStorage::new(base_path, MetricType::Ratio)?
            }
        )
    }

    pub fn with_config(base_path: &Path, config: MetricConfig) -> MetricResult<RatioMetric<TStorage>> {
        Ok(
            RatioMetric {
                primary_tags_storage: PrimaryTagsStorage::with_config(base_path, config)?
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

    pub fn primary_tags(&self) -> impl Iterator<Item=&PrimaryTag> {
        self.primary_tags_storage.primary_tags()
    }

    fn operation<T: StreamingOperation<Ratio, ExpressionValue>, F: Fn(Option<&TimeRangeStatistics<RatioU32>>) -> T>(&self,
                                                                                                                    query: Query,
                                                                                                                    create_op: F,
                                                                                                                    require_statistics: bool) -> OperationResult {
        let (start_time, end_time) = query.time_range.int_range();
        assert!(end_time > start_time);

        let apply = |tags_filter: &TagsFilter| {
            let mut streaming_operations = Vec::new();
            for (primary_tag, tags_filter) in self.primary_tags_storage.iter_for_query(tags_filter) {
                let storage = primary_tag.storage(None);
                if let Some(start_block_index) = helpers::find_block_index(storage, start_time) {
                    let stats = if require_statistics {
                        Some(
                            helpers::determine_statistics_for_time_range(
                                storage,
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
                    helpers::visit_datapoints_in_time_range(
                        storage,
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

            let streaming_operation = helpers::merge_operations(streaming_operations);
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

    fn operation_in_window<T: StreamingOperation<Ratio, ExpressionValue>, F: Fn(Option<&TimeRangeStatistics<Ratio>>) -> T>(&self,
                                                                                                                           query: Query,
                                                                                                                           duration: Duration,
                                                                                                                           create_op: F,
                                                                                                                           require_statistics: bool) -> OperationResult {
        let (start_time, end_time) = query.time_range.int_range();
        assert!(end_time > start_time);

        let duration = (duration.as_secs_f64() * TIME_SCALE as f64) as Time;

        let apply = |tags_filter: &TagsFilter| {
            let mut primary_tags_windowing = Vec::new();
            for (primary_tag, tags_filter) in self.primary_tags_storage.iter_for_query(tags_filter) {
                let storage = primary_tag.storage(None);
                if let Some(start_block_index) = helpers::find_block_index(storage, start_time) {
                    let mut windowing = MetricWindowing::new(start_time, end_time, duration);

                    let window_stats = if require_statistics {
                        let mut window_stats = windowing.create_windows(|| None);

                        helpers::visit_datapoints_in_time_range(
                            storage,
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

                    helpers::visit_datapoints_in_time_range(
                        storage,
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

            helpers::extract_operations_in_windows(
                helpers::merge_windowing(primary_tags_windowing),
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

impl<TStorage: MetricStorage<RatioU32>> GenericMetric for RatioMetric<TStorage> {
    fn stats(&self) {
        self.primary_tags_storage.stats();
    }

    fn add_primary_tag(&mut self, tag: PrimaryTag) -> MetricResult<()> {
        self.primary_tags_storage.add_primary_tag(tag)
    }

    fn add_auto_primary_tag(&mut self, key: &str) -> MetricResult<()> {
        self.primary_tags_storage.add_auto_primary_tag(key)
    }

    type Input = RatioInput;
    fn add(&mut self, time: f64, value: RatioInput, mut tags: Vec<Tag>) -> MetricResult<()> {
        let (primary_tag_key, mut primary_tag, secondary_tags) = self.primary_tags_storage.insert_tags(&mut tags)?;

        let result = primary_tag.add(
            time,
            value.value()?,
            secondary_tags,
            |last_datapoint, value| {
                last_datapoint.value += value;
            }
        );

        self.primary_tags_storage.return_tags(primary_tag_key, primary_tag);
        result
    }

    fn average(&self, query: Query) -> OperationResult {
        type Op = StreamingRatioValue<StreamingAverage<f64>>;
        apply_operation!(self, Op, query, |_| Op::from_default(), false)
    }

    fn sum(&self, query: Query) -> OperationResult {
        self.operation(query, |_| ratio_sum(), false)
    }

    fn max(&self, query: Query) -> OperationResult {
        type Op = StreamingRatioValue<StreamingMax<f64>>;
        apply_operation!(self, Op, query, |_| Op::from_default(), false)
    }

    fn min(&self, query: Query) -> OperationResult {
        type Op = StreamingRatioValue<StreamingMin<f64>>;
        apply_operation!(self, Op, query, |_| Op::from_default(), false)
    }

    fn percentile(&self, query: Query, percentile: i32) -> OperationResult {
        let create = |_: Option<&TimeRangeStatistics<RatioU32>>| {
            StreamingRatioValue::new(StreamingApproxPercentileTDigest::new(percentile))
        };

        type Op = StreamingRatioValue<StreamingApproxPercentileTDigest>;
        apply_operation!(self, Op, query, create, true)
    }

    fn average_in_window(&self, query: Query, duration: Duration) -> OperationResult {
        type Op = StreamingRatioValue<StreamingAverage<f64>>;
        apply_operation_in_window!(self, Op, query, duration, |_| Op::from_default(), false)
    }

    fn sum_in_window(&self, query: Query, duration: Duration) -> OperationResult {
        self.operation_in_window(query, duration, |_| ratio_sum(), false)
    }

    fn max_in_window(&self, query: Query, duration: Duration) -> OperationResult {
        type Op = StreamingRatioValue<StreamingMax<f64>>;
        apply_operation_in_window!(self, Op, query, duration, |_| Op::from_default(), false)
    }

    fn min_in_window(&self, query: Query, duration: Duration) -> OperationResult {
        type Op = StreamingRatioValue<StreamingMin<f64>>;
        apply_operation_in_window!(self, Op, query, duration, |_| Op::from_default(), false)
    }

    fn percentile_in_window(&self, query: Query, duration: Duration, percentile: i32) -> OperationResult {
        let create = |_: Option<&TimeRangeStatistics<Ratio>>| {
            StreamingRatioValue::new(StreamingApproxPercentileTDigest::new(percentile))
        };

        type Op = StreamingRatioValue<StreamingApproxPercentileTDigest>;
        apply_operation_in_window!(self, Op, query, duration, create, true)
    }

    fn scheduled(&mut self) {
        self.primary_tags_storage.scheduled();
    }
}

#[derive(Debug, Copy, Clone, Default)]
pub struct Ratio(u64, u64);

impl Ratio {
    pub fn numerator(&self) -> u64 {
        self.0
    }

    pub fn denominator(&self) -> u64 {
        self.1
    }

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
        if self.value() < other.value() {
            *self
        } else {
            other
        }
    }

    fn max(&self, other: Self) -> Self {
        if self.value() > other.value() {
            *self
        } else {
            other
        }
    }
}

impl ToExpressionValue for Ratio {
    fn to_value(&self) -> ExpressionValue {
        ExpressionValue::Ratio(*self)
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
        if self.value() < other.value() {
            *self
        } else {
            other
        }
    }

    fn max(&self, other: Self) -> Self {
        if self.value() > other.value() {
            *self
        } else {
            other
        }
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct RatioInput(pub CountInput, pub CountInput);

impl RatioInput {
    pub fn value(&self) -> MetricResult<RatioU32> {
        Ok(RatioU32(self.0.value()?, self.1.value()?))
    }
}