use std::path::Path;
use std::time::Duration;

use crate::metric::common::{PrimaryTagsStorage};
use crate::metric::metric_operations::{MetricWindowing};
use crate::metric::operations::{StreamingConvert, StreamingOperation, StreamingSum, StreamingTimeAverage};
use crate::{PrimaryTag, Query, TimeRange};
use crate::metric::metric_operations;
use crate::model::{Datapoint, MetricError, MetricResult, Time, TIME_SCALE};
use crate::storage::file::MetricStorageFile;
use crate::storage::MetricStorage;

pub type DefaultCountMetric = CountMetric<MetricStorageFile<u32>>;

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

    pub fn stats(&self) {
        self.primary_tags_storage.stats();
    }

    pub fn add_primary_tag(&mut self, tag: PrimaryTag) -> MetricResult<()> {
        self.primary_tags_storage.add_primary_tag(tag)
    }

    pub fn add(&mut self, time: f64, count: u16, tags: &[&str]) -> MetricResult<()> {
        let mut tags = tags.into_iter().cloned().collect::<Vec<_>>();
        let (primary_tag_key, mut primary_tag, secondary_tags) = self.primary_tags_storage.insert_tags(&mut tags)?;

        let time = (time * TIME_SCALE as f64).round() as Time;
        let value = count as u32;

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
                        self.primary_tags_storage.return_tags(primary_tag_key, primary_tag);
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

        self.primary_tags_storage.return_tags(primary_tag_key, primary_tag);
        Ok(())
    }

    pub fn sum(&self, query: Query) -> Option<f64> {
        assert!(query.input_transform.is_none(), "Input transform not supported.");
        self.operation(query, || StreamingConvert::<u64, f64, _, _>::new(StreamingSum::<u64>::default(), |x| x as f64))
    }

    pub fn average(&self, query: Query) -> Option<f64> {
        assert!(query.input_transform.is_none(), "Input transform not supported.");
        self.operation(query.clone(), || StreamingTimeAverage::<u64>::new(query.time_range))
    }

    fn operation<T: StreamingOperation<u64, f64>, F: Fn() -> T>(&self, query: Query, create_op: F) -> Option<f64> {
        let (start_time, end_time) = query.time_range.int_range();
        assert!(end_time > start_time);

        let mut streaming_operations = Vec::new();
        for (primary_tag_key, primary_tag) in self.primary_tags_storage.iter() {
            if let Some(tags_filter) = query.tags_filter.apply(&primary_tag.tags_index, primary_tag_key) {
                if let Some(start_block_index) = metric_operations::find_block_index(&primary_tag.storage, start_time) {
                    let mut streaming_operation = create_op();
                    metric_operations::visit_datapoints_in_time_range(
                        &primary_tag.storage,
                        start_time,
                        end_time,
                        tags_filter,
                        start_block_index,
                        false,
                        |_, datapoint| {
                            streaming_operation.add(datapoint.value as u64);
                        }
                    );

                    streaming_operations.push(streaming_operation);
                }
            }
        }

        if streaming_operations.is_empty() {
            return None;
        }

        let streaming_operation = metric_operations::merge_operations(streaming_operations);
        match query.output_transform {
            Some(operation) => operation.apply(streaming_operation.value()? as f64),
            None => streaming_operation.value().map(|x| x as f64)
        }
    }

    pub fn sum_in_window(&self, query: Query, duration: Duration) -> Vec<(f64, f64)> {
        assert!(query.input_transform.is_none(), "Input transform not supported.");
        self.operation_in_window(query, duration, |_, _| StreamingConvert::<u64, f64, _, _>::new(StreamingSum::<u64>::default(), |x| x as f64))
    }

    pub fn average_in_window(&self, query: Query, duration: Duration) -> Vec<(f64, f64)> {
        assert!(query.input_transform.is_none(), "Input transform not supported.");
        self.operation_in_window(query, duration, |start, end| StreamingTimeAverage::new(TimeRange::new(start, end)))
    }

    fn operation_in_window<T: StreamingOperation<u64, f64>, F: Fn(f64, f64) -> T>(&self,
                                                                                  query: Query,
                                                                                  duration: Duration,
                                                                                  create_op: F) -> Vec<(f64, f64)> {
        let (start_time, end_time) = query.time_range.int_range();
        assert!(end_time > start_time);

        let duration = (duration.as_secs_f64() * TIME_SCALE as f64) as u64;

        let mut primary_tags_windowing = Vec::new();
        for (primary_tag_value, primary_tag) in self.primary_tags_storage.iter() {
            if let Some(tags_filter) = query.tags_filter.apply(&primary_tag.tags_index, primary_tag_value) {
                if let Some(start_block_index) = metric_operations::find_block_index(&primary_tag.storage, start_time) {
                    let mut windowing = MetricWindowing::new(start_time, end_time, duration);

                    metric_operations::visit_datapoints_in_time_range(
                        &primary_tag.storage,
                        start_time,
                        end_time,
                        tags_filter,
                        start_block_index,
                        false,
                        |datapoint_time, datapoint| {
                            let window_index = windowing.get_window_index(datapoint_time);
                            windowing.get(window_index)
                                .get_or_insert_with(|| { create_op((datapoint_time / TIME_SCALE) as f64, ((datapoint_time + duration) / TIME_SCALE) as f64) })
                                .add(datapoint.value as u64);
                        }
                    );

                    primary_tags_windowing.push(windowing);
                }
            }
        }

        if primary_tags_windowing.is_empty() {
            return Vec::new();
        }

        metric_operations::extract_operations_in_windows(
            metric_operations::merge_windowing(primary_tags_windowing),
            |value| {
                let value = value?;

                match query.output_transform {
                    Some(operation) => operation.apply(value),
                    None => Some(value)
                }
            }
        )
    }
}
