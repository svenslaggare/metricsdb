use crate::model::{Datapoint, Tags, Time, TIME_SCALE};
use crate::metric::operations::StreamingOperation;
use crate::metric::tags::SecondaryTagsFilter;
use crate::storage::MetricStorage;
use crate::traits::MinMax;

pub fn find_block_index<TStorage: MetricStorage<E>, E: Copy>(storage: &TStorage, time: Time) -> Option<usize> {
    if storage.len() == 0 {
        return None;
    }

    let mut lower = 0;
    let mut upper = storage.len() - 1;
    while lower <= upper {
        let middle = lower + (upper - lower) / 2;
        // println!("{}, {}, {}", lower, upper, middle);
        if let Some((_, middle_time)) = storage.block_time_range(middle) {
            if time > middle_time {
                lower = middle + 1;
            } else if time < middle_time {
                upper = middle - 1;
            } else {
                break;
            }
        } else {
            break;
        }
    }

    Some(lower)
}

pub fn visit_datapoints_in_time_range<TStorage: MetricStorage<E>, F: FnMut(&Tags, Time, &Datapoint<E>), E: Copy>(storage: &TStorage,
                                                                                                                 start_time: Time,
                                                                                                                 end_time: Time,
                                                                                                                 tags_filter: SecondaryTagsFilter,
                                                                                                                 start_block_index: usize,
                                                                                                                 strict_ordering: bool,
                                                                                                                 mut apply: F) {
    for block_index in start_block_index..storage.len() {
        let (block_start_time, block_end_time) = storage.block_time_range(block_index).unwrap();
        if block_end_time >= start_time {
            let mut outside_time_range = false;

            if let Some(iterator) = storage.block_datapoints(block_index) {
                let mut sub_blocks_iterators = Vec::new();

                for (tags, datapoints) in iterator {
                    if tags_filter.accept(tags) {
                        let mut iterator = DatapointIterator::new(
                            start_time,
                            end_time,
                            block_start_time,
                            datapoints.iter()
                        );

                        if strict_ordering {
                            if iterator.peek().is_none() {
                                if iterator.outside_time_range {
                                    outside_time_range = true;
                                }

                                continue;
                            }

                            sub_blocks_iterators.push((tags, iterator));
                        } else {
                            for datapoint in &mut iterator {
                                apply(&tags, block_start_time + datapoint.time_offset as Time, datapoint);
                            }

                            if iterator.outside_time_range {
                                outside_time_range = true;
                            }
                        }
                    }
                }

                if strict_ordering {
                    let mut ordered_sub_blocks = (0..sub_blocks_iterators.len()).collect::<Vec<_>>();
                    while !ordered_sub_blocks.is_empty() {
                        ordered_sub_blocks.sort_by_key(|&number| sub_blocks_iterators[number].1.peek().unwrap().time_offset);
                        let selected_sub_block = ordered_sub_blocks[0];
                        let (selected_tags, selected_iterator) = &mut sub_blocks_iterators[selected_sub_block];

                        let datapoint = selected_iterator.next().unwrap();
                        apply(&selected_tags, block_start_time + datapoint.time_offset as Time, datapoint);

                        if selected_iterator.outside_time_range {
                            outside_time_range = true;
                        }

                        if selected_iterator.peek().is_none() {
                            ordered_sub_blocks.remove(0);
                        }
                    }
                }
            }

            if outside_time_range {
                break;
            }
        }
    }
}

pub fn determine_statistics_for_time_range<TStorage: MetricStorage<E>, E: Copy + MinMax>(storage: &TStorage,
                                                                                         start_time: Time,
                                                                                         end_time: Time,
                                                                                         tags_filter: SecondaryTagsFilter,
                                                                                         start_block_index: usize) -> TimeRangeStatistics<E> {
    let mut stats = TimeRangeStatistics::default();

    visit_datapoints_in_time_range(
        storage,
        start_time,
        end_time,
        tags_filter,
        start_block_index,
        false,
        |_, _, datapoint| {
            stats.handle(datapoint.value);
        }
    );

    stats
}

pub fn approx_datapoint_count_for_time_range<TStorage: MetricStorage<E>, E: Copy>(storage: &TStorage,
                                                                                  start_time: Time,
                                                                                  end_time: Time,
                                                                                  tags_filter: SecondaryTagsFilter,
                                                                                  start_block_index: usize) -> usize {
    let mut count = 0;
    for block_index in start_block_index..storage.len() {
        let (block_start_time, block_end_time) = storage.block_time_range(block_index).unwrap();
        if block_end_time >= start_time {
            let mut outside_time_range = false;

            if let Some(iterator) = storage.block_datapoints(block_index) {
                for (tags, datapoints) in iterator {
                    if tags_filter.accept(tags) {
                        count += datapoints.len();

                        if let Some(last_datapoint) = datapoints.last() {
                            if (block_start_time + last_datapoint.time_offset as Time) > end_time {
                                outside_time_range = true;
                            }
                        }
                    }
                }
            }

            if outside_time_range {
                break;
            }
        }
    }

    count
}

#[derive(Debug)]
pub struct TimeRangeStatistics<T> {
    pub count: usize,
    min: Option<T>,
    max: Option<T>
}

impl<T: MinMax + Copy> TimeRangeStatistics<T> {
    pub fn new(count: usize, min: T, max: T) -> TimeRangeStatistics<T> {
        TimeRangeStatistics {
            count,
            min: Some(min),
            max: Some(max)
        }
    }

    pub fn min(&self) -> T {
        self.min.unwrap()
    }

    pub fn max(&self) -> T {
        self.max.unwrap()
    }

    pub fn handle(&mut self, value: T) {
        self.count += 1;

        if self.min.is_none() {
            self.min = Some(value);
            self.max = Some(value);
            return;
        }

        let min = self.min.as_mut().unwrap();
        let max = self.max.as_mut().unwrap();
        *min = min.min(value);
        *max = max.max(value);
    }
}

impl<T> Default for TimeRangeStatistics<T> {
    fn default() -> Self {
        TimeRangeStatistics {
            count: 0,
            min: None,
            max: None
        }
    }
}

struct DatapointIterator<'a, T: Iterator<Item=&'a Datapoint<E>>, E: Copy> {
    start_time: Time,
    end_time: Time,
    block_start_time: Time,
    iterator: T,
    outside_time_range: bool,
    peeked: Option<Option<&'a Datapoint<E>>>
}

impl<'a, T: Iterator<Item=&'a Datapoint<E>>, E: Copy> DatapointIterator<'a, T, E> {
    pub fn new(start_time: Time,
               end_time: Time,
               block_start_time: Time,
               iterator: T) -> DatapointIterator<'a, T, E> {
        DatapointIterator {
            start_time,
            end_time,
            block_start_time,
            iterator,
            outside_time_range: false,
            peeked: None
        }
    }

    pub fn peek(&mut self) -> Option<&'a Datapoint<E>> {
        match self.peeked {
            Some(value) => value,
            None => {
                self.peeked = Some(self.next());
                self.peeked.unwrap()
            }
        }
    }
}

impl<'a, T: Iterator<Item=&'a Datapoint<E>>, E: Copy> Iterator for DatapointIterator<'a, T, E> {
    type Item = &'a Datapoint<E>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(element) = self.peeked.take() {
            return element;
        }

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

pub struct MetricWindowing<T> {
    windows: Vec<Option<T>>,
    duration: u64,
    start_time: Time
}

impl<T> MetricWindowing<T> {
    pub fn new(start_time: Time, end_time: Time, duration: u64) -> MetricWindowing<T> {
        let num_windows = (end_time - start_time) / duration;

        MetricWindowing {
            windows: (0..num_windows).map(|_| None).collect::<Vec<_>>(),
            duration,
            start_time
        }
    }

    pub fn len(&self) -> usize {
        self.windows.len()
    }

    pub fn get(&mut self, index: usize) -> &mut Option<T> {
        &mut self.windows[index]
    }

    pub fn get_timestamp(&self, window_index: usize) -> f64 {
        let timestamp = (window_index * self.duration as usize) as Time + self.start_time;
        (timestamp / TIME_SCALE) as f64
    }

    pub fn get_window_index(&self, time: Time) -> usize {
        ((time - self.start_time) / self.duration) as usize
    }

    pub fn create_windows<U, F: Fn() -> U>(&self, f: F) -> Vec<U> {
        (0..self.len()).map(|_| f()).collect::<Vec<_>>()
    }

    pub fn into_windows(self) -> Vec<Option<T>> {
        self.windows
    }
}

pub fn extract_operations_in_windows<
    T: StreamingOperation<TInput, TOutput>,
    F: Fn(Option<TOutput>) -> Option<TResult>,
    TInput, TOutput, TResult
>(windowing: MetricWindowing<T>, transform_output: F, remove_empty: bool) -> Vec<(f64, Option<TResult>)> {
    windowing.windows
        .iter()
        .enumerate()
        .filter(|(_, operation)| operation.is_some() || !remove_empty)
         .map(|(start, operation)| (
             windowing.get_timestamp(start),
             operation.as_ref().map(|operation| transform_output(operation.value())).flatten()
         ))
        .filter(|(_, value)| value.is_some() || !remove_empty)
        .collect()
}

pub fn merge_operations<TOp: StreamingOperation<TInput, TOutput>, TInput, TOutput>(mut streaming_operations: Vec<TOp>) -> TOp {
    let mut streaming_operation = streaming_operations.remove(0);
    for other_operation in streaming_operations.into_iter() {
        streaming_operation.merge(other_operation);
    }

    streaming_operation
}

pub fn merge_windowing<T: StreamingOperation<TInput, TOutput>, TInput, TOutput>(mut primary_tags_windowing: Vec<MetricWindowing<T>>) -> MetricWindowing<T> {
    let mut windowing = primary_tags_windowing.remove(0);
    for current_windowing in primary_tags_windowing.into_iter() {
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

    windowing
}

#[test]
fn test_order_datapoints1() {
    let sub_blocks = vec![
        vec![(4, "A1"), (6, "A2")],
        vec![(0, "B1"), (1, "B2"), (2, "B3"), (4, "B4")],
        vec![(2, "C1"), (3, "C2"), (5, "C3")]
    ];

    let mut sub_blocks_iterators = sub_blocks.iter().map(|sub_block| sub_block.iter().peekable()).collect::<Vec<_>>();
    let mut ordered_sub_blocks = (0..sub_blocks.len()).collect::<Vec<_>>();
    while !ordered_sub_blocks.is_empty() {
        ordered_sub_blocks.sort_by_key(|&number| sub_blocks_iterators[number].peek().unwrap().0);
        let selected_sub_block = ordered_sub_blocks[0];

        let element = sub_blocks_iterators[selected_sub_block].next().unwrap();
        println!("{:?}", element);
        if sub_blocks_iterators[selected_sub_block].peek().is_none() {
            ordered_sub_blocks.remove(0);
        }
    }
}
