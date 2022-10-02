use crate::model::{Datapoint, MinMax, Time, TIME_SCALE};
use crate::storage::MetricStorage;
use crate::tags::TagsFilter;

pub fn find_block_index<TStorage: MetricStorage<E>, E: Copy>(storage: &TStorage, time: Time) -> Option<usize> {
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

pub fn visit_datapoints_in_time_range<TStorage: MetricStorage<E>, F: FnMut(Time, &Datapoint<E>), E: Copy>(storage: &TStorage,
                                                                                                          start_time: Time,
                                                                                                          end_time: Time,
                                                                                                          tags_filter: TagsFilter,
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
                            block_end_time,
                            datapoints.iter()
                        );

                        if strict_ordering {
                            if iterator.peek().is_none() {
                                if iterator.outside_time_range {
                                    outside_time_range = true;
                                }

                                continue;
                            }

                            sub_blocks_iterators.push(iterator);
                        } else {
                            for datapoint in &mut iterator {
                                apply(block_start_time, datapoint);
                            }
                        }
                    }
                }

                if strict_ordering {
                    let mut ordered_sub_blocks = (0..sub_blocks_iterators.len()).collect::<Vec<_>>();
                    while !ordered_sub_blocks.is_empty() {
                        ordered_sub_blocks.sort_by_key(|&number| sub_blocks_iterators[number].peek().unwrap().time_offset);
                        let selected_sub_block = ordered_sub_blocks[0];
                        let selected_iterator = &mut sub_blocks_iterators[selected_sub_block];

                        apply(block_start_time, selected_iterator.next().unwrap());

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
                                                                                         tags_filter: TagsFilter,
                                                                                         start_block_index: usize) -> TimeRangeStatistics<E> {
    let mut stats = TimeRangeStatistics::default();

    visit_datapoints_in_time_range(
        storage,
        start_time,
        end_time,
        tags_filter,
        start_block_index,
        false,
        |_, datapoint| {
            stats.handle(datapoint.value);
        }
    );

    stats
}

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
    block_end_time: Time,
    iterator: T,
    outside_time_range: bool,
    peeked: Option<Option<&'a Datapoint<E>>>
}

impl<'a, T: Iterator<Item=&'a Datapoint<E>>, E: Copy> DatapointIterator<'a, T, E> {
    pub fn new(start_time: Time,
               end_time: Time,
               block_start_time: Time,
               block_end_time: Time,
               iterator: T) -> DatapointIterator<'a, T, E> {
        DatapointIterator {
            start_time,
            end_time,
            block_start_time,
            block_end_time,
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
    pub windows: Vec<Option<T>>,
    duration: u64,
    window_start: Time
}

impl<T> MetricWindowing<T> {
    pub fn new(start_time: Time, end_time: Time, duration: u64) -> MetricWindowing<T> {
        let window_start = start_time / duration;
        let num_windows = (end_time / duration) - window_start;

        MetricWindowing {
            windows: (0..num_windows).map(|_| None).collect::<Vec<_>>(),
            duration,
            window_start
        }
    }

    pub fn len(&self) -> usize {
        self.windows.len()
    }

    pub fn get_timestamp(&self, window_index: usize) -> f64 {
        (((window_index as u64 + self.window_start) * self.duration) / TIME_SCALE) as f64
    }

    pub fn get_window_index(&self, time: Time) -> usize {
        ((time / self.duration) - self.window_start) as usize
    }

    pub fn create_windows<U, F: Fn() -> U>(&self, f: F) -> Vec<U> {
        (0..self.len()).map(|_| f()).collect::<Vec<_>>()
    }
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