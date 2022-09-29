use crate::model::{Datapoint, Time};
use crate::storage::DatabaseStorage;
use crate::TagsFilter;

pub fn find_block_index<TStorage: DatabaseStorage>(storage: &TStorage, time: Time) -> Option<usize> {
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

pub fn visit_datapoints_in_time_range<TStorage: DatabaseStorage, F: FnMut(Time, &Datapoint)>(storage: &TStorage,
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

pub fn count_datapoints_in_time_range<TStorage: DatabaseStorage>(storage: &TStorage,
                                                                 start_time: Time,
                                                                 end_time: Time,
                                                                 tags_filter: TagsFilter,
                                                                 start_block_index: usize) -> usize {
    let mut count = 0;
    for block_index in start_block_index..storage.len() {
        let (block_start_time, block_end_time) = storage.block_time_range(block_index).unwrap();
        if block_end_time >= start_time {
            let mut outside_time_range = false;
            storage.visit_datapoints(block_index, |tags, datapoints| {
                if tags_filter.accept(tags) {
                    for datapoint in datapoints.iter() {
                        let datapoint_time = block_start_time + datapoint.time_offset as Time;
                        if datapoint_time > end_time {
                            outside_time_range = true;
                            break;
                        }

                        if datapoint_time >= start_time {
                            count += 1;
                        }
                    }
                }
            });

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
    outside_time_range: bool,
    peeked: Option<Option<&'a Datapoint>>
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
            outside_time_range: false,
            peeked: None
        }
    }

    pub fn peek(&mut self) -> Option<&'a Datapoint> {
        match self.peeked {
            Some(value) => value,
            None => {
                self.peeked = Some(self.next());
                self.peeked.unwrap()
            }
        }
    }
}

impl<'a, T: Iterator<Item=&'a Datapoint>> Iterator for DatapointIterator<'a, T> {
    type Item = &'a Datapoint;

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