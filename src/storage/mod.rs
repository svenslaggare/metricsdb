use std::path::Path;

use crate::model::{Datapoint, Tags, Time};

pub trait DatabaseStorage {
    fn new(base_path: &Path) -> Self;
    fn from_existing(base_path: &Path) -> Self;

    fn len(&self) -> usize;
    fn has_blocks(&self) -> bool;

    fn block_time_range(&self, index: usize) -> Option<(Time, Time)>;

    fn active_block_start_time(&self) -> Option<Time>;
    fn active_block_datapoints_mut(&mut self, tags: Tags) -> Option<&mut [Datapoint]>;

    fn create_block(&mut self, time: Time);
    fn add_datapoint(&mut self, tags: Tags, datapoint: Datapoint);

    fn create_block_with_datapoint(&mut self, time: Time, tags: Tags, datapoint: Datapoint) {
        self.create_block(time);
        self.add_datapoint(tags, datapoint);
    }

    fn visit_datapoints<F: FnMut(Tags, &[Datapoint])>(&self, block_index: usize, apply: F);
    fn block_datapoints<'a>(&'a self, block_index: usize) -> Option<Box<dyn Iterator<Item=(Tags, &[Datapoint])> + 'a>>;
}

pub mod file;
