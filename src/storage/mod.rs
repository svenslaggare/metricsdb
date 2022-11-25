use std::path::Path;

use crate::model::{Datapoint, MetricError, MetricResult, Tags, Time};

pub trait MetricStorage<E: Copy> {
    fn new(base_path: &Path, block_duration: u64, datapoint_duration: u64) -> MetricResult<Self> where Self: Sized;
    fn from_existing(base_path: &Path) -> MetricResult<Self> where Self: Sized;

    fn block_duration(&self) -> u64;
    fn datapoint_duration(&self) -> u64;

    fn len(&self) -> usize;

    fn block_time_range(&self, index: usize) -> Option<(Time, Time)>;

    fn active_block_time_range(&self) -> Option<(Time, Time)>;
    fn active_block_datapoints_mut(&mut self, tags: Tags) -> Option<&mut [Datapoint<E>]>;

    fn last_datapoint_mut(&mut self, tags: Tags) -> Option<&mut Datapoint<E>> {
         self.active_block_datapoints_mut(tags)
            .map(|datapoint| datapoint.last_mut())
            .flatten()
    }

    fn create_block(&mut self, time: Time) -> Result<(), MetricError>;
    fn add_datapoint(&mut self, tags: Tags, datapoint: Datapoint<E>) -> MetricResult<()>;

    fn create_block_with_datapoint(&mut self, time: Time, tags: Tags, datapoint: Datapoint<E>) -> MetricResult<()> {
        self.create_block(time)?;
        self.add_datapoint(tags, datapoint)?;
        Ok(())
    }

    type BlockIterator<'a>: Iterator<Item=(Tags, &'a [Datapoint<E>])> where Self: 'a, E: 'a;
    fn block_datapoints<'a>(&'a self, block_index: usize) -> Option<Self::BlockIterator<'a>>;

    fn scheduled(&mut self);
}

pub mod file;
pub mod memory_file;
