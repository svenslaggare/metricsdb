use crate::model::{Datapoint, Time};

pub struct DatabaseStorageVec {
    blocks: Vec<VecBlock>,
    active_block_index: Option<usize>
}

// impl DatabaseStorage for DatabaseStorageVec {
//     fn new(_base_path: &Path) -> Self {
//         DatabaseStorageVec {
//             blocks: Vec::new(),
//             active_block_index: None
//         }
//     }
//
//     fn from_existing(_base_path: &Path) -> Self {
//         DatabaseStorageVec {
//             blocks: Vec::new(),
//             active_block_index: None
//         }
//     }
//
//     fn len(&self) -> usize {
//         self.blocks.len()
//     }
//
//     fn has_blocks(&self) -> bool {
//         self.active_block_index.is_some()
//     }
//
//     fn block_start_time(&self, index: usize) -> Option<Time> {
//         self.blocks.get(index).map(|block| block.start_time)
//     }
//
//     fn active_block_start_time(&self) -> Option<Time> {
//         Some(self.blocks[self.active_block_index?].start_time)
//     }
//
//     fn active_block_datapoints_mut(&mut self) -> Option<&mut [Datapoint]> {
//         self.datapoints_mut(self.active_block_index?)
//     }
//
//     fn create_block(&mut self, time: Time, datapoint: Datapoint) {
//         self.active_block_index = Some(self.blocks.len());
//         self.blocks.push(VecBlock {
//             start_time: time,
//             datapoints: vec![datapoint]
//         });
//     }
//
//     fn add_datapoint(&mut self, datapoint: Datapoint) {
//         if let Some(active_block_index) = self.active_block_index {
//             self.blocks[active_block_index].datapoints.push(datapoint);
//         }
//     }
//
//     fn datapoints(&self, block_index: usize) -> Option<&[Datapoint]> {
//         let block = self.blocks.get(block_index)?;
//         Some(&block.datapoints[..])
//     }
//
//     fn datapoints_mut(&mut self, block_index: usize) -> Option<&mut [Datapoint]> {
//         let block = self.blocks.get_mut(block_index)?;
//         Some(&mut block.datapoints[..])
//     }
// }
//
struct VecBlock {
    start_time: Time,
    datapoints: Vec<Datapoint>
}