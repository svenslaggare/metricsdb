use std::path::Path;

use crate::memory_file::MemoryFile;
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

    fn visit_datapoints(&self, block_index: usize, apply: &mut dyn FnMut(Tags, &[Datapoint]));
}

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

pub struct DatabaseStorageFile {
    storage_file: MemoryFile,
    index_file: MemoryFile
}

impl DatabaseStorageFile {
    fn initialize(&mut self) {
        unsafe {
            *self.header_mut() = FileHeader {
                num_blocks: 0,
                active_block_index: 0,
                active_block_start: std::mem::size_of::<FileHeader>()
            };
        }
    }

    unsafe fn header(&self) -> *const FileHeader {
        std::mem::transmute(self.storage_file.ptr())
    }

    unsafe fn header_mut(&mut self) -> *mut FileHeader {
        std::mem::transmute(self.storage_file.ptr_mut())
    }

    fn index(&self) -> *const usize {
        self.index_file.ptr() as *const usize
    }

    fn index_mut(&mut self) -> *mut usize {
        self.index_file.ptr() as *mut usize
    }

    unsafe fn active_block(&self) -> *const FileBlock {
        std::mem::transmute(self.storage_file.ptr().add((*self.header()).active_block_start))
    }

    unsafe fn active_block_mut(&mut self) -> *mut FileBlock {
        std::mem::transmute(self.storage_file.ptr_mut().add((*self.header()).active_block_start))
    }

    fn block_at_ptr(&self, index: usize) -> Option<*const FileBlock> {
        if index >= self.len() {
            return None;
        }

        unsafe {
            let block_offset = *self.index().add(index);
            Some(self.storage_file.ptr().add(block_offset) as *const FileBlock)
        }
    }

    fn datapoints_mut(&mut self, block_index: usize) -> Option<&mut [Datapoint]> {
        unsafe {
            let block_ptr = self.block_at_ptr(block_index)?;
            let datapoints_ptr = (block_ptr as *const u8).add(std::mem::size_of::<FileBlock>()) as *mut Datapoint;
            Some(std::slice::from_raw_parts_mut(datapoints_ptr, (*block_ptr).num_datapoints))
        }
    }
}

const STORAGE_MAX_SIZE: usize = 1024 * 1024 * 1024;
const INDEX_MAX_SIZE: usize = 1024 * 1024 * 1024;

impl DatabaseStorage for DatabaseStorageFile {
    fn new(base_path: &Path) -> Self {
        let mut storage = DatabaseStorageFile {
            storage_file: MemoryFile::new(&base_path.join(Path::new("storage")), STORAGE_MAX_SIZE, true).unwrap(),
            index_file: MemoryFile::new(&base_path.join(Path::new("index")), INDEX_MAX_SIZE, true).unwrap()
        };

        storage.initialize();
        storage
    }

    fn from_existing(base_path: &Path) -> Self {
        DatabaseStorageFile {
            storage_file: MemoryFile::new(&base_path.join(Path::new("storage")), STORAGE_MAX_SIZE, false).unwrap(),
            index_file: MemoryFile::new(&base_path.join(Path::new("index")), INDEX_MAX_SIZE, false).unwrap()
        }
    }

    fn len(&self) -> usize {
        unsafe { (*self.header()).num_blocks }
    }

    fn has_blocks(&self) -> bool {
        self.len() > 0
    }

    fn block_time_range(&self, index: usize) -> Option<(Time, Time)> {
        unsafe { self.block_at_ptr(index).map(|block| ((*block).start_time, (*block).end_time)) }
    }

    fn active_block_start_time(&self) -> Option<Time> {
        if !self.has_blocks() {
            return None;
        }

        unsafe { Some((*self.active_block()).start_time) }
    }

    fn active_block_datapoints_mut(&mut self, tags: Tags) -> Option<&mut [Datapoint]> {
        if !self.has_blocks() {
            return None;
        }

        let block_index = unsafe { (*self.header_mut()).active_block_index };
        self.datapoints_mut(block_index)
    }

    fn create_block(&mut self, time: Time) {
        unsafe {
            if self.has_blocks() {
                (*self.header_mut()).active_block_start += (*self.active_block()).size;
                (*self.header_mut()).active_block_index += 1;
            }

            self.storage_file.try_grow_file(std::mem::size_of::<FileBlock>()).unwrap();
            *self.active_block_mut() = FileBlock {
                size: std::mem::size_of::<FileBlock>(),
                start_time: time,
                end_time: time,
                num_datapoints: 0,
                next_sub_block_offset: 0,
                sub_blocks: [Default::default(); 100]
            };
            (*self.header_mut()).num_blocks += 1;

            self.index_file.try_grow_file(std::mem::size_of::<usize>()).unwrap();
            *self.index_mut().add((*self.header()).active_block_index) = (*self.header()).active_block_start;
        }
    }

    fn add_datapoint(&mut self, tags: Tags, datapoint: Datapoint) {
        unsafe {
            let active_block = self.active_block_mut();
            (*active_block).end_time = (*active_block).end_time.max((*active_block).start_time + datapoint.time_offset as Time);

            let num_datapoints = (*active_block).num_datapoints;

            self.storage_file.try_grow_file(std::mem::size_of::<Datapoint>()).unwrap();
            let datapoint_ptr: *mut Datapoint = std::mem::transmute(
                self.storage_file.ptr_mut().add(
                    (*self.header()).active_block_start + std::mem::size_of::<FileBlock>()
                        + num_datapoints * std::mem::size_of::<Datapoint>()
                )
            );
            *datapoint_ptr = datapoint;

            (*active_block).num_datapoints += 1;
            (*active_block).size += std::mem::size_of::<Datapoint>();
        }
    }

    fn visit_datapoints(&self, block_index: usize, apply: &mut dyn FnMut(Tags, &[Datapoint])) {
        unsafe {
            if let Some(block_ptr) = self.block_at_ptr(block_index) {
                let datapoints_ptr = (block_ptr as *const u8).add(std::mem::size_of::<FileBlock>()) as *const Datapoint;
                let datapoints = std::slice::from_raw_parts(datapoints_ptr, (*block_ptr).num_datapoints);
                apply(0, datapoints);
            }
        }
    }
}

struct FileHeader {
    num_blocks: usize,
    active_block_index: usize,
    active_block_start: usize,
}

struct FileBlock {
    size: usize,
    start_time: Time,
    end_time: Time,
    num_datapoints: usize,
    next_sub_block_offset: usize,
    sub_blocks: [FileSubBlock; 100]
}

impl FileBlock {
    pub fn find_sub_block(&mut self, tags: Tags) -> Option<(&mut FileSubBlock)> {
        for sub_block in &mut self.sub_blocks {
            if sub_block.count > 0 && sub_block.tags == tags {
                return Some(sub_block);
            }
        }

        None
    }

    pub fn create_sub_block(&mut self, tags: Tags, count: u32) -> bool {
        for sub_block in &mut self.sub_blocks {
            if sub_block.capacity == 0 {
                sub_block.offset = self.next_sub_block_offset;
                sub_block.capacity = count;
                sub_block.count = 0;
                sub_block.tags = tags;
                self.next_sub_block_offset += count as usize * std::mem::size_of::<Datapoint>();
                return true;
            }
        }

        return false;
    }
}

#[derive(Clone, Copy)]
struct FileSubBlock {
    offset: usize,
    capacity: u32,
    count: u32,
    tags: Tags
}

impl FileSubBlock {
    pub fn datapoints(&self, block_ptr: *const FileBlock) -> &[Datapoint] {
        unsafe {
            let datapoints_ptr = (block_ptr as *const u8).add(std::mem::size_of::<FileBlock>() + self.offset) as *const Datapoint;
            std::slice::from_raw_parts(datapoints_ptr, self.count as usize)
        }
    }

    pub fn datapoints_mut(&self, block_ptr: *const FileBlock) -> &mut [Datapoint] {
        unsafe {
            let datapoints_ptr = (block_ptr as *const u8).add(std::mem::size_of::<FileBlock>() + self.offset) as *mut Datapoint;
            std::slice::from_raw_parts_mut(datapoints_ptr, self.count as usize)
        }
    }
}

impl Default for FileSubBlock {
    fn default() -> Self {
        FileSubBlock {
            offset: 0,
            capacity: 0,
            count: 0,
            tags: 0
        }
    }
}