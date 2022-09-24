use std::path::Path;

use crate::memory_file::MemoryFile;
use crate::model::{Datapoint, Time};

pub trait DatabaseStorage {
    fn new(base_path: &Path) -> Self;
    fn from_existing(base_path: &Path) -> Self;

    fn len(&self) -> usize;
    fn has_blocks(&self) -> bool;

    fn block_start_time(&self, index: usize) -> Option<Time>;
    fn block_time_range(&self, index: usize) -> Option<(Time, Time)> {
        let block_start_time = self.block_start_time(index)?;
        let block_end_time = block_start_time + self.datapoints(index)?.last()?.time_offset as Time;
        Some((block_start_time, block_end_time))
    }

    fn active_block_start_time(&self) -> Option<Time>;
    fn active_block_datapoints_mut(&mut self) -> Option<&mut [Datapoint]>;

    fn create_block(&mut self, time: Time, datapoint: Datapoint);
    fn add_datapoint(&mut self, datapoint: Datapoint);

    fn datapoints(&self, block_index: usize) -> Option<&[Datapoint]>;
    fn datapoints_mut(&mut self, block_index: usize) -> Option<&mut [Datapoint]>;
}

pub struct DatabaseStorageVec {
    blocks: Vec<VecBlock>,
    active_block_index: Option<usize>
}

impl DatabaseStorage for DatabaseStorageVec {
    fn new(_base_path: &Path) -> Self {
        DatabaseStorageVec {
            blocks: Vec::new(),
            active_block_index: None
        }
    }

    fn from_existing(_base_path: &Path) -> Self {
        DatabaseStorageVec {
            blocks: Vec::new(),
            active_block_index: None
        }
    }

    fn len(&self) -> usize {
        self.blocks.len()
    }

    fn has_blocks(&self) -> bool {
        self.active_block_index.is_some()
    }

    fn block_start_time(&self, index: usize) -> Option<Time> {
        self.blocks.get(index).map(|block| block.start_time)
    }

    fn active_block_start_time(&self) -> Option<Time> {
        Some(self.blocks[self.active_block_index?].start_time)
    }

    fn active_block_datapoints_mut(&mut self) -> Option<&mut [Datapoint]> {
        self.datapoints_mut(self.active_block_index?)
    }

    fn create_block(&mut self, time: Time, datapoint: Datapoint) {
        self.active_block_index = Some(self.blocks.len());
        self.blocks.push(VecBlock {
            start_time: time,
            datapoints: vec![datapoint]
        });
    }

    fn add_datapoint(&mut self, datapoint: Datapoint) {
        if let Some(active_block_index) = self.active_block_index {
            self.blocks[active_block_index].datapoints.push(datapoint);
        }
    }

    fn datapoints(&self, block_index: usize) -> Option<&[Datapoint]> {
        let block = self.blocks.get(block_index)?;
        Some(&block.datapoints[..])
    }

    fn datapoints_mut(&mut self, block_index: usize) -> Option<&mut [Datapoint]> {
        let block = self.blocks.get_mut(block_index)?;
        Some(&mut block.datapoints[..])
    }
}

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

    fn block_start_time(&self, index: usize) -> Option<Time> {
        unsafe { Some((*self.block_at_ptr(index)?).start_time) }
    }

    fn active_block_start_time(&self) -> Option<Time> {
        if !self.has_blocks() {
            return None;
        }

        unsafe { Some((*self.active_block()).start_time) }
    }

    fn active_block_datapoints_mut(&mut self) -> Option<&mut [Datapoint]> {
        if !self.has_blocks() {
            return None;
        }

        let block_index = unsafe { (*self.header_mut()).active_block_index };
        self.datapoints_mut(block_index)
    }

    fn create_block(&mut self, time: Time, datapoint: Datapoint) {
        unsafe {
            if self.has_blocks() {
                (*self.header_mut()).active_block_start += (*self.active_block()).size;
                (*self.header_mut()).active_block_index += 1;
            }

            self.storage_file.try_grow_file(std::mem::size_of::<FileBlock>()).unwrap();
            *self.active_block_mut() = FileBlock {
                size: std::mem::size_of::<FileBlock>(),
                start_time: time,
                num_datapoints: 0
            };
            (*self.header_mut()).num_blocks += 1;

            self.index_file.try_grow_file(std::mem::size_of::<usize>()).unwrap();
            *self.index_mut().add((*self.header()).active_block_index) = (*self.header()).active_block_start;
        }

        self.add_datapoint(datapoint);
    }

    fn add_datapoint(&mut self, datapoint: Datapoint) {
        unsafe {
            let active_block = self.active_block_mut();

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

    fn datapoints(&self, block_index: usize) -> Option<&[Datapoint]> {
        unsafe {
            let block_ptr = self.block_at_ptr(block_index)?;
            let datapoints_ptr = (block_ptr as *const u8).add(std::mem::size_of::<FileBlock>()) as *const Datapoint;
            Some(std::slice::from_raw_parts(datapoints_ptr, (*block_ptr).num_datapoints))
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

struct FileHeader {
    num_blocks: usize,
    active_block_index: usize,
    active_block_start: usize,
}

struct FileBlock {
    size: usize,
    start_time: Time,
    num_datapoints: usize
}