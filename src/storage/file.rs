use std::path::Path;

use crate::memory_file::MemoryFile;
use crate::model::{Datapoint, Time};
use crate::storage::DatabaseStorage;
use crate::Tags;

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

    fn allocate_sub_block_for_insertion(&mut self, block_ptr: *mut FileBlock, tags: Tags) -> Option<&mut FileSubBlock> {
        let default_capacity = 100;
        let growth_factor = 2;

        unsafe {
            let mut allocate_file = |active_block: *mut FileBlock, size: usize| {
                self.storage_file.try_grow_file(size).unwrap();
                (*active_block).size += size;
            };

            if let Some((sub_block_index, sub_block)) = (*block_ptr).find_sub_block(tags) {
                if sub_block.count < sub_block.capacity {
                    Some(sub_block)
                } else {
                    let desired_capacity = sub_block.count * growth_factor;
                    if let Some(increased_capacity) = (*block_ptr).try_extend(sub_block_index, sub_block, desired_capacity) {
                        allocate_file(block_ptr, increased_capacity as usize * std::mem::size_of::<Datapoint>());
                        Some(sub_block)
                    } else {
                        if let Some((new_sub_block, allocated)) = (*block_ptr).allocate_sub_block(tags, desired_capacity) {
                            if allocated {
                                allocate_file(block_ptr, new_sub_block.datapoints_size());
                            }

                            new_sub_block.replace_at(block_ptr, sub_block);
                            Some(new_sub_block)
                        } else {
                            None
                        }
                    }
                }
            } else {
                if let Some((sub_block, allocated)) = (*block_ptr).allocate_sub_block(tags, default_capacity) {
                    if allocated {
                        allocate_file(block_ptr, sub_block.datapoints_size());
                    }

                    Some(sub_block)
                } else {
                    None
                }
            }
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

        unsafe {
            (*self.active_block_mut()).datapoints_mut(tags)
        }
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
                num_sub_blocks: 0,
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

            let sub_block = self.allocate_sub_block_for_insertion(active_block, tags).unwrap();
            sub_block.add_datapoint(active_block, datapoint);
        }
    }

    fn visit_datapoints(&self, block_index: usize, apply: &mut dyn FnMut(Tags, &[Datapoint])) {
        unsafe {
            if let Some(block_ptr) = self.block_at_ptr(block_index) {
                for sub_block in &(*block_ptr).sub_blocks[..(*block_ptr).num_sub_blocks] {
                    apply(sub_block.tags, sub_block.datapoints(block_ptr));
                }
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
    num_sub_blocks: usize,
    next_sub_block_offset: usize,
    sub_blocks: [FileSubBlock; 100]
}

impl FileBlock {
    pub fn datapoints_mut(&mut self, tags: Tags) -> Option<&mut [Datapoint]> {
        let block_ptr = self as *mut FileBlock;
        let (_, sub_block) = self.find_sub_block(tags)?;
        Some(sub_block.datapoints_mut(block_ptr))
    }

    pub fn find_sub_block(&mut self, tags: Tags) -> Option<(usize, &mut FileSubBlock)> {
        for (index, sub_block) in self.sub_blocks[..self.num_sub_blocks].iter_mut().enumerate() {
            if sub_block.count > 0 && sub_block.tags == tags {
                return Some((index, sub_block));
            }
        }

        None
    }

    pub fn allocate_sub_block(&mut self, tags: Tags, capacity: u32) -> Option<(&mut FileSubBlock, bool)> {
        for sub_block in &mut self.sub_blocks {
            if sub_block.count == 0 && sub_block.capacity >= capacity {
                sub_block.tags = tags;
                return Some((sub_block, false));
            }

            if sub_block.capacity == 0 {
                sub_block.offset = self.next_sub_block_offset;
                sub_block.capacity = capacity;
                sub_block.count = 0;
                sub_block.tags = tags;
                self.num_sub_blocks += 1;
                self.next_sub_block_offset += sub_block.datapoints_size();
                return Some((sub_block, true));
            }
        }

        None
    }

    pub fn try_extend(&mut self, index: usize, sub_block: &mut FileSubBlock, new_capacity: u32) -> Option<u32> {
        if index == self.num_sub_blocks - 1 {
            assert!(new_capacity > sub_block.capacity);
            let increased_capacity = new_capacity - sub_block.capacity;
            self.next_sub_block_offset += increased_capacity as usize * std::mem::size_of::<Datapoint>();
            sub_block.capacity = new_capacity;
            Some(increased_capacity)
        } else {
            None
        }
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
    pub fn free(&mut self) {
        self.count = 0;
        self.tags = 0;
    }

    pub fn datapoints_size(&self) -> usize {
        std::mem::size_of::<Datapoint>() * self.capacity as usize
    }

    pub fn add_datapoint(&mut self, block_ptr: *const FileBlock, datapoint: Datapoint) {
        unsafe {
            let datapoints_ptr = (block_ptr as *const u8).add(std::mem::size_of::<FileBlock>() + self.offset) as *mut Datapoint;
            *datapoints_ptr.add(self.count as usize) = datapoint;
        }

        self.count += 1;
    }

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

    pub fn replace_at(&mut self, block_ptr: *const FileBlock, other: &mut FileSubBlock) {
        self.count = other.count;
        self.datapoints_mut(block_ptr).clone_from_slice(other.datapoints(block_ptr));
        other.free();
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