use std::marker::PhantomData;
use std::path::Path;

use crate::storage::memory_file::MemoryFile;
use crate::model::{Datapoint, MetricError, Time};
use crate::storage::MetricStorage;
use crate::Tags;

const STORAGE_MAX_SIZE: usize = 1024 * 1024 * 1024;
const INDEX_MAX_SIZE: usize = 1024 * 1024 * 1024;

pub struct MetricStorageFile<E> {
    storage_file: MemoryFile,
    index_file: MemoryFile,
    _phantom: PhantomData<E>
}

impl<E: Copy> MetricStorageFile<E> {
    fn initialize(&mut self, block_duration: u64, datapoint_duration: u64) {
        unsafe {
            *self.header_mut() = Header {
                num_blocks: 0,
                active_block_index: 0,
                active_block_start: std::mem::size_of::<Header>(),
                block_duration,
                datapoint_duration
            };
        }
    }

    unsafe fn header(&self) -> *const Header {
        std::mem::transmute(self.storage_file.ptr())
    }

    unsafe fn header_mut(&mut self) -> *mut Header {
        std::mem::transmute(self.storage_file.ptr_mut())
    }

    fn index(&self) -> *const usize {
        self.index_file.ptr() as *const usize
    }

    fn index_mut(&mut self) -> *mut usize {
        self.index_file.ptr() as *mut usize
    }

    unsafe fn active_block(&self) -> *const Block<E> {
        std::mem::transmute(self.storage_file.ptr().add((*self.header()).active_block_start))
    }

    unsafe fn active_block_mut(&mut self) -> *mut Block<E> {
        std::mem::transmute(self.storage_file.ptr_mut().add((*self.header()).active_block_start))
    }

    fn block_at_ptr(&self, index: usize) -> Option<*const Block<E>> {
        if index >= self.len() {
            return None;
        }

        unsafe {
            let block_offset = *self.index().add(index);
            Some(self.storage_file.ptr().add(block_offset) as *const Block<E>)
        }
    }

    fn allocate_sub_block_for_insertion(&mut self, block_ptr: *mut Block<E>, tags: Tags) -> Option<&mut SubBlock<E>> {
        let default_capacity = 100;
        let growth_factor = 2;

        unsafe {
            let mut allocate_file = |active_block: *mut Block<E>, size: usize| {
                self.storage_file.try_grow_file(size).unwrap();
                (*active_block).size += size;
            };

            if let Some((sub_block_index, sub_block)) = (*block_ptr).find_sub_block(tags) {
                if sub_block.count < sub_block.capacity {
                    Some(sub_block)
                } else {
                    let desired_capacity = sub_block.count * growth_factor;
                    if let Some(increased_capacity) = (*block_ptr).try_extend(sub_block_index, sub_block, desired_capacity) {
                        allocate_file(block_ptr, increased_capacity as usize * std::mem::size_of::<Datapoint<E>>());
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

impl<E: Copy> MetricStorage<E> for MetricStorageFile<E> {
    fn new(base_path: &Path, block_duration: u64, datapoint_duration: u64) -> Result<Self, MetricError> {
        let mut storage = MetricStorageFile {
            storage_file: MemoryFile::new(&base_path.join(Path::new("storage")), STORAGE_MAX_SIZE, true)?,
            index_file: MemoryFile::new(&base_path.join(Path::new("index")), INDEX_MAX_SIZE, true)?,
            _phantom: Default::default()
        };

        storage.initialize(block_duration, datapoint_duration);
        Ok(storage)
    }

    fn from_existing(base_path: &Path) -> Result<Self, MetricError> {
        Ok(
            MetricStorageFile {
                storage_file: MemoryFile::new(&base_path.join(Path::new("storage")), STORAGE_MAX_SIZE, false)?,
                index_file: MemoryFile::new(&base_path.join(Path::new("index")), INDEX_MAX_SIZE, false)?,
                _phantom: Default::default()
            }
        )
    }

    fn block_duration(&self) -> u64 {
        unsafe { (*self.header()).block_duration }
    }

    fn datapoint_duration(&self) -> u64 {
        unsafe { (*self.header()).datapoint_duration }
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

    fn active_block_datapoints_mut(&mut self, tags: Tags) -> Option<&mut [Datapoint<E>]> {
        if !self.has_blocks() {
            return None;
        }

        unsafe {
            (*self.active_block_mut()).datapoints_mut(tags)
        }
    }

    fn create_block(&mut self, time: Time) -> Result<(), MetricError> {
        unsafe {
            if self.has_blocks() {
                self.storage_file.sync(self.active_block() as *const u8, (*self.active_block()).size, false)?;
                (*self.header_mut()).active_block_start += (*self.active_block()).size;
                (*self.header_mut()).active_block_index += 1;
            }

            self.storage_file.try_grow_file(std::mem::size_of::<Block<E>>())?;
            *self.active_block_mut() = Block {
                size: std::mem::size_of::<Block<E>>(),
                start_time: time,
                end_time: time,
                num_sub_blocks: 0,
                next_sub_block_offset: 0,
                sub_blocks: [Default::default(); NUM_SUB_BLOCKS],
                _phantom: Default::default()
            };
            (*self.header_mut()).num_blocks += 1;

            self.index_file.try_grow_file(std::mem::size_of::<usize>())?;
            *self.index_mut().add((*self.header()).active_block_index) = (*self.header()).active_block_start;
        }

        Ok(())
    }

    fn add_datapoint(&mut self, tags: Tags, datapoint: Datapoint<E>) -> Result<(), MetricError> {
        unsafe {
            let active_block = self.active_block_mut();
            (*active_block).end_time = (*active_block).end_time.max((*active_block).start_time + datapoint.time_offset as Time);

            let sub_block = self.allocate_sub_block_for_insertion(active_block, tags).ok_or_else(|| MetricError::FailedToAllocateSubBlock)?;
            sub_block.add_datapoint(active_block, datapoint);
        }

        Ok(())
    }

    fn visit_datapoints<F: FnMut(Tags, &[Datapoint<E>])>(&self, block_index: usize, mut apply: F) {
        unsafe {
            if let Some(block_ptr) = self.block_at_ptr(block_index) {
                for sub_block in &(*block_ptr).sub_blocks[..(*block_ptr).num_sub_blocks] {
                    if sub_block.count > 0 {
                        apply(sub_block.tags, sub_block.datapoints(block_ptr));
                    }
                }
            }
        }
    }

    fn block_datapoints<'a>(&'a self, block_index: usize) -> Option<Box<dyn Iterator<Item=(Tags, &[Datapoint<E>])> + 'a>> {
        let block_ptr = self.block_at_ptr(block_index)?;
        Some(
            Box::new(
                SubBlockIterator {
                    block_ptr,
                    _phantom: Default::default(),
                    sub_block_index: 0,
                    num_sub_blocks: unsafe { (*block_ptr).num_sub_blocks }
                }
            )
        )
    }
}

struct SubBlockIterator<'a, E: Copy> {
    block_ptr: *const Block<E>,
    sub_block_index: usize,
    num_sub_blocks: usize,
    _phantom: PhantomData<&'a E>
}

impl<'a, E: Copy> Iterator for SubBlockIterator<'a, E> {
    type Item = (Tags, &'a [Datapoint<E>]);

    fn next(&mut self) -> Option<Self::Item> {
        while self.sub_block_index < self.num_sub_blocks {
            let current_index = self.sub_block_index;
            self.sub_block_index += 1;

            unsafe {
                let sub_block = &(*self.block_ptr).sub_blocks[current_index];
                if sub_block.count > 0 {
                    return Some((sub_block.tags, sub_block.datapoints(self.block_ptr)))
                }
            }
        }

        return None;
    }
}

struct Header {
    num_blocks: usize,
    block_duration: u64,
    datapoint_duration: u64,
    active_block_index: usize,
    active_block_start: usize,
}

const NUM_SUB_BLOCKS: usize = 100;

struct Block<E: Copy> {
    size: usize,
    start_time: Time,
    end_time: Time,
    num_sub_blocks: usize,
    next_sub_block_offset: usize,
    sub_blocks: [SubBlock<E>; NUM_SUB_BLOCKS],
    _phantom: PhantomData<E>
}

impl<E: Copy> Block<E> {
    pub fn datapoints_mut(&mut self, tags: Tags) -> Option<&mut [Datapoint<E>]> {
        let block_ptr = self as *mut Block<E>;
        let (_, sub_block) = self.find_sub_block(tags)?;
        Some(sub_block.datapoints_mut(block_ptr))
    }

    pub fn find_sub_block(&mut self, tags: Tags) -> Option<(usize, &mut SubBlock<E>)> {
        for (index, sub_block) in self.sub_blocks[..self.num_sub_blocks].iter_mut().enumerate() {
            if sub_block.count > 0 && sub_block.tags == tags {
                return Some((index, sub_block));
            }
        }

        None
    }

    pub fn allocate_sub_block(&mut self, tags: Tags, capacity: u32) -> Option<(&mut SubBlock<E>, bool)> {
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

    pub fn try_extend(&mut self, index: usize, sub_block: &mut SubBlock<E>, new_capacity: u32) -> Option<u32> {
        if index == self.num_sub_blocks - 1 {
            assert!(new_capacity > sub_block.capacity);
            let increased_capacity = new_capacity - sub_block.capacity;
            self.next_sub_block_offset += increased_capacity as usize * std::mem::size_of::<Datapoint<E>>();
            sub_block.capacity = new_capacity;
            Some(increased_capacity)
        } else {
            None
        }
    }
}

#[derive(Clone, Copy)]
struct SubBlock<E: Copy> {
    offset: usize,
    capacity: u32,
    count: u32,
    tags: Tags,
    _phantom: PhantomData<E>
}

impl<E: Copy> SubBlock<E> {
    pub fn free(&mut self) {
        self.count = 0;
        self.tags = 0;
    }

    pub fn datapoints_size(&self) -> usize {
        std::mem::size_of::<Datapoint<E>>() * self.capacity as usize
    }

    pub fn add_datapoint(&mut self, block_ptr: *const Block<E>, datapoint: Datapoint<E>) {
        unsafe {
            let datapoints_ptr = (block_ptr as *const u8).add(std::mem::size_of::<Block<E>>() + self.offset) as *mut Datapoint<E>;
            *datapoints_ptr.add(self.count as usize) = datapoint;
        }

        self.count += 1;
    }

    pub fn datapoints(&self, block_ptr: *const Block<E>) -> &[Datapoint<E>] {
        unsafe {
            let datapoints_ptr = (block_ptr as *const u8).add(std::mem::size_of::<Block<E>>() + self.offset) as *const Datapoint<E>;
            std::slice::from_raw_parts(datapoints_ptr, self.count as usize)
        }
    }

    pub fn datapoints_mut(&self, block_ptr: *const Block<E>) -> &mut [Datapoint<E>] {
        unsafe {
            let datapoints_ptr = (block_ptr as *const u8).add(std::mem::size_of::<Block<E>>() + self.offset) as *mut Datapoint<E>;
            std::slice::from_raw_parts_mut(datapoints_ptr, self.count as usize)
        }
    }

    pub fn replace_at(&mut self, block_ptr: *const Block<E>, other: &mut SubBlock<E>) {
        self.count = other.count;
        self.datapoints_mut(block_ptr).clone_from_slice(other.datapoints(block_ptr));
        other.free();
    }
}

impl<E: Copy> Default for SubBlock<E> {
    fn default() -> Self {
        SubBlock {
            offset: 0,
            capacity: 0,
            count: 0,
            tags: 0,
            _phantom: Default::default()
        }
    }
}