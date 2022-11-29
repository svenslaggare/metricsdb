use std::marker::PhantomData;
use std::path::{Component, Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

use crate::storage::memory_file::MemoryFile;
use crate::model::{Datapoint, MetricError, MetricResult, Tags, Time};
use crate::storage::{MetricStorage, MetricStorageConfig};

const STORAGE_MAX_SIZE: usize = 8 * 1024 * 1024 * 1024;
const INDEX_MAX_SIZE: usize = 1024 * 1024;
const SYNC_INTERVAL: Duration = Duration::new(2, 0);

pub struct FileMetricStorage<E> {
    base_path: PathBuf,
    metadata_file: MemoryFile,
    segments: Vec<Segment<E>>,
    last_sync: std::time::Instant,
    requires_sync: bool,
    _phantom: PhantomData<E>,
}

impl<E: Copy> FileMetricStorage<E> {
    fn initialize(&mut self, config: &MetricStorageConfig) -> MetricResult<()> {
        unsafe {
            *self.metadata_mut() = Metadata {
                max_segments: config.max_segments,
                segment_duration: config.segment_duration,
                block_duration: config.block_duration,
                datapoint_duration: config.datapoint_duration,
                num_segments: self.segments.len()
            };

            self.metadata_file.sync(self.metadata() as *const u8, std::mem::size_of::<Metadata>(), false)?;
        }

        Ok(())
    }

    unsafe fn metadata(&self) -> *const Metadata {
        std::mem::transmute(self.metadata_file.ptr())
    }

    unsafe fn metadata_mut(&mut self) -> *mut Metadata {
        std::mem::transmute(self.metadata_file.ptr_mut())
    }

    fn num_blocks_per_segment(&self) -> usize {
        (((self.segment_duration() / self.block_duration() + 9) / 10) * 10) as usize
    }

    fn active_segment_mut(&mut self) -> &mut Segment<E> {
        self.segments.last_mut().unwrap()
    }

    fn active_segment(&self) -> &Segment<E> {
        self.segments.last().unwrap()
    }

    fn max_segments(&self) -> Option<usize> {
        unsafe { (*self.metadata()).max_segments }
    }

    fn block_at_ptr(&self, index: usize) -> Option<*const Block<E>> {
        let num_blocks_per_segment = self.num_blocks_per_segment();
        let (segment_index, index) = (index / num_blocks_per_segment, index % num_blocks_per_segment);
        self.segments[segment_index].block_at_ptr(index)
    }

    fn try_sync_active_block(&mut self) {
        if self.requires_sync && ((std::time::Instant::now() - self.last_sync) >= SYNC_INTERVAL) {
            let ok = unsafe {
                let active_block_ptr = self.active_segment().active_block() as *const u8;
                let active_block_size = (*self.active_segment().active_block()).size;
                self.active_segment_mut().storage_file.sync(active_block_ptr, active_block_size, false).is_ok()
            };

            if ok {
                self.last_sync = std::time::Instant::now();
                self.requires_sync = false;
            }
        }
    }

    fn create_segment(&mut self) -> MetricResult<()> {
        let new_segment = Segment::new(
            &self.base_path,
            unsafe { (*self.metadata()).num_segments },
        )?;

        let active_segment = self.active_segment_mut();

        unsafe {
            let shrink_amount = (*active_segment.active_block_mut()).compact();
            active_segment.storage_file.shrink(shrink_amount);
            active_segment.storage_file.sync(
                active_segment.active_block() as *const u8,
                (*active_segment.active_block()).size,
                false
            )?;
        }

        self.segments.push(new_segment);

        unsafe {
            (*self.metadata_mut()).num_segments += 1;
            self.metadata_file.sync(self.metadata() as *const u8, std::mem::size_of::<Metadata>(), false)?;
        }

        Ok(())
    }

    fn try_remove_segments(&mut self) -> MetricResult<()> {
        if let Some(max_segments) = self.max_segments() {
            if self.segments.len() > max_segments {
                let segment = self.segments.remove(0);
                if let Err(err) = segment.remove() {
                    self.segments.insert(0, segment);
                    return Err(err);
                }
            }
        }

        Ok(())
    }
}

impl<E: Copy> MetricStorage<E> for FileMetricStorage<E> {
    fn new(base_path: &Path, config: MetricStorageConfig) -> Result<Self, MetricError> {
        let mut storage = FileMetricStorage {
            base_path: base_path.to_owned(),
            metadata_file: MemoryFile::new(&base_path.join("metadata"), std::mem::size_of::<Metadata>(), true)?,
            segments: vec![Segment::new(base_path, 0)?],
            last_sync: std::time::Instant::now(),
            requires_sync: false,
            _phantom: Default::default()
        };

        storage.initialize(&config)?;

        Ok(storage)
    }

    fn from_existing(base_path: &Path) -> Result<Self, MetricError> {
        let mut segments = Vec::new();
        for entry in std::fs::read_dir(base_path).map_err(|err| MetricError::FailedToLoadMetric(err))? {
            if let Ok(entry) = entry {
                if let Some(Component::Normal(component)) = entry.path().components().last() {
                    if let Some(component) = component.to_str() {
                        if component.ends_with(".storage") {
                            if let Some(segment_index) = component.split(".").next().map(|part| usize::from_str(part).ok()).flatten() {
                                segments.push((segment_index, Segment::from_existing(base_path, segment_index)?));
                            }
                        }
                    }
                }
            }
        }

        segments.sort_by_key(|(index, _)| *index);
        let segments = segments.into_iter().map(|(_, segment)| segment).collect::<Vec<_>>();

        Ok(
            FileMetricStorage {
                base_path: base_path.to_owned(),
                metadata_file: MemoryFile::new(&base_path.join("metadata"), std::mem::size_of::<Metadata>(), false)?,
                segments,
                last_sync: std::time::Instant::now(),
                requires_sync: false,
                _phantom: Default::default()
            }
        )
    }

    fn segment_duration(&self) -> u64 {
        unsafe { (*self.metadata()).segment_duration }
    }

    fn block_duration(&self) -> u64 {
        unsafe { (*self.metadata()).block_duration }
    }

    fn datapoint_duration(&self) -> u64 {
        unsafe { (*self.metadata()).datapoint_duration }
    }

    fn len(&self) -> usize {
        self.segments.iter().map(|segment| segment.len()).sum()
    }

    fn num_segments(&self) -> usize {
        self.segments.len()
    }

    fn time_range(&self) -> Option<(Time, Time)> {
        let mut start_time = None;
        let mut end_time = None;

        for segment in &self.segments {
            if let Some((segment_start, segment_end)) = segment.time_range() {
                start_time = start_time.map(|time: Time| time.min(segment_start)).or(Some(segment_start));
                end_time = end_time.map(|time: Time| time.max(segment_end)).or(Some(segment_end));
            }
        }

        match (start_time, end_time) {
            (Some(start_time), Some(end_time)) => Some((start_time, end_time)),
            _ => None
        }
    }

    fn block_time_range(&self, index: usize) -> Option<(Time, Time)> {
        let block_ptr = self.block_at_ptr(index)?;
        unsafe { Some((*block_ptr).time_range()) }
    }

    fn active_block_time_range(&self) -> Option<(Time, Time)> {
        self.active_segment().active_block_time_range()
    }

    fn active_block_datapoints_mut(&mut self, tags: Tags) -> Option<&mut [Datapoint<E>]> {
        self.active_segment_mut().active_block_datapoints_mut(tags)
    }

    fn create_block(&mut self, time: Time) -> Result<(), MetricError> {
        if self.active_segment().len() >= self.num_blocks_per_segment() {
            self.create_segment()?;
        }

        self.try_remove_segments()?;

        let active_segment = self.active_segment_mut();

        unsafe {
            active_segment.storage_file.try_grow_file(std::mem::size_of::<Block<E>>())?;
            active_segment.index_file.try_grow_file(std::mem::size_of::<usize>())?;

            if active_segment.has_blocks() {
                let shrink_amount = (*active_segment.active_block_mut()).compact();
                active_segment.storage_file.shrink(shrink_amount);

                active_segment.storage_file.sync(active_segment.active_block() as *const u8, (*active_segment.active_block()).size, false)?;

                (*active_segment.header_mut()).active_block_start += (*active_segment.active_block()).size;
                (*active_segment.header_mut()).active_block_index += 1;
            }

            *active_segment.active_block_mut() = Block::new(time);
            (*active_segment.header_mut()).num_blocks += 1;
            *active_segment.index_mut().add((*active_segment.header()).active_block_index) = (*active_segment.header()).active_block_start;

            let header_ptr = active_segment.header_mut() as *const u8;
            active_segment.storage_file.sync(header_ptr, std::mem::size_of::<Header>(), false)?;

            let index_ptr = active_segment.index_mut().add((*active_segment.header()).active_block_index) as *const u8;
            active_segment.index_file.sync(index_ptr, std::mem::size_of::<usize>(), false)?;
        }

        Ok(())
    }

    fn add_datapoint(&mut self, tags: Tags, datapoint: Datapoint<E>) -> Result<(), MetricError> {
        let active_segment = self.active_segment_mut();
        unsafe {
            let active_block = active_segment.active_block_mut();
            let datapoint_time = (*active_block).start_time + datapoint.time_offset as Time;

            let sub_block = active_segment.allocate_sub_block_for_insertion(active_block, tags)?;
            sub_block.add_datapoint(active_block, datapoint);
            (*active_block).end_time = (*active_block).end_time.max(datapoint_time);

            self.requires_sync = true;
        }

        self.try_sync_active_block();

        Ok(())
    }

    type BlockIterator<'a> = SubBlockDatapointsIterator<'a, E> where E: 'a;
    fn block_datapoints<'a>(&'a self, block_index: usize) -> Option<Self::BlockIterator<'a>> {
        let block_ptr = self.block_at_ptr(block_index)?;
        Some(SubBlockDatapointsIterator::new(unsafe { &*block_ptr }))
    }

    fn scheduled(&mut self) {
        self.try_sync_active_block();
    }
}

pub struct Segment<E> {
    storage_file: MemoryFile,
    index_file: MemoryFile,
    _phantom: PhantomData<E>,
}

impl<E: Copy> Segment<E> {
    fn new(base_path: &Path, segment_index: usize) -> Result<Self, MetricError> {
        let mut segment = Segment {
            storage_file: MemoryFile::new(&base_path.join(Path::new(&format!("{}.storage", segment_index))), STORAGE_MAX_SIZE, true)?,
            index_file: MemoryFile::new(&base_path.join(Path::new(&format!("{}.index", segment_index))), INDEX_MAX_SIZE, true)?,
            _phantom: Default::default()
        };

        segment.initialize();
        Ok(segment)
    }

    fn from_existing(base_path: &Path, segment_index: usize) -> Result<Self, MetricError> {
        Ok(
            Segment {
                storage_file: MemoryFile::new(&base_path.join(Path::new(&format!("{}.storage", segment_index))), STORAGE_MAX_SIZE, false)?,
                index_file: MemoryFile::new(&base_path.join(Path::new(&format!("{}.index", segment_index))), INDEX_MAX_SIZE, false)?,
                _phantom: Default::default()
            }
        )
    }

    fn initialize(&mut self) {
        unsafe {
            *self.header_mut() = Header {
                num_blocks: 0,
                active_block_index: 0,
                active_block_start: std::mem::size_of::<Header>()
            };
        }
    }

    fn allocate_sub_block_for_insertion(&mut self,
                                        block_ptr: *mut Block<E>,
                                        tags: Tags) -> MetricResult<&mut SubBlock<E>> {
        let default_capacity = 100;
        let growth_factor = 2;

        unsafe {
            if let Some((sub_block_index, sub_block)) = (*block_ptr).find_sub_block(tags) {
                if sub_block.count < sub_block.capacity {
                    Ok(sub_block)
                } else {
                    let desired_capacity = sub_block.count * growth_factor;
                    if let Some(increased_capacity) = (*block_ptr).try_extend(&mut self.storage_file, sub_block_index, sub_block, desired_capacity)? {
                        let size = increased_capacity as usize * std::mem::size_of::<Datapoint<E>>();
                        (*block_ptr).size += size;
                        Ok(sub_block)
                    } else {
                        let (new_sub_block, allocated) = (*block_ptr).allocate_sub_block(&mut self.storage_file, tags, desired_capacity)?;
                        if allocated {
                            (*block_ptr).size += new_sub_block.size();
                        }

                        new_sub_block.replace_at(block_ptr, sub_block);
                        Ok(new_sub_block)
                    }
                }
            } else {
                let (sub_block, allocated) = (*block_ptr).allocate_sub_block(&mut self.storage_file, tags, default_capacity)?;
                if allocated {
                    (*block_ptr).size += sub_block.size();
                }

                Ok(sub_block)
            }
        }
    }

    fn remove(&self) -> MetricResult<()> {
        std::fs::remove_file(self.storage_file.path()).map_err(|err| MetricError::FailedToRemoveMetric(err))?;

        // Ok if failed, because we use the storage file to define if a segment exists or not
        #[allow(unused_must_use)] {
            std::fs::remove_file(self.index_file.path()).map_err(|err| MetricError::FailedToRemoveMetric(err));
        }

        Ok(())
    }

    fn len(&self) -> usize {
        unsafe { (*self.header()).num_blocks }
    }

    fn has_blocks(&self) -> bool {
        self.len() > 0
    }

    fn time_range(&self) -> Option<(Time, Time)> {
        let (start_time, _) = self.block_time_range(0)?;
        let (_, end_time) = self.block_time_range(self.len() - 1)?;
        Some((start_time, end_time))
    }

    fn block_time_range(&self, index: usize) -> Option<(Time, Time)> {
        unsafe { self.block_at_ptr(index).map(|block| (*block).time_range()) }
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

    fn block_at_ptr(&self, index: usize) -> Option<*const Block<E>> {
        if index >= self.len() {
            return None;
        }

        unsafe {
            let block_offset = *self.index().add(index);
            Some(self.storage_file.ptr().add(block_offset) as *const Block<E>)
        }
    }

    unsafe fn active_block(&self) -> *const Block<E> {
        std::mem::transmute(self.storage_file.ptr().add((*self.header()).active_block_start))
    }

    unsafe fn active_block_mut(&mut self) -> *mut Block<E> {
        std::mem::transmute(self.storage_file.ptr_mut().add((*self.header()).active_block_start))
    }

    fn active_block_time_range(&self) -> Option<(Time, Time)> {
        if !self.has_blocks() {
            return None;
        }

        unsafe { Some(((*self.active_block()).start_time, (*self.active_block()).end_time)) }
    }

    fn active_block_datapoints_mut(&mut self, tags: Tags) -> Option<&mut [Datapoint<E>]> {
        if !self.has_blocks() {
            return None;
        }

        unsafe {
            (*self.active_block_mut()).datapoints_mut(tags)
        }
    }
}

#[repr(C)]
struct Metadata {
    max_segments: Option<usize>,
    segment_duration: u64,
    block_duration: u64,
    datapoint_duration: u64,
    num_segments: usize
}

#[repr(C)]
struct Header {
    num_blocks: usize,
    active_block_index: usize,
    active_block_start: usize
}

#[repr(C)]
struct Block<E: Copy> {
    size: usize,
    start_time: Time,
    end_time: Time,
    num_sub_blocks: usize,
    next_sub_block_offset: u32,
    _phantom: PhantomData<E>
}

impl<E: Copy> Block<E> {
    pub fn new(time: Time) -> Block<E> {
        Block {
            size: std::mem::size_of::<Block<E>>(),
            start_time: time,
            end_time: time,
            num_sub_blocks: 0,
            next_sub_block_offset: 0,
            _phantom: Default::default()
        }
    }

    pub fn compact(&mut self) -> usize {
        let block_ptr = self as *const Block<E>;

        let mut valid_sub_blocks = Vec::new();
        for (_, sub_block) in SubBlockMutIterator::new(self) {
            if sub_block.count > 0 {
                valid_sub_blocks.push((
                    sub_block.clone(),
                    sub_block.datapoints(block_ptr).iter().cloned().collect::<Vec<_>>()
                ));
            }
        }

        let mut new_size = std::mem::size_of_val(self);
        let mut num_sub_blocks = 0;
        let mut next_sub_block_offset = 0;
        for (sub_block_data, datapoints) in valid_sub_blocks.into_iter() {
            let sub_block = unsafe {
                let sub_block_offset = std::mem::size_of::<Block<E>>() + next_sub_block_offset as usize;
                &mut *((block_ptr as *const u8).add(sub_block_offset) as *mut SubBlock<E>)
            };

            *sub_block = sub_block_data;
            sub_block.offset = next_sub_block_offset;
            sub_block.datapoints_mut(block_ptr).clone_from_slice(&datapoints);

            num_sub_blocks += 1;
            next_sub_block_offset += sub_block.size() as u32;
            new_size += sub_block.size();
        }

        self.num_sub_blocks = num_sub_blocks;
        self.next_sub_block_offset = next_sub_block_offset;
        let decreased = self.size - new_size;
        self.size = new_size;
        decreased
    }

    pub fn datapoints_mut(&mut self, tags: Tags) -> Option<&mut [Datapoint<E>]> {
        let block_ptr = self as *mut Block<E>;
        let (_, sub_block) = self.find_sub_block(tags)?;
        Some(sub_block.datapoints_mut(block_ptr))
    }

    pub fn find_sub_block(&mut self, tags: Tags) -> Option<(usize, &mut SubBlock<E>)> {
        for (index, sub_block) in SubBlockMutIterator::new(self) {
            if sub_block.count > 0 && sub_block.tags == tags {
                return Some((index, sub_block));
            }
        }

        None
    }

    pub fn allocate_sub_block(&mut self,
                              storage_file: &mut MemoryFile,
                              tags: Tags,
                              capacity: u32) -> MetricResult<(&mut SubBlock<E>, bool)> {
        // Try using existing
        for (_, sub_block) in SubBlockMutIterator::new(self) {
            if sub_block.count == 0 && sub_block.capacity >= capacity {
                sub_block.tags = tags;
                return Ok((sub_block, false));
            }
        }

        // Allocate new
        storage_file.try_grow_file(
            std::mem::size_of::<SubBlock<E>>() + capacity as usize * std::mem::size_of::<Datapoint<E>>()
        ).map_err(|err| MetricError::MemoryFileError(err))?;

        let sub_block = unsafe {
            let block_ptr = self as *mut Block<E> as *const u8;
            &mut *(block_ptr.add(std::mem::size_of::<Block<E>>() + self.next_sub_block_offset as usize) as *mut SubBlock<E>)
        };

        sub_block.offset = self.next_sub_block_offset;
        sub_block.capacity = capacity;
        sub_block.count = 0;
        sub_block.tags = tags;

        self.num_sub_blocks += 1;
        self.next_sub_block_offset += sub_block.size() as u32;
        return Ok((sub_block, true));
    }

    pub fn try_extend(&mut self,
                      storage_file: &mut MemoryFile,
                      index: usize,
                      sub_block: &mut SubBlock<E>,
                      new_capacity: u32) -> MetricResult<Option<u32>> {
        if index == self.num_sub_blocks - 1 {
            assert!(new_capacity > sub_block.capacity);
            let increased_capacity = new_capacity - sub_block.capacity;

            let size = increased_capacity as usize * std::mem::size_of::<Datapoint<E>>();
            storage_file.try_grow_file(size).map_err(|err| MetricError::MemoryFileError(err))?;

            self.next_sub_block_offset += (increased_capacity as usize * std::mem::size_of::<Datapoint<E>>()) as u32;
            sub_block.capacity = new_capacity;
            Ok(Some(increased_capacity))
        } else {
            Ok(None)
        }
    }

    pub fn time_range(&self) -> (Time, Time) {
        (self.start_time, self.end_time)
    }
}

#[derive(Clone, Copy)]
#[repr(C)]
struct SubBlock<E: Copy> {
    offset: u32,
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

    pub fn size(&self) -> usize {
        std::mem::size_of::<SubBlock<E>>() + (std::mem::size_of::<Datapoint<E>>() * self.capacity as usize)
    }

    pub fn add_datapoint(&mut self, block_ptr: *const Block<E>, datapoint: Datapoint<E>) {
        self.count += 1;
        *self.datapoints_mut(block_ptr).last_mut().unwrap() = datapoint;
    }

    pub fn datapoints(&self, block_ptr: *const Block<E>) -> &[Datapoint<E>] {
        unsafe {
            let datapoints_ptr = (block_ptr as *const u8).add(self.datapoints_offset()) as *const Datapoint<E>;
            std::slice::from_raw_parts(datapoints_ptr, self.count as usize)
        }
    }

    pub fn datapoints_mut(&self, block_ptr: *const Block<E>) -> &mut [Datapoint<E>] {
        unsafe {
            let datapoints_ptr = (block_ptr as *const u8).add(self.datapoints_offset()) as *mut Datapoint<E>;
            std::slice::from_raw_parts_mut(datapoints_ptr, self.count as usize)
        }
    }

    fn datapoints_offset(&self) -> usize {
        std::mem::size_of::<Block<E>>() + self.offset as usize + std::mem::size_of::<SubBlock<E>>()
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

pub struct SubBlockDatapointsIterator<'a, E: Copy> {
    iterator: SubBlockIterator<'a, E>
}

impl<'a, E: Copy> SubBlockDatapointsIterator<'a, E> {
    fn new(block: &'a Block<E>) -> SubBlockDatapointsIterator<'a, E> {
        SubBlockDatapointsIterator {
            iterator: SubBlockIterator::new(block)
        }
    }
}

impl<'a, E: Copy> Iterator for SubBlockDatapointsIterator<'a, E> {
    type Item = (Tags, &'a [Datapoint<E>]);

    fn next(&mut self) -> Option<Self::Item> {
        let block_ptr = self.iterator.block as *const Block<E>;
        while let Some((_, sub_block)) = self.iterator.next() {
            if sub_block.count > 0 {
                return Some((sub_block.tags, sub_block.datapoints(block_ptr)))
            }
        }

        return None;
    }
}

struct SubBlockMutIterator<'a, E: Copy> {
    block: *const Block<E>,
    index: usize,
    offset: usize,
    _phantom: PhantomData<&'a E>
}

impl<'a, E: Copy> SubBlockMutIterator<'a, E> {
    pub fn new(block: *const Block<E>) -> SubBlockMutIterator<'a, E> {
        SubBlockMutIterator {
            block,
            index: 0,
            offset: std::mem::size_of::<Block<E>>(),
            _phantom: Default::default()
        }
    }
}

impl<'a, E: Copy> Iterator for SubBlockMutIterator<'a, E> {
    type Item = (usize, &'a mut SubBlock<E>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= unsafe { (*self.block).num_sub_blocks } {
            return None;
        }

        // Not really legal
        let block_ptr = self.block as *mut u8;

        let index = self.index;
        let sub_block = unsafe { &mut *(block_ptr.add(self.offset) as *mut SubBlock<E>) };

        self.offset += sub_block.size();
        self.index += 1;
        return Some((index, sub_block));
    }
}

struct SubBlockIterator<'a, E: Copy> {
    block: &'a Block<E>,
    index: usize,
    offset: usize,
}

impl<'a, E: Copy> SubBlockIterator<'a, E> {
    pub fn new(block: &'a Block<E>) -> SubBlockIterator<'a, E> {
        SubBlockIterator {
            block,
            index: 0,
            offset: std::mem::size_of_val(block)
        }
    }
}

impl<'a, E: Copy> Iterator for SubBlockIterator<'a, E> {
    type Item = (usize, &'a SubBlock<E>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.block.num_sub_blocks {
            return None;
        }

        let block_ptr = self.block as *const Block<E> as *const u8;

        let index = self.index;
        let sub_block = unsafe { &mut *(block_ptr.add(self.offset) as *mut SubBlock<E>) };

        self.offset += sub_block.size();
        self.index += 1;
        return Some((index, sub_block));
    }
}
