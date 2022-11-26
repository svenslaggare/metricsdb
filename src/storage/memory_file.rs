use std::ffi::{c_void};
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom};
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub enum MemoryFileError {
    FailedToMap(std::io::Error),
    FailedToSync,
    IO(std::io::Error)
}

pub struct MemoryFile {
    path: PathBuf,
    address: *mut c_void,
    size: usize,
    backing_size: usize,
    file: File
}

const PAGE_SIZE: usize = 4096;

impl MemoryFile {
    pub fn new(path: &Path, size: usize, create: bool) -> Result<MemoryFile, MemoryFileError> {
        let mut file = if create {
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(path)
                .map_err(|err| MemoryFileError::IO(err))?
        } else {
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(path)
                .map_err(|err| MemoryFileError::IO(err))?
        };

        let backing_size = if create {
            let backing_size = PAGE_SIZE as u64;
            file.set_len(backing_size).map_err(|err| MemoryFileError::IO(err))?;
            backing_size as usize
        } else {
            file_size(&mut file).map_err(|err| MemoryFileError::IO(err))? as usize
        };

        let address = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                file.as_raw_fd(),
                0
            )
        };

        if address == libc::MAP_FAILED {
            return Err(MemoryFileError::FailedToMap(std::io::Error::last_os_error()));
        }

        Ok(
            MemoryFile {
                path: path.to_owned(),
                address,
                size,
                file,
                backing_size
            }
        )
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn try_grow_file(&mut self, amount: usize) -> Result<(), MemoryFileError> {
        self.backing_size += amount;
        let actual_size = file_size(&mut self.file).map_err(|err| MemoryFileError::IO(err))? as usize;
        if self.backing_size > actual_size {
            let page_size = PAGE_SIZE as u64;
            self.file.set_len(
                ((self.backing_size as u64 + page_size - 1) / page_size) * page_size
            ).map_err(|err| MemoryFileError::IO(err))?;
        }

        Ok(())
    }

    pub fn shrink(&mut self, amount: usize) {
        if amount < self.backing_size {
            self.backing_size -= amount;
        } else {
            self.backing_size = 0;
        }
    }

    pub fn sync(&mut self, address: *const u8, size: usize, is_async: bool) -> Result<(), MemoryFileError> {
        unsafe {
            // The address that we invoke msync with must be aligned to pages
            let address = address as usize;
            let page_address = (address / PAGE_SIZE) * PAGE_SIZE;
            let end_address = address + size;
            let size = end_address - page_address;
            let address = page_address;

            let result = libc::msync(address as *mut _, size, if is_async {libc::MS_ASYNC} else {libc::MS_SYNC});
            if result == 0 {
                Ok(())
            } else {
                Err(MemoryFileError::FailedToSync)
            }
        }
    }

    pub fn bytes(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(self.address as *const u8, self.size)
        }
    }

    pub fn bytes_mut(&mut self) -> &mut [u8] {
        unsafe {
            std::slice::from_raw_parts_mut(self.address as *mut u8, self.size)
        }
    }

    pub fn ptr(&self) -> *const u8 {
        self.address as *mut u8
    }

    pub fn ptr_mut(&mut self) -> *mut u8 {
        self.address as *mut u8
    }
}

fn file_size(file: &mut File) -> std::io::Result<u64> {
    let old_pos = file.stream_position()?;
    let len = file.seek(SeekFrom::End(0))?;
    file.seek(SeekFrom::Start(old_pos))?;
    Ok(len)
}

unsafe impl Send for MemoryFile {}
unsafe impl Sync for MemoryFile {}

impl Drop for MemoryFile {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.address, self.size);
        }
    }
}