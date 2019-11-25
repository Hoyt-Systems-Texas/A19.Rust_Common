use std::fs::{ File, OpenOptions };
use std::cell::UnsafeCell;
use std::path::Path;
use std::io::Result;
use std::sync::atomic::{fence, Ordering};
use byteorder::{ByteOrder, BigEndian};
use crate::buffer:: DirectByteBuffer;
use crate::buffer::atomic_buffer::{calculate_offset_32, calculate_offset_16, calculate_offset_long, AtomicByteBuffer};
use memmap::MmapMut;

/// The internal implementation of the memory mapped buffer.
pub struct MemoryMappedInt {

    file: File,
    mmap: MmapMut,
    size: usize,
    max_message_size: usize
}

unsafe impl Sync for MemoryMappedInt {}
unsafe impl Send for MemoryMappedInt {}

impl MemoryMappedInt {
    
    /// Used to create a new memory mapped file with the specified file handler.
    /// # Arguments
    /// `file` - The file handler to map the buffer to.
    pub unsafe fn open(file: File) -> Result<Self> {
        let meta_data = file.metadata()?;
        let size = meta_data.len() as usize;
        let max_message_size = size / 64;
        match MmapMut::map_mut(&file) {
            Ok(mmap) => {
                Ok(MemoryMappedInt {
                    file,
                    mmap,
                    size,
                    max_message_size
                })
            }
            Err(e) => {
                Err(e)
            }
        }
    }

    /// Creates a new memory mapp int file.
    /// # Arguments
    /// `path` - The path of the memory mapped file to create.
    /// `buffer_size` - The size of the buffer to create.
    pub unsafe fn new<P: AsRef<Path>>(
        path: &P,
        buffer_size: usize) -> Result<MemoryMappedInt> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        file.set_len(buffer_size as u64)?;
        let mmap = MmapMut::map_mut(&file)?;
        let max_message_size = buffer_size / 64;
        Ok(MemoryMappedInt{
            file,
            mmap,
            size: buffer_size,
            max_message_size
        })
    }

    /// Forces a flush of the memory mapped file to storage.
    pub fn flush(&self) -> Result<()>{
        self.mmap.flush()
    }
}

impl DirectByteBuffer for MemoryMappedInt {

    fn capacity(&self) -> usize {
        self.size
    }

    fn get_u64(&self, position: &usize) -> u64 {        
        BigEndian::read_u64(&self.mmap[*position..calculate_offset_long(position)])
    }

    /// Puts an unsigned long at a location.
    /// `positon` - The positon to put the value.
    /// `value` - The value to put in the buffer.
    fn put_u64(&mut self, position: &usize, value: u64) {
        BigEndian::write_u64(&mut self.mmap[*position..calculate_offset_long(position)], value);
    }

    /// Used to get an signed i64.
    /// # Arguments
    /// `position` - The position to load the integer from.
    fn get_i64(&self, position: &usize) -> i64 {
        BigEndian::read_i64(&self.mmap[*position..calculate_offset_long(position)])
    }

    /// Puts an i64 value into the buffer.
    /// # Arguments
    /// `position` - The position to load the integer to.
    fn put_i64(&mut self, position: &usize, value: i64) {
        BigEndian::write_i64(
            &mut self.mmap[*position..calculate_offset_long(position)],
            value)
    }

    /// Used to get a unsigned u32.
    /// # Arguments
    /// `position` - The positon to load the integer from.
    fn get_u32(&self, position: &usize) -> u32 {
        BigEndian::read_u32(&self.mmap[*position..calculate_offset_32(position)])
    }

    /// Used to put an unsigned int into the buffer.
    /// # Arguments
    /// `position` - The position to put the integer.
    /// `value` - The value to put into the buffer.
    fn put_u32(&mut self, position: &usize, value: u32) {
        BigEndian::write_u32(
            &mut self.mmap[*position..calculate_offset_32(position)],
            value);
    }

    /// Used to get a signed i32.
    /// # Arguments
    /// `position` - The positon of the signed integer from.
    fn get_i32(&self, position: &usize) -> i32 {
        BigEndian::read_i32(
            &self.mmap[*position..calculate_offset_32(position)])
    }

    /// Used to put a signed i32 into the buffer.
    /// # Arguments
    /// `position` - The position to put the signed into into.
    /// `value` - The vale to put into the buffer.
    fn put_i32(&mut self, position: &usize, value: i32) {
        BigEndian::write_i32(
            &mut self.mmap[*position..calculate_offset_32(position)], 
            value);
    }

    /// Used to get a an unsigned u16.
    /// # Argments
    /// `position` - The position of the unsighed integer.
    fn get_u16(&self, position: &usize) -> u16 {
        BigEndian::read_u16(
            &self.mmap[*position..calculate_offset_16(position)])
    }

    /// Used to put an unsigned u16 into the buffer.
    /// # Arguments
    /// `position` - The positon to put the unsigned integer.
    /// `value` - The value to put into the buffer.
    fn put_u16(&mut self, position: &usize, value: u16) {
        BigEndian::write_u16(
            &mut self.mmap[*position..calculate_offset_16(position)],
            value)
    }

    /// Used to get a a signed i16.
    /// # Arguments
    /// `position` - The position of the signed integer.
    fn get_i16(&self, position: &usize) -> i16 {
        BigEndian::read_i16(
            &self.mmap[*position..calculate_offset_16(position)])
    }

    /// Used to put a signed i16 into the buffer.
    /// # Arguments
    /// `position` - The position to put the value to.
    /// `value` - The value to place into the buffer.
    fn put_i16(&mut self, position: &usize, value: i16) {
        BigEndian::write_i16(
            &mut self.mmap[*position..calculate_offset_16(position)],
            value)
    }

    /// Used to get the bytes for a range.
    /// # Arguments
    /// `position` - The position of the bytes start.
    /// `length` - The length of the bytes.
    fn get_bytes<'a>(&'a self, position: &usize, length: &usize) -> &'a [u8] {
        & self.mmap[*position..(*position + *length)]
    }

    /// Used to get the bytes as multiple.
    /// `position` - The position of the bytes to start.
    /// `length` - The length of the bytes to get.
    fn as_bytes_mut<'a>(&'a mut self, position: &usize, length: &usize) -> &'a mut [u8] {
        &mut self.mmap[*position..(*position + *length)]
    }

    fn max_message_size(&self) -> usize {
        self.max_message_size
    }

    fn set_bytes(&mut self, position: &usize, length: &usize, value: u8) {
        for b in self.as_bytes_mut(position, length).iter_mut() {
            *b = value;
        }
    }

    fn write_bytes(&mut self, position: &usize, bytes: &[u8]) {
        for (d, s) in self.as_bytes_mut(
            position,
            &bytes.len()).iter_mut().zip(
            bytes.iter()) {
                *d = *s;
            }
    }
}

impl AtomicByteBuffer for MemoryMappedInt {

    /// Used to get a u64 volatile value.
    /// # Arguments
    /// `position` - The position to read the volatile u64 from.
    fn get_u64_volatile(&self, position: &usize) -> u64 {
        fence(Ordering::Acquire);
        BigEndian::read_u64(&self.mmap[*position..calculate_offset_long(position)])
    }

    /// Write a u64 volatile value.
    /// # Arguments
    /// `position` - The position to write the unsigned value to.
    /// `value` - The value to write.
    fn put_u64_volatile(&mut self, position: &usize, value: &u64) {
        BigEndian::write_u64(
            &mut self.mmap[*position..calculate_offset_long(position)],
            *value);
        fence(Ordering::Release);
    }

    /// Get a signed i64 value.
    /// # Arguments
    /// `position` - The position to write the signed value to.
    /// `value` - The value to write.
    fn get_i64_volatile(&self, position: &usize) -> i64 {
        fence(Ordering::Acquire);
        BigEndian::read_i64(&self.mmap[*position..calculate_offset_long(position)])
    }

    /// Sets a signed i64 value.
    /// # Arguments
    /// `position` - The position to write the signed value to.
    /// `value` - The value to write.
    fn put_i64_volatile(&mut self, position: &usize, value: &i64) {
        BigEndian::write_i64(
            &mut self.mmap[*position..calculate_offset_long(position)],
            *value);
        fence(Ordering::Release);
    }

    /// Gets an unsigned u32.
    /// # Arguments
    /// `positon` - The position to get the signed integer.
    fn get_u32_volatile(&self, position: &usize) -> u32 {
        fence(Ordering::Acquire);
        BigEndian::read_u32(&self.mmap[*position..calculate_offset_long(position)])
    }

    ///  Puts an unsigned u32.
    ///  # Arguments
    ///  `position` - The position to put the unsiged integer.
    ///  `value` - The value to write at that location.
    fn put_u32_volatile(&mut self, position: &usize, value: &u32) {
        BigEndian::write_u32(
            &mut self.mmap[*position..calculate_offset_long(position)],
            *value);
        fence(Ordering::Release);
    }

    /// Gets an unsigned u32.
    /// # Arguments
    /// `positon` - The position to get the signed integer.
    fn get_i32_volatile(&self, position: &usize) -> i32 {
        fence(Ordering::Acquire);
        BigEndian::read_i32(&self.mmap[*position..calculate_offset_long(position)])
    }

    ///  Puts an unsigned u32.
    ///  # Arguments
    ///  `position` - The position to put the unsiged integer.
    ///  `value` - The value to write at that location.
    fn put_i32_volatile(&mut self, position: &usize, value: &i32) {
        BigEndian::write_i32(
            &mut self.mmap[*position..calculate_offset_long(position)],
            *value);
        fence(Ordering::Release);
    }
}

#[cfg(test)]
mod tests {

    use std::fs::remove_file;
    use std::path::Path;
    use crate::buffer::mmap_buffer::MemoryMappedInt;
    use crate::buffer::atomic_buffer::AtomicByteBuffer;

    #[test]
    pub fn create_test() {
        unsafe {
            let test_file = "/home/mrh0057/insert_test";
            let path = Path::new(test_file);
            if path.exists() {
                remove_file(path).unwrap();
            }
            let size = 128;
            let mut buffer = MemoryMappedInt::new(
                &test_file,
                size).unwrap();
            buffer.put_u32_volatile(&0, &124);
            let result = buffer.get_u32_volatile(&0);
            assert_eq!(124, result);
        }
    }
}
