use std::vec::Vec;
use std::sync::atomic::{fence, Ordering};
use byteorder::{ByteOrder, BigEndian};
use crate::buffer::DirectByteBuffer;
use a19_core::pow2::PowOf2;

pub trait AtomicByteBuffer: DirectByteBuffer {

    /// Used to get a u64 volatile value.
    /// # Arguments
    /// `position` - The position to read the volatile u64 from.
    fn get_u64_volatile(&self, position: &usize) -> u64;

    /// Write a u64 volatile value.
    /// # Arguments
    /// `position` - The position to write the unsigned value to.
    /// `value` - The value to write.
    fn put_u64_volatile(&mut self, position: &usize, value: u64);

    /// Get a signed i64 value.
    /// # Arguments
    /// `position` - The position to write the signed value to.
    /// `value` - The value to write.
    fn get_i64_volatile(&self, positon: &usize) -> i64;

    /// Sets a signed i64 value.
    /// # Arguments
    /// `position` - The position to write the signed value to.
    /// `value` - The value to write.
    fn put_i64_volatile(&mut self, position: &usize, value: i64);

    /// Gets an unsigned u32.
    /// # Arguments
    /// `positon` - The position to get the signed integer.
    fn get_u32_volatile(&self, position: &usize) -> u32;

    ///  Puts an unsigned u32.
    ///  # Arguments
    ///  `position` - The position to put the unsiged integer.
    ///  `value` - The value to write at that location.
    fn put_u32_volatile(&mut self, position: &usize, value: u32);

    /// Gets a signed i32.
    /// # Arguments
    /// `position` - The position to get the volatile int from.
    fn get_i32_volatile(&self, position: &usize) -> i32;

    /// Puts a volatile i32 into the buffer.
    /// # Arguments
    /// `position` - The position to read fine.
    /// `value` - The value to put into the buffer.
    fn put_i32_volatile(&mut self, position: &usize, value: i32);

}

pub struct AtomicByteBufferInt {
    capacity: usize,
    buffer: Vec<u8>,
    max_message_size: usize
}

impl AtomicByteBufferInt {

    /// Used to crate a new atomic usize.
    pub fn new(size: usize) -> Self {
        let correct_size = size.round_to_power_of_two();
        let buffer = vec![0; correct_size];
        let max_message_size = correct_size >> 3;
        AtomicByteBufferInt {
            capacity: correct_size,
            buffer,
            max_message_size
        }
    }
}

const LONG_SIZE:usize = 8;
const INT_SIZE:usize = 4;
const SHORT_SIZE:usize = 2;

fn calculate_offset_long(position: &usize) -> usize {
    *position + LONG_SIZE
}

fn calculate_offset_32(position: &usize) -> usize {
    *position + INT_SIZE
}

fn calculate_offset_16(position: &usize) -> usize {
    *position + SHORT_SIZE
}

impl DirectByteBuffer for AtomicByteBufferInt {

    /// Used to get the capcity of the buffer.
    fn capacity(&self) -> usize {
        self.capacity
    }

    /// used to get a u64 at a location.
    /// # Arguments
    /// `position` - The positon of the byte area.
    fn get_u64(&self, position: &usize) -> u64 {
        BigEndian::read_u64(&self.buffer[*position..calculate_offset_long(position)])
    }

    /// Puts an unsigned long at a location.
    /// `positon` - The positon to put the value.
    /// `value` - The value to put in the buffer.
    fn put_u64(&mut self, position: &usize, value: u64) {
        BigEndian::write_u64(&mut self.buffer[*position..calculate_offset_long(position)], value);
    }

    /// Used to get an signed i64.
    /// # Arguments
    /// `position` - The position to load the integer from.
    fn get_i64(&self, position: &usize) -> i64 {
        BigEndian::read_i64(&self.buffer[*position..calculate_offset_long(position)])
    }

    /// Puts an i64 value into the buffer.
    /// # Arguments
    /// `position` - The position to load the integer to.
    fn put_i64(&mut self, position: &usize, value: i64) {
        BigEndian::write_i64(
            &mut self.buffer[*position..calculate_offset_long(position)],
            value)
    }

    /// Used to get a unsigned u32.
    /// # Arguments
    /// `position` - The positon to load the integer from.
    fn get_u32(&self, position: &usize) -> u32 {
        BigEndian::read_u32(&self.buffer[*position..calculate_offset_32(position)])
    }

    /// Used to put an unsigned int into the buffer.
    /// # Arguments
    /// `position` - The position to put the integer.
    /// `value` - The value to put into the buffer.
    fn put_u32(&mut self, position: &usize, value: u32) {
        BigEndian::write_u32(
            &mut self.buffer[*position..calculate_offset_32(position)],
            value);
    }

    /// Used to get a signed i32.
    /// # Arguments
    /// `position` - The positon of the signed integer from.
    fn get_i32(&self, position: &usize) -> i32 {
        BigEndian::read_i32(
            &self.buffer[*position..calculate_offset_32(position)])
    }

    /// Used to put a signed i32 into the buffer.
    /// # Arguments
    /// `position` - The position to put the signed into into.
    /// `value` - The vale to put into the buffer.
    fn put_i32(&mut self, position: &usize, value: i32) {
        BigEndian::write_i32(
            &mut self.buffer[*position..calculate_offset_32(position)], 
            value);
    }

    /// Used to get a an unsigned u16.
    /// # Argments
    /// `position` - The position of the unsighed integer.
    fn get_u16(&self, position: &usize) -> u16 {
        BigEndian::read_u16(
            &self.buffer[*position..calculate_offset_16(position)])
    }

    /// Used to put an unsigned u16 into the buffer.
    /// # Arguments
    /// `position` - The positon to put the unsigned integer.
    /// `value` - The value to put into the buffer.
    fn put_u16(&mut self, position: &usize, value: u16) {
        BigEndian::write_u16(
            &mut self.buffer[*position..calculate_offset_16(position)],
            value)
    }

    /// Used to get a a signed i16.
    /// # Arguments
    /// `position` - The position of the signed integer.
    fn get_i16(&self, position: &usize) -> i16 {
        BigEndian::read_i16(
            &self.buffer[*position..calculate_offset_16(position)])
    }

    /// Used to put a signed i16 into the buffer.
    /// # Arguments
    /// `position` - The position to put the value to.
    /// `value` - The value to place into the buffer.
    fn put_i16(&mut self, position: &usize, value: i16) {
        BigEndian::write_i16(
            &mut self.buffer[*position..calculate_offset_16(position)],
            value)
    }

    /// Used to get the bytes for a range.
    /// # Arguments
    /// `position` - The position of the bytes start.
    /// `length` - The length of the bytes.
    fn get_bytes<'a>(&'a self, position: &usize, length: &usize) -> &'a [u8] {
        & self.buffer[*position..(*position + *length)]
    }

    /// Used to get the bytes as multiple.
    /// `position` - The position of the bytes to start.
    /// `length` - The length of the bytes to get.
    fn as_bytes_mut<'a>(&'a mut self, position: &usize, length: &usize) -> &'a mut [u8] {
        &mut self.buffer[*position..(*position + *length)]
    }

    fn max_message_size(&self) -> usize {
        self.max_message_size
    }
}

/// Have to use fence for now.  Need to find out if this is good enough for the compiler to not
/// cache the values in the buffer.  From the documentation it looks like it is but I'm not really
/// sure.
impl AtomicByteBuffer for AtomicByteBufferInt {
    /// Used to get a u64 volatile value.
    /// # Arguments
    /// `position` - The position to read the volatile u64 from.
    fn get_u64_volatile(&self, position: &usize) -> u64 {
        fence(Ordering::Acquire);
        BigEndian::read_u64(&self.buffer[*position..calculate_offset_long(position)])
    }

    /// Write a u64 volatile value.
    /// # Arguments
    /// `position` - The position to write the unsigned value to.
    /// `value` - The value to write.
    fn put_u64_volatile(&mut self, position: &usize, value: u64) {
        BigEndian::write_u64(
            &mut self.buffer[*position..calculate_offset_long(position)],
            value);
        fence(Ordering::Release);
    }

    /// Get a signed i64 value.
    /// # Arguments
    /// `position` - The position to write the signed value to.
    /// `value` - The value to write.
    fn get_i64_volatile(&self, position: &usize) -> i64 {
        fence(Ordering::Acquire);
        BigEndian::read_i64(&self.buffer[*position..calculate_offset_long(position)])
    }

    /// Sets a signed i64 value.
    /// # Arguments
    /// `position` - The position to write the signed value to.
    /// `value` - The value to write.
    fn put_i64_volatile(&mut self, position: &usize, value: i64) {
        BigEndian::write_i64(
            &mut self.buffer[*position..calculate_offset_long(position)],
            value);
        fence(Ordering::Release);
    }

    /// Gets an unsigned u32.
    /// # Arguments
    /// `positon` - The position to get the signed integer.
    fn get_u32_volatile(&self, position: &usize) -> u32 {
        fence(Ordering::Acquire);
        BigEndian::read_u32(&self.buffer[*position..calculate_offset_long(position)])
    }

    ///  Puts an unsigned u32.
    ///  # Arguments
    ///  `position` - The position to put the unsiged integer.
    ///  `value` - The value to write at that location.
    fn put_u32_volatile(&mut self, position: &usize, value: u32) {
        BigEndian::write_u32(
            &mut self.buffer[*position..calculate_offset_long(position)],
            value);
        fence(Ordering::Release);
    }

    /// Gets an unsigned u32.
    /// # Arguments
    /// `positon` - The position to get the signed integer.
    fn get_i32_volatile(&self, position: &usize) -> i32 {
        fence(Ordering::Acquire);
        BigEndian::read_i32(&self.buffer[*position..calculate_offset_long(position)])
    }

    ///  Puts an unsigned u32.
    ///  # Arguments
    ///  `position` - The position to put the unsiged integer.
    ///  `value` - The value to write at that location.
    fn put_i32_volatile(&mut self, position: &usize, value: i32) {
        BigEndian::write_i32(
            &mut self.buffer[*position..calculate_offset_long(position)],
            value);
        fence(Ordering::Release);
    }
}


#[cfg(test)]
mod tests {

    use crate::buffer::atomic_buffer::{AtomicByteBufferInt, AtomicByteBuffer};

    #[test]
    pub fn create_test() {
        let mut atomic_buffer = AtomicByteBufferInt::new(0x10000);
        atomic_buffer.put_u32_volatile(&0, 123);
        let result = atomic_buffer.get_u32_volatile(&0);
        assert_eq!(123, result);
    }

}
