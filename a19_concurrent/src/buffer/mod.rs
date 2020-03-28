pub mod atomic_buffer;
pub mod mmap_buffer;
pub mod ring_buffer;

/// Used to find the complement of power of 2.
/// # Arguments
/// `value` - The value to find the complement of.
#[inline]
pub fn complement(value: usize) -> usize {
    let v = value >> 1;
    !v ^ (v - 1)
}

/// Returns the alignment value.  The alignment must be a power of 2.
/// # Arguments
/// `value` - The value to align.
/// `alignment` - The amount of the alignment.  Must be a power of 2.
pub fn align(value: usize, alignment: usize) -> usize {
    (value + (alignment - 1)) & complement(alignment)
}

/// Used to find the next value.  This useful for appending on buffers.
/// # Arguments
/// `value` - The value to find the end of.
/// `amount` - The amount of the value to pad.
pub fn next_pos(value: usize, amount: usize) -> usize {
    let diff = value % amount;
    if diff == 0 {
        value
    } else {
        (amount - diff) + value
    }
}

pub enum ByteOrderType {
    LittleEndian,
    BigEndian,
}

/// A trait for a direct byte buffer.
pub trait DirectByteBuffer {
    /// Used to get the capcity of the buffer.
    fn capacity(&self) -> usize;

    /// used to get a u64 at a location.
    /// # Arguments
    /// `position` - The positon of the byte area.
    fn get_u64(&self, position: &usize) -> u64;

    /// Puts an unsigned long at a location.
    /// `positon` - The positon to put the value.
    /// `value` - The value to put in the buffer.
    fn put_u64(&mut self, position: &usize, value: u64);

    /// Used to get an signed i64.
    /// # Arguments
    /// `position` - The position to load the integer from.
    fn get_i64(&self, position: &usize) -> i64;

    /// Puts an i64 value into the buffer.
    /// # Arguments
    /// `position` - The position to load the integer to.
    fn put_i64(&mut self, position: &usize, value: i64);

    /// Used to get a unsigned u32.
    /// # Arguments
    /// `position` - The positon to load the integer from.
    fn get_u32(&self, position: &usize) -> u32;

    /// Used to put an unsigned int into the buffer.
    /// # Arguments
    /// `position` - The position to put the integer.
    /// `value` - The value to put into the buffer.
    fn put_u32(&mut self, position: &usize, value: u32);

    /// Used to get a signed i32.
    /// # Arguments
    /// `position` - The positon of the signed integer from.
    fn get_i32(&self, position: &usize) -> i32;

    /// Used to put a signed i32 into the buffer.
    /// # Arguments
    /// `position` - The position to put the signed into into.
    /// `value` - The vale to put into the buffer.
    fn put_i32(&mut self, position: &usize, value: i32);

    /// Used to get a an unsigned u16.
    /// # Argments
    /// `position` - The position of the unsighed integer.
    fn get_u16(&self, position: &usize) -> u16;

    /// Used to put an unsigned u16 into the buffer.
    /// # Arguments
    /// `position` - The positon to put the unsigned integer.
    /// `value` - The value to put into the buffer.
    fn put_u16(&mut self, position: &usize, value: u16);

    /// Used to get a a signed i16.
    /// # Arguments
    /// `position` - The position of the signed integer.
    fn get_i16(&self, position: &usize) -> i16;

    /// Used to put a signed i16 into the buffer.
    /// # Arguments
    /// `position` - The position to put the value to.
    /// `value` - The value to place into the buffer.
    fn put_i16(&mut self, position: &usize, value: i16);

    /// Used to get the maximum size of a message that can be put into the buffer.
    fn max_message_size(&self) -> usize;

    /// Used to get the bytes for a range.
    /// # Arguments
    /// `position` - The position of the bytes start.
    /// `length` - The length of the bytes.
    fn get_bytes<'a>(&'a self, position: &usize, length: &usize) -> &'a [u8];

    /// Used to get the bytes as multiple.
    /// `position` - The position of the bytes to start.
    /// `length` - The length of the bytes to get.
    fn as_bytes_mut<'a>(&'a mut self, position: &usize, length: &usize) -> &'a mut [u8];

    /// Set bytes on the buffer.
    /// # Arguments
    /// `position` - The position to starting writing the bytes to.
    /// `value` - The value to write.
    fn set_bytes(&mut self, position: &usize, length: &usize, value: u8);

    /// Writes a slice to the byte buffer.
    /// # Arguments
    /// `position` - The position to write the buffers to.
    /// `bytes` - The bytes to write to the buffer.
    fn write_bytes(&mut self, position: &usize, bytes: &[u8]);
}

#[cfg(test)]
mod test {

    use crate::buffer::{align, complement, next_pos};

    #[test]
    pub fn complement_test() {
        let result: usize = 18446744073709551608;
        assert_eq!(result, complement(8));
    }

    #[test]
    pub fn alignment_test() {
        let result: usize = 16;
        assert_eq!(result, align(9, 8));
        assert_eq!(result, align(10, 8));
        assert_eq!(result, align(11, 8));
        assert_eq!(24, align(17, 8));
        assert_eq!(24, align(18, 8));
        assert_eq!(24, align(19, 8));
        assert_eq!(24, align(20, 8));
        assert_eq!(24, align(21, 8));
        assert_eq!(24, align(22, 8));
        assert_eq!(24, align(23, 8));
        assert_eq!(24, align(24, 8));
    }

    #[test]
    pub fn next_pos_test() {
        let result = 128;
        assert_eq!(result, next_pos(97, 32));
        assert_eq!(result, next_pos(127, 32));
        assert_eq!(result, next_pos(128, 32));
    }
}
