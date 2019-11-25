use a19_concurrent::buffer::mmap_buffer::MemoryMappedInt;
use a19_concurrent::buffer::DirectByteBuffer;
use a19_concurrent::buffer::atomic_buffer::AtomicByteBuffer;
use a19_concurrent::buffer::{align};
use std::sync::atomic::{ fence, Ordering };
use std::path::Path;

/// Represents the storage of files.   It supports a singler writer with multiple readers.  This is
/// the building blocks for raft protocol.
pub struct MessageFileStore {
    /// The buffer we are writing to.
    buffer: MemoryMappedInt
}

const MESSAGE_ID: usize = 8;
const MESSAGE_TYPE: usize = 4;
const MESSAGE_SIZE: usize = 0;
const HEADER_SIZE: usize = 16;
const ALIGNMENT: usize = HEADER_SIZE;
const PADDING_MESSAGE_TYPE: i32 = -1;
const NOOP_MESSAGE_TYPE: i32 = -2;

/// Buffer format
///  0                   1                   2                   3
///  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
/// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
/// | Message Size total message size                               |
/// +---------------------------------------------------------------+ 32
/// | Message Type                                                  |
/// +---------------------------------------------------------------+ 64
/// | Message Id                                                    |
/// |                                                               | 96
/// |                                                               | 
/// +---------------------------------------------------------------+ 128
/// |                   Message Body                                ...
/// ...                                                             |
/// +---------------------------------------------------------------+
impl MessageFileStore {

    /// Creates a new file store.
    /// # Arguments
    /// path - The path to crate the file.
    /// file_size - The size of the file to crate.  This is preallocated for performance reasons.
    pub unsafe fn new<P: AsRef<Path>>(
        path: &P,
        file_size: usize) -> std::io::Result<Self> {
        // Need to make sure the file size is aligned correctly.
        let buffer = MemoryMappedInt::new(path, align(file_size, ALIGNMENT))?;
        Ok(MessageFileStore {
            buffer
        })
    }

}

type Result<T> = std::result::Result<T, Error>;

/// Potential message errors.
#[derive(Debug)]
pub enum Error {
    Full,
    InvalidMessageType(i32),
    FileError(std::io::Error),
    PositionOutOfRange(usize),
    NotEnoughSpace{message_size: u32, position: usize, capacity: usize, remaining: usize}
}

/// Represents the storage of messages.
pub trait MessageStore {

    /// The size of the file.
    fn size(&self) -> usize;

    /// Gets the maximum length of a message we can store in the file.
    fn max_message_size(&self) -> usize;

    /// Reads a message at a specified position.
    /// # Arguments
    /// `pos` - The starting position of the message.
    /// `act` - The action to run.
    fn read<'a>(&'a self,
        pos: &usize,
        act: fn(
            msg_type: i32,
            message_id: u64,
            bytes: &'a [u8])) -> Result<()>;

    /// Writes a message to the buffer.
    /// # Arguments
    /// `posiiton` - The position to write to the buffer.
    /// `msg_type_id` - The type of the message.
    /// `buffer` - THe buffer for the message.
    /// # returns
    /// The next position in the buffer.
    fn write(&mut self,
        position: &usize,
        msg_type_id: &i32,
        message_id: &u64,
        buffer: &[u8]) -> Result<usize>;
}

impl MessageFileStore {

    /// Calculates the position.
    /// # Arguments
    /// `position` - The starting position.
    fn calculate_msg_type_pos(position: &usize) -> usize {
        *position + MESSAGE_TYPE
    }

    /// Calculates the message position.
    /// # Arguments
    /// `position` - The starting position.
    fn calculate_msg_size_pos(position: &usize) -> usize {
        *position + MESSAGE_SIZE
    }

    /// Calculates the message position.
    /// # Arguments
    /// `position` - The starting position.
    fn calculate_body_pos(position: &usize) -> usize {
        *position + HEADER_SIZE
    }

    /// Calculates the position of the message id.
    /// # Arguments
    /// `position` - The position to calculate.
    fn calculate_message_id_pos(position: &usize) -> usize {
        *position + MESSAGE_ID
    }

    /// Forces the file to flush to disk.
    pub fn flush(&mut self) -> Result<()> {
        match self.buffer.flush() {
            Ok(_) => {
                Ok(())
            },
            Err(e) => {
                Err(Error::FileError(e))
            }
        }
    }
}


impl MessageStore for MessageFileStore {

    /// The size of the file.
    fn size(&self) -> usize {
        self.buffer.capacity()
    }

    /// Gets the maximum length of a message we can store in the file.
    fn max_message_size(&self) -> usize {
        self.buffer.max_message_size()
    }

    /// Reads a message at a specified position.
    /// # Arguments
    /// `pos` - The starting position of the message.
    /// `act` - The action to run.
    fn read<'a>(&'a self,
        pos: &usize,
        act: fn(
            msg_type: i32,
            message_id: u64,
            bytes: &'a [u8])) -> Result<()> {
        if *pos > self.size() - ALIGNMENT {
            Err(Error::PositionOutOfRange(*pos))
        } else {
            let size = self.buffer.get_u32(pos);
            let aligned = align(size as usize, ALIGNMENT);
            // Check to see if we have enough space.
            let remaining = self.buffer.capacity() - *pos;
            if remaining < aligned {
                Err(Error::NotEnoughSpace{
                    capacity: self.buffer.capacity(), 
                    message_size: size,
                    position: *pos,
                    remaining})
            } else {
                fence(Ordering::Acquire);
                let message_type = self.buffer.get_i32(&MessageFileStore::calculate_msg_type_pos(pos));
                let message_id = self.buffer.get_u64(&MessageFileStore::calculate_message_id_pos(pos));
                let body_size = size as usize - HEADER_SIZE;
                let bytes = self.buffer.get_bytes(&MessageFileStore::calculate_body_pos(pos), &body_size);
                act(message_type, message_id, bytes);
                Ok(())
            }
        }
    }

    /// Writes a message to the buffer.
    /// # Arguments
    /// `posiiton` - The position to write to the buffer.
    /// `msg_type_id` - The type of the message.
    /// `buffer` - THe buffer for the message.
    /// # returns
    /// The next position in the buffer.
    fn write(&mut self,
        position: &usize,
        msg_type_id: &i32,
        message_id: &u64,
        buffer: &[u8]) -> Result<usize> {
        let size = HEADER_SIZE + buffer.len();
        let aligned = align(size, ALIGNMENT); // This is the aligned value at 32 bits
        if *msg_type_id < 0 {
            Err(Error::InvalidMessageType(*msg_type_id))
        } else if self.size() < *position {
            Err(Error::PositionOutOfRange(*position))
        } else if aligned > (self.size() - *position) {
            Err(Error::Full)
        } else {
            let message_id_pos = MessageFileStore::calculate_message_id_pos(
                position
            );
            let message_size_pos = MessageFileStore::calculate_msg_size_pos(
                position);
            let message_type_pos = MessageFileStore::calculate_msg_type_pos(
                position);
            let message_body = MessageFileStore::calculate_body_pos(
                position);
            // Already verified it is small enought to fit in a u32.
            let size = size as u32;
            self.buffer.put_i32(&message_type_pos, *msg_type_id);
            self.buffer.put_u64(&message_id_pos, *message_id);
            self.buffer.write_bytes(&message_body, buffer);
            self.buffer.put_u32_volatile(&message_size_pos, &size);
            Ok(aligned)
        }
    }

}

#[cfg(test)]
mod tests {

    use crate::file::{ MessageFileStore, MessageStore };
    use std::fs::remove_file;
    use std::path::Path;

    #[test]
    pub fn create_test() {
        let test_file = Path::new("/home/mrh0057/rust_file_test");
        if test_file.exists() {
            remove_file(test_file).unwrap();
        }
        
        let mut store = unsafe { MessageFileStore::new(
            &test_file,
            2048).unwrap()
        };
        
        let bytes: Vec<u8>  = vec!(10, 11, 12, 13, 14, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32);
        store.write(&0, &1, &2, &bytes[0..8]).unwrap();
        store.flush().unwrap();
        store.read(&0, |msg_type, message_id, body| {
            assert_eq!(msg_type, 1);
            assert_eq!(message_id, 2);
            assert_eq!(body.len(), 8);
        }).unwrap();
    }
}
