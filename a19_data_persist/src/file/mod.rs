use a19_concurrent::buffer::mmap_buffer::MemoryMappedInt;
use a19_concurrent::buffer::DirectByteBuffer;
use a19_concurrent::buffer::atomic_buffer::AtomicByteBuffer;
use a19_concurrent::buffer::{align};
use std::fs::OpenOptions;
use std::sync::atomic::{ fence, Ordering };
use std::sync::Arc;
use std::cell::UnsafeCell;
use std::path::Path;

pub type MessageId = u64;
pub type MessageTypeId = i32;

pub struct MessageFileStoreRead {
    store: Arc<UnsafeCell<MessageFileStore>>
}

impl MessageFileStoreRead {

    /// Reads a message at a specified location.
    /// # Arguments
    /// `pos` - The position to read in.
    /// `act` - The action to run for the messages.
    /// # Returns
    /// The position of the next message.
    pub fn read<'a, F>(&'a self,
        pos: usize, 
        act: F)
        -> Result<usize> 
        where F: FnOnce(i32, u64, &'a [u8])
    {
        unsafe {
            let store = &mut *self.store.get();
            store.read(&pos, act)
        }
    }

    pub fn read_new(
        &self,
        pos: &usize) -> Result<MessageRead> {
        unsafe {
            let store = &mut *self.store.get();
            store.read_new(pos)
        }
    }
}

unsafe impl Sync for MessageFileStoreRead {}
unsafe impl Send for MessageFileStoreRead {}

pub struct MessageFileStoreWrite {
    store: Arc<UnsafeCell<MessageFileStore>>
}

impl MessageFileStoreWrite {

    /// Writes a message to the buffer.
    pub fn write(&self,
        position: &usize,
        msg_type_id: &i32,
        message_id: &u64,
        buffer: &[u8]) -> Result<usize> {
        unsafe {
            let store = &mut *self.store.get();
            store.write(
                position,
                msg_type_id,
                message_id,
                buffer)
        }
    }

    /// Flushes the memory mapped file to disk.
    pub fn flush(&self) -> Result<()> {
        unsafe {
            let store = &mut *self.store.get();
            store.flush()
        }
    }
}

unsafe impl Send for MessageFileStoreWrite {}

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
        file_size: usize) -> std::io::Result<(MessageFileStoreRead, MessageFileStoreWrite)> {
        // Need to make sure the file size is aligned correctly.
        let buffer = MemoryMappedInt::new(path, align(file_size, ALIGNMENT))?;
        let file_store = MessageFileStore {
            buffer
        };
        let cell = Arc::new(UnsafeCell::new(file_store));
        Ok((MessageFileStoreRead{
            store: cell.clone()
        }, MessageFileStoreWrite {
            store: cell
        }))
    }

    /// Used to open a buffer file.
    /// TODO check to see that the file is aligned.
    /// # Arguments
    /// `path` - The path of the file to open.
    /// # Return
    /// The read and writers for the file.
    pub unsafe fn open<P: AsRef<Path>> (
        path: &P) -> std::io::Result<(MessageFileStoreRead, MessageFileStoreWrite)> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(path)?;
        let buffer = MemoryMappedInt::open(file)?;
        let file_store = MessageFileStore {
            buffer
        };
        let cell = Arc::new(UnsafeCell::new(file_store));
        Ok((MessageFileStoreRead{
            store: cell.clone()
        }, MessageFileStoreWrite {
            store:cell
        }))
    }

    /// Opens a file for writting only.
    /// # Arguments
    /// `path` - The path of the file to open.
    pub unsafe fn open_write<P: AsRef<Path>> (
        path: &P) -> std::io::Result<MessageFileStoreWrite> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(path)?;
        let buffer = MemoryMappedInt::open(file)?;
        let file_store = MessageFileStore {
            buffer
        };
        let cell = Arc::new(UnsafeCell::new(file_store));
        Ok(MessageFileStoreWrite {
            store:cell
        })
    }

    /// Opens a file that is readonly.
    /// # Arguments
    /// `path` - The path of the file to open to read.
    pub unsafe fn open_readonly<P: AsRef<Path>> (
        path: &P) -> std::io::Result<MessageFileStoreRead> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(path)?;
        let buffer = MemoryMappedInt::open(file)?;
        let file_store = MessageFileStore {
            buffer
        };
        let cell = Arc::new(UnsafeCell::new(file_store));
        Ok(MessageFileStoreRead{
            store: cell.clone()
        })
    }
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<std::io::Error> for Error {

    fn from(err: std::io::Error) -> Self {
        Error::FileError(err)
    }
}

/// Potential message errors.
#[derive(Debug)]
pub enum Error {
    /// The file is full of messages a new one needs to be created.
    Full,
    /// There is a file io error.
    FileError(std::io::Error),
    /// The position is out of the range of the current file.
    PositionOutOfRange(usize),
    /// No message at the location.
    NoMessage,
    /// Not enough space to read the message.
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
    /// # Returns
    /// The position of the next message.
    fn read<'a, F>(&'a self,
        pos: &usize,
        act: F) -> Result<usize>
        where F: FnOnce(i32, u64, &'a [u8]);

    fn read_new<'a>(
        &'a self,
        pos: &usize) -> Result<MessageRead<'a>>;

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

/// Represents a read in message.
pub struct MessageRead<'a> {
    msg_type_id: MessageTypeId,
    message_id: MessageId,
    bytes: &'a [u8],
    next_pos: usize
}

impl<'a> MessageRead<'a> {

   fn new(
        msg_type_id: MessageTypeId,
        message_id: MessageId,
        bytes: &'a [u8],
        next_pos: usize) -> Self {
        MessageRead {
            msg_type_id,
            message_id,
            bytes,
            next_pos
        }
    }

    pub fn msgTypeId(&self) -> MessageTypeId {
        self.msg_type_id
    }

    pub fn messageId(&self) -> MessageId {
        self.message_id
    }

    pub fn bytes(&'a self) -> &'a [u8] {
        self.bytes
    }

    pub fn next_pos(&self) -> usize {
        self.next_pos
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
    fn read<'a, F>(&'a self,
        pos: &usize,
        act: F)
        -> Result<usize> 
        where F: FnOnce(i32, u64, &'a [u8])
    {
        if *pos > self.size() - ALIGNMENT {
            Err(Error::PositionOutOfRange(*pos))
        } else {
            fence(Ordering::Acquire);  // This fence is here so we get the latest value. LoadLoad
            let size = self.buffer.get_u32(pos);
            if size == 0 {
                Err(Error::NoMessage)
            } else {
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
                    let message_type = self.buffer.get_i32(&MessageFileStore::calculate_msg_type_pos(pos));
                    let message_id = self.buffer.get_u64(&MessageFileStore::calculate_message_id_pos(pos));
                    let body_size = size as usize - HEADER_SIZE;
                    let bytes = self.buffer.get_bytes(&MessageFileStore::calculate_body_pos(pos), &body_size);
                    act(message_type, message_id, bytes);
                    Ok(aligned + pos)
                }
            }
        }
    }

    fn read_new<'a>(
        &'a self,
        pos: &usize) -> Result<MessageRead<'a>> {
        if *pos > self.size() - ALIGNMENT {
            Err(Error::PositionOutOfRange(*pos))
        } else {
            fence(Ordering::Acquire);  // This fence is here so we get the latest value. LoadLoad
            let size = self.buffer.get_u32(pos);
            if size == 0 {
                Err(Error::NoMessage)
            } else {
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
                    let message_type = self.buffer.get_i32(&MessageFileStore::calculate_msg_type_pos(pos));
                    let message_id = self.buffer.get_u64(&MessageFileStore::calculate_message_id_pos(pos));
                    let body_size = size as usize - HEADER_SIZE;
                    let bytes = self.buffer.get_bytes(&MessageFileStore::calculate_body_pos(pos), &body_size);
                    let next_pos = aligned + pos;
                    Ok(MessageRead::new(message_type, message_id, bytes, next_pos))
                }
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
        if self.size() < *position {
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
            // Always write the size last since we are using this to check and we need a StoreStore
            // barrier here.  IE all of the previous stores need to be completed.
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
        
        let (read, write) = unsafe { MessageFileStore::new(
            &test_file,
            2048).unwrap()
        };
        
        let bytes: Vec<u8>  = vec!(10, 11, 12, 13, 14, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32);
        write.write(&0, &1, &2, &bytes[0..8]).unwrap();
        write.flush().unwrap();
        read.read(0, |msg_type, message_id, body| {
            assert_eq!(msg_type, 1);
            assert_eq!(message_id, 2);
            assert_eq!(body.len(), 8);
        }).unwrap();
        let read_again = unsafe {MessageFileStore::open_readonly(&test_file).unwrap()};
        read_again.read(0, |msg_type, message_id, body| {
            assert_eq!(msg_type, 1);
            assert_eq!(message_id, 2);
            assert_eq!(body.len(), 8);
        }).unwrap();
    }
}
