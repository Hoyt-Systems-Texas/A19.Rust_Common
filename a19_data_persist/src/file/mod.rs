use a19_concurrent::buffer::atomic_buffer::AtomicByteBuffer;
use a19_concurrent::buffer::mmap_buffer::MemoryMappedInt;
use a19_concurrent::buffer::DirectByteBuffer;
use a19_concurrent::buffer::{align, next_pos};
use std::cell::UnsafeCell;
use std::fs::OpenOptions;
use std::path::Path;
use std::sync::atomic::{fence, Ordering};
use std::sync::Arc;

pub type MessageId = u64;
pub type MessageTypeId = i32;

pub struct MessageFileStoreRead {
    store: Arc<UnsafeCell<MessageFileStore>>,
}

impl MessageFileStoreRead {
    /// Reads a message at a specified location.
    /// # Arguments
    /// `pos` - The position to read in.
    /// `act` - The action to run for the messages.
    /// # Returns
    /// The position of the next message.
    pub fn read<'a, F>(&'a self, pos: usize, act: F) -> Result<usize>
    where
        F: FnOnce(i32, u64, &'a [u8]),
    {
        unsafe {
            let store = &mut *self.store.get();
            store.read(&pos, act)
        }
    }

    pub fn read_new<'a>(&self, pos: &usize) -> Result<MessageRead<'a>> {
        unsafe {
            let store = &mut *self.store.get();
            store.read_new(pos)
        }
    }

    pub fn read_block<'a>(
        &'a self,
        pos: &usize,
        max_message_id: &u64,
        max_length: &usize,
    ) -> Result<MessageBlock<'a>> {
        let store = unsafe { &mut *self.store.get() };
        store.read_block(pos, max_message_id, max_length)
    }

    /// Reads in a raw block.  Useful if you are copying data in the buffer.
    /// # Arguments
    /// `pos` - The starting position.
    /// `length` - The length of the section to get.
    pub fn read_section<'a>(&'a self, pos: &usize, length: &usize) -> Result<&'a [u8]> {
        let store = unsafe { &mut *self.store.get() };
        store.read_section(pos, length)
    }

    pub fn is_end(&self, pos: &usize) -> bool {
        let store = unsafe { &mut *self.store.get() };
        store.is_end(pos)
    }
}

unsafe impl Sync for MessageFileStoreRead {}
unsafe impl Send for MessageFileStoreRead {}

#[derive(Debug)]
pub struct MessageFileStoreWrite {
    store: Arc<UnsafeCell<MessageFileStore>>,
}

impl MessageFileStoreWrite {
    /// Writes a message to the buffer.
    pub fn write(
        &self,
        position: &usize,
        msg_type_id: &i32,
        message_id: &u64,
        buffer: &[u8],
    ) -> Result<usize> {
        unsafe {
            let store = &mut *self.store.get();
            store.write(position, msg_type_id, message_id, buffer)
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
unsafe impl Sync for MessageFileStoreWrite {}

/// Represents the storage of files.   It supports a singler writer with multiple readers.  This is
/// the building blocks for raft protocol.
#[derive(Debug)]
pub struct MessageFileStore {
    /// The buffer we are writing to.
    buffer: MemoryMappedInt,
}

unsafe impl Send for MessageFileStore {}
unsafe impl Sync for MessageFileStore {}

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
        file_size: usize,
    ) -> std::io::Result<(MessageFileStoreRead, MessageFileStoreWrite)> {
        // Need to make sure the file size is aligned correctly.
        let buffer = MemoryMappedInt::new(path, next_pos(file_size, ALIGNMENT))?;
        let file_store = MessageFileStore { buffer };
        let cell = Arc::new(UnsafeCell::new(file_store));
        Ok((
            MessageFileStoreRead {
                store: cell.clone(),
            },
            MessageFileStoreWrite { store: cell },
        ))
    }

    /// Used to open a buffer file.
    /// TODO check to see that the file is aligned.
    /// # Arguments
    /// `path` - The path of the file to open.
    /// # Return
    /// The read and writers for the file.
    pub unsafe fn open<P: AsRef<Path>>(
        path: &P,
    ) -> std::io::Result<(MessageFileStoreRead, MessageFileStoreWrite)> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(path)?;
        let buffer = MemoryMappedInt::open(file)?;
        let file_store = MessageFileStore { buffer };
        let cell = Arc::new(UnsafeCell::new(file_store));
        Ok((
            MessageFileStoreRead {
                store: cell.clone(),
            },
            MessageFileStoreWrite { store: cell },
        ))
    }

    /// Opens a file for writting only.
    /// # Arguments
    /// `path` - The path of the file to open.
    pub unsafe fn open_write<P: AsRef<Path>>(
        path: &P,
        file_size: usize,
    ) -> std::io::Result<MessageFileStoreWrite> {
        let buffer = if path.as_ref().exists() {
            let file = OpenOptions::new().read(true).write(true).open(path)?;
            MemoryMappedInt::open(file)?
        } else {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(path)?;
            MemoryMappedInt::new(path, next_pos(file_size, ALIGNMENT))?
        };
        let file_store = MessageFileStore { buffer };
        let cell = Arc::new(UnsafeCell::new(file_store));
        Ok(MessageFileStoreWrite { store: cell })
    }

    /// Opens a file that is readonly.
    /// # Arguments
    /// `path` - The path of the file to open to read.
    pub unsafe fn open_readonly<P: AsRef<Path>>(path: &P) -> std::io::Result<MessageFileStoreRead> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(path)?;
        let buffer = MemoryMappedInt::open(file)?;
        let file_store = MessageFileStore { buffer };
        let cell = Arc::new(UnsafeCell::new(file_store));
        Ok(MessageFileStoreRead {
            store: cell.clone(),
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
    NotEnoughSpace {
        message_size: u32,
        position: usize,
        capacity: usize,
        remaining: usize,
    },
    InvalidFile,
    AlreadyExists,
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
    fn read<'a, F>(&'a self, pos: &usize, act: F) -> Result<usize>
    where
        F: FnOnce(i32, u64, &'a [u8]);

    /// Reads in a message from a buffer.
    /// # Arguments
    /// `pos` - The position to read in the memory.
    /// # Returns
    /// The message read in.
    fn read_new<'a>(&'a self, pos: &usize) -> Result<MessageRead<'a>>;

    /// Used to read in a block of messages.  Useful for sending the blocks over a network in a
    /// batch.
    /// # Arguments
    /// `pos` - The position to read in from.
    /// `max_message_id` - The maximum message id.
    /// `max_length` - The maximum length of the byte buffer to get.
    fn read_block<'a>(
        &'a self,
        pos: &usize,
        max_message_id: &u64,
        max_length: &usize,
    ) -> Result<MessageBlock<'a>>;

    /// Writes a message to the buffer.
    /// # Arguments
    /// `posiiton` - The position to write to the buffer.
    /// `msg_type_id` - The type of the message.
    /// `buffer` - THe buffer for the message.
    /// # returns
    /// The next position in the buffer.
    fn write(
        &mut self,
        position: &usize,
        msg_type_id: &i32,
        message_id: &u64,
        buffer: &[u8],
    ) -> Result<usize>;

    /// Used to read in a section.
    /// # Arguments
    /// `pos` - The position to start reading.
    fn read_section<'a>(&'a self, pos: &usize, length: &usize) -> Result<&'a [u8]>;

    fn is_end(&self, pos: &usize) -> bool;
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
            Ok(_) => Ok(()),
            Err(e) => Err(Error::FileError(e)),
        }
    }
}

/// Represents a read in message.
pub struct MessageRead<'a> {
    msg_type_id: MessageTypeId,
    message_id: MessageId,
    bytes: &'a [u8],
    next_pos: usize,
}

pub struct MessageBlock<'a> {
    pub message_id_start: u64,
    pub message_id_end: u64,
    pub bytes: &'a [u8],
    pub next_pos: usize,
}

impl<'a> MessageBlock<'a> {
    /// Creates a new message block.
    fn new(message_id_start: u64, message_id_end: u64, bytes: &'a [u8], next_pos: usize) -> Self {
        MessageBlock {
            message_id_start,
            message_id_end,
            bytes,
            next_pos,
        }
    }
}

impl<'a> MessageRead<'a> {
    fn new(
        msg_type_id: MessageTypeId,
        message_id: MessageId,
        bytes: &'a [u8],
        next_pos: usize,
    ) -> Self {
        MessageRead {
            msg_type_id,
            message_id,
            bytes,
            next_pos,
        }
    }

    #[inline]
    pub fn msgTypeId(&self) -> MessageTypeId {
        self.msg_type_id
    }

    #[inline]
    pub fn messageId(&self) -> MessageId {
        self.message_id
    }

    #[inline]
    pub fn bytes(&'a self) -> &'a [u8] {
        self.bytes
    }

    #[inline]
    pub fn next_pos(&self) -> usize {
        self.next_pos
    }

    #[inline]
    pub fn is_end(&self) -> bool {
        self.is_end()
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
    fn read<'a, F>(&'a self, pos: &usize, act: F) -> Result<usize>
    where
        F: FnOnce(i32, u64, &'a [u8]),
    {
        if *pos > self.size() - ALIGNMENT {
            Err(Error::PositionOutOfRange(*pos))
        } else {
            fence(Ordering::Acquire); // This fence is here so we get the latest value. LoadLoad
            let size = self.buffer.get_u32(pos);
            if size == 0 {
                Err(Error::NoMessage)
            } else {
                let aligned = next_pos(size as usize, ALIGNMENT);
                // Check to see if we have enough space.
                let remaining = self.buffer.capacity() - *pos;
                if remaining < aligned {
                    Err(Error::NotEnoughSpace {
                        capacity: self.buffer.capacity(),
                        message_size: size,
                        position: *pos,
                        remaining,
                    })
                } else {
                    let message_type = self
                        .buffer
                        .get_i32(&MessageFileStore::calculate_msg_type_pos(pos));
                    let message_id = self
                        .buffer
                        .get_u64(&MessageFileStore::calculate_message_id_pos(pos));
                    let body_size = size as usize - HEADER_SIZE;
                    let bytes = self
                        .buffer
                        .get_bytes(&MessageFileStore::calculate_body_pos(pos), &body_size);
                    act(message_type, message_id, bytes);
                    Ok(aligned + pos)
                }
            }
        }
    }

    fn read_section<'a>(&'a self, pos: &usize, length: &usize) -> Result<&'a [u8]> {
        fence(Ordering::Acquire);
        if *pos > self.size() - ALIGNMENT {
            Err(Error::PositionOutOfRange(*pos))
        } else if *pos + *length > self.size() {
            Err(Error::PositionOutOfRange(*pos))
        } else {
            Ok(self.buffer.get_bytes(pos, length))
        }
    }

    /// Used to read in a message.
    /// # Arguments
    /// `pos` - The position to read in the message at.
    fn read_new<'a>(&'a self, pos: &usize) -> Result<MessageRead<'a>> {
        if *pos > self.size() - ALIGNMENT {
            Err(Error::PositionOutOfRange(*pos))
        } else {
            fence(Ordering::Acquire); // This fence is here so we get the latest value. LoadLoad
            let size = self.buffer.get_u32(pos);
            if size == 0 {
                Err(Error::NoMessage)
            } else if self.is_end(pos) {
                Err(Error::Full)
            } else {
                let aligned = next_pos(size as usize, ALIGNMENT);
                // Check to see if we have enough space.
                let remaining = self.buffer.capacity() - *pos;
                if remaining < aligned {
                    Err(Error::NotEnoughSpace {
                        capacity: self.buffer.capacity(),
                        message_size: size,
                        position: *pos,
                        remaining,
                    })
                } else {
                    let message_type = self
                        .buffer
                        .get_i32(&MessageFileStore::calculate_msg_type_pos(pos));
                    let message_id = self
                        .buffer
                        .get_u64(&MessageFileStore::calculate_message_id_pos(pos));
                    let body_size = size as usize - HEADER_SIZE;
                    let bytes = self
                        .buffer
                        .get_bytes(&MessageFileStore::calculate_body_pos(pos), &body_size);
                    let next_pos = aligned + pos;
                    Ok(MessageRead::new(message_type, message_id, bytes, next_pos))
                }
            }
        }
    }

    fn read_block<'a>(
        &'a self,
        pos: &usize,
        max_message_id: &u64,
        max_length: &usize,
    ) -> Result<MessageBlock<'a>> {
        fence(Ordering::Acquire);
        let mut current_pos = *pos;
        let mut length = 0;
        let mut start_message_id = 0;
        let mut last_message_id = 0;
        loop {
            let size = next_pos(self.buffer.get_u32(&current_pos) as usize, HEADER_SIZE);
            if size == 0 || self.is_end(&current_pos) || size + length > *max_length {
                if length == 0 {
                    break Err(Error::NoMessage);
                } else {
                    break Ok(MessageBlock::new(
                        start_message_id,
                        last_message_id,
                        self.buffer.get_bytes(pos, &length),
                        *pos + length,
                    ));
                }
            } else {
                let message_id = self
                    .buffer
                    .get_u64(&MessageFileStore::calculate_message_id_pos(&current_pos));
                if message_id <= *max_message_id {
                    last_message_id = message_id;
                    if start_message_id == 0 {
                        start_message_id = message_id;
                    }
                    length += size;
                    current_pos += size;
                } else {
                    if start_message_id > 0 && length > 0 {
                        break Ok(MessageBlock::new(
                            start_message_id,
                            last_message_id,
                            self.buffer.get_bytes(pos, &length),
                            *pos + length,
                        ));
                    } else {
                        break Err(Error::NoMessage);
                    }
                }
            }
        }
    }

    fn is_end(&self, pos: &usize) -> bool {
        let next = pos + HEADER_SIZE;
        if next < self.size() {
            self.buffer.get_u32(pos) == std::u32::MAX
        } else {
            true
        }
    }

    /// Writes a message to the buffer.
    /// # Arguments
    /// `posiiton` - The position to write to the buffer.
    /// `msg_type_id` - The type of the message.
    /// `buffer` - THe buffer for the message.
    /// # returns
    /// The next position in the buffer.
    fn write(
        &mut self,
        position: &usize,
        msg_type_id: &i32,
        message_id: &u64,
        buffer: &[u8],
    ) -> Result<usize> {
        let size = HEADER_SIZE + buffer.len();
        let aligned = next_pos(size, ALIGNMENT); // This is the aligned value at 32 bits
        if self.size() < *position {
            Err(Error::PositionOutOfRange(*position))
        } else if aligned > (self.size() - *position) {
            // TODO make it full and write all 00s
            let ones = [255; 1000];
            let mut bucket = self.size() - *position;
            let mut pos = *position;
            loop {
                if bucket > 1000 {
                    self.buffer.write_bytes(&pos, &ones[..]);
                    bucket -= 1000;
                    pos += 1000;
                } else {
                    self.buffer.write_bytes(&pos, &ones[0..bucket]);
                    break;
                }
            }
            Err(Error::Full)
        } else {
            let message_id_pos = MessageFileStore::calculate_message_id_pos(position);
            let message_size_pos = MessageFileStore::calculate_msg_size_pos(position);
            let message_type_pos = MessageFileStore::calculate_msg_type_pos(position);
            let message_body = MessageFileStore::calculate_body_pos(position);
            // Already verified it is small enought to fit in a u32.
            let size = size as u32;
            self.buffer.put_i32(&message_type_pos, *msg_type_id);
            self.buffer.put_u64(&message_id_pos, *message_id);
            self.buffer.write_bytes(&message_body, buffer);
            // Always write the size last since we are using this to check and we need a StoreStore
            // barrier here.  IE all of the previous stores need to be completed.
            self.buffer.put_u32_volatile(&message_size_pos, &size);
            Ok(aligned + *position)
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::file::{MessageFileStore, MessageStore};
    use std::fs::remove_file;
    use std::path::Path;

    const TEST_DIR: &str = "/home/mrh0057/rust_file_test";

    /// Used to create a testing directory with the specified.
    fn create_test_file(name: &str) -> String {
        let file = format!("{}_{}", TEST_DIR, name);
        let path = Path::new(&file);
        if path.exists() {
            remove_file(&file).unwrap();
        }
        file
    }

    #[test]
    pub fn create_test() {
        let test_file = create_test_file("");
        let (read, write) = unsafe { MessageFileStore::new(&test_file, 2048).unwrap() };

        let read_again = unsafe { MessageFileStore::open_readonly(&test_file).unwrap() };
        let bytes: Vec<u8> = vec![10, 11, 12, 13, 14, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32];
        write.write(&0, &1, &2, &bytes[0..8]).unwrap();
        write.flush().unwrap();
        read.read(0, |msg_type, message_id, body| {
            assert_eq!(msg_type, 1);
            assert_eq!(message_id, 2);
            assert_eq!(body.len(), 8);
        })
        .unwrap();
        read_again
            .read(0, |msg_type, message_id, body| {
                assert_eq!(msg_type, 1);
                assert_eq!(message_id, 2);
                assert_eq!(body.len(), 8);
            })
            .unwrap();
    }

    #[test]
    pub fn read_block_test() {
        let test_file = create_test_file("read_block_test");
        let (read, write) = unsafe { MessageFileStore::new(&test_file, 2048).unwrap() };

        let bytes: Vec<u8> = vec![10, 11, 12, 13, 14, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32];
        let mut pos = write.write(&0, &2, &1, &bytes[0..8]).unwrap();
        pos = write.write(&pos, &2, &2, &bytes[0..8]).unwrap();
        pos = write.write(&pos, &2, &3, &bytes[0..9]).unwrap();
        write.write(&pos, &2, &4, &bytes[0..7]).unwrap();
        write.flush().unwrap();
        let r = read.read_block(&0, &3, &1024).unwrap();
        // Test reading the first 3 messages.
        assert_eq!(1, r.message_id_start);
        assert_eq!(3, r.message_id_end);
        assert_eq!(96, r.next_pos);

        // Test reading a maximum size.
        let r = read.read_block(&0, &3, &64).unwrap();
        assert_eq!(1, r.message_id_start);
        assert_eq!(2, r.message_id_end);
        assert_eq!(64, r.next_pos);

        // Test reading till the end.
        let r = read.read_block(&0, &10, &1024).unwrap();
        assert_eq!(1, r.message_id_start);
        assert_eq!(4, r.message_id_end);
        assert_eq!(128, r.next_pos);
    }
}
