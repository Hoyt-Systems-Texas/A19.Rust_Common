use crate::buffer::atomic_buffer::{AtomicByteBuffer, AtomicByteBufferInt};
use crate::buffer::{align, DirectByteBuffer};
use crate::queue::PaddedUsize;
use std::cell::UnsafeCell;
use std::sync::atomic::{fence, Ordering};
use std::sync::Arc;

pub struct BytesReadInfo {
    /// The starting index.
    pub start: usize,
    pub bytes_read: usize,
    pub messages_read: u32,
    /// The ending index.
    pub end: usize,
}

pub struct ManyToOneBufferReader {
    buffer: Arc<UnsafeCell<ManyToOneBufferInt>>,
}

impl ManyToOneBufferReader {
    /// Used to read in a series of messages.
    /// # Arguments
    /// `act` - The function to call to read in the message.
    /// `limit` - The maximum number of messages to process.
    pub fn read<'a, F>(&'a self, act: F, limit: u32) -> BytesReadInfo
    where
        F: FnMut(i32, &[u8]),
    {
        let buffer = unsafe { &mut *self.buffer.get() };
        buffer.read(act, limit)
    }

    /// Called when we are done reading the bytes in.
    /// # Arguments
    /// `read_info` - The info we are reading.
    pub fn read_completed(&self, read_info: &BytesReadInfo) {
        let buffer = unsafe { &mut *self.buffer.get() };
        buffer.read_completed(read_info);
    }
}

unsafe impl Send for ManyToOneBufferReader {}
unsafe impl Sync for ManyToOneBufferReader {}

pub struct ManyToOneBufferWriter {
    buffer: Arc<UnsafeCell<ManyToOneBufferInt>>,
}

impl ManyToOneBufferWriter {
    /// Writes to the buffer.  This call is thread safe.
    /// # Arguments
    /// `msg_type_id` - The type of the message.
    /// `buffer` - The buffer we are copying.
    /// # Returns
    /// Some with the index number otherwise the position.
    pub fn write(&self, msg_type_id: i32, buffer: &[u8]) -> Option<usize> {
        let me = unsafe { &mut *self.buffer.get() };
        me.write(msg_type_id, buffer)
    }
}

unsafe impl Send for ManyToOneBufferWriter {}
unsafe impl Sync for ManyToOneBufferWriter {}

/// Creates a many to one buffer with the writer and reader objects.
/// # Arguments
/// `size` - The size to create the buffer.
pub fn create_many_to_one(size: &usize) -> (ManyToOneBufferReader, ManyToOneBufferWriter) {
    let cell = Arc::new(UnsafeCell::new(ManyToOneBufferInt::new(*size)));
    (
        ManyToOneBufferReader {
            buffer: cell.clone(),
        },
        ManyToOneBufferWriter {
            buffer: cell.clone(),
        },
    )
}

struct ManyToOneBufferInt {
    producer: PaddedUsize,
    consumer: PaddedUsize,
    buffer: AtomicByteBufferInt,
    mask: usize,
}

unsafe impl Sync for ManyToOneBufferInt {}
unsafe impl Send for ManyToOneBufferInt {}

const HEADER_SIZE: usize = 8;
const ALIGNMENT: usize = HEADER_SIZE;
const MESSAGE_TYPE_OFFSET: usize = 4;
const MESSAGE_BODY_OFFSET: usize = 8;
const PADDING_MESSAGE_TYPE: i32 = -1;

/// Gets the size offset.
/// # Arguments
/// `position` - The position to calculate the offset from.
fn size_offset(position: &usize) -> usize {
    *position
}

/// Gets the message type offset.
/// # Arguments
/// `position` - The position to calculate the offset from.
fn message_type_offset(position: &usize) -> usize {
    *position + MESSAGE_TYPE_OFFSET
}

fn message_body_offset(position: &usize) -> usize {
    *position + MESSAGE_BODY_OFFSET
}

/// Buffer format
///  0                   1                   2                   3
///  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
/// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
/// | Message Size                                                  |
/// +---------------------------------------------------------------+ 32
/// | Message Type                                                  |
/// +---------------------------------------------------------------+ 64
/// |                   Message Body                                ...
/// ...                                                             |
/// +---------------------------------------------------------------+
pub trait RingBuffer {
    /// Gets the capcity of the buffer.
    fn capacity(&self) -> usize;

    /// Write a slice of bytes to the buffer.
    /// # Arguments
    /// `msg_type_id` - The type of the message.
    /// `buffer` - The buffer to write.
    fn write(&mut self, msg_type_id: i32, buffer: &[u8]) -> Option<usize>;

    /// Used to read in a series of messages.
    /// # Arguments
    /// `act` - The function to call to read in the message.
    /// `limit` - The maximum number of messages to process.
    fn read<'a, F>(&'a mut self, act: F, limit: u32) -> BytesReadInfo
    where
        F: FnMut(i32, &'a [u8]);

    /// Called when we are done reading the bytes in.
    fn read_completed(&mut self, read_info: &BytesReadInfo);

    /// The maximum length of a message.
    fn max_message_length(&self) -> usize;

    /// The current producer position.
    fn producer_position(&self) -> usize;

    /// The current consumer position.
    fn consumer_position(&self) -> usize;

    /// The current size of the buffer.
    fn size(&self) -> usize;
}

impl ManyToOneBufferInt {
    /// Used to crate a new ring buffer.
    pub fn new(size: usize) -> Self {
        let buffer = AtomicByteBufferInt::new(size);
        let mask = buffer.capacity() - 1;
        ManyToOneBufferInt {
            producer: PaddedUsize::new(0),
            consumer: PaddedUsize::new(0),
            buffer,
            mask,
        }
    }
}

impl RingBuffer for ManyToOneBufferInt {
    /// Gets the capcity of the buffer.
    fn capacity(&self) -> usize {
        self.buffer.capacity()
    }

    /// Write a slice of bytes to the buffer.
    /// # Arguments
    /// `msg_type_id` - The type of the message.
    /// `buffer` - The buffer to write.
    fn write(&mut self, msg_type_id: i32, buffer: &[u8]) -> Option<usize> {
        if msg_type_id < 0 {
            None
        } else {
            let record_length: usize = buffer.len() + HEADER_SIZE;
            let required_capacity = align(record_length, ALIGNMENT);
            let index = self.claim_capacity(required_capacity);
            match index {
                Some(i) => {
                    let record_length_i = record_length as i32;
                    self.buffer.put_i32(&size_offset(&i), -record_length_i);
                    fence(Ordering::Release);

                    self.buffer.put_i32(&message_type_offset(&i), msg_type_id);
                    self.buffer.write_bytes(&message_body_offset(&i), buffer);
                    self.buffer
                        .put_i32_volatile(&size_offset(&i), &record_length_i);
                    Some(i)
                }
                None => None,
            }
        }
    }

    /// Used to read the next message.
    /// # Arguments
    /// `act` - The function to call to read in the message.
    /// `limit` - The maximum number of messages to process.
    fn read<'a, F>(&'a mut self, mut act: F, limit: u32) -> BytesReadInfo
    where
        F: FnMut(i32, &'a [u8]),
    {
        let capcity = self.capacity();
        let head = self.consumer.counter.load(Ordering::Relaxed);
        let head_index = head & self.mask;
        let max_block_length = capcity - head_index;
        let mut bytes_read: usize = 0;
        let mut messages_read = 0;
        loop {
            if bytes_read >= max_block_length {
                break;
            } else {
                let record_index = head_index + bytes_read;
                let record_length = self.buffer.get_i32_volatile(&size_offset(&record_index));
                if record_length <= 0 {
                    break;
                } else {
                    bytes_read += align(record_length as usize, ALIGNMENT);

                    let message_type = self.buffer.get_i32(&message_type_offset(&record_index));
                    if message_type == PADDING_MESSAGE_TYPE {
                        // Goto the next message.
                    } else {
                        let record_length_u = record_length as usize;
                        let byte_arrays = self.buffer.get_bytes(
                            &(record_index + HEADER_SIZE),
                            &(record_length_u - HEADER_SIZE),
                        );
                        act(message_type, byte_arrays);
                        messages_read += 1;
                        if messages_read >= limit {
                            break;
                        }
                    }
                }
            }
        }
        BytesReadInfo {
            start: head_index,
            bytes_read,
            messages_read,
            end: head_index + bytes_read,
        }
    }

    fn read_completed(&mut self, read_info: &BytesReadInfo) {
        self.buffer
            .set_bytes(&read_info.start, &read_info.bytes_read, 0);
        self.consumer
            .counter
            .store(read_info.start + read_info.bytes_read, Ordering::Release);
    }

    /// The maximum length of a message.
    fn max_message_length(&self) -> usize {
        self.buffer.max_message_size()
    }

    /// The current producer position.
    fn producer_position(&self) -> usize {
        self.producer.counter.load(Ordering::Acquire)
    }

    /// The current consumer position.
    fn consumer_position(&self) -> usize {
        self.consumer.counter.load(Ordering::Release)
    }

    /// The current size of the buffer.
    fn size(&self) -> usize {
        self.buffer.capacity()
    }
}

impl ManyToOneBufferInt {
    /// Claims the capacity to write to the buffer.
    /// # Arguments
    /// `required_capacity` - The required capacity for the allocation.
    fn claim_capacity(&mut self, required_capacity: usize) -> Option<usize> {
        let capacity = self.capacity();
        loop {
            let head = self.consumer.counter.load(Ordering::Acquire);
            let tail = self.producer.counter.load(Ordering::Acquire);

            let available_capacity = capacity - (tail - head);
            if required_capacity > available_capacity {
                break None;
            } else {
                let tail_index = tail & self.mask;
                let buffer_end_length = capacity - tail_index;
                if required_capacity > buffer_end_length {
                    let head_index = head & self.mask;
                    if required_capacity > head_index {
                        break None;
                    } else {
                        let padding = buffer_end_length as i32;
                        match self.producer.counter.compare_exchange_weak(
                            tail,
                            0,
                            Ordering::SeqCst,
                            Ordering::Relaxed,
                        ) {
                            Ok(_) => {
                                self.buffer.put_i32(&size_offset(&tail_index), -1);
                                fence(Ordering::Release);

                                self.buffer.put_i32(
                                    &message_type_offset(&tail_index),
                                    PADDING_MESSAGE_TYPE,
                                );
                                self.buffer
                                    .put_i32_volatile(&size_offset(&tail_index), &padding);
                                break Some(0);
                            }
                            Err(_) => {
                                // Around again
                            }
                        }
                    }
                } else {
                    match self.producer.counter.compare_exchange_weak(
                        tail,
                        tail + required_capacity,
                        Ordering::SeqCst,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => break Some(tail_index),
                        Err(_) => {
                            // Around we go.
                        }
                    }
                }
                break Some(0);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::buffer::ring_buffer::{create_many_to_one, ManyToOneBufferInt, RingBuffer};
    use std::vec::Vec;

    #[test]
    pub fn create_test() {
        let mut buffer = ManyToOneBufferInt::new(0x100);

        let bytes: Vec<u8> = vec![10, 11, 12, 13, 14, 14, 16];
        let written = buffer.write(1, &bytes);
        assert_eq!(written, Some(0));
        let result = buffer.read(
            |msg_type_id, buffer| {
                assert_eq!(1, msg_type_id);
            },
            1000,
        );
        buffer.read_completed(&result);
    }

    #[test]
    pub fn fill_to_end_test() {
        let mut buffer = ManyToOneBufferInt::new(0x40);
        let bytes: Vec<u8> = vec![10, 11, 12, 13, 14, 14, 16];
        for i in 0..4 {
            match buffer.write(1, &bytes) {
                Some(i) => {}
                None => {
                    assert!(false);
                }
            }
        }
        match buffer.write(1, &bytes) {
            Some(i) => assert!(false),
            None => {}
        }
        let result = buffer.read(
            |msg_type_id, buffer| {
                assert_eq!(1, msg_type_id);
            },
            1000,
        );
        assert_eq!(64, result.bytes_read);
        assert_eq!(4, result.messages_read);
        buffer.read_completed(&result);
        for i in 0..4 {
            match buffer.write(1, &bytes) {
                Some(_) => {}
                None => {
                    assert!(false);
                }
            }
        }
    }

    #[test]
    pub fn padding_test() {
        let mut buffer = ManyToOneBufferInt::new(0x40);
        let bytes: Vec<u8> = vec![10, 11, 12, 13, 14, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32];
        for i in 0..2 {
            match buffer.write(1, &bytes) {
                Some(_) => {}
                None => {
                    assert!(false);
                }
            }
        }
        let result = buffer.read(
            |msg_type_id, buffer| {
                assert_eq!(1, msg_type_id);
            },
            1000,
        );
        assert_eq!(2, result.messages_read);
        buffer.read_completed(&result);
        match buffer.write(1, &bytes) {
            Some(_) => {}
            None => {
                assert!(false);
            }
        }

        let result = buffer.read(
            |msg_type_id, buffer| {
                assert_eq!(1, msg_type_id);
            },
            1000,
        );
        assert_eq!(0, result.messages_read);
        buffer.read_completed(&result);
        let result = buffer.read(
            |msg_type_id, buffer| {
                assert_eq!(1, msg_type_id);
            },
            1000,
        );
        assert_eq!(1, result.messages_read);
        buffer.read_completed(&result);
    }

    #[test]
    pub fn create_many_to_one_dual() {
        let (reader, writer) = create_many_to_one(&0x40);
        let bytes: Vec<u8> = vec![10, 11, 12, 13, 14, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32];
        for i in 0..2 {
            match writer.write(1, &bytes) {
                Some(_) => {}
                None => {
                    assert!(false);
                }
            }
        }
        let result = reader.read(
            |msg_type_id, buffer| {
                assert_eq!(1, msg_type_id);
            },
            1,
        );
        reader.read_completed(&result);
        assert_eq!(1, result.messages_read);
    }
}
