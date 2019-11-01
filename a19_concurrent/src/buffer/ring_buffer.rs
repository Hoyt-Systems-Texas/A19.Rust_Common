use std::sync::atomic::{Ordering, fence};
use crate::buffer::DirectByteBuffer;
use crate::queue::PaddedUsize;
use crate::buffer::atomic_buffer::{AtomicByteBuffer, AtomicByteBufferInt};

struct ManyToOneBufferInt {

    producer: PaddedUsize,
    consumer: PaddedUsize,
    buffer: AtomicByteBufferInt,
    mask: usize
}

const HEADER_SIZE: usize = 8;
const ALIGNMENT: usize = HEADER_SIZE;
const SIZE_OFFSET: usize = 0;
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
/// ---------------------------------------------------------------- 0
/// Message Size                   | Message Type
/// ---------------------------------------------------------------- 64
pub trait RingBuffer {

    /// Gets the capcity of the buffer.
    fn capacity(&self) -> usize;

    /// Write a slice of bytes to the buffer.
    /// # Arguments
    /// `msg_type_id` - The type of the message.
    /// `buffer` - The buffer to write.
    fn write(&mut self, msg_type_id: i32, buffer: &[u8]) -> bool;

    /// Used to read the next message.
    /// # Arguments
    /// `act` - The function to call to read in the message.
    /// `limit` - The maximum number of messages to process.
    fn read<'a>(
        &'a mut self,
        act: fn(msgType: i32, bytes: &'a [u8]),
        limit: usize) -> usize;

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
    fn new(size: usize) -> Self {
        let buffer = AtomicByteBufferInt::new(size);
        let mask = buffer.capacity() - 1;
        ManyToOneBufferInt {
            producer: PaddedUsize::new(0),
            consumer: PaddedUsize::new(0),
            buffer,
            mask
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
    fn write(&mut self, msg_type_id: i32, buffer: &[u8]) -> bool {
        false
    }

    /// Used to read the next message.
    /// # Arguments
    /// `act` - The function to call to read in the message.
    /// `limit` - The maximum number of messages to process.
    fn read<'a>(
        &'a mut self,
        act: fn(msgType: i32, bytes: &'a [u8]),
        limit: usize) -> usize {
        0
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
            let head = self.consumer.counter.load(Ordering::Release);
            let tail = self.producer.counter.load(Ordering::Release);

            let available_capacity = capacity - (tail - head);
            if (required_capacity > available_capacity) {
                break None
            } else {
                let tail_index = tail & self.mask;
                let buffer_end_length = capacity - tail_index;
                if (required_capacity > buffer_end_length) {
                    let head_index = head & self.mask;
                    if (required_capacity > head_index) {
                        break None
                    } else {
                        let padding = buffer_end_length as i32;
                        match self.producer.counter.compare_exchange_weak(
                            tail,
                            0,
                            Ordering::SeqCst,
                            Ordering::Relaxed) {
                            Ok(_) => {
                                self.buffer.put_i32(
                                    &size_offset(&tail_index),
                                    -1);
                                fence(Ordering::Release);
                                
                                self.buffer.put_i32(
                                    &message_body_offset(&tail_index),
                                    PADDING_MESSAGE_TYPE);
                                self.buffer.put_i32_volatile(
                                    &size_offset(&tail_index),
                                    padding);
                                break Some(0)
                            },
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
                        Ordering::Relaxed) {
                        Ok(_) => {
                            break Some(tail)
                        },
                        Err(_) => {
                            // Around we go.
                        }
                    }
                }
                break Some(0)
            }
        }
    }
}
