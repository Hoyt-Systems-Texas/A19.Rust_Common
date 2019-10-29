use std::sync::atomic::{AtomicUsize, Ordering};
use std::vec::Vec;
use std::mem::replace;
use a19_core::pow2::PowOf2;
use std::thread;
use crate::queue::{ConcurrentQueue, PaddedUsize};
use std::cell::UnsafeCell;

struct MpscNode<T> {
    id: AtomicUsize,
    value: Option<T>,
}

struct MpscQueue<T> {
    mask: usize,
    ring_buffer: Vec<MpmcNode<T>>,
    capacity: usize,
    sequence_number: PaddedUsize,
    producer: PaddedUsize
}

unsafe impl<T> Send for MpmcQueue<T> {}

impl<T> MpmcQueue<T> {

    fn new(queue_size: usize) -> Self {
        let power_of_2 = queue_size.round_to_power_of_two();
        let mut queue = MpscQueue {
            ring_buffer: Vec::with_capacity(power_of_2),
            capacity: power_of_2,
            mask: power_of_2 - 1,
            sequence_number: PaddedUsize {
                padding: [0; 31],
                counter: AtomicUsize::new(0)
            },
            producer: PaddedUsize {
                padding: [0; 31],
                counter: AtomicUsize::new()
            }
        };
        for _ in 0..power_of_2 {
            let node = MpscNode {
                id: AtomicUsize::new(0),
                value: None
            };
            queue.ring_buffer.push(node);
        }
        queue
    }
}
