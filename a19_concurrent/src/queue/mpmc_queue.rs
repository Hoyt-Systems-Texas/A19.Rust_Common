use crate::queue::{ConcurrentQueue, PaddedUsize};
use a19_core::pow2::PowOf2;
use std::cell::UnsafeCell;
use std::mem::replace;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::vec::Vec;

struct MpmcNode<T> {
    id: AtomicUsize,
    value: Option<T>,
}

pub struct MpmcQueueWrap<T> {
    queue: UnsafeCell<MpmcQueue<T>>,
}

unsafe impl<T> Sync for MpmcQueueWrap<T> {}
unsafe impl<T> Send for MpmcQueueWrap<T> {}

impl<T> MpmcQueueWrap<T> {
    pub fn new(queue_size: usize) -> Self {
        let queue = UnsafeCell::new(MpmcQueue::new(queue_size));
        MpmcQueueWrap { queue }
    }

    pub fn poll(&self) -> Option<T> {
        unsafe {
            let queue = &mut *self.queue.get();
            queue.poll()
        }
    }

    pub fn offer(&self, v: T) -> bool {
        unsafe {
            let queue = &mut *self.queue.get();
            queue.offer(v)
        }
    }

    pub fn drain(&self, act: fn(T), limit: usize) -> usize {
        unsafe {
            let queue = &mut *self.queue.get();
            queue.drain(act, limit)
        }
    }
}

struct MpmcQueue<T> {
    mask: usize,
    ring_buffer: Vec<MpmcNode<T>>,
    capacity: usize,
    sequence_number: PaddedUsize,
    producer: PaddedUsize,
}

impl<T> MpmcQueue<T> {
    fn new(queue_size: usize) -> Self {
        let power_of_2 = queue_size.round_to_power_of_two();
        let mut queue = MpmcQueue {
            ring_buffer: Vec::with_capacity(power_of_2),
            capacity: power_of_2,
            mask: power_of_2 - 1,
            sequence_number: PaddedUsize {
                padding: [0; 15],
                counter: AtomicUsize::new(1),
            },
            producer: PaddedUsize {
                padding: [0; 15],
                counter: AtomicUsize::new(1),
            },
        };
        for _ in 0..power_of_2 {
            let node = MpmcNode {
                id: AtomicUsize::new(0),
                value: None,
            };
            queue.ring_buffer.push(node);
        }
        queue
    }

    #[inline]
    fn pos(&self, index: usize) -> usize {
        index & self.mask
    }
}

unsafe impl<T> Sync for MpmcQueue<T> {}
unsafe impl<T> Send for MpmcQueue<T> {}

impl<T> ConcurrentQueue<T> for MpmcQueue<T> {
    /// Used to poll the queue and moves the value to the option if there is a value.
    fn poll(&mut self) -> Option<T> {
        loop {
            let s_index = self.sequence_number.counter.load(Ordering::Relaxed);
            let p_index = self.producer.counter.load(Ordering::Relaxed);
            if p_index > s_index {
                unsafe {
                    let last_pos = self.pos(s_index);
                    let node = self.ring_buffer.get_unchecked_mut(last_pos);
                    let node_id = node.id.load(Ordering::Acquire);
                    // Verify the node id matches the index id.
                    if node_id == s_index {
                        // Try and claim the slot.
                        match self.sequence_number.counter.compare_exchange_weak(
                            s_index,
                            s_index + 1,
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                        ) {
                            Ok(_) => {
                                let v = replace(&mut node.value, Option::None);
                                node.id.store(0, Ordering::Release);
                                break v;
                            }
                            Err(_) => {}
                        }
                    } else {
                        thread::yield_now()
                        /*
                        i = i + 1;
                        if i > 1_000_000_000 {
                            panic!(format!(
                                "Got stuck on {}:{}:{}:{}:{}",
                                s_index, p_index, node_id, last_pos, self.capacity
                            ))
                        }
                        */
                    }
                }
            } else {
                break None;
            }
        }
    }

    /// A quick way to drain all of the values from the queue.
    /// # Arguments
    /// `act` - The action to run against the queue.
    /// # Returns
    /// The number of items that where returned.
    fn drain(&mut self, act: fn(T), limit: usize) -> usize {
        loop {
            let p_index = self.producer.counter.load(Ordering::Relaxed);
            let s_index = self.sequence_number.counter.load(Ordering::Relaxed);
            if p_index <= s_index {
                break 0;
            } else {
                let elements_left = p_index - s_index;
                let request = limit.min(elements_left);
                // Have to do this a little bit different.
                match self.sequence_number.counter.compare_exchange_weak(
                    s_index,
                    s_index + request,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        for i in 0..request {
                            let pos = self.pos(s_index + i);
                            let node = unsafe { self.ring_buffer.get_unchecked_mut(pos) };
                            loop {
                                let node_id = node.id.load(Ordering::Acquire);
                                if node_id == s_index + i {
                                    let v = replace(&mut node.value, Option::None);
                                    // Need a Store/Store barrier to make sure this is done last.
                                    node.id.store(0, Ordering::Release);
                                    match v {
                                        None => panic!("Found a None!"),
                                        Some(t_value) => act(t_value),
                                    }
                                    break;
                                } else {
                                    thread::yield_now();
                                    /*
                                    fail_count = fail_count + 1;
                                    if fail_count > 1_000_000_000 {
                                        panic!("Failed to get the node!");
                                    }
                                    */
                                }
                            }
                        }
                        break request;
                    }
                    Err(_) => {}
                }
                break 0;
            }
        }
    }

    /// Offers a value to the queue.  Returns true if the value was successfully added.
    /// # Arguments
    /// `value` - The vale to add to the queue.
    fn offer(&mut self, value: T) -> bool {
        let capacity = self.capacity;
        loop {
            let p_index = self.producer.counter.load(Ordering::Relaxed);
            let c_index = self.sequence_number.counter.load(Ordering::Relaxed);
            if p_index < capacity || p_index - capacity < c_index {
                let pos = self.pos(p_index);
                let mut node = unsafe { self.ring_buffer.get_unchecked_mut(pos) };
                if node.id.load(Ordering::Acquire) == 0 {
                    match self.producer.counter.compare_exchange_weak(
                        p_index,
                        p_index + 1,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => {
                            node.value = Some(value);
                            // Need a Store/Store barrier to make sure this is done last.
                            node.id.store(p_index, Ordering::Release);
                            break true;
                        }
                        Err(_) => {}
                    }
                } else {
                    thread::yield_now();
                }
            } else {
                break false;
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::queue::mpmc_queue::{MpmcQueue, MpmcQueueWrap};
    use crate::queue::ConcurrentQueue;
    use std::sync::Arc;
    use std::thread;
    use std::vec::Vec;
    use time_test::time_test;

    #[test]
    pub fn create_queue_test() {
        let mut queue: MpmcQueue<u64> = MpmcQueue::new(128);
        assert_eq!(128, queue.ring_buffer.len());

        queue.offer(1);
        let result = queue.poll();
        assert_eq!(Some(1), result);
    }

    #[test]
    pub fn use_thread_queue_test() {
        time_test!();
        let queue: Arc<MpmcQueueWrap<usize>> = Arc::new(MpmcQueueWrap::new(1_000_000));
        let write_thread_num = 2;
        let mut write_threads: Vec<thread::JoinHandle<_>> = Vec::with_capacity(write_thread_num);
        let spins: usize = 10_000_000;
        for _ in 0..write_thread_num {
            let write_queue = queue.clone();
            let write_thread = thread::spawn(move || {
                for i in 0..(spins / write_thread_num) {
                    while !write_queue.offer(i) {
                        thread::yield_now()
                    }
                }
            });
            write_threads.push(write_thread);
        }

        let thread_num: usize = 2;
        let mut read_threads: Vec<thread::JoinHandle<_>> = Vec::with_capacity(thread_num);
        let read_spins = spins / thread_num;
        for _ in 0..thread_num {
            let read_queue = queue.clone();
            let read_thread = thread::spawn(move || {
                let mut count = 0;
                loop {
                    let result = read_queue.poll();
                    match result {
                        Some(_) => {
                            count = count + 1;
                            if count == read_spins {
                                break;
                            }
                        }
                        _ => {
                            thread::yield_now();
                        }
                    }
                }
            });
            read_threads.push(read_thread);
        }

        for num in 0..write_thread_num {
            write_threads
                .remove(write_thread_num - num - 1)
                .join()
                .unwrap();
        }
        for num in 0..thread_num {
            read_threads.remove(thread_num - num - 1).join().unwrap();
        }
    }

    #[test]
    pub fn use_thread_queue_test_drain() {
        time_test!();
        let queue: Arc<MpmcQueueWrap<usize>> = Arc::new(MpmcQueueWrap::new(1_000_000));
        let write_queue = queue.clone();
        let spins: usize = 10_000_000;
        let write_thread = thread::spawn(move || {
            for i in 0..spins {
                while !write_queue.offer(i) {
                    thread::yield_now()
                }
            }
        });

        let thread_num: usize = 2;
        let mut read_threads: Vec<thread::JoinHandle<_>> = Vec::with_capacity(thread_num);
        let read_spins = spins / thread_num;
        for _ in 0..thread_num {
            let read_queue = queue.clone();
            let read_thread = thread::spawn(move || {
                let mut count = 0;
                loop {
                    let result = read_queue.drain(|_| {}, 1000);
                    count = count + result;
                    if count == 0 {
                        thread::yield_now();
                    }
                    if count > (read_spins - 1000 * thread_num) {
                        break;
                    }
                }
            });
            read_threads.push(read_thread);
        }

        write_thread.join().unwrap();
        for num in 0..thread_num {
            read_threads.remove(thread_num - num - 1).join().unwrap();
        }
    }
}
