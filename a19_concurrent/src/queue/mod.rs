use std::sync::atomic::AtomicUsize;

pub mod mpmc_queue;
pub mod mpsc_queue;
pub mod spsc_queue;

pub struct PaddedUsize {
    // Make sure we are on one cache line.
    #[warn(dead_code)]
    padding: [usize; 15], 
    pub counter: AtomicUsize
}

impl PaddedUsize {
    pub fn new(initial_value: usize) -> Self {
        PaddedUsize {
            padding: [0; 15],
            counter: AtomicUsize::new(0)
        }
    } 
}


pub trait ConcurrentQueue<T> {

    /// Used to poll the queue and moves the value to the option if there is a value.
    fn poll(&mut self) -> Option<T>;

    /// Offers a value to the queue.  Returns true if the value was successfully added.
    /// # Arguments
    /// `value` - The vale to add to the queue.
    fn offer(&mut self, value: T) -> bool;

    /// A quick way to drain all of the values from the queue.
    /// # Arguments
    /// `act` - The action to run against the queue.
    /// # Returns
    /// The number of items that where returned.
    fn drain(&mut self, act: fn(T), limit: usize) -> usize;
}
