pub mod mpmc_queue;

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
