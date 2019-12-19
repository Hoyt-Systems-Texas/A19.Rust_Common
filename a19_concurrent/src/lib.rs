#[macro_use]
extern crate time_test;

use std::sync::atomic::AtomicU64;

pub mod queue;
pub mod buffer;
pub mod map;
pub mod event;

pub struct PaddedU64 {
    // Make sure we are on one cache line.
    #[warn(dead_code)]
    padding: [u64; 15], 
    pub counter: AtomicU64
}

impl PaddedU64 {
    pub fn new(initial_value: u64) -> Self {
        PaddedU64 {
            padding: [0; 15],
            counter: AtomicU64::new(0)
        }
    } 
}
