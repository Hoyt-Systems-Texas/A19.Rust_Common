//! Contains helpers for working with JavaScript.
use rand::{thread_rng, RngCore};

const JS_MAX_INT_MASK: u64 = 0b00011111_11111111_11111111_11111111_11111111_11111111_11111111;

/// Used to get the next javascript random number generator.
pub fn next_js_rand() -> u64 {
    let mut thread = thread_rng();
    thread.next_u64() & JS_MAX_INT_MASK
}
