use rand::{thread_rng, RngCore};
use std::time::SystemTime;

pub mod js;
pub mod pow2;
pub mod validation;

/// Generates a secure random number that is 128 bit long.
pub fn next_u128_rand() -> u128 {
    let mut thread = thread_rng();
    let first = thread.next_u64() as u128;
    let last = thread.next_u64() as u128;
    first | last << 64
}

/// Used to get the current time in seconds.
pub fn current_time_secs() -> u64 {
    match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
        Ok(n) => n.as_secs(),
        Err(_) => 0
    }
}

pub fn current_time_ms() -> u64 {
    match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
        Ok(n) => n.as_millis() as u64,
        Err(_) => 0
    }
}

#[cfg(test)]
mod test {

    use crate::{next_u128_rand, current_time_secs };
    use std::u128;
    use std::u64;

    #[test]
    pub fn next_u128_test() {
        let val = next_u128_rand();
        let max_u64 = u64::MAX as u128;
        let max_u128 = u128::MAX >> 2;
        assert!(val > max_u64);
        assert!(val > max_u128);
    }

    #[test]
    pub fn current_time_secs_test() {
        let time = current_time_secs();
        assert!(time > 10000)
    }
}
