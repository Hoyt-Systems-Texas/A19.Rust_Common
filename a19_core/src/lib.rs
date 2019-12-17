use rand::{thread_rng, RngCore};

pub mod pow2;
pub mod js;
pub mod validation;

/// Generates a secure random number that is 128 bit long.
pub fn next_u128_rand() -> u128 {
    let mut thread = thread_rng();
    let first = thread.next_u64() as u128;
    let last = thread.next_u64() as u128;
    first | last << 64
}

#[cfg(test)]
mod test {

    use std::u64;
    use std::u128;
    use crate::next_u128_rand;

    #[test]
    pub fn next_u128_test() {
        let val = next_u128_rand();
        let max_u64 = u64::MAX as u128;
        let max_u128 = u128::MAX >> 2;
        assert!(val > max_u64);
        assert!(val > max_u128);
    }
}
