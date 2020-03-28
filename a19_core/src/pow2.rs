pub trait PowOf2 {
    fn round_to_power_of_two(&self) -> Self;
    fn next_power_of_two(&self) -> Self;
    fn is_power_of_2(&self) -> bool;
}

impl PowOf2 for u64 {
    fn round_to_power_of_two(&self) -> u64 {
        if self.is_power_of_2() {
            *self
        } else {
            self.next_power_of_two()
        }
    }
    fn next_power_of_two(&self) -> u64 {
        let mut x = *self;
        x |= x >> 1;
        x |= x >> 2;
        x |= x >> 4;
        x |= x >> 8;
        x |= x >> 16;
        x + 1
    }

    fn is_power_of_2(&self) -> bool {
        self & (self - 1) == 0
    }
}

impl PowOf2 for usize {
    fn round_to_power_of_two(&self) -> usize {
        if self.is_power_of_2() {
            *self
        } else {
            self.next_power_of_two()
        }
    }
    fn next_power_of_two(&self) -> usize {
        let mut x = *self;
        x |= x >> 1;
        x |= x >> 2;
        x |= x >> 4;
        x |= x >> 8;
        x |= x >> 16;
        x + 1
    }

    fn is_power_of_2(&self) -> bool {
        self & (self - 1) == 0
    }
}

#[cfg(test)]
pub mod tests {

    use crate::pow2::PowOf2;

    #[test]
    pub fn is_power_of_2_test_u64() {
        assert!((2 as u64).is_power_of_2());
        assert!((64 as u64).is_power_of_2());
    }

    #[test]
    pub fn is_power_of_2_no_test_u64() {
        assert!(!(3 as u64).is_power_of_2());
        assert!(!(9 as u64).is_power_of_2());
    }

    #[test]
    pub fn next_power_of_2_u64() {
        assert_eq!(8, (7 as u64).next_power_of_two());
        assert_eq!(16, (9 as u64).next_power_of_two());
    }

    #[test]
    pub fn is_power_of_2_test_usize() {
        assert!((2 as usize).is_power_of_2());
        assert!((64 as usize).is_power_of_2());
    }

    #[test]
    pub fn is_power_of_2_no_test_usize() {
        assert!(!(3 as usize).is_power_of_2());
        assert!(!(9 as usize).is_power_of_2());
    }

    #[test]
    pub fn next_power_of_2_usize() {
        assert_eq!(8, (7 as usize).next_power_of_two());
        assert_eq!(16, (9 as usize).next_power_of_two());
    }
}
