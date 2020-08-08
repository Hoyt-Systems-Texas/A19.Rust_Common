pub mod message_generated;

use message_generated::Guid;
use std::u64;

const LAST_MASK: u128 = u64::MAX as u128;

pub fn create_guid(guid: u128) -> Guid {
    Guid::new((guid >> 64 & LAST_MASK) as u64, (guid & LAST_MASK) as u64)
}

pub fn get_guid(guid: Guid) -> u128 {
    let first = (guid.first() as u128) << 64;
    first | (guid.last() as u128)
}

#[cfg(test)]
mod test {

    use super::*;
    use std::u128;

    #[test]
    fn create_guid_test() {
        let max_guid = u128::MAX;
        let guid = create_guid(max_guid);
        let parsed = get_guid(guid);
        assert_eq!(max_guid, parsed);
    }
}