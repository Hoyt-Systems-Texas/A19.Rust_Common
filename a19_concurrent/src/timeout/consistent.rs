use std::collections::VecDeque;

///! A timeout data structure designed where you always just append with a fixed value.  The structure timeout
///! is designed to be called manually.
#[derive(Debug, Eq, PartialEq)]
pub struct TimeoutEntry<K> {
    /// The key for the entry.
    pub key: K,
    /// When the entry expires.
    pub expires: u64,
    /// The current version of the entry.
    pub version: u64,
}

pub struct TimeoutFixed<K> {
    queue: VecDeque<TimeoutEntry<K>>,
}

impl<K> TimeoutFixed<K> {
    pub fn with_capacity(capacity: usize) -> Self {
        TimeoutFixed {
            queue: VecDeque::with_capacity(capacity),
        }
    }

    /// Adds an new entry.
    pub fn add(&mut self, key: K, expires: u64, version: u64) {
        self.queue.push_back(TimeoutEntry {
            key,
            expires,
            version,
        });
    }

    /// Get the top entry if it's expired.
    /// # Arguments
    /// `expires` - The time when it expires.
    pub fn pop_expired(&mut self, expires: u64) -> Option<TimeoutEntry<K>> {
        if let Some(t) = self.queue.front() {
            if t.expires < expires {
                self.queue.pop_front()
            } else {
                None
            }
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::timeout::consistent::{TimeoutEntry, TimeoutFixed};

    #[test]
    pub fn test_timeout_not_expired() {
        let mut timeouts = TimeoutFixed::with_capacity(20);
        timeouts.add(1, 10, 2);
        timeouts.add(2, 12, 3);
        assert_eq!(None, timeouts.pop_expired(1));
    }

    #[test]
    pub fn test_timeout_expired() {
        let mut timeouts = TimeoutFixed::with_capacity(10);
        timeouts.add(1, 10, 2);
        timeouts.add(2, 12, 3);
        assert_eq!(
            Some(TimeoutEntry {
                expires: 10,
                key: 1,
                version: 2,
            }),
            timeouts.pop_expired(13)
        );
        assert_eq!(
            Some(TimeoutEntry {
                expires: 12,
                key: 2,
                version: 3,
            }),
            timeouts.pop_expired(13)
        );
        assert_eq!(None, timeouts.pop_expired(0));
    }
}
