use std::collections::hash_map::Values;
use std::{hash::Hash , collections::HashMap};

pub struct MultimapLookup<K1: Hash + Clone + Eq, K2: Hash + Clone + Eq, V> {
	submap_size: usize,
	map: HashMap<K1, HashMap<K2, V>>,
}

impl<K1: Hash + Clone + Eq, K2: Hash + Clone + Eq, V> MultimapLookup<K1, K2, V> {

	pub fn new(capacity: usize, submap_size: usize) -> Self {
		Self {
			submap_size,
			map: HashMap::with_capacity(capacity),
		}
	}

	pub fn insert(&mut self, k1: K1, k2: K2, v: V) {
		if let Some(internal) = self.map.get_mut(&k1) {
			internal.insert(k2, v);
		} else {
			let mut internal = HashMap::with_capacity(self.submap_size);
			internal.insert(k2, v);
			self.map.insert(k1, internal);
		}
	}

	pub fn get_mut(&mut self, k1: K1, k2: K2) -> Option<&mut V> {
		if let Some(internal) = self.map.get_mut(&k1) {
			internal.get_mut(&k2)
		} else {
			None
		}
	}

	pub fn get(&self, k1: K1, k2: K2) -> Option<& V> {
		if let Some(internal) = self.map.get(&k1) {
			internal.get(&k2)
		} else {
			None
		}
	}

	pub fn get_iter(&self, k1: &K1) -> Option<Values<K2, V>> {
		if let Some(map) = self.map.get(k1) {
			Some(map.values())
		} else {
			None
		}
	}

	pub fn remove(&mut self, k1: K1, k2: K2) -> Option<V> {
		if let Some(internal) = self.map.get_mut(&k1) {
			internal.remove(&k2)
		} else {
			None
		}
	}

	pub fn contains_key(&self, k1: &K1) -> bool {
		self.map.contains_key(k1)
	}
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	pub fn insert_test() {
		let mut map = MultimapLookup::new(10, 5);
		map.insert(1, 2, "v1");
		map.insert(1, 3, "v2");
		assert_eq!(Some(&"v1"), map.get(1, 2));
		assert_eq!(Some(&mut "v2"), map.get_mut(1, 3));
	}

	#[test]
	pub fn remote_test() {
		let mut map = MultimapLookup::new(10, 2);
		map.insert(1, 2, "v2");
		map.insert(1, 3, "v3");
		assert_eq!(Some("v2"), map.remove(1, 2));
	}
}