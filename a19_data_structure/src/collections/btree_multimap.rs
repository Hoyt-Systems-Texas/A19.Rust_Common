use std::{hash::Hash, collections::BTreeMap};

pub struct BTreeMultiMap<K: Eq + Hash + Ord, V> {
	map: BTreeMap<K, Vec<V>>,
	empty: [V; 0],
}

impl<K: Eq + Hash + Ord, V> BTreeMultiMap<K, V> {

	/// Inserts a value in the btree
	/// `key` - The key of the value to insert.
	/// `value` - The value to add.
	pub fn insert(&mut self, key: K, value: V) {
		if let Some(values) = self.map.get_mut(&key) {
			values.push(value);
		} else {
			self.map.insert(key, vec![value]);
		}
	}

	pub fn get(&self, k: &K) -> &[V] {
		if let Some(v) = self.map.get(k) {
			v
		} else {
			&self.empty
		}
	}
}