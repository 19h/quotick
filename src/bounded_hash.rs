//! Fixed-capacity hash indexes for authoritative in-process state.
//!
//! Values are stored densely while a separately reserved open-addressed bucket
//! array maps keys to dense entry indexes. Insertion, replacement, lookup, and
//! backward-shift deletion perform no allocation after construction. Dense
//! `swap_remove` keeps iteration proportional to occupied entries rather than
//! bucket capacity.

use std::collections::hash_map::RandomState;
use std::fmt;
use std::hash::{BuildHasher, Hash};

/// Construction or finite-capacity failure for one bounded hash index.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BoundedHashError {
    /// The requested maximum cannot be represented by the table layout.
    CapacityLayout,
    /// Constructor-owned dense or bucket storage could not be reserved.
    Allocation,
    /// A new identity would exceed the configured semantic maximum.
    Full {
        /// Configured maximum occupied entries.
        maximum: usize,
    },
}

impl fmt::Display for BoundedHashError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CapacityLayout => {
                formatter.write_str("bounded hash capacity cannot be represented")
            }
            Self::Allocation => formatter.write_str("bounded hash storage allocation failed"),
            Self::Full { maximum } => {
                write!(formatter, "bounded hash capacity {maximum} is exhausted")
            }
        }
    }
}

impl std::error::Error for BoundedHashError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Bucket {
    Empty,
    Occupied { hash: u64, entry_index: usize },
}

#[derive(Clone)]
struct DenseEntry<K, V> {
    key: K,
    value: V,
    bucket_index: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Search {
    Found {
        bucket_index: usize,
        entry_index: usize,
    },
    Vacant {
        bucket_index: usize,
    },
}

/// A fixed-maximum dense hash map with no post-construction storage growth.
pub(crate) struct BoundedHashMap<K, V, S = RandomState> {
    entries: Vec<DenseEntry<K, V>>,
    buckets: Vec<Bucket>,
    maximum: usize,
    hash_builder: S,
}

impl<K, V, S> Clone for BoundedHashMap<K, V, S>
where
    K: Clone,
    V: Clone,
    S: Clone,
{
    fn clone(&self) -> Self {
        let mut entries = Vec::with_capacity(self.maximum);
        entries.extend(self.entries.iter().cloned());
        Self {
            entries,
            buckets: self.buckets.clone(),
            maximum: self.maximum,
            hash_builder: self.hash_builder.clone(),
        }
    }
}

impl<K, V> BoundedHashMap<K, V, RandomState>
where
    K: Eq + Hash,
{
    /// Fallibly reserves the complete dense and bucket layouts.
    pub(crate) fn try_new(maximum: usize) -> Result<Self, BoundedHashError> {
        Self::try_with_hasher(maximum, RandomState::new())
    }
}

impl<K, V, S> BoundedHashMap<K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    fn try_with_hasher(maximum: usize, hash_builder: S) -> Result<Self, BoundedHashError> {
        let bucket_count = bucket_count(maximum)?;
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(maximum)
            .map_err(|_| BoundedHashError::Allocation)?;
        let mut buckets = Vec::new();
        buckets
            .try_reserve_exact(bucket_count)
            .map_err(|_| BoundedHashError::Allocation)?;
        buckets.resize(bucket_count, Bucket::Empty);
        Ok(Self {
            entries,
            buckets,
            maximum,
            hash_builder,
        })
    }

    /// Returns the occupied entry count.
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns whether the map contains no entries.
    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns the configured semantic entry maximum.
    pub(crate) const fn maximum(&self) -> usize {
        self.maximum
    }

    /// Returns constructor-allocated dense-entry capacity.
    pub(crate) fn capacity(&self) -> usize {
        self.entries.capacity()
    }

    /// Returns constructor-allocated lookup-bucket capacity.
    #[cfg(test)]
    pub(crate) fn bucket_capacity(&self) -> usize {
        self.buckets.capacity()
    }

    /// Returns the number of initialized lookup buckets.
    pub(crate) fn bucket_count(&self) -> usize {
        self.buckets.len()
    }

    /// Returns whether `key` is present.
    pub(crate) fn contains_key(&self, key: &K) -> bool {
        self.entry_index(key).is_some()
    }

    /// Returns the value associated with `key`.
    pub(crate) fn get(&self, key: &K) -> Option<&V> {
        let entry_index = self.entry_index(key)?;
        Some(&self.entries[entry_index].value)
    }

    /// Returns the mutable value associated with `key`.
    pub(crate) fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        let entry_index = self.entry_index(key)?;
        Some(&mut self.entries[entry_index].value)
    }

    /// Inserts or replaces one value without allocating.
    ///
    /// A replacement succeeds even when the map is at its semantic maximum.
    pub(crate) fn try_insert(&mut self, key: K, value: V) -> Result<Option<V>, BoundedHashError> {
        let hash = self.hash_builder.hash_one(&key);
        match self.search(&key, hash) {
            Search::Found { entry_index, .. } => Ok(Some(std::mem::replace(
                &mut self.entries[entry_index].value,
                value,
            ))),
            Search::Vacant { bucket_index } => {
                if self.entries.len() >= self.maximum {
                    return Err(BoundedHashError::Full {
                        maximum: self.maximum,
                    });
                }
                assert!(
                    self.entries.len() < self.entries.capacity(),
                    "bounded hash dense reservation disappeared"
                );
                let entry_index = self.entries.len();
                self.entries.push(DenseEntry {
                    key,
                    value,
                    bucket_index,
                });
                self.buckets[bucket_index] = Bucket::Occupied { hash, entry_index };
                Ok(None)
            }
        }
    }

    /// Inserts or replaces one value, asserting prior semantic capacity proof.
    pub(crate) fn insert(&mut self, key: K, value: V) -> Option<V> {
        self.try_insert(key, value)
            .expect("bounded hash insertion must be capacity-preflighted")
    }

    /// Removes one key and returns its value without allocating.
    pub(crate) fn remove(&mut self, key: &K) -> Option<V> {
        let hash = self.hash_builder.hash_one(key);
        let Search::Found {
            bucket_index,
            entry_index,
        } = self.search(key, hash)
        else {
            return None;
        };
        self.erase_bucket(bucket_index);
        let removed = self.entries.swap_remove(entry_index);
        if entry_index < self.entries.len() {
            let moved_bucket_index = self.entries[entry_index].bucket_index;
            let Bucket::Occupied {
                entry_index: indexed_entry,
                ..
            } = &mut self.buckets[moved_bucket_index]
            else {
                panic!("bounded hash moved entry lost its bucket");
            };
            *indexed_entry = entry_index;
        }
        Some(removed.value)
    }

    /// Removes every entry while retaining the complete allocation.
    pub(crate) fn clear(&mut self) {
        for entry in &self.entries {
            self.buckets[entry.bucket_index] = Bucket::Empty;
        }
        self.entries.clear();
    }

    /// Iterates values in dense process-local order.
    pub(crate) fn values(&self) -> impl ExactSizeIterator<Item = &V> {
        self.entries.iter().map(|entry| &entry.value)
    }

    /// Mutably iterates values in dense process-local order.
    pub(crate) fn values_mut(&mut self) -> impl ExactSizeIterator<Item = &mut V> {
        self.entries.iter_mut().map(|entry| &mut entry.value)
    }

    /// Iterates keys in dense process-local order.
    pub(crate) fn keys(&self) -> impl ExactSizeIterator<Item = &K> {
        self.entries.iter().map(|entry| &entry.key)
    }

    /// Iterates key/value pairs in dense process-local order.
    pub(crate) fn iter(&self) -> impl ExactSizeIterator<Item = (&K, &V)> {
        self.entries.iter().map(|entry| (&entry.key, &entry.value))
    }

    /// Audits dense/bucket cross-links, hashes, uniqueness, and fixed layout.
    pub(crate) fn validate_layout(&self) -> Result<(), &'static str> {
        let expected_buckets = bucket_count(self.maximum)
            .map_err(|_| "bounded hash semantic maximum has no valid bucket layout")?;
        if self.buckets.len() != expected_buckets || !self.buckets.len().is_power_of_two() {
            return Err("bounded hash bucket layout contradicts its semantic maximum");
        }
        if self.entries.capacity() < self.maximum || self.entries.len() > self.maximum {
            return Err("bounded hash dense capacity or cardinality contradicts its maximum");
        }
        for (entry_index, entry) in self.entries.iter().enumerate() {
            let Bucket::Occupied {
                hash,
                entry_index: indexed_entry,
            } = self.buckets[entry.bucket_index]
            else {
                return Err("bounded hash dense entry references an empty bucket");
            };
            if indexed_entry != entry_index {
                return Err("bounded hash dense entry references another entry's bucket");
            }
            if self.search(&entry.key, hash)
                != (Search::Found {
                    bucket_index: entry.bucket_index,
                    entry_index,
                })
            {
                return Err("bounded hash key is not uniquely reachable from its home bucket");
            }
        }
        Ok(())
    }

    #[cfg(test)]
    fn validate_complete_layout(&self) -> Result<(), &'static str> {
        self.validate_layout()?;
        let mut occupied_buckets = 0_usize;
        for (bucket_index, bucket) in self.buckets.iter().copied().enumerate() {
            let Bucket::Occupied { hash, entry_index } = bucket else {
                continue;
            };
            occupied_buckets += 1;
            let Some(entry) = self.entries.get(entry_index) else {
                return Err("bounded hash bucket references an absent dense entry");
            };
            if entry.bucket_index != bucket_index {
                return Err("bounded hash bucket and dense entry cross-links disagree");
            }
            if self.hash_builder.hash_one(&entry.key) != hash {
                return Err("bounded hash bucket retains the wrong key hash");
            }
        }
        if occupied_buckets != self.entries.len() {
            return Err("bounded hash occupied bucket count differs from dense cardinality");
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn shrink_to_fit(&mut self) {
        self.entries.shrink_to_fit();
    }

    fn entry_index(&self, key: &K) -> Option<usize> {
        let hash = self.hash_builder.hash_one(key);
        match self.search(key, hash) {
            Search::Found { entry_index, .. } => Some(entry_index),
            Search::Vacant { .. } => None,
        }
    }

    fn search(&self, key: &K, hash: u64) -> Search {
        let mask = self.buckets.len() - 1;
        let mut bucket_index = hash_index(hash) & mask;
        loop {
            match self.buckets[bucket_index] {
                Bucket::Empty => return Search::Vacant { bucket_index },
                Bucket::Occupied {
                    hash: candidate_hash,
                    entry_index,
                } if candidate_hash == hash && self.entries[entry_index].key == *key => {
                    return Search::Found {
                        bucket_index,
                        entry_index,
                    };
                }
                Bucket::Occupied { .. } => {
                    bucket_index = (bucket_index + 1) & mask;
                }
            }
        }
    }

    fn erase_bucket(&mut self, mut hole: usize) {
        let mask = self.buckets.len() - 1;
        self.buckets[hole] = Bucket::Empty;
        let mut scan = (hole + 1) & mask;
        loop {
            let Bucket::Occupied { hash, entry_index } = self.buckets[scan] else {
                return;
            };
            let ideal_bucket = hash_index(hash) & mask;
            if probe_distance(ideal_bucket, scan, mask) > probe_distance(ideal_bucket, hole, mask) {
                self.buckets[hole] = self.buckets[scan];
                self.buckets[scan] = Bucket::Empty;
                self.entries[entry_index].bucket_index = hole;
                hole = scan;
            }
            scan = (scan + 1) & mask;
        }
    }
}

impl<K, V, S> fmt::Debug for BoundedHashMap<K, V, S>
where
    K: Eq + Hash + fmt::Debug,
    V: fmt::Debug,
    S: BuildHasher,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_map().entries(self.iter()).finish()
    }
}

impl<K, V, S> PartialEq for BoundedHashMap<K, V, S>
where
    K: Eq + Hash,
    V: PartialEq,
    S: BuildHasher,
{
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len()
            && self
                .iter()
                .all(|(key, value)| other.get(key) == Some(value))
    }
}

impl<K, V, S> Eq for BoundedHashMap<K, V, S>
where
    K: Eq + Hash,
    V: Eq,
    S: BuildHasher,
{
}

/// A fixed-maximum hash set backed by [`BoundedHashMap`].
#[derive(Clone)]
pub(crate) struct BoundedHashSet<K, S = RandomState> {
    values: BoundedHashMap<K, (), S>,
}

impl<K> BoundedHashSet<K, RandomState>
where
    K: Eq + Hash,
{
    /// Fallibly reserves the complete set layout.
    pub(crate) fn try_new(maximum: usize) -> Result<Self, BoundedHashError> {
        Ok(Self {
            values: BoundedHashMap::try_new(maximum)?,
        })
    }
}

impl<K, S> BoundedHashSet<K, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    /// Inserts one identity, returning whether it was new.
    pub(crate) fn insert(&mut self, key: K) -> bool {
        self.values.insert(key, ()).is_none()
    }

    /// Returns whether the identity is present.
    pub(crate) fn contains(&self, key: &K) -> bool {
        self.values.contains_key(key)
    }

    /// Returns the occupied identity count.
    pub(crate) fn len(&self) -> usize {
        self.values.len()
    }

    /// Returns whether the set is empty.
    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Returns the configured semantic identity maximum.
    #[cfg(test)]
    pub(crate) fn maximum(&self) -> usize {
        self.values.maximum()
    }

    /// Returns constructor-allocated dense-entry capacity.
    pub(crate) fn capacity(&self) -> usize {
        self.values.capacity()
    }

    /// Iterates identities in dense process-local order.
    pub(crate) fn iter(&self) -> impl ExactSizeIterator<Item = &K> {
        self.values.keys()
    }

    /// Audits the backing map layout.
    pub(crate) fn validate_layout(&self) -> Result<(), &'static str> {
        self.values.validate_layout()
    }
}

impl<K, S> fmt::Debug for BoundedHashSet<K, S>
where
    K: Eq + Hash + fmt::Debug,
    S: BuildHasher,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_set().entries(self.iter()).finish()
    }
}

impl<K, S> PartialEq for BoundedHashSet<K, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len() && self.iter().all(|key| other.contains(key))
    }
}

impl<K, S> Eq for BoundedHashSet<K, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
}

const fn probe_distance(home: usize, index: usize, mask: usize) -> usize {
    index.wrapping_sub(home) & mask
}

#[cfg(target_pointer_width = "64")]
fn hash_index(hash: u64) -> usize {
    usize::from_ne_bytes(hash.to_ne_bytes())
}

fn bucket_count(maximum: usize) -> Result<usize, BoundedHashError> {
    maximum
        .checked_mul(2)
        .ok_or(BoundedHashError::CapacityLayout)?
        .max(1)
        .checked_next_power_of_two()
        .ok_or(BoundedHashError::CapacityLayout)
}

#[cfg(target_pointer_width = "32")]
fn hash_index(hash: u64) -> usize {
    let bytes = hash.to_ne_bytes();
    usize::from_ne_bytes([
        bytes[0] ^ bytes[4],
        bytes[1] ^ bytes[5],
        bytes[2] ^ bytes[6],
        bytes[3] ^ bytes[7],
    ])
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::hash::{BuildHasher, Hasher};

    use super::{BoundedHashError, BoundedHashMap, BoundedHashSet};

    #[derive(Clone, Copy, Debug, Default)]
    struct ConstantBuildHasher;

    struct ConstantHasher;

    impl Hasher for ConstantHasher {
        fn finish(&self) -> u64 {
            u64::MAX
        }

        fn write(&mut self, _bytes: &[u8]) {}
    }

    impl BuildHasher for ConstantBuildHasher {
        type Hasher = ConstantHasher;

        fn build_hasher(&self) -> Self::Hasher {
            ConstantHasher
        }
    }

    #[test]
    fn collision_clusters_survive_head_middle_tail_removal_and_dense_swaps() {
        let mut map = BoundedHashMap::try_with_hasher(8, ConstantBuildHasher).unwrap();
        for key in 1_u64..=8 {
            assert_eq!(map.try_insert(key, key * 10), Ok(None));
        }
        assert_eq!(
            map.try_insert(9, 90),
            Err(BoundedHashError::Full { maximum: 8 })
        );
        assert_eq!(map.try_insert(4, 444), Ok(Some(40)));
        assert_eq!(map.remove(&1), Some(10));
        assert_eq!(map.remove(&4), Some(444));
        assert_eq!(map.remove(&8), Some(80));
        for key in [2_u64, 3, 5, 6, 7] {
            assert_eq!(map.get(&key), Some(&(key * 10)));
        }
        assert_eq!(map.try_insert(9, 90), Ok(None));
        assert_eq!(map.try_insert(10, 100), Ok(None));
        assert_eq!(map.try_insert(11, 110), Ok(None));
        assert_eq!(map.len(), 8);
        assert_eq!(map.capacity(), 8);
        assert_eq!(map.bucket_capacity(), 16);
    }

    #[test]
    fn generated_churn_matches_std_map_without_capacity_growth() {
        const MAXIMUM: usize = 64;
        let mut actual = BoundedHashMap::try_new(MAXIMUM).unwrap();
        let dense_capacity = actual.capacity();
        let bucket_capacity = actual.bucket_capacity();
        let mut expected = HashMap::new();
        let mut state = 0x9e37_79b9_7f4a_7c15_u64;
        for step in 0_u64..100_000 {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let key = (state >> 32) % 257;
            match state & 3 {
                0 | 1 => {
                    let value = state ^ step;
                    if expected.contains_key(&key) || expected.len() < MAXIMUM {
                        assert_eq!(
                            actual.try_insert(key, value).unwrap(),
                            expected.insert(key, value)
                        );
                    } else {
                        assert_eq!(
                            actual.try_insert(key, value),
                            Err(BoundedHashError::Full { maximum: MAXIMUM })
                        );
                    }
                }
                2 => assert_eq!(actual.remove(&key), expected.remove(&key)),
                _ => assert_eq!(actual.get(&key), expected.get(&key)),
            }
            assert_eq!(actual.len(), expected.len());
            assert_eq!(actual.capacity(), dense_capacity);
            assert_eq!(actual.bucket_capacity(), bucket_capacity);
            for (key, value) in &expected {
                assert_eq!(actual.get(key), Some(value));
            }
        }
    }

    #[test]
    fn set_identity_semantics_and_clear_retain_allocation() {
        let mut set = BoundedHashSet::try_new(3).unwrap();
        assert!(set.is_empty());
        assert!(set.insert(7_u64));
        assert!(!set.insert(7));
        assert!(set.insert(8));
        assert!(set.insert(9));
        assert_eq!(set.len(), 3);
        assert_eq!(set.maximum(), 3);
        assert!(set.contains(&8));
        let capacity = set.capacity();
        set.values.clear();
        assert!(set.is_empty());
        assert_eq!(set.capacity(), capacity);
        assert!(set.insert(10));
    }

    #[test]
    fn unrepresentable_layout_and_zero_capacity_fail_closed() {
        assert!(matches!(
            BoundedHashMap::<u64, u64>::try_new(usize::MAX),
            Err(BoundedHashError::CapacityLayout)
        ));
        let mut map = BoundedHashMap::<u64, u64>::try_new(0).unwrap();
        assert_eq!(map.maximum(), 0);
        assert_eq!(
            map.try_insert(1, 1),
            Err(BoundedHashError::Full { maximum: 0 })
        );
        assert_eq!(map.remove(&1), None);
    }

    #[test]
    fn clone_retains_complete_semantic_headroom_and_hash_layout() {
        let mut original = BoundedHashMap::try_new(8).unwrap();
        original.insert(1_u64, 10_u64);
        original.insert(2, 20);
        let mut cloned = original.clone();
        assert_eq!(cloned, original);
        assert!(cloned.capacity() >= 8);
        assert_eq!(cloned.bucket_capacity(), original.bucket_capacity());
        for key in 3_u64..=8 {
            assert_eq!(cloned.try_insert(key, key * 10), Ok(None));
        }
        assert_eq!(cloned.len(), 8);
        assert_eq!(original.len(), 2);
    }

    #[test]
    fn layout_audit_rejects_hash_cross_link_and_duplicate_bucket_corruption() {
        let mut map = BoundedHashMap::try_new(4).unwrap();
        map.insert(1_u64, 10_u64);
        map.insert(2, 20);
        map.validate_complete_layout().unwrap();

        let first_bucket = map.entries[0].bucket_index;
        let original_bucket = map.buckets[first_bucket];
        let super::Bucket::Occupied { hash, entry_index } = original_bucket else {
            panic!("inserted entry must have an occupied bucket")
        };
        map.buckets[first_bucket] = super::Bucket::Occupied {
            hash: hash ^ 1,
            entry_index,
        };
        assert!(map.validate_complete_layout().is_err());
        map.buckets[first_bucket] = original_bucket;

        map.entries[0].bucket_index = (first_bucket + 1) & (map.buckets.len() - 1);
        assert!(map.validate_complete_layout().is_err());
        map.entries[0].bucket_index = first_bucket;

        let empty_bucket = map
            .buckets
            .iter()
            .position(|bucket| *bucket == super::Bucket::Empty)
            .unwrap();
        map.buckets[empty_bucket] = original_bucket;
        assert!(map.validate_complete_layout().is_err());
    }
}
