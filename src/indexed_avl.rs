use std::cmp::Ordering;
use std::collections::TryReserveError;

// An AVL tree holding every addressable Rust allocation has height below 128:
// the minimum node count at height h follows N(h) = N(h-1) + N(h-2) + 1.
// A fixed iterator stack therefore avoids an auxiliary heap allocation while
// retaining a corruption guard substantially above the valid height bound.
const MAX_AVL_HEIGHT: usize = usize::BITS as usize * 2;

#[derive(Clone, Copy, Debug)]
struct Node<K, V> {
    key: K,
    value: V,
    left: Option<usize>,
    right: Option<usize>,
    height: u16,
}

#[derive(Clone, Copy, Debug)]
enum Slot<K, V> {
    Occupied(Node<K, V>),
    Vacant { next: Option<usize> },
}

/// A finitely bounded ordered map backed by a stable-slot AVL arena.
///
/// Construction fallibly reserves the complete slot allocation. Insertion and
/// deletion subsequently perform no heap allocation: removed slots form an
/// intrusive free list and are reused before the arena's initialized prefix can
/// grow. Keys and values move between slots during two-child deletion, so slot
/// indices are deliberately not exposed.
#[derive(Debug)]
pub(crate) struct IndexedAvlMap<K, V> {
    root: Option<usize>,
    slots: Vec<Slot<K, V>>,
    free_head: Option<usize>,
    len: usize,
    max_entries: usize,
}

impl<K, V> IndexedAvlMap<K, V> {
    pub(crate) fn try_with_capacity(max_entries: usize) -> Result<Self, TryReserveError> {
        let mut slots = Vec::new();
        slots.try_reserve_exact(max_entries)?;
        Ok(Self {
            root: None,
            slots,
            free_head: None,
            len: 0,
            max_entries,
        })
    }

    pub(crate) const fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn has_insertion_capacity(&self) -> bool {
        self.free_head.is_some()
            || (self.slots.len() < self.max_entries && self.slots.len() < self.slots.capacity())
    }

    pub(crate) fn allocation_capacity(&self) -> usize {
        self.slots.capacity()
    }

    pub(crate) fn storage_len(&self) -> usize {
        self.slots.len()
    }
}

impl<K: Ord + Copy, V: Copy> IndexedAvlMap<K, V> {
    pub(crate) fn get(&self, key: &K) -> Option<&V> {
        self.find_index(key).map(|index| &self.node(index).value)
    }

    pub(crate) fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        let index = self.find_index(key)?;
        Some(&mut self.node_mut(index).value)
    }

    pub(crate) fn insert(&mut self, key: K, value: V) -> Option<V> {
        if let Some(index) = self.find_index(&key) {
            return Some(std::mem::replace(&mut self.node_mut(index).value, value));
        }
        assert!(
            self.has_insertion_capacity(),
            "indexed AVL insertion must have pre-reserved capacity"
        );
        let index = self.allocate_slot(key, value);
        self.root = Some(self.insert_index(self.root, index, key));
        self.len += 1;
        None
    }

    pub(crate) fn remove(&mut self, key: &K) -> Option<V> {
        let original_index = self.find_index(key)?;
        let original_value = self.node(original_index).value;
        let mut removed_index = None;
        self.root = self.delete_index(self.root, key, &mut removed_index);
        let removed_index = removed_index.expect("found AVL key must remove one occupied slot");
        self.release_slot(removed_index);
        self.len -= 1;
        Some(original_value)
    }

    pub(crate) fn first_key_value(&self) -> Option<(&K, &V)> {
        let index = self.minimum_index(self.root?);
        let node = self.node(index);
        Some((&node.key, &node.value))
    }

    pub(crate) fn last_key_value(&self) -> Option<(&K, &V)> {
        let index = self.maximum_index(self.root?);
        let node = self.node(index);
        Some((&node.key, &node.value))
    }

    pub(crate) fn predecessor_key(&self, key: &K) -> Option<&K> {
        let mut current = self.root;
        let mut candidate = None;
        while let Some(index) = current {
            let node = self.node(index);
            if node.key < *key {
                candidate = Some(index);
                current = node.right;
            } else {
                current = node.left;
            }
        }
        candidate.map(|index| &self.node(index).key)
    }

    pub(crate) fn successor_key(&self, key: &K) -> Option<&K> {
        let mut current = self.root;
        let mut candidate = None;
        while let Some(index) = current {
            let node = self.node(index);
            if node.key > *key {
                candidate = Some(index);
                current = node.left;
            } else {
                current = node.right;
            }
        }
        candidate.map(|index| &self.node(index).key)
    }

    pub(crate) fn iter(&self) -> Iter<'_, K, V> {
        Iter::new(self)
    }

    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.len > self.max_entries {
            return Err("indexed AVL cardinality exceeds its finite bound".to_owned());
        }
        if self.slots.len() > self.max_entries || self.slots.capacity() < self.max_entries {
            return Err("indexed AVL arena capacity contradicts its finite bound".to_owned());
        }
        if self.root.is_none() != (self.len == 0) {
            return Err("indexed AVL root/length state is inconsistent".to_owned());
        }

        let mut states = vec![0_u8; self.slots.len()];
        let reachable = match self.root {
            Some(root) => self.validate_subtree(root, None, None, 0, &mut states)?.0,
            None => 0,
        };
        if reachable != self.len {
            return Err("indexed AVL reachable cardinality differs from length".to_owned());
        }

        let mut free_seen = vec![false; self.slots.len()];
        let mut current = self.free_head;
        while let Some(index) = current {
            let slot = self
                .slots
                .get(index)
                .copied()
                .ok_or_else(|| "indexed AVL free list references an absent slot".to_owned())?;
            if std::mem::replace(&mut free_seen[index], true) {
                return Err("indexed AVL free list contains a cycle".to_owned());
            }
            current = match slot {
                Slot::Vacant { next } => next,
                Slot::Occupied(_) => {
                    return Err("indexed AVL free list references an occupied slot".to_owned());
                }
            };
        }

        let mut occupied = 0_usize;
        for (index, slot) in self.slots.iter().enumerate() {
            match slot {
                Slot::Occupied(_) => {
                    occupied += 1;
                    if states[index] != 2 {
                        return Err("indexed AVL contains an unreachable occupied slot".to_owned());
                    }
                }
                Slot::Vacant { .. } if !free_seen[index] => {
                    return Err("indexed AVL contains an unlinked vacant slot".to_owned());
                }
                Slot::Vacant { .. } => {}
            }
        }
        if occupied != self.len {
            return Err("indexed AVL occupied-slot count differs from length".to_owned());
        }
        Ok(())
    }

    fn validate_subtree(
        &self,
        index: usize,
        lower: Option<K>,
        upper: Option<K>,
        depth: usize,
        states: &mut [u8],
    ) -> Result<(usize, u16), String> {
        if depth >= MAX_AVL_HEIGHT {
            return Err("indexed AVL exceeds the addressable height bound".to_owned());
        }
        let slot = self
            .slots
            .get(index)
            .copied()
            .ok_or_else(|| "indexed AVL child references an absent slot".to_owned())?;
        if states[index] != 0 {
            return Err("indexed AVL contains a cycle or duplicate child".to_owned());
        }
        states[index] = 1;
        let node = match slot {
            Slot::Occupied(node) => node,
            Slot::Vacant { .. } => {
                return Err("indexed AVL child references a vacant slot".to_owned());
            }
        };
        if lower.is_some_and(|bound| node.key <= bound)
            || upper.is_some_and(|bound| node.key >= bound)
        {
            return Err("indexed AVL violates strict key ordering".to_owned());
        }
        let (left_count, left_height) = match node.left {
            Some(left) => self.validate_subtree(left, lower, Some(node.key), depth + 1, states)?,
            None => (0, 0),
        };
        let (right_count, right_height) = match node.right {
            Some(right) => {
                self.validate_subtree(right, Some(node.key), upper, depth + 1, states)?
            }
            None => (0, 0),
        };
        if left_height.abs_diff(right_height) > 1 {
            return Err("indexed AVL balance factor exceeds one".to_owned());
        }
        let expected_height = left_height
            .max(right_height)
            .checked_add(1)
            .expect("valid AVL height must fit u16");
        if node.height != expected_height {
            return Err("indexed AVL cached height is inconsistent".to_owned());
        }
        states[index] = 2;
        let count = left_count
            .checked_add(right_count)
            .and_then(|value| value.checked_add(1))
            .ok_or_else(|| "indexed AVL reachable count is exhausted".to_owned())?;
        Ok((count, expected_height))
    }

    fn find_index(&self, key: &K) -> Option<usize> {
        let mut current = self.root;
        while let Some(index) = current {
            let node = self.node(index);
            match key.cmp(&node.key) {
                Ordering::Less => current = node.left,
                Ordering::Greater => current = node.right,
                Ordering::Equal => return Some(index),
            }
        }
        None
    }

    fn insert_index(&mut self, root: Option<usize>, inserted: usize, key: K) -> usize {
        let Some(root) = root else {
            return inserted;
        };
        match key.cmp(&self.node(root).key) {
            Ordering::Less => {
                let child = self.insert_index(self.node(root).left, inserted, key);
                self.node_mut(root).left = Some(child);
            }
            Ordering::Greater => {
                let child = self.insert_index(self.node(root).right, inserted, key);
                self.node_mut(root).right = Some(child);
            }
            Ordering::Equal => unreachable!("duplicate AVL key must be replaced before insertion"),
        }
        self.rebalance(root)
    }

    fn delete_index(
        &mut self,
        root: Option<usize>,
        key: &K,
        removed: &mut Option<usize>,
    ) -> Option<usize> {
        let root = root?;
        match key.cmp(&self.node(root).key) {
            Ordering::Less => {
                let child = self.delete_index(self.node(root).left, key, removed);
                self.node_mut(root).left = child;
            }
            Ordering::Greater => {
                let child = self.delete_index(self.node(root).right, key, removed);
                self.node_mut(root).right = child;
            }
            Ordering::Equal => {
                let (left, right) = {
                    let node = self.node(root);
                    (node.left, node.right)
                };
                match (left, right) {
                    (None, child) | (child, None) => {
                        *removed = Some(root);
                        return child;
                    }
                    (Some(_), Some(right)) => {
                        let successor = self.minimum_index(right);
                        let (successor_key, successor_value) = {
                            let node = self.node(successor);
                            (node.key, node.value)
                        };
                        {
                            let node = self.node_mut(root);
                            node.key = successor_key;
                            node.value = successor_value;
                        }
                        let child = self.delete_index(Some(right), &successor_key, removed);
                        self.node_mut(root).right = child;
                    }
                }
            }
        }
        Some(self.rebalance(root))
    }

    fn rebalance(&mut self, index: usize) -> usize {
        self.update_height(index);
        let balance = self.balance_factor(index);
        if balance > 1 {
            let left = self
                .node(index)
                .left
                .expect("positive AVL balance requires a left child");
            if self.balance_factor(left) < 0 {
                let rotated = self.rotate_left(left);
                self.node_mut(index).left = Some(rotated);
            }
            return self.rotate_right(index);
        }
        if balance < -1 {
            let right = self
                .node(index)
                .right
                .expect("negative AVL balance requires a right child");
            if self.balance_factor(right) > 0 {
                let rotated = self.rotate_right(right);
                self.node_mut(index).right = Some(rotated);
            }
            return self.rotate_left(index);
        }
        index
    }

    fn rotate_left(&mut self, root: usize) -> usize {
        let pivot = self
            .node(root)
            .right
            .expect("left rotation requires a right child");
        let transferred = self.node(pivot).left;
        self.node_mut(root).right = transferred;
        self.node_mut(pivot).left = Some(root);
        self.update_height(root);
        self.update_height(pivot);
        pivot
    }

    fn rotate_right(&mut self, root: usize) -> usize {
        let pivot = self
            .node(root)
            .left
            .expect("right rotation requires a left child");
        let transferred = self.node(pivot).right;
        self.node_mut(root).left = transferred;
        self.node_mut(pivot).right = Some(root);
        self.update_height(root);
        self.update_height(pivot);
        pivot
    }

    fn update_height(&mut self, index: usize) {
        let (left, right) = {
            let node = self.node(index);
            (node.left, node.right)
        };
        self.node_mut(index).height = self
            .height(left)
            .max(self.height(right))
            .checked_add(1)
            .expect("valid AVL height must fit u16");
    }

    fn balance_factor(&self, index: usize) -> i32 {
        let node = self.node(index);
        i32::from(self.height(node.left)) - i32::from(self.height(node.right))
    }

    fn height(&self, index: Option<usize>) -> u16 {
        index.map_or(0, |index| self.node(index).height)
    }

    fn minimum_index(&self, mut index: usize) -> usize {
        while let Some(left) = self.node(index).left {
            index = left;
        }
        index
    }

    fn maximum_index(&self, mut index: usize) -> usize {
        while let Some(right) = self.node(index).right {
            index = right;
        }
        index
    }

    fn allocate_slot(&mut self, key: K, value: V) -> usize {
        let node = Node {
            key,
            value,
            left: None,
            right: None,
            height: 1,
        };
        if let Some(index) = self.free_head {
            self.free_head = match self.slots.get(index).copied() {
                Some(Slot::Vacant { next }) => next,
                Some(Slot::Occupied(_)) | None => {
                    unreachable!("AVL free head must reference a vacant slot")
                }
            };
            self.slots[index] = Slot::Occupied(node);
            index
        } else {
            let index = self.slots.len();
            self.slots.push(Slot::Occupied(node));
            index
        }
    }

    fn release_slot(&mut self, index: usize) {
        debug_assert!(matches!(self.slots[index], Slot::Occupied(_)));
        self.slots[index] = Slot::Vacant {
            next: self.free_head,
        };
        self.free_head = Some(index);
    }

    fn node(&self, index: usize) -> &Node<K, V> {
        match &self.slots[index] {
            Slot::Occupied(node) => node,
            Slot::Vacant { .. } => panic!("indexed AVL path references a vacant slot"),
        }
    }

    fn node_mut(&mut self, index: usize) -> &mut Node<K, V> {
        match &mut self.slots[index] {
            Slot::Occupied(node) => node,
            Slot::Vacant { .. } => panic!("indexed AVL path references a vacant slot"),
        }
    }
}

impl<K: Ord + Copy + PartialEq, V: Copy + PartialEq> PartialEq for IndexedAvlMap<K, V> {
    fn eq(&self, other: &Self) -> bool {
        self.len == other.len
            && self.iter().zip(other.iter()).all(
                |((left_key, left_value), (right_key, right_value))| {
                    left_key == right_key && left_value == right_value
                },
            )
    }
}

impl<K: Ord + Copy + Eq, V: Copy + Eq> Eq for IndexedAvlMap<K, V> {}

#[cfg(test)]
impl<K: Clone, V: Clone> Clone for IndexedAvlMap<K, V> {
    fn clone(&self) -> Self {
        let mut slots = Vec::with_capacity(self.max_entries);
        slots.extend(self.slots.iter().cloned());
        Self {
            root: self.root,
            slots,
            free_head: self.free_head,
            len: self.len,
            max_entries: self.max_entries,
        }
    }
}

pub(crate) struct Iter<'a, K, V> {
    map: &'a IndexedAvlMap<K, V>,
    front: [usize; MAX_AVL_HEIGHT],
    front_depth: usize,
    back: [usize; MAX_AVL_HEIGHT],
    back_depth: usize,
    remaining: usize,
}

impl<'a, K: Ord + Copy, V: Copy> Iter<'a, K, V> {
    fn new(map: &'a IndexedAvlMap<K, V>) -> Self {
        let mut iterator = Self {
            map,
            front: [0; MAX_AVL_HEIGHT],
            front_depth: 0,
            back: [0; MAX_AVL_HEIGHT],
            back_depth: 0,
            remaining: map.len,
        };
        iterator.push_left(map.root);
        iterator.push_right(map.root);
        iterator
    }

    fn push_left(&mut self, mut current: Option<usize>) {
        while let Some(index) = current {
            assert!(
                self.front_depth < MAX_AVL_HEIGHT,
                "indexed AVL iterator exceeds its valid height bound"
            );
            self.front[self.front_depth] = index;
            self.front_depth += 1;
            current = self.map.node(index).left;
        }
    }

    fn push_right(&mut self, mut current: Option<usize>) {
        while let Some(index) = current {
            assert!(
                self.back_depth < MAX_AVL_HEIGHT,
                "indexed AVL iterator exceeds its valid height bound"
            );
            self.back[self.back_depth] = index;
            self.back_depth += 1;
            current = self.map.node(index).right;
        }
    }
}

impl<'a, K: Ord + Copy, V: Copy> Iterator for Iter<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        self.front_depth -= 1;
        let index = self.front[self.front_depth];
        let node = self.map.node(index);
        self.push_left(node.right);
        self.remaining -= 1;
        Some((&node.key, &node.value))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl<K: Ord + Copy, V: Copy> DoubleEndedIterator for Iter<'_, K, V> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        self.back_depth -= 1;
        let index = self.back[self.back_depth];
        let node = self.map.node(index);
        self.push_right(node.left);
        self.remaining -= 1;
        Some((&node.key, &node.value))
    }
}

impl<K: Ord + Copy, V: Copy> ExactSizeIterator for Iter<'_, K, V> {}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{IndexedAvlMap, Slot};

    #[test]
    fn all_rotation_shapes_preserve_total_order_and_reverse_iteration() {
        for insertion_order in [[30_i64, 20, 10], [10, 20, 30], [30, 10, 20], [10, 30, 20]] {
            let mut map = IndexedAvlMap::try_with_capacity(3).unwrap();
            for key in insertion_order {
                assert_eq!(map.insert(key, key * 10), None);
            }
            map.validate().unwrap();
            assert_eq!(
                map.iter()
                    .map(|(&key, &value)| (key, value))
                    .collect::<Vec<_>>(),
                vec![(10, 100), (20, 200), (30, 300)]
            );
            assert_eq!(
                map.iter().rev().map(|(&key, _)| key).collect::<Vec<_>>(),
                vec![30, 20, 10]
            );
            assert_eq!(map.first_key_value(), Some((&10, &100)));
            assert_eq!(map.last_key_value(), Some((&30, &300)));
            assert_eq!(map.predecessor_key(&20), Some(&10));
            assert_eq!(map.successor_key(&20), Some(&30));
            let mut mixed = map.iter();
            assert_eq!(mixed.next().map(|(&key, _)| key), Some(10));
            assert_eq!(mixed.next_back().map(|(&key, _)| key), Some(30));
            assert_eq!(mixed.next().map(|(&key, _)| key), Some(20));
            assert_eq!(mixed.next_back(), None);
        }
    }

    #[test]
    fn leaf_one_child_and_two_child_deletion_rebalance_and_reuse_slots() {
        let mut map = IndexedAvlMap::try_with_capacity(8).unwrap();
        for key in [40_i64, 20, 60, 10, 30, 50, 70, 25] {
            map.insert(key, key);
        }
        let allocation_capacity = map.allocation_capacity();
        let storage_len = map.storage_len();

        assert_eq!(map.remove(&10), Some(10));
        assert_eq!(map.remove(&30), Some(30));
        assert_eq!(map.remove(&40), Some(40));
        map.validate().unwrap();
        assert_eq!(
            map.iter().map(|(&key, _)| key).collect::<Vec<_>>(),
            vec![20, 25, 50, 60, 70]
        );

        for key in [55_i64, 65, 75] {
            map.insert(key, key);
        }
        map.validate().unwrap();
        assert_eq!(map.storage_len(), storage_len);
        assert_eq!(map.allocation_capacity(), allocation_capacity);
    }

    #[test]
    fn replacement_and_topology_independent_equality_are_exact() {
        let mut left = IndexedAvlMap::try_with_capacity(5).unwrap();
        let mut right = IndexedAvlMap::try_with_capacity(5).unwrap();
        for key in [30_i64, 10, 20] {
            left.insert(key, key);
        }
        for key in [10_i64, 30, 20] {
            right.insert(key, key);
        }
        assert_eq!(left, right);
        assert_eq!(left.insert(20, 999), Some(20));
        assert_ne!(left, right);
        assert_eq!(left.get(&20), Some(&999));
        *left.get_mut(&20).unwrap() = 20;
        assert_eq!(left, right);
    }

    #[test]
    fn finite_capacity_and_unrepresentable_reservation_fail_closed() {
        let mut map = IndexedAvlMap::try_with_capacity(1).unwrap();
        assert!(map.has_insertion_capacity());
        map.insert(1_i64, 1_i64);
        assert!(!map.has_insertion_capacity());
        assert!(IndexedAvlMap::<i64, i64>::try_with_capacity(usize::MAX).is_err());
    }

    #[test]
    fn generated_insert_remove_stream_matches_btree_model() {
        let mut actual = IndexedAvlMap::try_with_capacity(257).unwrap();
        let mut expected = BTreeMap::new();
        let mut state = 0x8f31_72d4_c690_a5eb_u64;
        for operation in 0..20_000_u64 {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let key = i64::try_from(state % 257).unwrap() - 128;
            state = state.rotate_left(23) ^ operation;
            if state & 1 == 0 {
                let value = state ^ operation;
                assert_eq!(actual.insert(key, value), expected.insert(key, value));
            } else {
                assert_eq!(actual.remove(&key), expected.remove(&key));
            }

            actual.validate().unwrap();
            assert_eq!(
                actual
                    .iter()
                    .map(|(&actual_key, &value)| (actual_key, value))
                    .collect::<Vec<_>>(),
                expected
                    .iter()
                    .map(|(&expected_key, &value)| (expected_key, value))
                    .collect::<Vec<_>>()
            );
            assert_eq!(
                actual.first_key_value().map(|(&key, &value)| (key, value)),
                expected
                    .first_key_value()
                    .map(|(&key, &value)| (key, value))
            );
            assert_eq!(
                actual.last_key_value().map(|(&key, &value)| (key, value)),
                expected.last_key_value().map(|(&key, &value)| (key, value))
            );
            let query = i64::try_from(state.rotate_right(7) % 257).unwrap() - 128;
            assert_eq!(
                actual.predecessor_key(&query).copied(),
                expected.range(..query).next_back().map(|(&key, _)| key)
            );
            assert_eq!(
                actual.successor_key(&query).copied(),
                expected
                    .range((std::ops::Bound::Excluded(query), std::ops::Bound::Unbounded))
                    .next()
                    .map(|(&key, _)| key)
            );
        }
    }

    #[test]
    fn invariant_audit_rejects_tree_and_free_list_corruption() {
        let mut map = IndexedAvlMap::try_with_capacity(4).unwrap();
        for key in [2_i64, 1, 3] {
            map.insert(key, key);
        }

        let root = map.root.unwrap();
        let mut bad_height = map.clone();
        let Slot::Occupied(node) = &mut bad_height.slots[root] else {
            unreachable!();
        };
        node.height += 1;
        assert!(bad_height.validate().unwrap_err().contains("height"));

        let mut cycle = map.clone();
        let Slot::Occupied(node) = &mut cycle.slots[root] else {
            unreachable!();
        };
        node.left = Some(root);
        assert!(cycle.validate().unwrap_err().contains("cycle"));

        map.remove(&1);
        let free = map.free_head.unwrap();
        map.slots[free] = Slot::Vacant { next: Some(free) };
        assert!(
            map.validate()
                .unwrap_err()
                .contains("free list contains a cycle")
        );
    }
}
