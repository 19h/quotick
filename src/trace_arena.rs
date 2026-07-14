//! Safe constructor-owned append-only storage shared by immutable event traces.

use std::collections::TryReserveError;
use std::fmt;
use std::sync::{Arc, OnceLock};

/// Fixed-capacity slots initialized once by one shard writer and then immutable.
pub(crate) struct AppendOnlyTraceArena<T> {
    slots: Vec<OnceLock<T>>,
}

impl<T> fmt::Debug for AppendOnlyTraceArena<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AppendOnlyTraceArena")
            .field("capacity", &self.slots.len())
            .finish_non_exhaustive()
    }
}

impl<T> AppendOnlyTraceArena<T> {
    /// Fallibly allocates and initializes the complete fixed slot envelope.
    pub(crate) fn try_with_capacity(maximum: usize) -> Result<Arc<Self>, TryReserveError> {
        let mut slots = Vec::new();
        slots.try_reserve_exact(maximum)?;
        slots.resize_with(maximum, OnceLock::new);
        Ok(Arc::new(Self { slots }))
    }

    pub(crate) fn capacity(&self) -> usize {
        self.slots.len()
    }

    pub(crate) fn allocated_capacity(&self) -> usize {
        self.slots.capacity()
    }

    /// Initializes one previously unwritten slot.
    pub(crate) fn write_once(&self, index: usize, value: T) {
        assert!(
            index < self.slots.len(),
            "append-only trace-arena write must be in bounds"
        );
        assert!(
            self.slots[index].set(value).is_ok(),
            "append-only trace-arena slots are initialized once"
        );
    }

    /// Returns one initialized published value.
    pub(crate) fn get(&self, index: usize) -> &T {
        self.slots[index]
            .get()
            .expect("published trace-arena slot must be initialized")
    }

    #[cfg(test)]
    pub(crate) fn slot_pointer(&self, index: usize) -> *const T {
        self.slots[index]
            .get()
            .map_or(std::ptr::null(), std::ptr::from_ref)
    }

    #[cfg(test)]
    pub(crate) fn copy_prefix_to(&self, target: &Self, length: usize)
    where
        T: Copy,
    {
        for index in 0..length {
            target.write_once(index, *self.get(index));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::AppendOnlyTraceArena;

    #[test]
    fn slots_publish_once_without_allocation_growth() {
        let arena = AppendOnlyTraceArena::<u64>::try_with_capacity(2).unwrap();
        let allocated = arena.allocated_capacity();
        assert!(arena.slot_pointer(0).is_null());
        arena.write_once(0, 17_u64);
        assert_eq!(*arena.get(0), 17);
        assert_eq!(arena.allocated_capacity(), allocated);
    }

    #[test]
    #[should_panic(expected = "initialized once")]
    fn a_published_slot_cannot_be_rewritten() {
        let arena = AppendOnlyTraceArena::try_with_capacity(1).unwrap();
        arena.write_once(0, 1_u64);
        arena.write_once(0, 2_u64);
    }
}
