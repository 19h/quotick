//! Exact unsigned accounting aggregates with an allocation-free common case.

use std::cmp::Ordering;
use std::fmt;

const DECIMAL_BASE: u128 = 10_000_000_000_000_000_000;

/// An exact non-negative integer used for aggregate accounting evidence.
///
/// Values through [`u128::MAX`] remain inline and require no heap allocation.
/// Larger values spill into canonical little-endian `u64` limbs. The type has
/// no numerical ceiling; allocator and address-space limits remain governed by
/// the process resource model.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LedgerMagnitude {
    representation: Representation,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Representation {
    Inline(u128),
    Wide(Vec<u64>),
}

impl LedgerMagnitude {
    /// Exact zero.
    pub const ZERO: Self = Self::from_u128(0);

    /// Constructs one inline magnitude.
    #[must_use]
    pub const fn from_u128(value: u128) -> Self {
        Self {
            representation: Representation::Inline(value),
        }
    }

    /// Returns the exact value when it fits in `u128`.
    #[must_use]
    pub const fn as_u128(&self) -> Option<u128> {
        match self.representation {
            Representation::Inline(value) => Some(value),
            Representation::Wide(_) => None,
        }
    }

    /// Returns whether the value is zero.
    #[must_use]
    pub const fn is_zero(&self) -> bool {
        matches!(self.representation, Representation::Inline(0))
    }

    /// Returns the number of significant binary digits, or `None` only when
    /// the platform `usize` cannot represent that metadata cardinality.
    #[must_use]
    pub fn significant_bits(&self) -> Option<usize> {
        match &self.representation {
            Representation::Inline(0) => Some(0),
            Representation::Inline(value) => {
                usize::try_from(u128::BITS - value.leading_zeros()).ok()
            }
            Representation::Wide(limbs) => {
                let high = *limbs.last()?;
                let complete_limb_bits = limbs.len().checked_sub(1)?.checked_mul(64)?;
                complete_limb_bits.checked_add(usize::try_from(64 - high.leading_zeros()).ok()?)
            }
        }
    }

    /// Adds one `u128` value exactly.
    pub fn add_u128(&mut self, value: u128) {
        if value == 0 {
            return;
        }
        match &mut self.representation {
            Representation::Inline(current) => {
                if let Some(sum) = current.checked_add(value) {
                    *current = sum;
                    return;
                }
                self.representation = Representation::Wide(add_inline_values(*current, value));
            }
            Representation::Wide(limbs) => add_to_wide(limbs, value),
        }
    }
}

impl Default for LedgerMagnitude {
    fn default() -> Self {
        Self::ZERO
    }
}

impl From<u128> for LedgerMagnitude {
    fn from(value: u128) -> Self {
        Self::from_u128(value)
    }
}

impl Ord for LedgerMagnitude {
    fn cmp(&self, other: &Self) -> Ordering {
        match (&self.representation, &other.representation) {
            (Representation::Inline(left), Representation::Inline(right)) => left.cmp(right),
            (Representation::Inline(_), Representation::Wide(_)) => Ordering::Less,
            (Representation::Wide(_), Representation::Inline(_)) => Ordering::Greater,
            (Representation::Wide(left), Representation::Wide(right)) => left
                .len()
                .cmp(&right.len())
                .then_with(|| left.iter().rev().cmp(right.iter().rev())),
        }
    }
}

impl PartialOrd for LedgerMagnitude {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Display for LedgerMagnitude {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(value) = self.as_u128() {
            return value.fmt(formatter);
        }
        let Representation::Wide(source) = &self.representation else {
            return Err(fmt::Error);
        };
        let mut limbs = source.clone();
        let mut chunks = Vec::new();
        while !limbs.is_empty() {
            let mut remainder = 0_u128;
            for limb in limbs.iter_mut().rev() {
                let dividend = (remainder << 64) | u128::from(*limb);
                *limb = u64::try_from(dividend / DECIMAL_BASE).map_err(|_| fmt::Error)?;
                remainder = dividend % DECIMAL_BASE;
            }
            while limbs.last() == Some(&0) {
                limbs.pop();
            }
            chunks.push(u64::try_from(remainder).map_err(|_| fmt::Error)?);
        }
        let Some(last) = chunks.pop() else {
            return formatter.write_str("0");
        };
        write!(formatter, "{last}")?;
        for chunk in chunks.iter().rev() {
            write!(formatter, "{chunk:019}")?;
        }
        Ok(())
    }
}

fn add_inline_values(left: u128, right: u128) -> Vec<u64> {
    let [left_low, left_high] = split_u128(left);
    let [right_low, right_high] = split_u128(right);
    let (low, low_carry) = left_low.overflowing_add(right_low);
    let (high_without_low_carry, high_carry) = left_high.overflowing_add(right_high);
    let (high, propagated_carry) = high_without_low_carry.overflowing_add(u64::from(low_carry));
    debug_assert!(high_carry || propagated_carry);
    vec![low, high, 1]
}

fn add_to_wide(limbs: &mut Vec<u64>, value: u128) {
    debug_assert!(limbs.len() >= 3);
    let [value_low, value_high] = split_u128(value);
    let (low, low_carry) = limbs[0].overflowing_add(value_low);
    limbs[0] = low;
    let (high_without_low_carry, high_carry) = limbs[1].overflowing_add(value_high);
    let (high, propagated_carry) = high_without_low_carry.overflowing_add(u64::from(low_carry));
    limbs[1] = high;
    let mut carry = high_carry || propagated_carry;
    let mut index = 2;
    while carry && index < limbs.len() {
        let (next, next_carry) = limbs[index].overflowing_add(1);
        limbs[index] = next;
        carry = next_carry;
        index += 1;
    }
    if carry {
        limbs.push(1);
    }
}

fn split_u128(value: u128) -> [u64; 2] {
    let bytes = value.to_le_bytes();
    [
        u64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]),
        u64::from_le_bytes([
            bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
        ]),
    ]
}
