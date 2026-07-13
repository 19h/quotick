// quotick/src/macros.rs
//! Internal helper macros for reducing boilerplate.

/// Implements the `Record` trait for a given struct.
///
/// This macro requires that the struct has a field `hd: RecordHeader` and
/// implements `bytemuck::Pod`.
///
/// # Usage
///
/// ```rust,ignore
/// impl_record!(TradeMsg, RType::Trade);
/// ```
#[macro_export]
macro_rules! impl_record {
    ($name:ident, $rtype:expr) => {
        // All records must be POD (Plain Old Data) for safe, zero-copy casting.
        unsafe impl bytemuck::Pod for $name {}
        unsafe impl bytemuck::Zeroable for $name {}

        impl $crate::record::Record for $name {
            const RTYPE: $crate::enums::RType = $rtype;

            #[inline]
            fn header(&self) -> &$crate::record::RecordHeader {
                &self.hd
            }
        }
    };
}

/// A helper function to format a byte slice as a string for debug output.
pub fn cstr_to_str(bytes: &[u8]) -> &str {
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    std::str::from_utf8(&bytes[..end]).unwrap_or("<invalid_utf8>")
}

/// Implements a custom `Debug` trait for a record struct to provide cleaner output.
///
/// # Usage
///
/// ```rust,ignore
/// custom_debug_record_header!(RecordHeader);
/// ```
#[macro_export]
macro_rules! custom_debug_record_header {
    ($name:ident) => {
        impl std::fmt::Debug for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_struct(stringify!($name))
                    .field("length", &self.length())
                    .field("rtype", &self.rtype())
                    .field("publisher_id", &self.publisher_id())
                    .field("instrument_id", &self.instrument_id())
                    .field("ts_event", &self.ts_event())
                    .finish()
            }
        }
    };
}

/// Implements `PartialEq` for a record struct by comparing all fields via accessors.
///
/// This macro compares the header first,
/// then each specified field by calling its accessor method.
///
/// # Usage
///
/// ```rust,ignore
/// impl_eq_record!(TradeMsg, price, size, action, side, flags, channel_id, ts_in_delta);
/// ```
#[macro_export]
macro_rules! impl_eq_record {
    ($name:ident, $($field:ident),+) => {
        impl PartialEq for $name {
            fn eq(&self, other: &Self) -> bool {
                // The header's PartialEq implementation is a safe byte-wise comparison.
                self.header() == other.header()
                $(
                    && self.$field() == other.$field()
                )*
            }
        }
        impl Eq for $name {}
    };
}
