//! The schema fingerprint the two peers compare before the first query
//!
//! The protocol's worst failure mode is not a corrupt frame, which `bytecheck` catches. It is a
//! client and a server compiled from *different but structurally similar* schemas — a field
//! reordered, a variant inserted, a type widened. rkyv validates that bytes are well formed for
//! the type it is told to read, not that the peer meant that type, so both peers are individually
//! consistent and only their agreement is wrong.
//!
//! The derive macros fold every part of a schema that can reach the wire into one `u64` at compile
//! time, and the two peers exchange it in the handshake. A mismatch becomes a refused connection
//! naming both fingerprints instead of undefined behaviour.
//!
//! # Invariants
//!
//! **Every mixed value is followed by a separator.** Without one, `("ab", "c")` and `("a", "bc")`
//! hash the same, so renaming a pair of adjacent fields would pass the check.
//! `the_separator_stops_concatenation_colliding` is what holds this.
//!
//! **This is a mistake detector, not authentication.** A hostile peer can trivially send whatever
//! fingerprint it likes. Proving who a peer is needs an authentication exchange, which this is
//! not.

/// The FNV-1a 64 offset basis, which is where every fingerprint starts
pub const SEED: u64 = 0xcbf2_9ce4_8422_2325;

/// The FNV-1a 64 prime
const PRIME: u64 = 0x0000_0100_0000_01b3;

/// The byte mixed in after every value to keep neighbouring values from running together
const SEPARATOR: u8 = 0xff;

/// This field is part of its table's partition key
pub const ROLE_PARTITION: u8 = 1 << 0;

/// This field is part of its table's sort key
pub const ROLE_SORT: u8 = 1 << 1;

/// This field can be filtered on
pub const ROLE_FILTER: u8 = 1 << 2;

/// This field can be updated
pub const ROLE_UPDATE: u8 = 1 << 3;

/// Mix a slice of bytes into a hash
///
/// # Arguments
///
/// * `hash` - The hash to mix into
/// * `bytes` - The bytes to mix in
pub const fn mix_bytes(mut hash: u64, bytes: &[u8]) -> u64 {
    // walk the bytes by index, since iterators are not available in a const fn
    let mut i = 0;
    while i < bytes.len() {
        // fold this byte in and scatter it across the whole hash
        hash ^= bytes[i] as u64;
        hash = hash.wrapping_mul(PRIME);
        i += 1;
    }
    hash
}

/// Mix the separator that ends one value into a hash
///
/// # Arguments
///
/// * `hash` - The hash to mix into
pub const fn mix_sep(hash: u64) -> u64 {
    mix_bytes(hash, &[SEPARATOR])
}

/// Mix a string and the separator that ends it into a hash
///
/// # Arguments
///
/// * `hash` - The hash to mix into
/// * `text` - The string to mix in
pub const fn mix_str(hash: u64, text: &str) -> u64 {
    mix_sep(mix_bytes(hash, text.as_bytes()))
}

/// Mix a number and the separator that ends it into a hash
///
/// # Arguments
///
/// * `hash` - The hash to mix into
/// * `value` - The number to mix in
pub const fn mix_u64(hash: u64, value: u64) -> u64 {
    mix_sep(mix_bytes(hash, &value.to_le_bytes()))
}

/// Mix one field of one table into a hash
///
/// The archived size and alignment are mixed alongside the spelling of the type because the
/// spelling alone can lie. A type alias whose definition changes from `u32` to `u64` keeps its
/// spelling, and that is the one direction that is dangerous — two peers agreeing when they should
/// not. Two spellings of the same type disagreeing is the safe direction, and is left as is.
///
/// # Arguments
///
/// * `hash` - The hash to mix into
/// * `name` - The name of this field
/// * `ty` - The type of this field, as it was written
/// * `size` - The size of this field's archived form
/// * `align` - The alignment of this field's archived form
/// * `index` - The position of this field in its table
/// * `roles` - The roles this field plays in a query
pub const fn mix_field(
    hash: u64,
    name: &str,
    ty: &str,
    size: usize,
    align: usize,
    index: usize,
    roles: u8,
) -> u64 {
    // mix what the field is called and what it was declared as
    let hash = mix_str(hash, name);
    let hash = mix_str(hash, ty);
    // mix what that type actually turns into once it is archived
    let hash = mix_u64(hash, size as u64);
    let hash = mix_u64(hash, align as u64);
    // mix where this field sits and what a query can do with it
    let hash = mix_u64(hash, index as u64);
    mix_u64(hash, roles as u64)
}
