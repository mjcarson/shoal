//! A global allocator that counts, so the harness can say whether a crate allocates on each call

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};

/// The system allocator, with every allocation counted
pub struct CountingAlloc;

/// How many allocations have been made since the last reset
static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);

/// How many bytes have been allocated since the last reset
static BYTES: AtomicU64 = AtomicU64::new(0);

// SAFETY: every call is forwarded to the system allocator unchanged; the counters are only added to
unsafe impl GlobalAlloc for CountingAlloc {
    /// Count an allocation and forward it
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // count it before handing it on
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        // SAFETY: the caller upholds `alloc`'s contract, which is the system allocator's
        unsafe { System.alloc(layout) }
    }

    /// Count a zeroed allocation and forward it
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // count it before handing it on
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        // SAFETY: the caller upholds `alloc_zeroed`'s contract, which is the system allocator's
        unsafe { System.alloc_zeroed(layout) }
    }

    /// Forward a free; frees are not counted
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: `ptr` came from this allocator, which is the system allocator
        unsafe { System.dealloc(ptr, layout) }
    }

    /// Count a reallocation as one allocation of the new size and forward it
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // a growth is an allocation as far as a hot path is concerned
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
        // SAFETY: the caller upholds `realloc`'s contract, which is the system allocator's
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

/// Run one closure and say how many allocations and bytes it made
///
/// # Arguments
///
/// * `f` - The call to count
pub fn count<T>(f: impl FnOnce() -> T) -> (T, u64, u64) {
    // start both counters at zero
    ALLOCATIONS.store(0, Ordering::Relaxed);
    BYTES.store(0, Ordering::Relaxed);
    // make the call
    let out = f();
    // read what it did
    let allocations = ALLOCATIONS.load(Ordering::Relaxed);
    let bytes = BYTES.load(Ordering::Relaxed);
    (out, allocations, bytes)
}
