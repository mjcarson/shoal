//! A count of the allocations each thread makes, for the unit tests that hold a step of a shard's
//! loop to a fixed number of them
//! ([item 353](../../../docs/src/appendix/known-issues.md#353-light-scan-steps-pass-100-µs-on-their-own-cpu-which-a-kernel-without-irq-time-accounting-does-not-tell-from-interrupts))
//!
//! Item 353's lab runs found a scan page's capture held past 100 µs by its allocations, the
//! allocator's slow paths inside them, not by its work. A capture made a dozen or more, a vector and
//! a deque grown as they filled, a deque a run of its archive map. This is the test binary's global
//! allocator: the system's, each allocation counted on the thread that made it, so a test counts
//! what one call made and nothing another test's threads did. Only `shoal-core`'s unit tests link
//! it; every program keeps its own allocator.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

thread_local! {
    /// The allocations this thread has made, a reallocation counted as one
    static MADE: Cell<u64> = const { Cell::new(0) };
}

/// The system's allocator, each allocation counted on its thread
pub struct Counting;

// SAFETY: every call is the system allocator's own, with the layout and pointer it was given; the
// count is a thread local of a const initializer, which never allocates
unsafe impl GlobalAlloc for Counting {
    /// Allocate, counted
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // counted, then the system's
        MADE.with(|made| made.set(made.get() + 1));
        // SAFETY: the caller's layout, as the trait requires
        unsafe { System.alloc(layout) }
    }

    /// Allocate zeroed, counted
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // counted, then the system's
        MADE.with(|made| made.set(made.get() + 1));
        // SAFETY: the caller's layout, as the trait requires
        unsafe { System.alloc_zeroed(layout) }
    }

    /// Free, uncounted
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: a pointer this allocator gave out with this layout, as the trait requires
        unsafe { System.dealloc(ptr, layout) }
    }

    /// Reallocate, counted as one allocation
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // counted, then the system's
        MADE.with(|made| made.set(made.get() + 1));
        // SAFETY: a pointer this allocator gave out with this layout, as the trait requires
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

/// The test binary's allocator
#[global_allocator]
static COUNTING: Counting = Counting;

/// How many allocations a call made on this thread
///
/// # Arguments
///
/// * `call` - The call
pub fn allocations_of<T>(call: impl FnOnce() -> T) -> (T, u64) {
    // the count before and after, on this thread alone
    let before = MADE.with(Cell::get);
    let made = call();
    let after = MADE.with(Cell::get);
    (made, after - before)
}
