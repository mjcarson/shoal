//! The pages of a paged archive map that point lookups have read, kept up to a budget
//!
//! A run never changes once it is written, so a cached page is never stale: it is keyed by its
//! run's id and its number, and a page of a run that was merged away is simply never asked for
//! again and ages out ([F76](../../../../../../../docs/src/features/paged-archive-map.md)).

use lru::LruCache;
use std::rc::Rc;

use super::page::{Page, PAGE_SIZE};

/// What one cached page costs beyond its bytes: its key, its handle and the cache's links
const PAGE_OVERHEAD: usize = 64;

/// The least recently used pages a map's lookups have read, up to a budget in bytes
#[derive(Debug)]
pub struct PageCache {
    /// The pages, by run and page number
    pages: LruCache<(u64, u32), Rc<Page>>,
    /// The most bytes of pages held
    budget: usize,
}

impl PageCache {
    /// An empty cache with a budget
    ///
    /// # Arguments
    ///
    /// * `budget` - The most bytes of pages held, below one page for no cache
    #[must_use]
    pub fn new(budget: usize) -> Self {
        PageCache {
            pages: LruCache::unbounded(),
            budget,
        }
    }

    /// A cached page, marked as the most recently used
    ///
    /// # Arguments
    ///
    /// * `run` - The run's id
    /// * `page` - The page's number
    pub fn get(&mut self, run: u64, page: u32) -> Option<Rc<Page>> {
        self.pages.get(&(run, page)).cloned()
    }

    /// Keep a page, evicting the least recently used ones past the budget
    ///
    /// # Arguments
    ///
    /// * `run` - The run's id
    /// * `page` - The page's number
    /// * `bytes` - The page
    pub fn insert(&mut self, run: u64, page: u32, bytes: Rc<Page>) {
        // a budget below one page caches nothing
        if self.budget < PAGE_SIZE + PAGE_OVERHEAD {
            return;
        }
        self.pages.put((run, page), bytes);
        // the oldest pages go until the rest fit
        while self.bytes() > self.budget {
            if self.pages.pop_lru().is_none() {
                break;
            }
        }
    }

    /// Drop every page of a run that was merged away
    ///
    /// # Arguments
    ///
    /// * `run` - The run's id
    pub fn forget(&mut self, run: u64) {
        // the keys of its pages, then each taken out
        let gone: Vec<(u64, u32)> = self
            .pages
            .iter()
            .filter(|((id, _), _)| *id == run)
            .map(|(key, _)| *key)
            .collect();
        for key in gone {
            self.pages.pop(&key);
        }
    }

    /// The bytes the cached pages hold
    #[must_use]
    pub fn bytes(&self) -> usize {
        self.pages.len() * (PAGE_SIZE + PAGE_OVERHEAD)
    }

    /// How many pages are cached
    #[must_use]
    pub fn len(&self) -> usize {
        self.pages.len()
    }
}
