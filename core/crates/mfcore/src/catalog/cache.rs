//! Small in-memory LRU cache with a time-to-live.

use std::num::NonZeroUsize;
use std::time::{Duration, Instant};

use lru::LruCache;
use parking_lot::Mutex;

/// Thread-safe LRU of cloneable values that expire after `ttl`.
pub(crate) struct TtlCache<V> {
    inner: Mutex<LruCache<String, (Instant, V)>>,
    ttl: Duration,
}

impl<V: Clone> TtlCache<V> {
    pub(crate) fn new(capacity: usize, ttl: Duration) -> Self {
        let capacity = NonZeroUsize::new(capacity).unwrap_or(NonZeroUsize::MIN);
        Self { inner: Mutex::new(LruCache::new(capacity)), ttl }
    }

    pub(crate) fn get(&self, key: &str) -> Option<V> {
        let mut inner = self.inner.lock();
        match inner.get(key) {
            Some((at, value)) if at.elapsed() < self.ttl => Some(value.clone()),
            Some(_) => {
                inner.pop(key);
                None
            }
            None => None,
        }
    }

    pub(crate) fn put(&self, key: impl Into<String>, value: V) {
        self.inner.lock().put(key.into(), (Instant::now(), value));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evicts_least_recent_and_expired() {
        let cache = TtlCache::new(2, Duration::from_secs(60));
        cache.put("a", 1);
        cache.put("b", 2);
        assert_eq!(cache.get("a"), Some(1));
        cache.put("c", 3);
        assert_eq!(cache.get("b"), None);
        assert_eq!(cache.get("a"), Some(1));
        let expired = TtlCache::new(2, Duration::ZERO);
        expired.put("a", 1);
        assert_eq!(expired.get("a"), None);
        assert_eq!(TtlCache::<u8>::new(0, Duration::from_secs(1)).get("x"), None);
    }
}
