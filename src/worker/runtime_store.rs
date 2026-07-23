use crate::common::{PARTITIONS_AMOUNT, now_millis};
use dashmap::DashMap;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Record {
    pub expiration_time_ms: u64,
    pub value: Vec<u8>,
}

#[derive(Default, Clone)]
pub struct RuntimeStore {
    /// partition -> key -> record
    cache: Arc<DashMap<u16, DashMap<u64, Arc<Record>>>>,
}

impl RuntimeStore {
    pub fn new() -> Self {
        Self {
            cache: Default::default(),
        }
    }
}

impl RuntimeStore {
    pub fn remove_partition_if_empty(&self, partition: u16) {
        if let dashmap::mapref::entry::Entry::Occupied(occupied) = self.cache.entry(partition)
            && occupied.get().is_empty()
        {
            occupied.remove();
        }
    }

    pub fn delete(&self, key: u64) {
        let partition = (key as usize % PARTITIONS_AMOUNT) as u16;
        let removed = if let Some(map) = self.cache.get(&partition) {
            map.remove(&key).is_some()
        } else {
            false
        };

        if removed {
            self.remove_partition_if_empty(partition);
        }
    }

    pub fn get(&self, key: u64) -> Option<Arc<Record>> {
        let partition = (key as usize % PARTITIONS_AMOUNT) as u16;

        let (res, removed) = if let Some(map) = self.cache.get(&partition) {
            match map.entry(key) {
                dashmap::mapref::entry::Entry::Occupied(occupied) => {
                    let exp_time = occupied.get().expiration_time_ms;
                    if exp_time == 0 || exp_time > now_millis() {
                        (Some(occupied.get().clone()), false)
                    } else {
                        occupied.remove();
                        (None, true)
                    }
                }
                dashmap::mapref::entry::Entry::Vacant(_) => (None, false),
            }
        } else {
            (None, false)
        };

        if res.is_none() && removed {
            self.remove_partition_if_empty(partition);
        }
        res
    }

    pub fn put(&self, key: u64, value: Vec<u8>, expiration_time_ms: u64) {
        let partition = (key as usize % PARTITIONS_AMOUNT) as u16;
        let map = self.cache.entry(partition).or_default();
        map.insert(
            key,
            Arc::new(Record {
                expiration_time_ms,
                value,
            }),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_runtime_store_expiration() {
        let store = RuntimeStore::new();
        let key = 1u64;
        let value = vec![1, 2, 3];

        let now = now_millis();
        store.put(key, value.clone(), now + 100);

        let record = store.get(key).expect("Record should be present");
        assert_eq!(record.value, value);

        std::thread::sleep(Duration::from_millis(150));

        assert!(store.get(key).is_none());

        let partition = (key as usize % PARTITIONS_AMOUNT) as u16;
        assert!(!store.cache.contains_key(&partition));
    }

    #[test]
    fn test_remove_partition_if_empty() {
        let store = RuntimeStore::new();
        let key = 1u64;
        let partition = (key as usize % PARTITIONS_AMOUNT) as u16;

        store.put(key, vec![1], now_millis() + 1000);
        assert!(store.cache.contains_key(&partition));

        store.remove_partition_if_empty(partition);
        assert!(store.cache.contains_key(&partition));

        std::thread::sleep(Duration::from_millis(1100));
        assert!(store.get(key).is_none());

        assert!(!store.cache.contains_key(&partition));
    }

    #[test]
    fn test_runtime_store_delete() {
        let store = RuntimeStore::new();
        let key = 1u64;
        let partition = (key as usize % PARTITIONS_AMOUNT) as u16;

        store.put(key, vec![1, 2, 3], 0);
        assert!(store.cache.contains_key(&partition));
        assert!(store.get(key).is_some());

        store.delete(key);
        assert!(store.get(key).is_none());
        assert!(!store.cache.contains_key(&partition));
    }
}
