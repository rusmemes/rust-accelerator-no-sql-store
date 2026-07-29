use crate::common::{PARTITIONS_AMOUNT, now_millis};
use dashmap::DashMap;
use std::collections::HashSet;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Record {
    pub expiration_time_ms: u64,
    pub creation_time_ms: u64,
    pub value: Vec<u8>,
}

#[derive(Default, Clone)]
pub struct RuntimeStore {
    cache: Arc<DashMap<PartitionId, DashMap<Key, Arc<Record>>>>,
}

impl RuntimeStore {
    pub fn new() -> Self {
        Self {
            cache: Default::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PartitionId(pub u16);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Key(pub u64);

impl From<Key> for PartitionId {
    fn from(key: Key) -> Self {
        PartitionId((key.0 as usize % PARTITIONS_AMOUNT) as u16)
    }
}


impl RuntimeStore {
    pub fn remove_from_partition(&self, partition: PartitionId, keys: &[Key]) {
        if let dashmap::mapref::entry::Entry::Occupied(mut occupied) = self.cache.entry(partition) {
            let key_to_record = occupied.get_mut();
            for key in keys {
                key_to_record.remove(key);
            }
            if key_to_record.is_empty() {
                occupied.remove();
            }
        }
    }

    pub fn get_partition_records(
        &self,
        partition: PartitionId,
        amount: usize,
    ) -> Vec<(Key, Arc<Record>)> {
        if let Some(entry) = self.cache.get(&partition) {
            return entry
                .value()
                .iter()
                .take(amount)
                .map(|entry| (entry.key().clone(), entry.value().clone()))
                .collect();
        }
        vec![]
    }

    pub fn unexpected_partitions(&self, expected: &HashSet<PartitionId>) -> Vec<PartitionId> {
        // todo: remove it later
        self.cache
            .iter()
            .filter_map(|entry| {
                if expected.contains(entry.key()) || entry.value().is_empty() {
                    None
                } else {
                    Some(*entry.key())
                }
            })
            .collect()
    }

    pub fn remove_partition_if_empty(&self, partition: PartitionId) {
        if let dashmap::mapref::entry::Entry::Occupied(occupied) = self.cache.entry(partition)
            && occupied.get().is_empty()
        {
            occupied.remove();
        }
    }

    pub fn delete(&self, key: Key) {
        let partition = key.into();
        let removed = if let Some(map) = self.cache.get(&partition) {
            map.remove(&key).is_some()
        } else {
            false
        };

        if removed {
            self.remove_partition_if_empty(partition);
        }
    }

    pub fn get(&self, key: Key) -> Option<Arc<Record>> {
        let partition = key.into();

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

    pub fn put(&self, key: Key, record: Record) {
        let partition = key.into();
        let map = self.cache.entry(partition).or_default();
        match map.entry(key) {
            dashmap::mapref::entry::Entry::Occupied(mut occupied) => {
                if occupied.get().creation_time_ms <= record.creation_time_ms {
                    occupied.insert(Arc::new(record));
                }
            }
            dashmap::mapref::entry::Entry::Vacant(occupied) => {
                occupied.insert(Arc::new(record));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_runtime_store_expiration() {
        let store = RuntimeStore::new();
        let key = Key(1);
        let value = vec![1, 2, 3];

        let now = now_millis();
        store.put(
            key,
            Record {
                expiration_time_ms: now + 100,
                creation_time_ms: now,
                value: value.clone(),
            },
        );

        let record = store.get(key).expect("Record should be present");
        assert_eq!(record.value, value);

        std::thread::sleep(Duration::from_millis(150));

        assert!(store.get(key).is_none());

        let partition = PartitionId::from(key);
        assert!(!store.cache.contains_key(&partition));
    }

    #[test]
    fn test_remove_partition_if_empty() {
        let store = RuntimeStore::new();
        let key = Key(1);
        let partition = PartitionId::from(key);

        let now = now_millis();
        store.put(
            key,
            Record {
                expiration_time_ms: now + 1000,
                creation_time_ms: now,
                value: vec![1],
            },
        );
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
        let key = Key(1);
        let partition = PartitionId::from(key);

        store.put(
            key,
            Record {
                value: vec![1, 2, 3],
                expiration_time_ms: 0,
                creation_time_ms: 0,
            },
        );
        assert!(store.cache.contains_key(&partition));
        assert!(store.get(key).is_some());

        store.delete(key);
        assert!(store.get(key).is_none());
        assert!(!store.cache.contains_key(&partition));
    }

    #[test]
    fn test_runtime_store_put_ordering() {
        let store = RuntimeStore::new();
        let key = Key(1);

        store.put(
            key,
            Record {
                value: vec![1],
                expiration_time_ms: 0,
                creation_time_ms: 100,
            },
        );
        assert_eq!(store.get(key).unwrap().value, vec![1]);

        store.put(
            key,
            Record {
                value: vec![2],
                expiration_time_ms: 0,
                creation_time_ms: 50,
            },
        );
        assert_eq!(store.get(key).unwrap().value, vec![1]);

        store.put(
            key,
            Record {
                value: vec![3],
                expiration_time_ms: 0,
                creation_time_ms: 150,
            },
        );

        assert_eq!(store.get(key).unwrap().value, vec![3]);

        store.put(
            key,
            Record {
                value: vec![4],
                expiration_time_ms: 0,
                creation_time_ms: 150,
            },
        );
        assert_eq!(store.get(key).unwrap().value, vec![4]);
    }

    #[test]
    fn test_remove_from_partition() {
        let store = RuntimeStore::new();
        let key1 = Key(1);
        let key2 = Key((PARTITIONS_AMOUNT + 1) as u64); // Тот же раздел, что и key1
        let partition = PartitionId::from(key1);

        store.put(
            key1,
            Record {
                value: vec![1],
                expiration_time_ms: 0,
                creation_time_ms: 0,
            },
        );
        store.put(
            key2,
            Record {
                value: vec![2],
                expiration_time_ms: 0,
                creation_time_ms: 0,
            },
        );

        assert!(store.cache.contains_key(&partition));

        store.remove_from_partition(partition, &[key1]);
        assert!(store.get(key1).is_none());
        assert!(store.get(key2).is_some());
        assert!(store.cache.contains_key(&partition));

        store.remove_from_partition(partition, &[key2]);
        assert!(store.get(key2).is_none());
        assert!(!store.cache.contains_key(&partition));
    }

    #[test]
    fn test_get_partition_records() {
        let store = RuntimeStore::new();
        let key1 = Key(1);
        let key2 = Key((PARTITIONS_AMOUNT + 1) as u64);
        let partition = PartitionId::from(key1);

        store.put(
            key1,
            Record {
                value: vec![1],
                expiration_time_ms: 0,
                creation_time_ms: 0,
            },
        );
        store.put(
            key2,
            Record {
                value: vec![2],
                expiration_time_ms: 0,
                creation_time_ms: 0,
            },
        );

        let records = store.get_partition_records(partition, 10);
        assert_eq!(records.len(), 2);
        let keys: HashSet<Key> = records.iter().map(|(k, _)| *k).collect();
        assert!(keys.contains(&key1));
        assert!(keys.contains(&key2));

        let records_limited = store.get_partition_records(partition, 1);
        assert_eq!(records_limited.len(), 1);
    }

    #[test]
    fn test_unexpected_partitions() {
        let store = RuntimeStore::new();
        let key1 = Key(1);
        let key2 = Key(2);
        let p1 = PartitionId::from(key1);
        let p2 = PartitionId::from(key2);

        store.put(
            key1,
            Record {
                value: vec![1],
                expiration_time_ms: 0,
                creation_time_ms: 0,
            },
        );
        store.put(
            key2,
            Record {
                value: vec![2],
                expiration_time_ms: 0,
                creation_time_ms: 0,
            },
        );

        let expected = HashSet::from([p1]);
        let unexpected = store.unexpected_partitions(&expected);
        assert_eq!(unexpected, vec![p2]);

        let expected_both = HashSet::from([p1, p2]);
        assert!(store.unexpected_partitions(&expected_both).is_empty());
    }

    #[test]
    fn test_runtime_store_no_expiration() {
        let store = RuntimeStore::new();
        let key = Key(1);
        let now = now_millis();

        store.put(
            key,
            Record {
                value: vec![1],
                expiration_time_ms: 0,
                creation_time_ms: now,
            },
        );
        std::thread::sleep(Duration::from_millis(100));
        assert!(store.get(key).is_some());
    }

    #[test]
    fn test_runtime_store_concurrent_ops() {
        let store = Arc::new(RuntimeStore::new());
        let mut handles = vec![];

        for i in 0..100 {
            let store_clone = store.clone();
            handles.push(std::thread::spawn(move || {
                for j in 0..1000 {
                    let key = Key((i * 1000 + j) as u64);
                    store_clone.put(
                        key,
                        Record {
                            value: vec![1],
                            expiration_time_ms: 0,
                            creation_time_ms: 0,
                        },
                    );
                    if j % 2 == 0 {
                        store_clone.delete(key);
                    }
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let mut total_count = 0;
        for i in 0..PARTITIONS_AMOUNT {
            total_count += store.get_partition_records(PartitionId(i as u16), 10000).len();
        }
        assert_eq!(total_count, 50000);
    }
}
