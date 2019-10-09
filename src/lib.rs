//! A crate for managing key-value stores

#![deny(missing_docs)]

use std::collections::HashMap;

/// Represents a key-value store
#[derive(Default)]
pub struct KvStore {
    map: HashMap<String, String>,
}

impl KvStore {
    /// Creates a new `KvStore`
    /// ```rust
    /// use kvs::KvStore;
    /// let store = KvStore::new();
    /// ```
    pub fn new() -> KvStore {
        KvStore::default()
    }

    /// Sets the `value` for a given `key`.
    /// Will update if `key` already exists.
    /// ```rust
    /// use kvs::KvStore;
    /// let mut store = KvStore::new();
    /// store.set("Hello".to_owned(), "World".to_owned());
    /// ```
    pub fn set(&mut self, key: String, value: String) {
        self.map.insert(key, value);
    }

    /// Gets the value, given the `key`.
    /// Returns an `Option<String>`.
    /// ```rust
    /// use kvs::KvStore;
    /// let store = KvStore::new();
    /// match store.get("Hello".to_owned()) {
    ///   Some(value) => println!("{:?}", value),
    ///   None => println!("Not found.")
    /// }
    /// ```
    pub fn get(&self, key: String) -> Option<String> {
        let value = self.map.get(&key)?.to_owned();

        Some(value)
    }

    /// Removes the value, given a `key`.
    /// Idempotent. Will do nothing if `key` is not present.
    /// ```rust
    /// use kvs::KvStore;
    /// let mut store = KvStore::new();
    /// store.remove("Hello".to_owned());
    /// ```
    pub fn remove(&mut self, key: String) {
        self.map.remove(&key);
    }
}
