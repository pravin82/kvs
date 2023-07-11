use core::result;
use std::collections::HashMap;
extern crate failure;
use failure::Error;

pub struct KvStore {
    memory_db: HashMap<String, String>,
}

impl Default for KvStore {
    fn default() -> Self {
        KvStore::new()
    }
}

impl KvStore {
    pub fn new() -> KvStore {
        let memory_db = HashMap::new();
        KvStore { memory_db }
    }
    pub fn set(&mut self, key: String, value: String) -> Option<String> {
        self.memory_db.insert(key, value)
    }
    pub fn get(&self, key: String) -> Option<String> {
        self.memory_db.get(&key).cloned()
    }

    pub fn remove(&mut self, key: String) -> Option<String> {
        self.memory_db.remove(&key)
    }
}
pub type Result<T> = result::Result<T, Error>;
