use std::collections::HashMap;

pub struct KvStore {
    memoryDB: HashMap<String, String>
}

impl KvStore {

    pub fn new()->KvStore{
        let mut memoryDB = HashMap::new();
        KvStore{memoryDB}
    }
    pub fn set(&mut self, key:String, value:String) -> Option<String> {
       self.memoryDB.insert(key, value)
    }
    pub fn get(&self,key:String) -> Option<String>{
       self.memoryDB.get(&key).cloned()
    }

    pub fn remove(&mut self, key:String) -> Option<String>{
       self.memoryDB.remove(&key)
    }
}