pub struct KvStore;

impl KvStore {
    pub fn new()->KvStore{
        KvStore
    }
    pub fn set(&self,key:String, value:String){
        panic!("Not implemented")
    }
    pub fn get(&self,key:String) -> Option<String>{
        panic!("Not implemented")
    }

    pub fn remove(&self,key:String){
        panic!("Not implemented")
    }
}