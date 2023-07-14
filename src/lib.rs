use core::result;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::path::Path;
use serde::{Serialize, Deserialize};
use std::io::prelude::*;
extern crate serde_json;



extern crate failure;
use failure::Error;
use serde_json::to_string;

pub struct KvStore {
    file: File,
    memory_db:HashMap<String,String>
}


impl KvStore {

    pub fn set(&mut self, key: String, value: String) -> Result<Option<String>> {
        let log = Log{command: Command::SET,key:key.to_string(),value:value.to_string()};
        let log_str = serde_json::to_string(&log);
        let resp = writeln!(self.file,"{}",log_str.unwrap());
        Ok(Some("SUCCESS".to_string()))
    }
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let mut buf = String::new();
          self.file.read_to_string(&mut buf)
            .unwrap();
        for log_str in buf.lines(){
            let log: Log = serde_json::from_str(log_str).unwrap();
            self.memory_db.insert(log.key,log.value);

        }
        let value = self.memory_db.get(&*key);
        Ok(value.cloned())
    }

    pub fn remove(&mut self, key: String) -> Result<Option<String>> {
        let log = Log{command: Command::RM,key:key.to_string(), value: "".to_string() };
        let log_str = serde_json::to_string(&log);
        let resp = writeln!(self.file,"{}",log_str.unwrap());
        Ok((Some("SUCCESS".to_string())))
    }
    pub fn open(path:&Path)->Result<KvStore>{
        let memory_db = HashMap::new();
        let mut file = OpenOptions::new()
            .write(true)
            .append(true)
            .read(true)
            .create(true)
            .open(path.join("log.txt"))
            .unwrap();
        Ok (KvStore{file,memory_db})
    }

}
pub type Result<T> = result::Result<T, Error>;

#[derive(Serialize, Deserialize)]
#[derive(Debug)]
enum Command{
    SET,
    RM,
    GET
}
#[derive(Serialize, Deserialize)]
#[derive(Debug)]
pub struct Log{
    command:Command,
    key:String,
    value:String
}

