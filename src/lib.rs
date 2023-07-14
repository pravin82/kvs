use core::result;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::path::Path;
use serde::{Serialize, Deserialize};
use std::io::prelude::*;
extern crate serde_json;
use failure::Fail;
use std::io;



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
        let log_str = serde_json::to_string(&log).unwrap();
        //println!("log string to be written: {}", log_str);
        let resp = writeln!(self.file,"{}",log_str);
        if(resp.is_ok()){
            self.memory_db.insert(key, value);
        }
        Ok(Some("SUCCESS".to_string()))
    }
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let value = self.memory_db.get(&*key);
        Ok(value.cloned())
    }

    pub fn remove(&mut self, key: String) -> Result<Option<String>> {
        let log = Log{command: Command::RM,key:key.to_string(), value: "".to_string() };
        let value = self.get(key.clone()).unwrap();
        if let Some(i) = value {
            let log_str = serde_json::to_string(&log);
            let resp = writeln!(self.file,"{}",log_str.unwrap());
            if(resp.is_ok()){
              self.memory_db.remove(&*key);
            }
            return Ok(Some("SUCCESS".to_string()))
        } else {
         return   Err(KvsError::KeyNotFound);
        }


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
        let mut store = KvStore{file,memory_db};
        store.start_up();
        Ok (store)
    }

    fn start_up(&mut self){
        let mut buf = String::new();
        self.file.read_to_string(&mut buf)
            .unwrap();
        for log_str in buf.lines(){
            let log: Log = serde_json::from_str(log_str).unwrap();
            match log.command {
                Command::SET  => self.memory_db.insert(log.key,log.value),
                Command::RM => self.memory_db.remove(&*log.key),
                _ => Some("SUCCESS".to_string())
            };


        }
    }

}
pub type Result<T> = result::Result<T, KvsError>;

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

/// Error type for kvs.
#[derive(Fail, Debug)]
pub enum KvsError {
    /// IO error.
    #[fail(display = "{}", _0)]
    Io(#[cause] io::Error),
    /// Serialization or deserialization error.
    #[fail(display = "{}", _0)]
    Serde(#[cause] serde_json::Error),
    /// Removing non-existent key error.
    #[fail(display = "Key not found")]
    KeyNotFound,
    /// Unexpected command type error.
    /// It indicated a corrupted log or a program bug.
    #[fail(display = "Unexpected command type")]
    UnexpectedCommandType,
}

