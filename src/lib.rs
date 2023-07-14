use core::result;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::path::Path;
use serde::{Serialize, Deserialize};
use std::io::prelude::*;
extern crate serde_json;
use failure::Fail;
use std::io;
use std::io::{BufReader, SeekFrom};


extern crate failure;
use failure::Error;
use serde_json::to_string;

pub struct KvStore {
    file: File,
    memory_db:HashMap<String,u64>
}


impl KvStore {

    pub fn set(&mut self, key: String, value: String) -> Result<Option<String>> {
        let log = Log{command: Command::SET,key:key.to_string(),value:value.to_string()};
        let log_str = serde_json::to_string(&log).unwrap();
        let current_position = self.file.stream_position().unwrap();
        let resp = writeln!(self.file,"{}",log_str);
        if(resp.is_ok()){
            self.memory_db.insert(key, current_position);
        }
        Ok(Some("SUCCESS".to_string()))
    }
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let mut f = BufReader::new(&self.file);
        let mut buf = String::new();
        let line_offset = self.memory_db.get(&*key);
        if let Some(i) = line_offset {
            f.seek(SeekFrom::Start(*line_offset.unwrap()));
            let value = self.memory_db.get(&*key);
            f.read_line(&mut buf);
            let log: Log = serde_json::from_str(&*buf).unwrap();
            Ok(Some(log.value))

        }
        else {
            Ok(None)
        }

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
        let mut f = BufReader::new(&self.file);
        let mut byte_size = 1;
        let mut line_offset = Ok(0);
        while  byte_size > 0 {
            let mut buf = String::new();
             line_offset = f.stream_position();
            byte_size =  f.read_line(&mut buf)
                .unwrap();
            if(byte_size > 0){
               // println!("BUF: {}", buf);
                let log: Log = serde_json::from_str(&*buf).unwrap();
               // println!("log: {:?}", log);
                match log.command {
                    Command::SET  => self.memory_db.insert(log.key,line_offset.unwrap()),
                    Command::RM => self.memory_db.remove(&*log.key),
                    _ => Some(0u64)
                };
            }

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

