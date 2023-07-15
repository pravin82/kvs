use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use serde::{Serialize, Deserialize};
use std::io::prelude::*;
extern crate serde_json;
use std::{fs};
use std::io::{BufReader, SeekFrom};
pub use error::{KvsError,Result};
mod error;

extern crate failure;

pub struct KvStore {
    path: PathBuf,
    file: File,
    memory_db:HashMap<String,u64>,
    size:usize
}

pub struct KvsEngine;



impl KvStore {

    pub fn set(&mut self, key: String, value: String) -> Result<Option<String>> {

        let log = Log{command: Command::SET,key:key.to_string(),value:value.to_string()};
        let log_str = serde_json::to_string(&log).unwrap();
        let current_position = self.file.stream_position().unwrap();
        let resp = writeln!(self.file,"{}",log_str);
        if resp.is_ok(){
            self.memory_db.insert(key, current_position);
            self.size = self.size + log_str.len()
        }
        if self.size >= 100000{ self.compact();}

        Ok(Some("SUCCESS".to_string()))
    }
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let mut f = BufReader::new(&self.file);
        let mut buf = String::new();
        let line_offset = self.memory_db.get(&*key);
        if let Some(_i) = line_offset {
            f.seek(SeekFrom::Start(*line_offset.unwrap()));
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
        if let Some(_i) = value {
            let log_str = serde_json::to_string(&log);
            let resp = writeln!(self.file,"{}",log_str.unwrap());
            if resp.is_ok(){
              self.memory_db.remove(&*key);
            }
            return Ok(Some("SUCCESS".to_string()))
        } else {
         return   Err(KvsError::KeyNotFound);
        }


    }
    pub fn open(path: &Path)->Result<KvStore>{
        let memory_db = HashMap::new();
        let  file = OpenOptions::new()
            .write(true)
            .append(true)
            .read(true)
            .create(true)
            .open(path.join("log.txt"))
            .unwrap();
        let path_buf = path.to_path_buf();
        let mut store = KvStore{path:path_buf,file,memory_db, size: 0 };
        store.start_up();
        Ok (store)
    }

    fn get_new_file(&self) ->File{
        let  file = OpenOptions::new()
            .write(true)
            .append(true)
            .read(true)
            .create(true)
            .open(&self.path.join("log_new.txt"))
            .unwrap();
        return file;

    }

    fn compact(&mut self){
        let mut new_file = self.get_new_file();
        let mut f = BufReader::new(File::open(self.path.join("log.txt")).unwrap());
        let mut byte_size = 1;
        let mut line_offset = Ok(0);
        while  byte_size > 0 {
            let mut buf = String::new();
            line_offset = f.stream_position();
            byte_size =  f.read_line(&mut buf)
                .unwrap();
            if byte_size > 0{
                let log: Log = serde_json::from_str(&*buf).unwrap();
                let log_str = serde_json::to_string(&log).unwrap();
                match log.command {
                    Command::SET  => {
                      let key_offset =   self.memory_db.get(&*log.key).unwrap();
                        if key_offset.clone()  == line_offset.unwrap() {
                             writeln!(new_file,"{}",log_str);
                        }
                    }
                    _ => ()
                };
            }

        }
        fs::remove_file(&self.path.join("log.txt"));
        fs::rename(self.path.join("log_new.txt"), self.path.join("log.txt"));
        let  file = OpenOptions::new()
            .write(true)
            .append(true)
            .read(true)
            .create(true)
            .open(self.path.join("log.txt"))
            .unwrap();
        self.file = file;
        self.size = 0;
        self.start_up();
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
            if byte_size > 0{
                let log: Log = serde_json::from_str(&*buf).unwrap();
                match log.command {
                    Command::SET  => self.memory_db.insert(log.key,line_offset.unwrap()),
                    Command::RM => self.memory_db.remove(&*log.key),
                    _ => Some(0u64)
                };
            }

        }

    }

}

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


