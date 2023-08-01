use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use serde::{Serialize, Deserialize};
use std::io::prelude::*;
extern crate serde_json;
use std::{fs};
use std::io::{BufReader, SeekFrom};
use sled::Db;
pub use error::{KvsError,Result};
mod error;
mod kvs_engine;
mod kv_store;
pub mod thread_pool;

pub use kvs_engine::KvsEngine;
pub use kv_store::KvStore;


extern crate failure;






#[derive(Clone)]
pub struct SledKvsEngine{
    db:Db
}

impl SledKvsEngine {
    pub fn open(path: &Path) -> sled::Result<SledKvsEngine> {
        let tree = sled::open(path).unwrap();
        Ok(SledKvsEngine{db:tree})
    }
}

impl KvsEngine for SledKvsEngine{
    fn set( &self, key: String, value: String) -> Result<Option<String>> {
         self.db.insert(key,sled::IVec::from(value.as_bytes()));
        Ok(Some("SUCCESS".to_string()))
    }

    fn get( &self, key: String) -> Result<Option<String>> {
       let resp =  self.db.get(key).unwrap();
        if let Some(i) = resp {
            let val = std::str::from_utf8 (&i).unwrap();
            Ok(Some(val.parse().unwrap()))
        } else {
            Ok(None)
        }

    }

    fn remove( &self, key: String) -> Result<Option<String>> {
        let resp = self.db.remove(key).unwrap();
        if let Some(i) = resp {
            Ok(Some("SUCCESS".to_string()))
        } else {
            Err(KvsError::KeyNotFound)
        }
    }
}



#[derive(Serialize, Deserialize)]
#[derive(Debug)]
pub enum Command{
    SET,
    RM,
    GET
}
#[derive(Serialize, Deserialize)]
#[derive(Debug)]
pub struct Log{
    pub command:Command,
    pub key:String,
    pub value:String
}
#[derive(Serialize, Deserialize)]
#[derive(Debug)]
pub struct Msg{
    pub status:String,
    pub value:String
}



