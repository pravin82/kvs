use std::collections::HashMap;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use crate::{Command, KvsEngine, KvsError, Log, Msg};
use super::error;
use error::Result;
use std::io::Write;
use std::clone::Clone;
use std::ops::Deref;
use std::sync::{Arc, Mutex};


#[derive(Clone)]
pub struct KvStore {
    path: PathBuf,
    file: Arc<Mutex<File>>,
    memory_db:Arc<Mutex<HashMap<String,u64>>>,
    size:Arc<Mutex<usize>>
}


impl KvStore {


    pub fn open(path: &Path)->Result<KvStore>{
        let memory_db =Arc::new(Mutex::new( HashMap::new()));
        let  file = OpenOptions::new()
            .write(true)
            .append(true)
            .read(true)
            .create(true)
            .open(path.join("log.txt"))
            .unwrap();
        let path_buf = path.to_path_buf();
        let arc_file = Arc::new(Mutex::new(file));
        let mut store = KvStore{path:path_buf,file:arc_file,memory_db, size: Arc::new(Mutex::new(0)) };
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

    fn compact( &self){
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
                let memory_db = self.memory_db.deref().lock().unwrap();
                match log.command {
                    Command::SET  => {
                        let key_offset =   memory_db.get(&*log.key).unwrap();
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
        let mut mut_file = self.file.lock().unwrap();
        let mut mut_size = self.size.lock().unwrap();
       *mut_file = file;
        *mut_size = 0;
        self.start_up();
    }

    fn start_up(&self){
        let binding = self.file.lock().unwrap();
        let file = binding.deref();
        let mut f = BufReader::new(file);
        let mut byte_size = 1;
        let mut line_offset = Ok(0);
        while  byte_size > 0 {
            let mut buf = String::new();
            line_offset = f.stream_position();
            byte_size =  f.read_line(&mut buf)
                .unwrap();
            if byte_size > 0{
                let log: Log = serde_json::from_str(&*buf).unwrap();
                let mut memory_db = self.memory_db.lock().unwrap();
                match log.command {
                    Command::SET  => memory_db.insert(log.key,line_offset.unwrap()),
                    Command::RM => memory_db.remove(&*log.key),
                    _ => Some(0u64)
                };
            }

        }

    }
    pub fn handle_str_msg( &self,msg:String)->Msg{
        let log: Log = serde_json::from_str(&*msg).unwrap();
        self.handle_log(log)
    }
    fn handle_log( &self,log:Log)->Msg{
        return match log.command {
            Command::SET=> {
                let rs =   self.set(log.key,log.value);
                Msg{status:"SUCCESS".to_string(), value:"".to_string()}
            }
            Command::GET=> {
                let rs =   self.get(log.key).unwrap();
                if let Some(i) = rs {
                    Msg{status:"SUCCESS".to_string(), value:i}
                }
                else {
                    Msg{status: "SUCCESS".to_string(), value:"Key not found".to_string()}
                }

            }
            Command::RM=> {
                let rs =   self.remove(log.key).unwrap_or_else(|_|None);
                let mut status = "SUCCESS".to_string();
                let mut value = "".to_string();
                println!("RS Msg:{:?}",rs);
                if let Some(i) = rs {

                }
                else {
                    status = "ERROR".to_string();
                    value = "Key not found".to_string()

                }
                let msg =  Msg{status:status, value:value};
                println!("RM Msg:{:?}",msg);
                msg



            }
        }

    }

}

impl KvsEngine for KvStore {
    fn set(&self, key: String, value: String) -> Result<Option<String>> {
        let log = Log{command: Command::SET,key:key.to_string(),value:value.to_string()};
        let log_str = serde_json::to_string(&log).unwrap();
        let mut file = self.file.lock().unwrap();
        let current_position = file.deref().stream_position().unwrap();
        let resp = writeln!(file,"{}",log_str);
        let mut memory_db = self.memory_db.lock().unwrap();
        if resp.is_ok(){
            memory_db.insert(key, current_position);
            let mut size= self.size.lock().unwrap();
            *size = *size + log_str.len()

        }
        let  size = self.size.lock().unwrap().deref().clone();
        if size >= 100000{ self.compact();}

        Ok(Some("SUCCESS".to_string()))
    }
    fn get( &self, key: String) -> Result<Option<String>> {
        let binding = self.memory_db.lock().unwrap();
        let memory_db = binding.deref();
        let file = self.file.lock().unwrap().try_clone().unwrap();
        let mut f = BufReader::new(file);
        let mut buf = String::new();
        let line_offset = memory_db.get(&*key);
        println!("GET MEMDB: {:?}", self.memory_db);
        println!("Get LO:{:?}", line_offset);
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

    fn remove( &self, key: String) -> Result<Option<String>> {
        let log = Log{command: Command::RM,key:key.to_string(), value: "".to_string() };
        let mut file = self.file.lock().unwrap();
        let mut memory_db = self.memory_db.lock().unwrap();
        let value = self.get(key.clone()).unwrap();
        if let Some(i) = value {
            let log_str = serde_json::to_string(&log);
            let resp = writeln!(*file,"{}",log_str.unwrap());
            if resp.is_ok(){
                memory_db.remove(&*key);
            }
            return Ok(Some("SUCCESS".to_string()))

        }

        return Err(KvsError::KeyNotFound);


    }

}