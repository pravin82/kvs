use super::error;
pub trait KvsEngine{
     fn set(&mut self, key: String, value: String) -> error::Result<Option<String>>;
     fn get(&mut self, key: String) -> error::Result<Option<String>>;
     fn remove(&mut self, key: String) -> error::Result<Option<String>> ;

}