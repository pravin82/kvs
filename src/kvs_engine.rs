use super::error;
pub trait KvsEngine{
     fn set( &self, key: String, value: String) -> error::Result<Option<String>>;
     fn get( &self, key: String) -> error::Result<Option<String>>;
     fn remove( &self, key: String) -> error::Result<Option<String>> ;

}