use std::thread;
use super::error;
use error::Result;

pub struct NaiveThreadPool;
pub struct SharedQueueThreadPool;
pub struct RayonThreadPool;

pub trait ThreadPool{
    fn spawn<F>(&self,job:F)
    where
    F:FnOnce() + Send + 'static
    ;
    fn new(threads:u32)->Result<Self>
    where
        Self: Sized;
}

impl ThreadPool for NaiveThreadPool{
    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static {
        let handler = thread::spawn(|| {
            job()
        });

        handler.join().unwrap();
    }

    fn new(threads: u32) -> Result<Self> where Self: Sized {
        Ok(NaiveThreadPool)
    }
}

impl ThreadPool for SharedQueueThreadPool{
    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static {
        todo!()
    }

    fn new(threads: u32) -> Result<Self> where Self: Sized {
        todo!()
    }
}

impl ThreadPool for RayonThreadPool{
    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static {
        todo!()
    }

    fn new(threads: u32) -> Result<Self> where Self: Sized {
        todo!()
    }
}