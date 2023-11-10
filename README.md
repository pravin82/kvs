# kvs
Key value store database written in Rust built by following this course
 https://github.com/pingcap/talent-plan
It stores the data in disk and  maintains the location where value is stored in memory. When the database restarts it populates it's memory with key and disk offset.

For running the server cd into project directory and  run this command
```
 cargo run --bin kvs-server
```
For running the client cd into project directory and run this command
```
cargo run --bin kvs-client
```

