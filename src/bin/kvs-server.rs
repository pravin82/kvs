extern crate clap;

use std::env::current_dir;
use std::io;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::ops::Deref;
use clap::{App, Arg, SubCommand};
use std::process::exit;
use kvs::KvStore;
use log::{info, warn,error};
use log::*;

fn main() {
    stderrlog::new().module(module_path!()).init().unwrap();
    error!("version: {}",env!("CARGO_PKG_VERSION") );
    let engine_in_use = "".to_string();
    let matches = App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .subcommand(
            SubCommand::with_name("set")
                .about("Set the value of a string key to a string")
                .arg(Arg::with_name("KEY").help("A string key").required(true))
                .arg(
                    Arg::with_name("VALUE")
                        .help("The string value of the key")
                        .required(true),
                ),
        )
        .subcommand(
            SubCommand::with_name("get")
                .about("Get the string value of a given string key")
                .arg(Arg::with_name("KEY").help("A string key").required(true)),
        )
        .subcommand(
            SubCommand::with_name("rm")
                .about("Remove a given key")
                .arg(Arg::with_name("KEY").help("A string key").required(true)),
        )
        .arg(Arg::with_name("engine")
            .short("c")
            .long("engine")
            .value_name("ENGINE")
            .help("Storage engine for store")
            .takes_value(true))
        .arg(Arg::with_name("addr")
            .short("a")
            .long("addr")
            .value_name("addr")
            .help("Addr of the host")
            .takes_value(true))
        .get_matches();
    let addr = matches.value_of("addr").unwrap_or_else(||"127.0.0.1:4000");
    let engine =  matches.value_of("engine").unwrap_or_else(||"kvs");
    println!("engine_in_use ,{}", engine_in_use);
    println!("engine , {}", engine);
    if(engine_in_use != "" && engine != engine_in_use) {
        println!("Wrong engine selected");
        exit(1)
    };
    error!("Config \n host:{} \n engine: {}", addr, engine);
    let listener = TcpListener::bind(addr).unwrap();
    let mut store = KvStore::open(current_dir().unwrap().as_path()).unwrap();
    // accept connections and process them serially
    for mut stream_res in listener.incoming() {
        let mut stream = stream_res.unwrap();
        println!("stream: {:?}",stream);
        let mut reader = BufReader::new(&mut stream);
        let received: Vec<u8> = reader.fill_buf().unwrap().to_vec();
        reader.consume(received.len());
       let rs =  String::from_utf8(received)
            .map(|msg| store.handle_str_msg(msg))
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Couldn't parse received string as utf8",
                )
            });
        let value = rs.unwrap();
        println!("value to be sent: {:?}", value);
        let value_str = serde_json::to_string(&value).unwrap();
        stream.write(value_str.as_bytes());



    }


    match matches.subcommand() {
        ("set", Some(_matches)) => {
            let key = _matches.value_of("KEY").unwrap();
            let value =  _matches.value_of("VALUE").unwrap();
            let mut store = KvStore::open(current_dir().unwrap().as_path()).unwrap();
            store.set(key.to_string(), value.to_string());
            exit(0);
        }
        ("get", Some(_matches)) => {
            let key = _matches.value_of("KEY").unwrap();
            let mut store = KvStore::open(current_dir().unwrap().as_path()).unwrap();
            let value =  store.get(key.to_string()).unwrap();
            if let Some(i) = value{
                println!("{}",i);
            } else {
                println!("Key not found");
            }
            exit(0);
        }
        ("-V", Some(_matches)) => {
            eprintln!(env!("CARGO_PKG_VERSION"))
        }
        ("rm", Some(_matches)) => {
            let key = _matches.value_of("KEY").unwrap();
            let mut store = KvStore::open(current_dir().unwrap().as_path()).unwrap();
            let value =  store.get(key.to_string()).unwrap();
           if let Some(i) = value{
                  store.remove(key.to_string());
            } else {
                println!("Key not found");
               exit(1);
            }
            exit(0);
        }
        _ => unreachable!(),
    }
}




