extern crate clap;

use std::env::current_dir;
use std::ops::Deref;
use clap::{App, AppSettings, Arg, SubCommand};
use std::process::exit;
use tempfile::TempDir;
use kvs::KvStore;
use kvs::Log;

fn main() {
    let matches = App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .setting(AppSettings::DisableHelpSubcommand)
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .setting(AppSettings::VersionlessSubcommands)
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
        .get_matches();
    match matches.subcommand() {
        ("set", Some(_matches)) => {
            let key = _matches.value_of("KEY").unwrap();
            let value =  _matches.value_of("VALUE").unwrap();
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

