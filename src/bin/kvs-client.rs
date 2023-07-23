use std::io;
use std::io::{BufRead, BufReader, Write};
use clap::AppSettings;
use kvs::{Log, Msg, Result};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::exit;
use structopt::StructOpt;

const DEFAULT_LISTENING_ADDRESS: &str = "127.0.0.1:4000";
const ADDRESS_FORMAT: &str = "IP:PORT";

#[derive(StructOpt, Debug)]
#[structopt(
name = "kvs-client",
raw(global_settings = "&[\
                           AppSettings::DisableHelpSubcommand,\
                           AppSettings::VersionlessSubcommands]")
)]

struct Opt {
    #[structopt(subcommand)]
    command: Command,
}
#[derive(StructOpt, Debug)]
enum Command {
    #[structopt(name = "get", about = "Get the string value of a given string key")]
    Get {
        #[structopt(name = "KEY", help = "A string key")]
        key: String,
        #[structopt(
        long,
        help = "Sets the server address",
        raw(value_name = "ADDRESS_FORMAT"),
        raw(default_value = "DEFAULT_LISTENING_ADDRESS"),
        parse(try_from_str)
        )]
        addr: SocketAddr,
    },
    #[structopt(name = "set", about = "Set the value of a string key to a string")]
    Set {
        #[structopt(name = "KEY", help = "A string key")]
        key: String,
        #[structopt(name = "VALUE", help = "The string value of the key")]
        value: String,
        #[structopt(
        long,
        help = "Sets the server address",
        raw(value_name = "ADDRESS_FORMAT"),
        raw(default_value = "DEFAULT_LISTENING_ADDRESS"),
        parse(try_from_str)
        )]
        addr: SocketAddr,
    },
    #[structopt(name = "rm", about = "Remove a given string key")]
    Remove {
        #[structopt(name = "KEY", help = "A string key")]
        key: String,
        #[structopt(
        long,
        help = "Sets the server address",
        raw(value_name = "ADDRESS_FORMAT"),
        raw(default_value = "DEFAULT_LISTENING_ADDRESS"),
        parse(try_from_str)
        )]
        addr: SocketAddr,
    },
}
fn main() {
    let opt = Opt::from_args();
    if let Err(e) = run(opt) {
        eprintln!("{:?}", e);
        exit(1);
    }
}

fn run(opt: Opt) -> Result<()> {
    match opt.command {
        Command::Get { key, addr } => {
            let mut stream = TcpStream::connect(addr).unwrap();
            let message = Log{command:kvs::Command::GET,key:key, value: "".parse().unwrap() };
            let message_str = serde_json::to_string(&message).unwrap();
            stream.write(message_str.as_bytes());
            let mut reader = BufReader::new(&mut stream);
            let received: Vec<u8> = reader.fill_buf().unwrap().to_vec();
            reader.consume(received.len());
          String::from_utf8(received)
                .map(|msg| handle_server_msg(msg))
                .map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Couldn't parse received string as utf8",
                    )
                });
        }

        Command::Set { key, value, addr } => {
            let mut stream = TcpStream::connect(addr).unwrap();
            let message = Log{command:kvs::Command::SET,key:key, value:value};
            let message_str = serde_json::to_string(&message).unwrap();
            stream.write(message_str.as_bytes());
        }
        Command::Remove { key, addr } => {
            let mut stream = TcpStream::connect(addr).unwrap();
            let message = Log{command:kvs::Command::RM,key:key, value: "".parse().unwrap() };
            let message_str = serde_json::to_string(&message).unwrap();
            stream.write(message_str.as_bytes());

            let mut reader = BufReader::new(&mut stream);
            let received: Vec<u8> = reader.fill_buf().unwrap().to_vec();
            reader.consume(received.len());
            String::from_utf8(received)
                .map(|msg| handle_server_msg(msg))
                .map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Couldn't parse received string as utf8",
                    )
                });
        }
    }
    Ok(())
}

fn handle_server_msg(msg_str:String){
    let msg: Msg = serde_json::from_str(&*msg_str).unwrap();
    if(msg.status == "SUCCESS" && msg.value != "") {println!("{}",msg.value)}
    if (msg.status == "ERROR"){
        eprintln!("Key not found");
        exit(1)
    }

}



