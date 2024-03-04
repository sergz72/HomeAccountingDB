mod db;
mod entities;
mod core;
mod json_db_config;
mod binary_db_config;

use std::env::args;
use std::io::Error;
use crate::binary_db_config::BinaryDBConfiguration;
use crate::db::HomeAccountingDB;
use crate::json_db_config::JsonDBConfiguration;

fn usage() -> Result<(), Error> {
    println!("Usage: home_accounting_db data_folder_path\n  test_json date\n  test date aes_key_file");
    println!("  migrate source_folder_path aes_key\n  server port rsa_key_file");
    return Ok(());
}

fn main() -> Result<(), Error> {
    let arguments: Vec<String> = args().skip(1).collect();
    let l = arguments.len();
    if l < 3 || l > 4 {
        return usage();
    }
    let aes_key = [0u8; 32];
    match arguments[1].as_str() {
        "test_json" => {
            if l != 3 {
                usage()
            } else {
                let db = HomeAccountingDB::load(arguments[0].clone(), Box::new(JsonDBConfiguration::new()), 1000000)?;
                db.test(arguments[2].clone())
            }
        }
        "test" => {
            if l != 4 {
                usage()
            } else {
                let db = HomeAccountingDB::load(arguments[0].clone(), Box::new(BinaryDBConfiguration::new(aes_key)), 1000000)?;
                db.test(arguments[2].clone())
            }
        }
        "migrate" => {
            if l != 4 {
                usage()
            } else {
                let db = HomeAccountingDB::load(arguments[2].clone(), Box::new(JsonDBConfiguration::new()), 1000000)?;
                db.migrate(arguments[0].clone())
            }
        }
        "server" => {
            if l != 4 {
                usage()
            } else {
                todo!()
            }
        }
        _ => usage()
    }
}
