mod db;
mod entities;
mod time_series_data;

use std::env::args;
use std::io::Error;
use crate::db::HomeAccountingDB;

fn usage() -> Result<(), Error> {
    println!("Usage: home_accounting_db [test_json date|test date|migrate source_folder_path|server port]data_folder_path");
    return Ok(());
}

fn main() -> Result<(), Error> {
    let arguments: Vec<String> = args().skip(1).collect();
    if arguments.len() != 3 {
        return usage();
    }
    match arguments[0].as_str() {
        "test_json" => {
            let db = HomeAccountingDB::load(true, arguments[2].clone())?;
            db.test(arguments[1].clone())
        }
        "test" => {
            let db = HomeAccountingDB::load(false, arguments[2].clone())?;
            db.test(arguments[1].clone())
        }
        "migrate" => {
            let db = HomeAccountingDB::load(true, arguments[1].clone())?;
            db.migrate(arguments[2].clone())
        }
        "server" => {
            todo!()
        }
        _ => usage()
    }
}
