use std::fs::File;
use std::io::{BufReader, Error};
use std::ops::Add;
use serde::de::DeserializeOwned;

pub trait DataSource<T> {
    fn load(&self, file_name: String, add_extension: bool) -> Result<T, Error>;
    fn save(&self, data: &T, file_name: String) -> Result<(), Error>;
}

pub struct JsonDataSource {}
impl<'de, T: DeserializeOwned> DataSource<T> for JsonDataSource {
    fn load(&self, file_name: String, add_extension: bool) -> Result<T, Error> {
        let fname = if add_extension {file_name.add(".json")} else {file_name};
        let file = File::open(fname)?;
        let reader = BufReader::new(file);
        Ok(serde_json::from_reader(reader)?)
    }

    fn save(&self, data: &T, file_name: String) -> Result<(), Error> {
        todo!()
    }
}
