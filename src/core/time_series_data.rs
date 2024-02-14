use std::collections::BTreeMap;
use std::fs;
use std::io::{Error, ErrorKind};
use std::marker::PhantomData;
use std::num::ParseIntError;
use std::ops::Add;
use crate::core::data_source::DataSource;

pub trait Loader<K> {
    fn load(&mut self, file_name: String, data_source: &Box<dyn DataSource<K>>, date: Option<u64>) -> Result<(), Error>;
}

pub struct TimeSeriesData<T: Loader<K>, K> {
    pub map: BTreeMap<u64, T>,
    phantom: PhantomData<K>
}

impl<T: Loader<K>, K> TimeSeriesData<T, K> {
    pub fn load(data_folder_path: String, source: Box<dyn DataSource<K>>,
                index_calculator: fn(u64) -> u64,
                date_parser: fn(&FileInfo) -> Result<(u64, bool), Error>,
                creator: fn() -> T) -> Result<TimeSeriesData<T, K>, Error> {
        let file_list = get_file_list(data_folder_path)?;
        let mut map = BTreeMap::new();
        for file in file_list {
            let (date, set_date) = date_parser(&file)?;
            let key = index_calculator(date);
            let date_option = if set_date {Some(date)} else {None};
            map.entry(key).or_insert(creator()).load(file.name, &source, date_option)?;
        }
        Ok(TimeSeriesData{map, phantom: Default::default() })
    }
}

pub struct FileInfo {
    folder: String,
    name: String
}

impl FileInfo {
    pub fn convert_folder_name_to_number(&self) -> Result<u64, Error> {
        self.folder.parse()
            .map_err(|e: ParseIntError|Error::new(ErrorKind::InvalidData, "convert_folder_name_to_number: ".to_string() + e.to_string().as_str()))
    }
}

fn get_file_list(data_folder_path: String) -> Result<Vec<FileInfo>, Error> {
    let files = fs::read_dir(data_folder_path.clone())?;
    let mut result = Vec::new();
    for file in files {
        let f = file.unwrap();
        let file_name = f.file_name().into_string()
            .map_err(|_|Error::new(ErrorKind::InvalidData, "invalid file name"))?;
        let path = data_folder_path.clone().add("/").add(file_name.as_str());
        if f.file_type().unwrap().is_dir() {
            let mut files = get_file_list(path)?.into_iter()
                .map(|mut f|{f.folder = file_name.clone(); f}).collect();
            result.append(&mut files);
        } else {
            result.push(FileInfo{folder: "".to_string(), name: path})
        }
    }
    Ok(result)
}
