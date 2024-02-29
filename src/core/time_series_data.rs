use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::io::{Error, ErrorKind};
use std::num::ParseIntError;
use std::ops::Add;

pub struct FileWithDate {
    pub name: String,
    pub date: u64
}

pub trait DatedSource<T> {
    fn load(&mut self, files: Vec<FileWithDate>) -> Result<T, Error>;
    fn parse_date(&self, info: &FileInfo) -> Result<u64, Error>;
}

pub struct TimeSeriesData<T> {
    pub map: BTreeMap<u64, T>
}

impl<T> TimeSeriesData<T> {
    pub fn load(data_folder_path: String, mut source: Box<dyn DatedSource<T>>,
                index_calculator: fn(u64) -> u64) -> Result<TimeSeriesData<T>, Error> {
        let mut file_map = HashMap::new();
        for file in get_file_list(data_folder_path)? {
            let date = source.parse_date(&file)?;
            let key = index_calculator(date);
            file_map.entry(key).or_insert(Vec::new())
                .push(FileWithDate { name: file.name, date });
        }
        let mut map = BTreeMap::new();
        for (key, files) in file_map {
            map.insert(key, source.load(files)?);
        }
        Ok(TimeSeriesData{map})
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
