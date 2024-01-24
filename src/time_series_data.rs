use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::fs::File;
use std::io::{BufReader, Error, ErrorKind, Read, Write};
use std::num::ParseIntError;
use std::ops::{Add, Range};
use std::sync::Arc;

pub trait BinaryData {
    fn serialize(&self, output: &mut Vec<u8>);
}

pub struct TimeSeriesData<T: BinaryData> {
    configuration: Arc<dyn TimeSeriesDataConfiguration<T>>,
    suffix: String,
    pub map: BTreeMap<u64, T>
}

impl<T: BinaryData> TimeSeriesData<T> {
    pub fn load(configuration: Arc<dyn TimeSeriesDataConfiguration<T>>, suffix: String) -> Result<TimeSeriesData<T>, Error>{
        let file_list = get_file_list(configuration.data_folder_path().add(suffix.as_str()))?;
        let mut map = BTreeMap::new();
        for file in file_list {
            let mut data = configuration.load_file(&file)?;
            map.append(&mut data);
        }
        Ok(TimeSeriesData{configuration, suffix, map})
    }

    pub fn save(&self, from: u64, to: u64) -> Result<(), Error> {
        let file_names = self.configuration.get_file_names(from, to);
        for (file_name, range) in file_names {
            let mut data: Vec<u8> = Vec::new();
            let mut counter: usize = 0;
            for (index, value) in self.map.range(range) {
                data.extend_from_slice(&index.to_le_bytes());
                value.serialize(&mut data);
                counter += 1;
            }
            if counter > 0 {
                let full_name = self.suffix.add("/").add(file_name.as_str());
                let mut output = counter.to_le_bytes().to_vec();
                output.extend(data);
                self.configuration.save_file(full_name, output)?;
            }
        }
        Ok(())
    }
}

pub fn load_file(file_name: String) -> Result<Vec<u8>, Error> {
    let mut f = File::open(file_name)?;
    let mut data = Vec::new();
    f.read_to_end(&mut data)?;
    Ok(data)
}

/*pub fn create_file(file_name: &String, data: Vec<u8>) -> Result<(), Error> {
    let mut f = File::create(file_name)?;
    f.write_all(data.as_slice())
}*/

pub struct FileInfo {
    folder: String,
    name: String
}

impl FileInfo {
    pub fn is_json(&self) -> bool {
        self.name.ends_with(".json")
    }

    pub fn is_folder_empty(&self) -> bool {
        self.folder.is_empty()
    }

    pub fn convert_folder_name_to_number(&self) -> Result<u64, Error> {
        self.folder.parse()
            .map_err(|e: ParseIntError|Error::new(ErrorKind::InvalidData, "convert_folder_name_to_number: ".to_string() + e.to_string().as_str()))
    }

    pub fn load_file(&self) -> Result<BufReader<File>, Error> {
        let file = File::open(&self.name)?;
        Ok(BufReader::new(file))
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

pub trait TimeSeriesDataConfiguration<T> {
    fn data_folder_path(&self) -> String;
    fn load_file(&self, file_info: &FileInfo) -> Result<BTreeMap<u64, T>, Error>;
    fn get_file_names(&self, from: u64, to: u64) -> HashMap<String, Range<u64>>;
    fn save_file(&self, file_name: String, data: Vec<u8>) -> Result<(), Error>;
}
