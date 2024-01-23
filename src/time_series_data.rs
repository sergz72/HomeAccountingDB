use std::collections::BTreeMap;
use std::fs;
use std::fs::File;
use std::io::{BufReader, Error, ErrorKind, Read};
use std::num::ParseIntError;
use std::ops::Add;
use std::sync::Arc;

pub struct TimeSeriesData<T> {
    pub map: BTreeMap<usize, T>
}

impl<T> TimeSeriesData<T> {
    pub fn load(configuration: Arc<dyn TimeSeriesDataConfiguration<T>>, suffix: &str) -> Result<TimeSeriesData<T>, Error>{
        let file_list = get_file_list(configuration.data_folder_path().add(suffix))?;
        let mut map = BTreeMap::new();
        for file in file_list {
            let mut data = configuration.load_file(&file)?;
            map.append(&mut data);
        }
        Ok(TimeSeriesData{map})
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

    pub fn convert_folder_name_to_number(&self) -> Result<usize, Error> {
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
    fn load_file(&self, file_info: &FileInfo) -> Result<BTreeMap<usize, T>, Error>;
}
