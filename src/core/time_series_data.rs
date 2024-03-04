use std::collections::{BTreeMap, HashMap, HashSet};
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
    fn save(&self, data: &T, data_folder_path: &String, date: u64) -> Result<(), Error>;
    fn get_files(&self, data_folder_path: &String, date: u64) -> Result<Vec<FileWithDate>, Error>;
}

struct DataHolder<T> {
    data: Option<T>,
    last_access_time: u64
}

impl<T> DataHolder<T> {
    fn new(value: T) -> DataHolder<T> {
        DataHolder{data: Some(value), last_access_time: std::time::UNIX_EPOCH.elapsed().unwrap().as_millis() as u64}
    }

    fn empty() -> DataHolder<T> {
        DataHolder{data: None, last_access_time: std::time::UNIX_EPOCH.elapsed().unwrap().as_millis() as u64}
    }
    
    fn set(&mut self, value: T) {
        self.data = Some(value);
        self.last_access_time = std::time::UNIX_EPOCH.elapsed().unwrap().as_millis() as u64;
    }

    fn unset(&mut self) {
        self.data = None;
    }
}

pub struct TimeSeriesData<T> {
    source: Box<dyn DatedSource<T>>,
    data_folder_path: String,
    max_active_items: u64,
    active_items: u64,
    map: BTreeMap<u64, DataHolder<T>>,
    modified: HashSet<u64>,
    lru: BTreeMap<u64, HashSet<u64>>
}

impl<T> TimeSeriesData<T> {
    pub fn load(data_folder_path: String, source: Box<dyn DatedSource<T>>,
                index_calculator: fn(u64) -> u64, max_active_items: u64)
        -> Result<TimeSeriesData<T>, Error> {
        let mut file_map = HashMap::new();
        for file in get_file_list(data_folder_path.clone())? {
            let date = source.parse_date(&file)?;
            let key = index_calculator(date);
            file_map.entry(key).or_insert(Vec::new())
                .push(FileWithDate { name: file.name, date });
        }
        let mut data = TimeSeriesData{source, data_folder_path, max_active_items, active_items: 0,
            map: BTreeMap::new(), modified: HashSet::new(), lru: BTreeMap::new()};
        for (key, files) in file_map {
            data.load_files(key, files)?;
        }
        Ok(data)
    }

    pub fn init(data_folder_path: String, mut source: Box<dyn DatedSource<T>>,
                index_calculator: fn(u64) -> u64, max_active_items: u64) -> Result<TimeSeriesData<T>, Error> {
        let mut map = BTreeMap::new();
        for file in get_file_list(data_folder_path.clone())? {
            let date = source.parse_date(&file)?;
            let key = index_calculator(date);
            map.insert(key, DataHolder::empty());
        }
        Ok(TimeSeriesData{source, data_folder_path, max_active_items, active_items: 0, map,
            modified: HashSet::new(), lru: BTreeMap::new()})
    }

    fn load_files(&mut self, key: u64, files: Vec<FileWithDate>) -> Result<(), Error> {
        let v = self.source.load(files)?;
        self.add(key, v, false)
    }
    
    pub fn add(&mut self, key: u64, v: T, add_to_modified: bool) -> Result<(), Error> {
        self.cleanup()?;
        let h = DataHolder::new(v);
        let t = h.last_access_time;
        self.map.insert(key, h);
        self.add_to_lru(key, t);
        if add_to_modified {
            self.modified.insert(key);
        }
        self.active_items += 1;
        Ok(())
    }
    
    fn cleanup(&mut self) -> Result<(), Error> {
        while self.active_items >= self.max_active_items {
            self.remove_by_lru()?;
        }
        Ok(())
    }
    
    fn remove_by_lru(&mut self) -> Result<(), Error> {
        let e = self.lru.first_entry().unwrap();
        let filtered: Vec<u64> = e.get().iter()
            .map(|v|*v)
            .filter(|v|self.modified.contains(v))
            .collect();
        for idx in filtered {
            let v = self.map.get(&idx).unwrap();
            self.source.save(v.data.as_ref().unwrap(),
                             &self.data_folder_path, idx)?;
            self.modified.remove(&idx);
        }
        let deleted = e.remove();
        deleted.iter().for_each(|k|self.map.get_mut(k).unwrap().unset());
        self.active_items -= deleted.len() as u64;
        Ok(())
    }
    
    fn add_to_lru(&mut self, key: u64, time: u64) {
        self.lru.entry(time).or_insert(HashSet::new()).insert(key);
    }

    pub fn get(&mut self, idx: u64) -> Result<Option<&T>, Error> {
        if let Some((_, h)) = self.map.range_mut(..=idx).last() {
            let v = self.get_t(idx, h)?;
            Ok(Some(v))
        } else {
            Ok(None)
        }
    }
    
    pub fn get_range(&mut self, from: u64, to: u64) -> Result<Vec<(u64, &mut T)>, Error> {
        let mut result = Vec::new();
        for (k, v) in self.map.range_mut(from..=to) {
            let v = self.get_t(*k, v)?;
            result.push((*k, v));
        }
        Ok(result)
    }

    fn get_t(&mut self, key: u64, v: &mut DataHolder<T>) -> Result<&mut T, Error> {
        if let Some(v) = &mut v.data {
            return Ok(v);
        }
        let files = self.source.get_files(&self.data_folder_path, key)?;
        let t = self.source.load(files)?;
        v.set(t);
        self.add_to_lru(key, v.last_access_time);
        Ok(v.data.as_mut().unwrap())
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
