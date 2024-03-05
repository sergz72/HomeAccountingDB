use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{Error, ErrorKind};
use std::num::ParseIntError;
use std::ops::Add;

pub struct FileWithDate {
    pub name: String,
    pub date: u32
}

pub trait DatedSource<T> {
    fn load(&mut self, files: Vec<FileWithDate>) -> Result<T, Error>;
    fn parse_date(&self, info: &FileInfo) -> Result<u32, Error>;
    fn save(&self, data: &T, data_folder_path: &String, idx: usize) -> Result<(), Error>;
    fn get_files(&self, data_folder_path: &String, date: u32) -> Result<Vec<FileWithDate>, Error>;
}

pub trait IndexCalculator {
    fn calculate_index(&self, date: u32) -> isize;
}

#[derive(Copy, Clone)]
struct DataHolder<T> {
    data: Option<T>,
    prev: Option<usize>,
    next: Option<usize>
}

impl<T> DataHolder<T> {
    fn new(value: T, next: Option<usize>) -> DataHolder<T> {
        DataHolder{data: Some(value), next, prev: None}
    }

    fn empty() -> DataHolder<T> {
        DataHolder{data: None, next: None, prev: None}
    }
    
    fn set(&mut self, value: T) {
        self.data = Some(value);
    }

    fn unset(&mut self) {
        self.data = None;
    }
}

pub struct TimeSeriesData<T: Copy, const CAPACITY: usize> {
    source: Box<dyn DatedSource<T>>,
    data_folder_path: String,
    max_active_items: usize,
    active_items: usize,
    data: [Option<DataHolder<T>>; CAPACITY],
    modified: HashSet<usize>,
    head: Option<usize>,
    tail: Option<usize>
}

fn get_index(index_calculator: &Box<dyn IndexCalculator>, date: u32) -> Result<usize, Error> {
    let idx = index_calculator.calculate_index(date);
    if idx < 0 {
        Err(Error::new(ErrorKind::InvalidInput, "wrong date"))
    } else {
        Ok(idx as usize)
    }
}

impl<T: Copy, const CAPACITY: usize> TimeSeriesData<T, CAPACITY> {
    pub fn load<const N: usize>(data_folder_path: String, source: Box<dyn DatedSource<T>>,
                index_calculator: &Box<dyn IndexCalculator>, max_active_items: usize)
        -> Result<TimeSeriesData<T, N>, Error> {
        let mut file_map = HashMap::new();
        for file in get_file_list(data_folder_path.clone())? {
            let date = source.parse_date(&file)?;
            let key = get_index(index_calculator, date)?;
            file_map.entry(key).or_insert(Vec::new())
                .push(FileWithDate { name: file.name, date });
        }
        let mut data = TimeSeriesData{source, data_folder_path, max_active_items, active_items: 0,
            data: [None; N], modified: HashSet::new(), head: None, tail: None};
        for (key, files) in file_map {
            data.load_files(key, files)?;
        }
        Ok(data)
    }

    pub fn init<const N: usize>(data_folder_path: String, mut source: Box<dyn DatedSource<T>>,
                index_calculator: &Box<dyn IndexCalculator>, max_active_items: usize)
        -> Result<TimeSeriesData<T, N>, Error> {
        let mut data = [None; N];
        for file in get_file_list(data_folder_path.clone())? {
            let date = source.parse_date(&file)?;
            let key = get_index(index_calculator, date)?;
            data[key] = Some(DataHolder::empty());
        }
        Ok(TimeSeriesData{source, data_folder_path, max_active_items, active_items: 0, data,
            modified: HashSet::new(), head: None, tail: None})
    }

    fn load_files(&mut self, key: usize, files: Vec<FileWithDate>) -> Result<(), Error> {
        let v = self.source.load(files)?;
        self.add(key, v, false)
    }
    
    pub fn add(&mut self, key: usize, v: T, add_to_modified: bool) -> Result<(), Error> {
        self.cleanup()?;
        let h = self.add_to_lru(key, v);
        self.data.insert(key, Some(h));
        if add_to_modified {
            self.modified.insert(key);
        }
        self.active_items += 1;
        Ok(())
    }
    
    fn add_to_lru(&mut self, key: usize, v: T) -> DataHolder<T> {
        let h = DataHolder::new(v, self.head);
        if let Some(idx) = self.head {
            self.data[idx].unwrap().prev = Some(key); 
        }
        self.head = Some(key);
        h
    }
    
    fn cleanup(&mut self) -> Result<(), Error> {
        while self.active_items >= self.max_active_items {
            self.remove_by_lru()?;
        }
        Ok(())
    }
    
    fn remove_by_lru(&mut self) -> Result<(), Error> {
        if let Some(idx) = self.tail {
            let mut t = self.data[idx].as_mut().unwrap();
            if self.modified.contains(&idx) {
                self.source.save(&t.data.unwrap(), &self.data_folder_path, idx)?;
                self.modified.remove(&idx);
            }
            self.detach(idx, t);
        }
        Ok(())
    }

    fn detach(&mut self, idx: usize, t: &mut DataHolder<T>) {
        t.data = None;
        self.active_items -= 1;
        if idx == self.head {
            
        }
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
    pub fn convert_folder_name_to_number(&self) -> Result<u32, Error> {
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
