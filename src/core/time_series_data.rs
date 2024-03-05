use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::io::{Error, ErrorKind};
use std::num::ParseIntError;
use std::ops::Add;
use std::sync::Arc;

pub struct FileWithDate {
    pub name: String,
    pub date: u32
}

pub trait DatedSource<T> {
    fn load(&mut self, files: Vec<FileWithDate>) -> Result<T, Error>;
    fn parse_date(&self, info: &FileInfo) -> Result<u32, Error>;
    fn save(&self, data: &T, data_folder_path: &String, date: u32) -> Result<(), Error>;
    fn get_files(&self, data_folder_path: &String, date: u32) -> Result<Vec<FileWithDate>, Error>;
}

struct DataHolder<T> {
    data: Option<T>,
    key:  u32,
    prev: Option<Arc<DataHolder<T>>>,
    next: Option<Arc<DataHolder<T>>>
}

impl<T> DataHolder<T> {
    fn new(key: u32, value: T, next: Option<Arc<DataHolder<T>>>) -> DataHolder<T> {
        DataHolder{key, data: Some(value), next, prev: None}
    }

    fn empty(key: u32) -> DataHolder<T> {
        DataHolder{key, data: None, next: None, prev: None}
    }
    
    fn set(&mut self, value: T) {
        self.data = Some(value);
    }

    fn unset(&mut self) {
        self.data = None;
    }
}

pub struct TimeSeriesData<T> {
    source: Box<dyn DatedSource<T>>,
    data_folder_path: String,
    max_active_items: usize,
    active_items: usize,
    map: BTreeMap<u32, Arc<DataHolder<T>>>,
    modified: HashSet<u32>,
    head: Option<Arc<DataHolder<T>>>,
    tail: Option<Arc<DataHolder<T>>>
}

impl<'a, T> TimeSeriesData<T> {
    pub fn load(data_folder_path: String, source: Box<dyn DatedSource<T>>,
                index_calculator: fn(u32) -> u32, max_active_items: usize)
        -> Result<TimeSeriesData<T>, Error> {
        let mut file_map = HashMap::new();
        for file in get_file_list(data_folder_path.clone())? {
            let date = source.parse_date(&file)?;
            let key = index_calculator(date);
            file_map.entry(key).or_insert(Vec::new())
                .push(FileWithDate { name: file.name, date });
        }
        let mut data = TimeSeriesData{source, data_folder_path, max_active_items, active_items: 0,
            map: BTreeMap::new(), modified: HashSet::new(), head: None, tail: None};
        for (key, files) in file_map {
            data.load_files(key, files)?;
        }
        Ok(data)
    }

    pub fn init(data_folder_path: String, source: Box<dyn DatedSource<T>>,
                index_calculator: fn(u32) -> u32, max_active_items: usize)
        -> Result<TimeSeriesData<T>, Error> {
        let mut map = BTreeMap::new();
        for file in get_file_list(data_folder_path.clone())? {
            let date = source.parse_date(&file)?;
            let key = index_calculator(date);
            map.insert(key, Arc::new(DataHolder::empty(key)));
        }
        Ok(TimeSeriesData{source, data_folder_path, max_active_items, active_items: 0, map,
            modified: HashSet::new(), head: None, tail: None})
    }

    fn load_files(&mut self, key: u32, files: Vec<FileWithDate>) -> Result<(), Error> {
        let v = self.source.load(files)?;
        self.add(key, v, false)
    }
    
    pub fn add(&mut self, key: u32, v: T, add_to_modified: bool) -> Result<(), Error> {
        self.cleanup()?;
        let h = self.add_to_lru(key, v);
        self.map.insert(key, h);
        if add_to_modified {
            self.modified.insert(key);
        }
        self.active_items += 1;
        Ok(())
    }
    
    fn add_to_lru(&mut self, key: u32, v: T) -> Arc<DataHolder<T>> {
        let mut h = Arc::new(DataHolder::new(key, v, self.head.clone()));
        let a = Some(h.clone()); 
        if let Some(hh) = &self.head {
            hh.prev = a.clone();
        }
        self.head = a;
        h
    }
    
    fn cleanup(&mut self) -> Result<(), Error> {
        while self.active_items >= self.max_active_items {
            self.remove_by_lru()?;
        }
        Ok(())
    }
    
    fn remove_by_lru(&mut self) -> Result<(), Error> {
        if let Some(h) = &self.tail {
            if self.modified.contains(&h.key) {
                self.source.save(h.data.as_ref().unwrap(), &self.data_folder_path, h.key)?;
                self.modified.remove(&h.key);
            }
            self.detach(h.clone());
        }
        Ok(())
    }

    fn detach(&mut self, t: Arc<DataHolder<T>>) {
        t.data = None;
        self.active_items -= 1;
        if let Some(next) = &t.next {
            next.prev = t.prev.clone();
        } else {
            self.tail = t.prev.clone();
        }
        if let Some(prev) = &t.prev {
            prev.next = t.next.clone();
        } else {
            self.head = t.next.clone();
        }
    }
    
    pub fn get(&mut self, mut idx: u32) -> Result<Option<&T>, Error> {
        if let Some((real_idx, h)) = self.map.range_mut(..=idx).last() {
            let v = self.get_t(*real_idx, h.clone())?;
            Ok(Some(v))
        } else {
            Ok(None)
        }
    }
    
    pub fn get_range(&mut self, from: u32, to: u32) -> Result<Vec<(u32, &mut T)>, Error> {
        let mut result = Vec::new();
        for (k, v) in self.map.range(from..=to) {
            let t = self.get_t(*k, v.clone())?;
            result.push((*k, t));
        }
        Ok(result)
    }

    fn move_to_front(&mut self, v: Arc<DataHolder<T>>) {
        todo!()
    }
    
    fn get_t(&mut self, key: u32, v: Arc<DataHolder<T>>) -> Result<&mut T, Error> {
        if let Some(d) = &mut v.data {
            self.move_to_front(v);
            return Ok(d);
        }
        let files = self.source.get_files(&self.data_folder_path, key)?;
        let t = self.source.load(files)?;
        let h = self.add_to_lru(key, t);
        Ok(h.data.as_mut().unwrap())
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
