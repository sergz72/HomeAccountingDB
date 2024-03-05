use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::io::{Error, ErrorKind};
use std::num::ParseIntError;
use std::ops::{Add, Deref};
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard};

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
    data: Option<Rc<Mutex<T>>>,
    key:  u64,
    prev: Option<u64>,
    next: Option<u64>
}

impl<T> DataHolder<T> {
    fn new(key: u64, value: T, next: Option<u64>) -> DataHolder<T> {
        DataHolder{key, data: Some(Rc::new(Mutex::new(value))), next, prev: None}
    }

    fn empty(key: u64) -> DataHolder<T> {
        DataHolder{key, data: None, next: None, prev: None}
    }
    
    fn set(&mut self, value: T, next: Option<u64>) {
        _ = self.data.insert(Rc::new(Mutex::new(value)));
        self.prev = None;
        self.next = next;
    }

    fn set_next(&mut self, next: Option<u64>) {
        self.prev = None;
        self.next = next;
    }
    
    fn unset(&mut self) {
        self.data.take();
    }
}

pub struct TimeSeriesData<T> {
    source: Mutex<Box<dyn DatedSource<T>>>,
    data_folder_path: String,
    max_active_items: usize,
    active_items: AtomicUsize,
    map: BTreeMap<u64, Mutex<DataHolder<T>>>,
    modified: Mutex<HashSet<u64>>,
    head: Mutex<Option<u64>>,
    tail: Mutex<Option<u64>>
}

impl<'a, T> TimeSeriesData<T> {
    pub fn load(data_folder_path: String, source: Box<dyn DatedSource<T>>,
                index_calculator: fn(u64) -> u64, max_active_items: usize)
        -> Result<TimeSeriesData<T>, Error> {
        let mut file_map = HashMap::new();
        for file in get_file_list(data_folder_path.clone())? {
            let date = source.parse_date(&file)?;
            let key = index_calculator(date);
            file_map.entry(key).or_insert(Vec::new())
                .push(FileWithDate { name: file.name, date });
        }
        let mut data = TimeSeriesData::new(data_folder_path, source, max_active_items);
        for (key, files) in file_map {
            data.load_files(key, files)?;
        }
        Ok(data)
    }
    
    pub fn new(data_folder_path: String, source: Box<dyn DatedSource<T>>, max_active_items: usize) -> TimeSeriesData<T> {
        TimeSeriesData{source: Mutex::new(source), data_folder_path, max_active_items,
            active_items: AtomicUsize::new(0), map: BTreeMap::new(), modified: Mutex::new(HashSet::new()),
            head: Mutex::new(None), tail: Mutex::new(None)}
    }

    pub fn init(data_folder_path: String, source: Box<dyn DatedSource<T>>,
                index_calculator: fn(u64) -> u64, max_active_items: usize)
        -> Result<TimeSeriesData<T>, Error> {
        let mut map = BTreeMap::new();
        for file in get_file_list(data_folder_path.clone())? {
            let date = source.parse_date(&file)?;
            let key = index_calculator(date);
            map.insert(key, Mutex::new(DataHolder::empty(key)));
        }
        Ok(TimeSeriesData{source: Mutex::new(source), data_folder_path, max_active_items,
            active_items: AtomicUsize::new(0), map, modified: Mutex::new(HashSet::new()),
            head: Mutex::new(None), tail: Mutex::new(None)})
    }

    fn load_files(&mut self, key: u64, files: Vec<FileWithDate>) -> Result<(), Error> {
        let v = self.source.lock().unwrap().load(files)?;
        self.add(key, v, false)
    }
    
    pub fn add(&mut self, key: u64, v: T, add_to_modified: bool) -> Result<(), Error> {
        self.cleanup()?;
        let h = self.add_to_lru(key, v);
        self.map.insert(key, h);
        if add_to_modified {
            self.modified.lock().unwrap().insert(key);
        }
        self.active_items.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
    
    fn add_to_lru(&self, key: u64, v: T) -> Mutex<DataHolder<T>> {
        let h = Mutex::new(DataHolder::new(key, v, self.head.lock().unwrap().clone()));
        self.attach(key);
        h
    }
    
    fn attach(&self, key: u64) {
        if let Some(hh) = self.head.lock().unwrap().as_ref() {
            self.map.get(hh).unwrap().lock().unwrap().next = Some(key);
        } else {
            _ = self.tail.lock().unwrap().insert(key);
        }
        _ = self.head.lock().unwrap().insert(key);
    }
    
    fn cleanup(&self) -> Result<(), Error> {
        while self.active_items.load(Ordering::Relaxed) >= self.max_active_items {
            self.remove_by_lru()?;
        }
        Ok(())
    }
    
    fn remove_by_lru(&self) -> Result<(), Error> {
        if let Some(h) = self.tail.lock().unwrap().as_ref() {
            let mut l = self.modified.lock().unwrap(); 
            if l.contains(h) {
                self.source.lock().unwrap().save(self.map.get(&h).unwrap().lock().unwrap().data.as_ref().unwrap().lock().unwrap().deref(), &self.data_folder_path, *h)?;
                l.remove(h);
            }
            self.detach(*h);
        }
        Ok(())
    }

    fn detach(&self, idx: u64) {
        let mut data = self.map.get(&idx).unwrap().lock().unwrap();
        data.data = None;
        self.active_items.fetch_sub(1, Ordering::Relaxed);
        if let Some(next) = data.next {
            self.map.get(&next).unwrap().lock().unwrap().prev = data.prev;
        } else {
            *self.tail.lock().unwrap() = data.prev;
        }
        if let Some(prev) = data.prev {
            self.map.get(&prev).unwrap().lock().unwrap().next = data.next;
        } else {
            *self.head.lock().unwrap() = data.next;
        }
    }
    
    pub fn get(&mut self, idx: u64) -> Result<Option<Rc<Mutex<T>>>, Error> {
        if let Some((real_idx, d)) = self.map.range(..=idx).last() {
            let v = self.get_t(*real_idx, d)?;
            Ok(Some(v))
        } else {
            Ok(None)
        }
    }
    
    pub fn get_range(&mut self, from: u64, to: u64) -> Result<Vec<(u64, Rc<Mutex<T>>)>, Error> {
        let mut result = Vec::new();
        for (k, d) in self.map.range(from..=to) {
            let t = self.get_t(*k, d)?;
            result.push((*k, t));
        }
        Ok(result)
    }

    fn move_to_front(&self, idx: u64, mut d: MutexGuard<DataHolder<T>>) {
        let mut l = self.head.lock().unwrap();
        let prev = l.clone();
        d.set_next(prev);
        let _ = l.insert(idx);
    }
    
    fn get_t(&self, key: u64, d: &Mutex<DataHolder<T>>) -> Result<Rc<Mutex<T>>, Error> {
        let v = d.lock().unwrap();
        if let Some(d) = v.data.clone() {
            self.move_to_front(key, v);
            return Ok(d);
        }
        let mut l = self.source.lock().unwrap();
        let files = l.get_files(&self.data_folder_path, key)?;
        let t = l.load(files)?;
        let mut v = self.map.get(&key).unwrap().lock().unwrap();
        v.set(t, self.head.lock().unwrap().clone());
        self.attach(key);
        Ok(v.data.as_ref().unwrap().clone())
    }
    
    pub fn get_active_items(&self) -> usize {
        self.active_items.load(Ordering::Relaxed)
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
