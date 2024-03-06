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
        Ok(())
    }
    
    fn add_to_lru(&self, key: u64, v: T) -> Mutex<DataHolder<T>> {
        let h = Mutex::new(DataHolder::new(key, v, self.head.lock().unwrap().clone()));
        self.attach(key);
        h
    }
    
    fn attach(&self, key: u64) {
        if let Some(hh) = self.head.lock().unwrap().as_ref() {
            self.map.get(hh).unwrap().lock().unwrap().prev = Some(key);
        } else {
            _ = self.tail.lock().unwrap().insert(key);
        }
        _ = self.head.lock().unwrap().insert(key);
        self.active_items.fetch_add(1, Ordering::Relaxed);
    }
    
    fn cleanup(&self) -> Result<(), Error> {
        while self.active_items.load(Ordering::Relaxed) >= self.max_active_items {
            self.remove_by_lru()?;
        }
        Ok(())
    }
    
    fn remove_by_lru(&self) -> Result<(), Error> {
        let lock = self.tail.lock().unwrap();
        if let Some(h) = lock.as_ref() {
            let mut l = self.modified.lock().unwrap(); 
            if l.contains(h) {
                self.source.lock().unwrap().save(self.map.get(&h).unwrap().lock().unwrap().data.as_ref().unwrap().lock().unwrap().deref(),
                                                 &self.data_folder_path, *h)?;
                l.remove(h);
            }
            self.detach(*h, lock);
        }
        Ok(())
    }

    fn detach(&self, idx: u64, mut l: MutexGuard<Option<u64>>) {
        let mut data = self.map.get(&idx).unwrap().lock().unwrap();
        data.data = None;
        self.active_items.fetch_sub(1, Ordering::Relaxed);
        if let Some(next) = data.next {
            self.map.get(&next).unwrap().lock().unwrap().prev = data.prev;
        } else {
            *l = data.prev;
        }
        if let Some(prev) = data.prev {
            self.map.get(&prev).unwrap().lock().unwrap().next = data.next;
        } else {
            *self.head.lock().unwrap() = data.next;
        }
    }
    
    pub fn get(&self, idx: u64) -> Result<Option<Rc<Mutex<T>>>, Error> {
        if let Some((real_idx, d)) = self.map.range(..=idx).last() {
            let v = self.get_t(*real_idx, d)?;
            Ok(Some(v))
        } else {
            Ok(None)
        }
    }
    
    pub fn get_range(&self, from: u64, to: u64) -> Result<Vec<(u64, Rc<Mutex<T>>)>, Error> {
        let mut result = Vec::new();
        for (k, d) in self.map.range(from..=to) {
            let t = self.get_t(*k, d)?;
            result.push((*k, t));
        }
        Ok(result)
    }

    fn move_to_front(&self, idx: u64) {
        self.detach(idx, self.tail.lock().unwrap());
        let mut head = self.head.lock().unwrap();
        let head_idx = head.clone();
        let mut v = self.map.get(&idx).unwrap().lock().unwrap();
        v.next = head_idx;
        v.prev = None;
        let _ = head.insert(idx);
        let _ = self.map.get(&head_idx.unwrap()).unwrap().lock().unwrap().prev.insert(idx);
    }
    
    fn get_t(&self, key: u64, d: &Mutex<DataHolder<T>>) -> Result<Rc<Mutex<T>>, Error> {
        let mut v = d.lock().unwrap();
        if let Some(d) = v.data.clone() {
            drop(v);
            self.move_to_front(key);
            return Ok(d);
        }
        self.cleanup()?;
        let mut l = self.source.lock().unwrap();
        let files = l.get_files(&self.data_folder_path, key)?;
        let t = l.load(files)?;
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

#[cfg(test)]
mod tests {
    use std::io::Error;
    use crate::core::time_series_data::{DatedSource, FileInfo, FileWithDate, TimeSeriesData};

    struct TestData{}
    struct TestDataSource{}

    impl DatedSource<TestData> for TestDataSource {
        fn load(&mut self, files: Vec<FileWithDate>) -> Result<TestData, Error> {
            Ok(TestData{})
        }

        fn parse_date(&self, info: &FileInfo) -> Result<u64, Error> {
            todo!()
        }

        fn save(&self, data: &TestData, data_folder_path: &String, date: u64) -> Result<(), Error> {
            todo!()
        }

        fn get_files(&self, data_folder_path: &String, date: u64) -> Result<Vec<FileWithDate>, Error> {
            Ok(Vec::new())
        }
    }

    #[test]
    fn test_lru_list() -> Result<(), Error> {
        let mut data = TimeSeriesData::new("".to_string(), Box::new(TestDataSource{}), 500);
        for i in 0..3 {
            data.add(i, TestData{}, false)?;
        }
        let head = data.head.lock().unwrap().unwrap();
        assert_eq!(head, 2);
        assert_eq!(data.tail.lock().unwrap().unwrap(), 0);
        let item = data.map.get(&head).unwrap().lock().unwrap();
        assert_eq!(item.prev, None);
        assert_eq!(item.next, Some(1));
        let item = data.map.get(&1).unwrap().lock().unwrap();
        assert_eq!(item.prev, Some(2));
        assert_eq!(item.next, Some(0));
        let item = data.map.get(&0).unwrap().lock().unwrap();
        assert_eq!(item.prev, Some(1));
        assert_eq!(item.next, None);
        Ok(())
    }

    #[test]
    fn test_lru_expire_and_move_to_front() -> Result<(), Error> {
        let mut data = TimeSeriesData::new("".to_string(), Box::new(TestDataSource{}), 500);
        for i in 0..1000 {
            data.add(i, TestData{}, false)?;
        }
        let head = data.head.lock().unwrap().unwrap();
        assert_eq!(head, 999);
        assert_eq!(data.tail.lock().unwrap().unwrap(), 500);
        assert_eq!(data.get_active_items(), 500);

        let item = data.map.get(&500).unwrap().lock().unwrap();
        assert_eq!(item.prev, Some(501));
        assert_eq!(item.next, None);
        drop(item);
        let item = data.map.get(&999).unwrap().lock().unwrap();
        assert_eq!(item.prev, None);
        assert_eq!(item.next, Some(998));
        drop(item);
        
        let _ = data.get(501)?;

        let head = data.head.lock().unwrap().unwrap();
        assert_eq!(head, 501);
        assert_eq!(data.tail.lock().unwrap().unwrap(), 500);
        let item = data.map.get(&head).unwrap().lock().unwrap();
        assert_eq!(item.prev, None);
        assert_eq!(item.next, Some(999));
        let item = data.map.get(&998).unwrap().lock().unwrap();
        assert_eq!(item.prev, Some(999));
        assert_eq!(item.next, Some(997));

        let item = data.map.get(&500).unwrap().lock().unwrap();
        assert_eq!(item.prev, Some(502));
        assert_eq!(item.next, None);
        let item = data.map.get(&502).unwrap().lock().unwrap();
        assert_eq!(item.prev, Some(503));
        assert_eq!(item.next, Some(500));
        
        Ok(())
    }

    #[test]
    fn test_lru_load() -> Result<(), Error> {
        let mut data = TimeSeriesData::new("".to_string(), Box::new(TestDataSource {}), 500);
        for i in 0..1000 {
            data.add(i, TestData {}, false)?;
        }

        let _ = data.get(499)?;
        let head = data.head.lock().unwrap().unwrap();
        assert_eq!(head, 499);
        assert_eq!(data.tail.lock().unwrap().unwrap(), 501);
        assert_eq!(data.get_active_items(), 500);
        
        Ok(())
    }
}