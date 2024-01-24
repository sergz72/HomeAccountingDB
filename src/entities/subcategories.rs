use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Error};
use std::ops::Add;
use std::sync::Arc;
use serde::Deserialize;
use crate::time_series_data::TimeSeriesDataConfiguration;

#[derive(Deserialize)]
pub struct Subcategory {
    pub id: u64,
    pub name: String,
    pub code: Option<String>,
    #[serde(rename = "operationCodeId")]
    pub operation_code: String,
    #[serde(rename = "categoryId")]
    pub category: u64
}

#[derive(Deserialize)]
struct Category {
    pub id: u64,
    pub name: String
}

impl Subcategory {
    pub fn load<T>(config: Arc<dyn TimeSeriesDataConfiguration<T>>) -> Result<HashMap<u64, Subcategory>, Error> {
        let file = File::open(config.data_folder_path().add("/subcategories.json"))?;
        let reader = BufReader::new(file);
        let subcategories: Vec<Subcategory> = serde_json::from_reader(reader)?;
        let result = subcategories.into_iter().map(|c|(c.id, c)).collect();
        Ok(result)
    }
}

pub fn load_categories<T>(config: Arc<dyn TimeSeriesDataConfiguration<T>>) -> Result<HashMap<u64, String>, Error> {
    let file = File::open(config.data_folder_path().add("/categories.json"))?;
    let reader = BufReader::new(file);
    let categories: Vec<Category> = serde_json::from_reader(reader)?;
    let result = categories.into_iter().map(|c|(c.id, c.name)).collect();
    Ok(result)
}