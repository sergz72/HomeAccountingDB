use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use std::ops::Add;
use serde::{Deserialize, Deserializer};
use serde::de::Unexpected;
use crate::core::data_source::DataSource;

#[derive(Clone)]
pub enum SubcategoryCode {
    Comb,
    Comc,
    Fuel,
    Prcn,
    Incc,
    Expc,
    Exch,
    Trfr,
    None
}

#[derive(Clone)]
pub enum SubcategoryOperationCode {
    Incm,
    Expn,
    Spcl
}

#[derive(Deserialize)]
pub struct Subcategory {
    pub id: u64,
    pub name: String,
    #[serde(deserialize_with = "code_deserialize")]
    pub code: SubcategoryCode,
    #[serde(rename = "operationCodeId", deserialize_with = "operation_code_deserialize")]
    pub operation_code: SubcategoryOperationCode,
    #[serde(rename = "categoryId")]
    pub category: u64
}

fn code_deserialize<'de, D>(deserializer: D) -> Result<SubcategoryCode, D::Error>
    where
        D: Deserializer<'de>,
{
    let v: Option<String> = Deserialize::deserialize(deserializer)?;
    if v.is_none() {
        return Ok(SubcategoryCode::None);
    }
    let s = v.unwrap();
    match s.as_str() {
        "COMB" => Ok(SubcategoryCode::Comb),
        "COMC" => Ok(SubcategoryCode::Comc),
        "FUEL" => Ok(SubcategoryCode::Fuel),
        "PRCN" => Ok(SubcategoryCode::Prcn),
        "INCC" => Ok(SubcategoryCode::Incc),
        "EXPC" => Ok(SubcategoryCode::Expc),
        "EXCH" => Ok(SubcategoryCode::Exch),
        "TRFR" => Ok(SubcategoryCode::Trfr),
        _ => Err(serde::de::Error::invalid_value(Unexpected::Str(s.as_str()), &"subcategory code"))
    }
}

fn operation_code_deserialize<'de, D>(deserializer: D) -> Result<SubcategoryOperationCode, D::Error>
    where
        D: Deserializer<'de>,
{
    let v: String = Deserialize::deserialize(deserializer)?;
    match v.as_str() {
        "INCM" => Ok(SubcategoryOperationCode::Incm),
        "EXPN" => Ok(SubcategoryOperationCode::Expn),
        "SPCL" => Ok(SubcategoryOperationCode::Spcl),
        _ => Err(serde::de::Error::invalid_value(Unexpected::Str(v.as_str()), &"subcategory operation code"))
    }
}

#[derive(Deserialize)]
pub struct Category {
    pub id: u64,
    pub name: String
}


pub struct Subcategories {
    map: HashMap<u64, Subcategory>
}

impl Subcategories {
    pub fn load(data_folder_path: String, source: Box<dyn DataSource<Vec<Subcategory>>>)
        -> Result<Subcategories, Error> {
        let subcategories = source.load(data_folder_path.add("/subcategories"), true)?;
        let map = subcategories.into_iter().map(|c|(c.id, c)).collect();
        Ok(Subcategories{map})
    }

    pub fn get(&self, id: u64) -> Result<&Subcategory, Error> {
        self.map.get(&id).ok_or(Error::new(ErrorKind::InvalidData, "invalid subcategory id"))
    }
}

pub struct Categories {
    map: HashMap<u64, Category>
}

impl Categories {
    pub fn load(data_folder_path: String, source: Box<dyn DataSource<Vec<Category>>>)
               -> Result<Categories, Error> {
        let categories = source.load(data_folder_path.add("/categories"), true)?;
        let map = categories.into_iter().map(|c|(c.id, c)).collect();
        Ok(Categories {map})
    }

    pub fn get(&self, id: u64) -> Result<&Category, Error> {
        self.map.get(&id).ok_or(Error::new(ErrorKind::InvalidData, "invalid category id"))
    }
}
