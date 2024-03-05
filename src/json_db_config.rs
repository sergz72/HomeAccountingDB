use std::io::Error;
use crate::core::data_source::{DataSource, JsonDataSource};
use crate::core::time_series_data::{DatedSource, FileInfo, FileWithDate};
use crate::db::DBConfiguration;
use crate::entities::accounts::Account;
use crate::entities::finance_operations::{FinanceOperation, FinanceRecord};
use crate::entities::subcategories::{Category, Subcategory};

pub struct JsonDBConfiguration {
}

impl JsonDBConfiguration {
    pub fn new() -> JsonDBConfiguration {
        JsonDBConfiguration{}
    }
}
impl DBConfiguration for JsonDBConfiguration {
    fn get_accounts_source(&self) -> Box<dyn DataSource<Vec<Account>>> {
        Box::new(JsonDataSource{})
    }

    fn get_categories_source(&self) -> Box<dyn DataSource<Vec<Category>>> {
        Box::new(JsonDataSource{})
    }

    fn get_subcategories_source(&self) -> Box<dyn DataSource<Vec<Subcategory>>> {
        Box::new(JsonDataSource{})
    }

    fn get_main_data_source(&self) -> Box<dyn DatedSource<FinanceRecord>> {
        Box::new(JsonDatedSource{})
    }
}

struct JsonDatedSource {
}

impl DatedSource<FinanceRecord> for JsonDatedSource {
    fn load(&mut self, files: Vec<FileWithDate>) -> Result<FinanceRecord, Error> {
        let mut operations = Vec::new();
        for file in files {
            let mut ops: Vec<FinanceOperation> = JsonDataSource{}.load(file.name, false)?;
            ops.iter_mut().for_each(|op|op.date = file.date);
            operations.append(&mut ops);
        }
        Ok(FinanceRecord::new(operations))
    }

    fn parse_date(&self, info: &FileInfo) -> Result<u32, Error> {
        info.convert_folder_name_to_number()
    }

    fn save(&self, data: &FinanceRecord, data_folder_path: &String, date: u32) -> Result<(), Error> {
        todo!()
    }

    fn get_files(&self, data_folder_path: &String, date: u32) -> Result<Vec<FileWithDate>, Error> {
        todo!()
    }
}