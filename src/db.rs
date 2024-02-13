use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use std::ops::Add;
use std::time::Instant;
use crate::core::data_source::JsonDataSource;
use crate::core::time_series_data::TimeSeriesData;
use crate::entities::accounts::Accounts;
use crate::entities::finance_operations::{FinanceChanges, FinanceOperation, FinanceRecord};
use crate::entities::subcategories::{Categories, Subcategories};

pub struct HomeAccountingDB {
    data: TimeSeriesData<FinanceRecord, Vec<FinanceOperation>>,
    accounts: Accounts,
    categories: Categories,
    subcategories: Subcategories
}

impl HomeAccountingDB {
    pub fn load(use_json: bool, data_folder_path: String, aes_key: [u8; 32]) -> Result<HomeAccountingDB, Error> {
        let finance_source = Box::new(JsonDataSource{});
        let accounts_source = Box::new(JsonDataSource{});
        let subcategories_source = Box::new(JsonDataSource{});
        let categories_source = Box::new(JsonDataSource{});
        let start = Instant::now();
        let data =
            TimeSeriesData::load(data_folder_path.clone().add("/dates"), finance_source,
                                 |fi|fi.convert_folder_name_to_number().map(|v|v/100),
            ||FinanceRecord::new())?;
        let accounts = Accounts::load(data_folder_path.clone(), accounts_source)?;
        let categories = Categories::load(data_folder_path.clone(), categories_source)?;
        let subcategories = Subcategories::load(data_folder_path, subcategories_source)?;
        let mut db = HomeAccountingDB{data, accounts, categories, subcategories};
        println!("Database loaded in {} ms", start.elapsed().as_millis());
        let start = Instant::now();
        db.build_totals(0)?;
        println!("Totals calculation finished in {} us", start.elapsed().as_micros());
        Ok(db)
    }

    fn build_totals(&mut self, from: u64) -> Result<(), Error> {
        let mut changes: Option<HashMap<u64, FinanceChanges>> = None;
        for (_, v) in &mut self.data.map.range_mut(from..) {
            if let Some(c) = &changes {
                v.totals = c.iter()
                    .map(|(account, changes)|(*account, changes.get_end_balance())).collect();
            } else {
                changes = Some(v.create_changes());
            }
            v.update_changes(changes.as_mut().unwrap(), &self.accounts, &self.subcategories)?;
        }
        Ok(())
    }

    pub fn test(&self, date_str: String) -> Result<(), Error> {
        let (date, record) = if date_str == "last" {
            let (d, r) = self.data.map.last_key_value()
                .ok_or(Error::new(ErrorKind::InvalidData, "db is empty"))?;
            (*d, r)
        } else {
            let d: u64 = date_str.parse()
                .map_err(|_|Error::new(ErrorKind::InvalidInput, "invalid date"))?;
            let r = self.data.map.get(&d)
                .ok_or(Error::new(ErrorKind::InvalidInput, "no operations for this date"))?;
            (d, r)
        };
        println!("{}", date);
        record.print_changes(&self.accounts, &self.subcategories)
    }

    pub fn migrate(&self, dest_folder: String) -> Result<(), Error> {
        todo!()
    }
}
