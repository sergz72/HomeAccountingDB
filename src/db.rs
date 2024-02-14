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

fn index_calculator(date: u64) -> u64 {date / 100}

impl HomeAccountingDB {
    pub fn load(use_json: bool, data_folder_path: String, aes_key: [u8; 32]) -> Result<HomeAccountingDB, Error> {
        let finance_source = Box::new(JsonDataSource{});
        let accounts_source = Box::new(JsonDataSource{});
        let subcategories_source = Box::new(JsonDataSource{});
        let categories_source = Box::new(JsonDataSource{});
        let start = Instant::now();
        let data =
            TimeSeriesData::load(data_folder_path.clone().add("/dates"), finance_source,
                                 index_calculator,
                                 |fi|fi.convert_folder_name_to_number().map(|v|(v, true)),
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
        let mut changes: Option<FinanceChanges> = None;
        let idx = index_calculator(from);
        for (_, v) in &mut self.data.map.range_mut(idx..) {
            if let Some(c) = &changes {
                v.totals = c.build_totals();
            }
            changes = Some(v.build_changes(&self.accounts, &self.subcategories)?);
        }
        Ok(())
    }

    fn build_ops_and_changes(&self, date: u64) -> Result<(Vec<&FinanceOperation>, FinanceChanges), Error> {
        let idx = index_calculator(date);
        if let Some((_, record)) = self.data.map.range(..=idx).last() {
            let mut changes = record.create_changes();
            record.update_changes(&mut changes, 0, date - 1, &self.accounts, &self.subcategories)?;
            let totals = changes.build_totals();
            let mut changes = FinanceChanges::new(&totals);
            record.update_changes(&mut changes, date, date, &self.accounts, &self.subcategories)?;
            Ok((record.get_ops(date), changes))
        } else {
            Ok((Vec::new(), FinanceChanges::empty()))
        }
    }

    pub fn test(&self, date_str: String) -> Result<(), Error> {
        let d: u64 = date_str.parse()
            .map_err(|_|Error::new(ErrorKind::InvalidInput, "invalid date"))?;
        let (_, changes) = self.build_ops_and_changes(d)?;
        println!("{}", d);
        changes.print(&self.accounts, &self.subcategories)
    }

    pub fn migrate(&self, dest_folder: String) -> Result<(), Error> {
        todo!()
    }
}
