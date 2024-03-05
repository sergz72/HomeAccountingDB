use std::io::{Error, ErrorKind};
use std::ops::Add;
use std::time::Instant;
use crate::core::data_source::DataSource;
use crate::core::time_series_data::{DatedSource, TimeSeriesData};
use crate::entities::accounts::{Account, Accounts};
use crate::entities::finance_operations::{FinanceChanges, FinanceOperation, FinanceRecord};
use crate::entities::subcategories::{Categories, Category, Subcategories, Subcategory};

pub trait DBConfiguration {
    fn get_accounts_source(&self) ->  Box<dyn DataSource<Vec<Account>>>;
    fn get_categories_source(&self) ->  Box<dyn DataSource<Vec<Category>>>;
    fn get_subcategories_source(&self) ->  Box<dyn DataSource<Vec<Subcategory>>>;
    fn get_main_data_source(&self) -> Box<dyn DatedSource<FinanceRecord>>;
}

pub struct HomeAccountingDB {
    data: TimeSeriesData<FinanceRecord>,
    accounts: Accounts,
    categories: Categories,
    subcategories: Subcategories
}

fn index_calculator(date: u32) -> u32 {date / 100}

impl HomeAccountingDB {
    pub fn load(data_folder_path: String, data_source: Box<dyn DBConfiguration>, max_active_items: usize)
        -> Result<HomeAccountingDB, Error> {
        let start = Instant::now();
        let data =
            TimeSeriesData::load(data_folder_path.clone().add("/dates"), data_source.get_main_data_source(),
                                 index_calculator, max_active_items)?;
        let accounts = Accounts::load(data_folder_path.clone(), data_source.get_accounts_source())?;
        let categories = Categories::load(data_folder_path.clone(), data_source.get_categories_source())?;
        let subcategories = Subcategories::load(data_folder_path, data_source.get_subcategories_source())?;
        let mut db = HomeAccountingDB{data, accounts, categories, subcategories};
        println!("Database loaded in {} ms", start.elapsed().as_millis());
        let start = Instant::now();
        db.build_totals(0)?;
        println!("Totals calculation finished in {} us", start.elapsed().as_micros());
        Ok(db)
    }

    fn build_totals(&mut self, from: u32) -> Result<(), Error> {
        let mut changes: Option<FinanceChanges> = None;
        let idx = index_calculator(from);
        for (_, v) in self.data.get_range(idx, 99999999)? {
            if let Some(c) = &changes {
                v.totals = c.build_totals();
            }
            changes = Some(v.build_changes(&self.accounts, &self.subcategories)?);
        }
        Ok(())
    }

    fn build_ops_and_changes(&mut self, date: u32) -> Result<(Vec<&FinanceOperation>, FinanceChanges), Error> {
        let idx = index_calculator(date);
        if let Some(record) = self.data.get(idx)? {
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

    pub fn test(&mut self, date_str: String) -> Result<(), Error> {
        let d: u32 = date_str.parse()
            .map_err(|_|Error::new(ErrorKind::InvalidInput, "invalid date"))?;
        let (_, changes) = self.build_ops_and_changes(d)?;
        println!("{}", d);
        changes.print(&self.accounts)
    }

    pub fn migrate(&self, dest_folder: String) -> Result<(), Error> {
        todo!()
    }
}
