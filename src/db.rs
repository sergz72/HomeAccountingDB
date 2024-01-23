use std::collections::{BTreeMap, HashMap};
use std::io::{Error, ErrorKind};
use std::sync::Arc;
use crate::entities::accounts::Accounts;
use crate::entities::finance_operations::{FinanceOperation, FinanceRecord};
use crate::entities::subcategories::{load_categories, Subcategory};
use crate::time_series_data::{FileInfo, TimeSeriesData, TimeSeriesDataConfiguration};

pub struct HomeAccountingDB {
    data: TimeSeriesData<FinanceRecord>,
    accounts: Accounts,
    categories: HashMap<usize, String>,
    subcategories: HashMap<usize, Subcategory>
}

impl HomeAccountingDB {
    pub fn load(use_json: bool, data_folder_path: String) -> Result<HomeAccountingDB, Error> {
        let config: Arc<dyn TimeSeriesDataConfiguration<FinanceRecord>> =
            Arc::new(HomeAccountingConfiguration{use_json, data_folder_path});
        let data = TimeSeriesData::load(config.clone(), "/dates")?;
        let accounts = Accounts::load(config.clone())?;
        let categories = load_categories(config.clone())?;
        let subcategories = Subcategory::load(config)?;
        let mut db = HomeAccountingDB{data, accounts, categories, subcategories};
        db.build_totals(0)?;
        Ok(db)
    }

    fn build_totals(&mut self, from: usize) -> Result<(), Error> {
        let mut totals: Option<HashMap<usize, isize>> = None;
        for (_, v) in &mut self.data.map.range_mut(from..) {
            if let Some(t) = totals {
                v.totals = t.clone()
            }
            let changes = v.build_changes(&self.accounts, &self.subcategories)?;
            totals = Some(changes.into_iter()
                .map(|(account, changes)|(account, changes.get_end_balance())).collect());
        }
        Ok(())
    }

    pub fn test(&self, date_str: String) -> Result<(), Error> {
        let (date, record) = if date_str == "last" {
            let (d, r) = self.data.map.last_key_value()
                .ok_or(Error::new(ErrorKind::InvalidData, "db is empty"))?;
            (*d, r)
        } else {
            let d: usize = date_str.parse()
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

struct HomeAccountingConfiguration {
    use_json: bool,
    data_folder_path: String
}

impl TimeSeriesDataConfiguration<FinanceRecord> for HomeAccountingConfiguration {
    fn data_folder_path(&self) -> String {
        self.data_folder_path.clone()
    }

    fn load_file(&self, file_info: &FileInfo) -> Result<BTreeMap<usize, FinanceRecord>, Error> {
        if self.use_json {
            return load_finance_operations_from_json(file_info);
        }
        Ok(BTreeMap::new())
    }
}

fn load_finance_operations_from_json(file_info: &FileInfo) -> Result<BTreeMap<usize, FinanceRecord>, Error> {
    let mut result = BTreeMap::new();
    if !file_info.is_folder_empty() && file_info.is_json() {
        let date = file_info.convert_folder_name_to_number()?;
        let reader = file_info.load_file()?;
        let ops: Vec<FinanceOperation> = serde_json::from_reader(reader)?;
        result.insert(date, FinanceRecord{ operations: ops, totals: HashMap::new() });
    }
    Ok(result)
}
