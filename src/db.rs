use std::collections::{BTreeMap, HashMap};
use std::io::{Error, ErrorKind};
use std::ops::Range;
use std::sync::Arc;
use std::time::Instant;
use crate::entities::accounts::Accounts;
use crate::entities::finance_operations::{FinanceChanges, FinanceOperation, FinanceRecord};
use crate::entities::subcategories::{load_categories, Subcategory};
use crate::time_series_data::{FileInfo, TimeSeriesData, TimeSeriesDataConfiguration};

pub struct HomeAccountingDB {
    data: TimeSeriesData<FinanceRecord>,
    accounts: Accounts,
    categories: HashMap<u64, String>,
    subcategories: HashMap<u64, Subcategory>
}

impl HomeAccountingDB {
    pub fn load(use_json: bool, data_folder_path: String, aes_key: [u8; 32]) -> Result<HomeAccountingDB, Error> {
        let config: Arc<dyn TimeSeriesDataConfiguration<FinanceRecord>> =
            Arc::new(HomeAccountingConfiguration{use_json, data_folder_path, aes_key});
        let start = Instant::now();
        let data = TimeSeriesData::load(config.clone(), "/dates".to_string())?;
        let accounts = Accounts::load(config.clone())?;
        let categories = load_categories(config.clone())?;
        let subcategories = Subcategory::load(config)?;
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

struct HomeAccountingConfiguration {
    use_json: bool,
    data_folder_path: String,
    aes_key: [u8; 32]
}

impl TimeSeriesDataConfiguration<FinanceRecord> for HomeAccountingConfiguration {
    fn data_folder_path(&self) -> String {
        self.data_folder_path.clone()
    }

    fn load_file(&self, file_info: &FileInfo) -> Result<BTreeMap<u64, FinanceRecord>, Error> {
        if self.use_json {
            return load_finance_operations_from_json(file_info);
        }
        Ok(BTreeMap::new())
    }

    fn get_file_names(&self, from: u64, to: u64) -> HashMap<String, Range<u64>> {
        (from..=to).step_by(100).map(|v| {
            let v100 = v / 100;
            let name = format!("financeOperations{}.bin", v100);
            (name, v100*100..(v100+1)*100)
        }).collect()
    }

    fn save_file(&self, file_name: String, data: Vec<u8>) -> Result<(), Error> {
        Ok(())
    }
}

fn load_finance_operations_from_json(file_info: &FileInfo) -> Result<BTreeMap<u64, FinanceRecord>, Error> {
    let mut result = BTreeMap::new();
    if !file_info.is_folder_empty() && file_info.is_json() {
        let date = file_info.convert_folder_name_to_number()?;
        let reader = file_info.load_file()?;
        let ops: Vec<FinanceOperation> = serde_json::from_reader(reader)?;
        result.insert(date, FinanceRecord{ operations: ops, totals: HashMap::new() });
    }
    Ok(result)
}
