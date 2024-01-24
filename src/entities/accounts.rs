use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Error, ErrorKind};
use std::ops::Add;
use std::sync::Arc;
use serde::Deserialize;
use crate::time_series_data::TimeSeriesDataConfiguration;

pub struct Accounts {
    map: HashMap<u64, Account>,
    cash_accounts: HashMap<String, u64>
}

impl Accounts {
    pub fn load<T>(config: Arc<dyn TimeSeriesDataConfiguration<T>>) -> Result<Accounts, Error> {
        let file = File::open(config.data_folder_path().add("/accounts.json"))?;
        let reader = BufReader::new(file);
        let accounts: Vec<Account> = serde_json::from_reader(reader)?;
        let map = accounts.iter().map(|c|(c.id, c.clone())).collect();
        let cash_accounts = accounts.into_iter().filter(|a|a.is_cash)
            .map(|a|(a.currency, a.id)).collect();
        Ok(Accounts{map, cash_accounts})
    }

    pub fn get_cash_account(&self, account: u64) -> Result<u64, Error> {
        match self.map.get(&account) {
            Some(a) => {
                match self.cash_accounts.get(&a.currency) {
                    Some(id) => Ok(*id),
                    None => Err(Error::new(ErrorKind::InvalidData, "no cash account found"))
                }
            },
            None => Err(Error::new(ErrorKind::InvalidData, "invalid account id"))
        }
    }

    pub fn get_name(&self, id: u64) -> Result<String, Error> {
        self.map.get(&id).ok_or(Error::new(ErrorKind::InvalidData, "invalid account id"))
            .map(|a|a.name.clone())
    }
}

#[derive(Deserialize, Clone)]
pub struct Account {
    id: u64,
    name: String,
    #[serde(rename = "valutaCode")]
    currency: String,
    #[serde(rename = "activeTo")]
    active_to: Option<Vec<u64>>,
    #[serde(rename = "isCash")]
    is_cash: bool
}
