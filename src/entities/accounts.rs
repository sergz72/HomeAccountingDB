use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use std::ops::Add;
use serde::{Deserialize, Deserializer};
use crate::core::data_source::DataSource;
use crate::entities::common::date_deserialize;

pub struct Accounts {
    map: HashMap<u64, Account>,
}

impl Accounts {
    pub fn load(data_folder_path: String, source: Box<dyn DataSource<Vec<Account>>>)
        -> Result<Accounts, Error> {
        let mut accounts = source.load(data_folder_path.add("/accounts"), true)?;
        let cash_accounts: HashMap<String, u64> = accounts.iter()
            .filter(|a|a.cash_account.is_none())
            .map(|a|(a.currency.clone(), a.id)).collect();
        for a in accounts.iter_mut() {
            if a.cash_account.is_some() {
                let cash_account = *cash_accounts.get(&a.currency)
                    .ok_or(Error::new(ErrorKind::InvalidData, "load - no cash account found"))?;
                a.cash_account = Some(cash_account)
            }
        }
        let map = accounts.into_iter().map(|c|(c.id, c)).collect();
        Ok(Accounts{map})
    }

    pub fn get_cash_account(&self, account: u64) -> Result<Option<u64>, Error> {
        match self.map.get(&account) {
            Some(a) => Ok(a.cash_account),
            None => Err(Error::new(ErrorKind::InvalidData, "invalid account id"))
        }
    }

    pub fn get(&self, id: u64) -> Result<&Account, Error> {
        self.map.get(&id).ok_or(Error::new(ErrorKind::InvalidData, "invalid account id"))
    }
}

fn is_cash_deserialize<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
    where
        D: Deserializer<'de>,
{
    let v: bool = Deserialize::deserialize(deserializer)?;
    return if v {Ok(None)} else {Ok(Some(0))};
}

#[derive(Deserialize, Clone)]
pub struct Account {
    id: u64,
    pub name: String,
    #[serde(rename = "valutaCode")]
    currency: String,
    #[serde(rename = "activeTo", deserialize_with = "date_deserialize")]
    active_to: Option<u64>,
    #[serde(rename = "isCash", deserialize_with = "is_cash_deserialize")]
    cash_account: Option<u64>
}
