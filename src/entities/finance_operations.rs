use std::collections::HashMap;
use std::fmt;
use std::io::{Error, ErrorKind};
use serde::{Deserialize, Deserializer};
use serde::de::{Unexpected, Visitor};
use crate::core::data_source::DataSource;
use crate::core::time_series_data::Loader;
use crate::entities::accounts::Accounts;
use crate::entities::subcategories::{Subcategories, SubcategoryCode, SubcategoryOperationCode};
use crate::entities::common::date_deserialize;

pub struct FinanceChanges {
    start_balance: i64,
    income: i64,
    expenditure: i64
}

impl FinanceChanges {
    pub fn new(start_balance: i64) -> FinanceChanges {
        FinanceChanges{start_balance, income: 0, expenditure: 0}
    }

    pub fn get_end_balance(&self) -> i64 {
        self.start_balance + self.income as i64 - self.expenditure as i64
    }

    pub fn handle_income(&mut self, summa: i64) -> Result<(), Error> {
        self.income += summa;
        Ok(())
    }

    pub fn handle_expenditure(&mut self, summa: i64) -> Result<(), Error> {
        self.expenditure += summa;
        Ok(())
    }
}

pub struct FinanceRecord {
    pub operations: Vec<FinanceOperation>,
    pub totals: HashMap<u64, i64>
}

impl FinanceRecord {

    pub fn new() -> FinanceRecord {
        FinanceRecord{operations: Vec::new(), totals: HashMap::new()}
    }

    pub fn create_changes(&self) -> HashMap<u64, FinanceChanges> {
        self.totals.clone().into_iter()
            .map(|(account, summa)|(account, FinanceChanges::new(summa))).collect()
    }

    pub fn build_changes(&self, accounts: &Accounts,
                         subcategories: &Subcategories) -> Result<HashMap<u64, FinanceChanges>, Error> {
        let mut ch = self.create_changes();
        for op in &self.operations {
            op.apply(&mut ch, accounts, subcategories)?;
        }
        Ok(ch)
    }

    pub fn update_changes(&self, ch: &mut HashMap<u64, FinanceChanges>, accounts: &Accounts,
                         subcategories: &Subcategories) -> Result<(), Error> {
        for op in &self.operations {
            op.apply(ch, accounts, subcategories)?;
        }
        Ok(())
    }

    pub fn print_changes(&self, accounts: &Accounts, subcategories: &Subcategories)
        -> Result<(), Error> {
        let changes = self.build_changes(accounts, subcategories)?;
        for (account, change) in changes {
            let name = accounts.get_name(account)?;
            println!("{}: {} {} {} {}", name, change.start_balance, change.income,
                     change.expenditure, change.get_end_balance());
        }
        Ok(())
    }
}

impl Loader<Vec<FinanceOperation>> for FinanceRecord {
    fn load(&mut self, file_name: String, source: &Box<dyn DataSource<Vec<FinanceOperation>>>)
            -> Result<(), Error> {
        let mut data = source.load(file_name, false)?;
        self.operations.append(&mut data);
        Ok(())
    }
}

#[derive(Deserialize)]
pub struct FinanceOperation {
    #[serde(alias = "Id", alias = "id")]
    pub date: u64,
    #[serde(alias = "AccountId", alias = "accountId")]
    account: u64,
    #[serde(alias = "SubcategoryId", alias = "subcategoryId")]
    subcategory: u64,
    #[serde(alias = "Amount", alias = "amount", deserialize_with = "deserialize_summa3")]
    amount: Option<u64>,
    #[serde(alias = "Summa", alias = "summa", deserialize_with = "deserialize_summa2")]
    summa: i64,
    #[serde(alias = "FinOpProperies", alias = "finOpProperies", deserialize_with = "deserialize_parameters")]
    parameters: Vec<FinOpParameter>
}

fn deserialize_summa2<'de, D>(deserializer: D) -> Result<i64, D::Error>
    where
        D: Deserializer<'de>,
{
    // define a visitor that deserializes
    // `ActualData` encoded as json within a string
    struct JsonStringVisitor;

    impl<'de> Visitor<'de> for JsonStringVisitor {
        type Value = i64;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a float or integer")
        }

        fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E> where E: serde::de::Error {
            Ok((v * 100.0).round() as i64)
        }

        fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E> where E: serde::de::Error {
            Ok(v)
        }

        fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E> where E: serde::de::Error {
            Ok(v as i64)
        }
    }

    // use our visitor to deserialize an `ActualValue`
    deserializer.deserialize_any(JsonStringVisitor)
}

fn deserialize_summa3<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
    where
        D: Deserializer<'de>,
{
    // define a visitor that deserializes
    // `ActualData` encoded as json within a string
    struct JsonStringVisitor;

    impl<'de> Visitor<'de> for JsonStringVisitor {
        type Value = Option<u64>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a float or integer or null")
        }

        fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E> where E: serde::de::Error {
            Ok(Some((v * 1000.0).round() as u64))
        }

        fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E> where E: serde::de::Error {
            Ok(Some(v))
        }

        fn visit_unit<E>(self) -> Result<Self::Value, E> where E: serde::de::Error {
            Ok(None)
        }
    }

    // use our visitor to deserialize an `ActualValue`
    deserializer.deserialize_any(JsonStringVisitor)
}

fn deserialize_parameters<'de, D>(deserializer: D) -> Result<Vec<FinOpParameter>, D::Error>
    where
        D: Deserializer<'de>,
{
    let v: Option<Vec<FinOpParameterJson>> = Deserialize::deserialize(deserializer)?;
    let mut result = Vec::new();
    if v.is_some() {
        for p in v.unwrap() {
            let pp = match p.code.as_str() {
                "AMOU" => p.numeric_value.ok_or(serde::de::Error::invalid_value(Unexpected::Option, &"AMOU: numeric value expected"))
                    .map(|v|FinOpParameter::Amou(v)),
                "DIST" => p.numeric_value.ok_or(serde::de::Error::invalid_value(Unexpected::Option,&"DIST: numeric value expected"))
                    .map(|v|FinOpParameter::Dist(v)),
                "PPTO" => p.numeric_value.ok_or(serde::de::Error::invalid_value(Unexpected::Option, &"PPTO: numeric value expected"))
                    .map(|v|FinOpParameter::Ppto(v)),
                "SECA" => p.numeric_value.ok_or(serde::de::Error::invalid_value(Unexpected::Option, &"SECA: numeric value expected"))
                    .map(|v|FinOpParameter::Seca(v)),
                "NETW" => p.string_value.ok_or(serde::de::Error::invalid_value(Unexpected::Option,&"NETW: string value expected"))
                    .map(|v|FinOpParameter::Netw(v)),
                "TYPE" => p.string_value.ok_or(serde::de::Error::invalid_value(Unexpected::Option,&"TYPE: string value expected"))
                    .map(|v|FinOpParameter::Typ(v)),
                _ => return Err(serde::de::Error::invalid_value(Unexpected::Str(p.code.as_str()),
                                                                &"finOpParameter code"))
            }?;
            result.push(pp);
        }
    }
    return Ok(result);
}

impl FinanceOperation {
    pub fn apply(&self, changes: &mut HashMap<u64, FinanceChanges>, accounts: &Accounts,
                 subcategories: &Subcategories) -> Result<(), Error> {
        let (code, operation_code) = subcategories.get_codes(self.subcategory)?;
        match operation_code {
            SubcategoryOperationCode::Incm => get_account_changes(changes, self.account).handle_income(self.summa),
            SubcategoryOperationCode::Expn => get_account_changes(changes, self.account).handle_expenditure(self.summa),
            SubcategoryOperationCode::Spcl => {
                match code {
                    // Пополнение карточного счета наличными
                    SubcategoryCode::Incc => self.handle_incc(changes, accounts),
                    // Снятие наличных в банкомате
                    SubcategoryCode::Expc => self.handle_expc(changes, accounts),
                    // Обмен валюты
                    SubcategoryCode::Exch => self.handle_exch(changes),
                    // Перевод средств между платежными картами
                    SubcategoryCode::Trfr => self.handle_trfr(changes),
                    _ => Err(Error::new(ErrorKind::InvalidData, "invalid subcategory code"))
                }
            }
        }
    }

    fn handle_incc(&self, changes: &mut HashMap<u64, FinanceChanges>,
                   accounts: &Accounts) -> Result<(), Error> {
        get_account_changes(changes, self.account).handle_income(self.summa)?;
        // cash account for corresponding currency code
        let cash_account = accounts.get_cash_account(self.account)?;
        if let Some(a) = cash_account {
            get_account_changes(changes, a).handle_expenditure(self.summa)
        } else {
            Ok(())
        }
    }

    fn handle_expc(&self, changes: &mut HashMap<u64, FinanceChanges>, accounts: &Accounts) -> Result<(), Error> {
        get_account_changes(changes, self.account).handle_expenditure(self.summa)?;
        // cash account for corresponding currency code
        let cash_account = accounts.get_cash_account(self.account)?;
        if let Some(a) = cash_account {
            get_account_changes(changes, a).handle_income(self.summa)
        } else {
            Ok(())
        }
    }

    fn handle_exch(&self, changes: &mut HashMap<u64, FinanceChanges>) -> Result<(), Error> {
        if let Some(a) = self.amount {
            return self.handle_trfr_with_summa(changes, (a as i64) / 10)
        }
        Ok(())
    }

    fn handle_trfr(&self, changes: &mut HashMap<u64, FinanceChanges>) -> Result<(), Error> {
        self.handle_trfr_with_summa(changes, self.summa)
    }

    fn handle_trfr_with_summa(&self, changes: &mut HashMap<u64, FinanceChanges>, summa: i64) -> Result<(), Error> {
        get_account_changes(changes, self.account).handle_expenditure(summa)?;
        if self.parameters.len() == 1 {
            if let FinOpParameter::Seca(a) = self.parameters[0] {
                get_account_changes(changes, a).handle_income(self.summa)?;
            }
        }
        Ok(())
    }
}

fn get_account_changes(changes: &mut HashMap<u64, FinanceChanges>, account: u64) -> &mut FinanceChanges {
    changes.entry(account).or_insert(FinanceChanges::new(0))
}

#[derive(Deserialize)]
struct FinOpParameterJson {
    #[serde(alias = "NumericValue", alias = "numericValue")]
    numeric_value: Option<u64>,
    #[serde(alias = "StringValue", alias = "stringValue")]
    string_value: Option<String>,
    #[serde(alias = "DateValue", alias = "dateValue", deserialize_with = "date_deserialize")]
    date_value: Option<u64>,
    #[serde(alias = "PropertyCode", alias = "propertyCode")]
    code: String
}

pub enum FinOpParameter {
    Amou(u64),
    Dist(u64),
    Netw(String),
    Ppto(u64),
    Seca(u64),
    Typ(String)
}
