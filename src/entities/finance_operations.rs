use std::collections::HashMap;
use std::fmt;
use std::io::{Error, ErrorKind};
use serde::{Deserialize, Deserializer};
use serde::de::{Unexpected, Visitor};
use crate::entities::accounts::Accounts;
use crate::entities::subcategories::{Subcategories, SubcategoryCode, SubcategoryOperationCode};
use crate::entities::common::date_deserialize;

pub struct FinanceChange {
    start_balance: i64,
    income: i64,
    expenditure: i64
}

impl FinanceChange {
    pub fn new(start_balance: i64) -> FinanceChange {
        FinanceChange{start_balance, income: 0, expenditure: 0}
    }

    pub fn get_end_balance(&self) -> i64 {
        self.start_balance + self.income - self.expenditure
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

pub struct FinanceChanges {
    changes: HashMap<u64, FinanceChange>
}

impl FinanceChanges {
    pub fn new(totals: &HashMap<u64, i64>) -> FinanceChanges {
        let changes = totals.iter()
            .map(|(account, summa)|(*account, FinanceChange::new(*summa))).collect();
        FinanceChanges{changes}
    }

    pub fn empty() -> FinanceChanges {FinanceChanges{changes: HashMap::new()}}

    pub fn build_totals(&self) -> HashMap<u64, i64> {
        self.changes.iter()
            .map(|(account, changes)|(*account, changes.get_end_balance())).collect()
    }

    fn get_account_changes(&mut self, account: u64) -> &mut FinanceChange {
        self.changes.entry(account).or_insert(FinanceChange::new(0))
    }

    pub fn print(&self, accounts: &Accounts) -> Result<(), Error> {
        for (account, change) in &self.changes {
            let acc = accounts.get(*account)?;
            println!("{}: {} {} {} {}", acc.name, change.start_balance, change.income,
                     change.expenditure, change.get_end_balance());
        }
        Ok(())
    }
}

pub struct FinanceRecord {
    pub operations: Vec<FinanceOperation>,
    pub totals: HashMap<u64, i64>
}

impl FinanceRecord {
    pub fn new(operations: Vec<FinanceOperation>) -> FinanceRecord {
        FinanceRecord{operations, totals: HashMap::new()}
    }

    pub fn create_changes(&self) -> FinanceChanges {
        FinanceChanges::new(&self.totals)
    }

    pub fn build_changes(&self, accounts: &Accounts,
                         subcategories: &Subcategories) -> Result<FinanceChanges, Error> {
        let mut ch = self.create_changes();
        for op in &self.operations {
            op.apply(&mut ch, accounts, subcategories)?;
        }
        Ok(ch)
    }

    pub fn update_changes(&self, ch: &mut FinanceChanges, from: usize, to: usize,
                          accounts: &Accounts, subcategories: &Subcategories) -> Result<(), Error> {
        for op in &self.operations {
            if op.within(from, to) {
                op.apply(ch, accounts, subcategories)?;
            }
        }
        Ok(())
    }

    pub fn get_ops(&self, date: usize) -> Vec<FinanceOperation> {
        let ops: Vec<FinanceOperation> = self.operations.iter()
            .filter(|op|op.date == date)
            .map(|op|op.copy())
            .collect();
        ops
    }
}

#[derive(Deserialize)]
pub struct FinanceOperation {
    #[serde(alias = "Id", alias = "id")]
    pub date: usize,
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
    pub fn apply(&self, changes: &mut FinanceChanges, accounts: &Accounts,
                 subcategories: &Subcategories) -> Result<(), Error> {
        let subcategory = subcategories.get(self.subcategory)?;
        match subcategory.operation_code {
            SubcategoryOperationCode::Incm => changes.get_account_changes(self.account).handle_income(self.summa),
            SubcategoryOperationCode::Expn => changes.get_account_changes(self.account).handle_expenditure(self.summa),
            SubcategoryOperationCode::Spcl => {
                match subcategory.code {
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

    fn handle_incc(&self, changes: &mut FinanceChanges,
                   accounts: &Accounts) -> Result<(), Error> {
        changes.get_account_changes(self.account).handle_income(self.summa)?;
        // cash account for corresponding currency code
        let cash_account = accounts.get_cash_account(self.account)?;
        if let Some(a) = cash_account {
            changes.get_account_changes(a).handle_expenditure(self.summa)
        } else {
            Ok(())
        }
    }

    fn handle_expc(&self, changes: &mut FinanceChanges, accounts: &Accounts) -> Result<(), Error> {
        changes.get_account_changes(self.account).handle_expenditure(self.summa)?;
        // cash account for corresponding currency code
        let cash_account = accounts.get_cash_account(self.account)?;
        if let Some(a) = cash_account {
            changes.get_account_changes(a).handle_income(self.summa)
        } else {
            Ok(())
        }
    }

    fn handle_exch(&self, changes: &mut FinanceChanges) -> Result<(), Error> {
        if let Some(a) = self.amount {
            return self.handle_trfr_with_summa(changes, (a as i64) / 10)
        }
        Ok(())
    }

    fn handle_trfr(&self, changes: &mut FinanceChanges) -> Result<(), Error> {
        self.handle_trfr_with_summa(changes, self.summa)
    }

    fn handle_trfr_with_summa(&self, changes: &mut FinanceChanges, summa: i64) -> Result<(), Error> {
        changes.get_account_changes(self.account).handle_expenditure(summa)?;
        if self.parameters.len() == 1 {
            if let FinOpParameter::Seca(a) = self.parameters[0] {
                changes.get_account_changes(a).handle_income(self.summa)?;
            }
        }
        Ok(())
    }

    pub fn within(&self, from: usize, to: usize) -> bool {
        self.date >= from && self.date <= to
    }
    
    fn copy(&self) -> FinanceOperation {
        FinanceOperation{
            date: self.date,
            account: self.account,
            subcategory: self.subcategory,
            amount: self.amount,
            summa: self.summa,
            parameters: self.parameters.clone(),
        }
    }
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

#[derive(Clone)]
pub enum FinOpParameter {
    Amou(u64),
    Dist(u64),
    Netw(String),
    Ppto(u64),
    Seca(u64),
    Typ(String)
}
