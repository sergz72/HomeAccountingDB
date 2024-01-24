use std::collections::HashMap;
use std::fmt;
use std::io::{Error, ErrorKind};
use serde::{Deserialize, Deserializer};
use serde::de::Visitor;
use crate::entities::accounts::Accounts;
use crate::entities::finance_operations::FinOpParameterValue::{DateValue, StringValue, U64Value};
use crate::entities::subcategories::Subcategory;
use crate::time_series_data::BinaryData;

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

impl BinaryData for FinanceRecord {
    fn serialize(&self, output: &mut Vec<u8>) {
        output.extend_from_slice(&self.operations.len().to_le_bytes());
        for op in &self.operations {
            op.serialize(output);
        }
    }
}

impl FinanceRecord {
    pub fn build_changes(&self, accounts: &Accounts,
                         subcategories: &HashMap<u64, Subcategory>) -> Result<HashMap<u64, FinanceChanges>, Error> {
        let mut ch: HashMap<u64, FinanceChanges> = self.totals.clone().into_iter()
            .map(|(account, summa)|(account, FinanceChanges::new(summa))).collect();
        for op in &self.operations {
            op.apply(&mut ch, accounts, subcategories)?;
        }
        Ok(ch)
    }

    pub fn print_changes(&self, accounts: &Accounts, subcategories: &HashMap<u64, Subcategory>)
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

struct FinOpParameters(HashMap<String, FinOpParameterValue>);

impl<'de> Deserialize<'de> for FinOpParameters {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where D: Deserializer<'de>,
    {
        let s: Option<Vec<FinOpParameterJson>> = Deserialize::deserialize(deserializer)?;
        if let Some(ss) = s {
            let params = ss.into_iter()
                .map(|p| (p.code.clone(), convert_parameter(p))).collect();
            return Ok(FinOpParameters(params));
        }
        Ok(FinOpParameters(HashMap::new()))
    }
}

fn convert_parameter(p: FinOpParameterJson) -> FinOpParameterValue {
    if let Some(n) = p.numeric_value {
        return U64Value(n)
    } else if let Some(s) = p.string_value {
        return StringValue(s)
    } else if let Some(d) = p.date_value {
        return DateValue(d)
    }
    U64Value(0)
}

#[derive(Deserialize)]
pub struct FinanceOperation {
    #[serde(alias = "Id", alias = "id")]
    pub id: u64,
    #[serde(alias = "AccountId", alias = "accountId")]
    account: u64,
    #[serde(alias = "SubcategoryId", alias = "subcategoryId")]
    subcategory: u64,
    #[serde(alias = "Amount", alias = "amount", deserialize_with = "deserialize_summa3")]
    amount: Option<u64>,
    #[serde(alias = "Summa", alias = "summa", deserialize_with = "deserialize_summa2")]
    summa: i64,
    #[serde(alias = "FinOpProperies", alias = "finOpProperies")]
    parameters: FinOpParameters
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

impl BinaryData for FinanceOperation {
    fn serialize(&self, output: &mut Vec<u8>) {
        output.extend_from_slice(&self.id.to_le_bytes());
        output.extend_from_slice(&self.account.to_le_bytes());
        output.extend_from_slice(&self.subcategory.to_le_bytes());
        output.extend_from_slice(&self.summa.to_le_bytes());
        output.extend_from_slice(&self.amount.unwrap_or(u64::MAX).to_le_bytes());
        output.extend_from_slice(&self.id.to_le_bytes());
        output.extend_from_slice(&self.id.to_le_bytes());
    }
}

impl FinanceOperation {
    pub fn apply(&self, changes: &mut HashMap<u64, FinanceChanges>, accounts: &Accounts,
                 subcategories: &HashMap<u64, Subcategory>) -> Result<(), Error> {
        let subcategory = subcategories.get(&self.subcategory)
            .ok_or(Error::new(ErrorKind::InvalidData, "unknown subcategory"))?;
        match subcategory.operation_code.as_str() {
            "INCM" => get_account_changes(changes, self.account).handle_income(self.summa),
            "EXPN" => get_account_changes(changes, self.account).handle_expenditure(self.summa),
            "SPCL" => {
                let code = subcategory.code.as_ref()
                    .ok_or(Error::new(ErrorKind::InvalidData, "missing subcategory code"))?;
                match code.as_str() {
                    // Пополнение карточного счета наличными
                    "INCC" => self.handle_incc(changes, accounts),
                    // Снятие наличных в банкомате
                    "EXPC" => self.handle_expc(changes, accounts),
                    // Обмен валюты
                    "EXCH" => self.handle_exch(changes),
                    // Перевод средств между платежными картами
                    "TRFR" => self.handle_trfr(changes),
                    _ => Err(Error::new(ErrorKind::InvalidData, "unknown subcategory code"))
                }
            },
            _ => Err(Error::new(ErrorKind::InvalidData, "unknown operation code"))
        }
    }

    fn handle_incc(&self, changes: &mut HashMap<u64, FinanceChanges>,
                   accounts: &Accounts) -> Result<(), Error> {
        get_account_changes(changes, self.account).handle_income(self.summa)?;
        // cash account for corresponding currency code
        let cash_account = accounts.get_cash_account(self.account)?;
        get_account_changes(changes, cash_account).handle_expenditure(self.summa)
    }

    fn handle_expc(&self, changes: &mut HashMap<u64, FinanceChanges>, accounts: &Accounts) -> Result<(), Error> {
        get_account_changes(changes, self.account).handle_expenditure(self.summa)?;
        // cash account for corresponding currency code
        let cash_account = accounts.get_cash_account(self.account)?;
        get_account_changes(changes, cash_account).handle_income(self.summa)
    }

    fn handle_exch(&self, changes: &mut HashMap<u64, FinanceChanges>) -> Result<(), Error> {
        if let Some(a) = &self.amount {
            return self.handle_trfr_with_summa(changes, (*a as i64) / 10)
        }
        Ok(())
    }

    fn handle_trfr(&self, changes: &mut HashMap<u64, FinanceChanges>) -> Result<(), Error> {
        self.handle_trfr_with_summa(changes, self.summa)
    }

    fn handle_trfr_with_summa(&self, changes: &mut HashMap<u64, FinanceChanges>, summa: i64) -> Result<(), Error> {
        get_account_changes(changes, self.account).handle_expenditure(summa)?;
        if let Some(p) = self.parameters.0.get(&"SECA".to_string()) {
            if let U64Value(v) = p {
                get_account_changes(changes, *v).handle_income(self.summa)?;
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
    #[serde(alias = "DateValue", alias = "dateValue")]
    date_value: Option<Vec<u64>>,
    #[serde(alias = "PropertyCode", alias = "propertyCode")]
    code: String
}

pub enum FinOpParameterValue {
    U64Value(u64),
    StringValue(String),
    DateValue(Vec<u64>)
}
