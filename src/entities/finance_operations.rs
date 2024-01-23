use std::collections::HashMap;
use std::fmt;
use std::io::{Error, ErrorKind};
use serde::{Deserialize, Deserializer};
use serde::de::Visitor;
use crate::entities::accounts::Accounts;
use crate::entities::finance_operations::FinOpParameterValue::{DateValue, StringValue, UsizeValue};
use crate::entities::subcategories::Subcategory;

pub struct FinanceChanges {
    start_balance: isize,
    income: isize,
    expenditure: isize
}

impl FinanceChanges {
    pub fn new(start_balance: isize) -> FinanceChanges {
        FinanceChanges{start_balance, income: 0, expenditure: 0}
    }

    pub fn get_end_balance(&self) -> isize {
        self.start_balance + self.income as isize - self.expenditure as isize
    }

    pub fn handle_income(&mut self, summa: isize) -> Result<(), Error> {
        self.income += summa;
        Ok(())
    }

    pub fn handle_expenditure(&mut self, summa: isize) -> Result<(), Error> {
        self.expenditure += summa;
        Ok(())
    }
}

pub struct FinanceRecord {
    pub operations: Vec<FinanceOperation>,
    pub totals: HashMap<usize, isize>
}

impl FinanceRecord {
    pub fn build_changes(&self, accounts: &Accounts,
                         subcategories: &HashMap<usize, Subcategory>) -> Result<HashMap<usize, FinanceChanges>, Error> {
        let mut ch: HashMap<usize, FinanceChanges> = self.totals.clone().into_iter()
            .map(|(account, summa)|(account, FinanceChanges::new(summa))).collect();
        for op in &self.operations {
            op.apply(&mut ch, accounts, subcategories)?;
        }
        Ok(ch)
    }

    pub fn print_changes(&self, accounts: &Accounts, subcategories: &HashMap<usize, Subcategory>)
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
        return UsizeValue(n)
    } else if let Some(s) = p.string_value {
        return StringValue(s)
    } else if let Some(d) = p.date_value {
        return DateValue(d)
    }
    UsizeValue(0)
}

#[derive(Deserialize)]
pub struct FinanceOperation {
    #[serde(alias = "Id", alias = "id")]
    pub id: usize,
    #[serde(alias = "AccountId", alias = "accountId")]
    account: usize,
    #[serde(alias = "SubcategoryId", alias = "subcategoryId")]
    subcategory: usize,
    #[serde(alias = "Amount", alias = "amount", deserialize_with = "deserialize_summa3")]
    amount: Option<usize>,
    #[serde(alias = "Summa", alias = "summa", deserialize_with = "deserialize_summa2")]
    summa: isize,
    #[serde(alias = "FinOpProperies", alias = "finOpProperies")]
    parameters: FinOpParameters
}

fn deserialize_summa2<'de, D>(deserializer: D) -> Result<isize, D::Error>
    where
        D: Deserializer<'de>,
{
    // define a visitor that deserializes
    // `ActualData` encoded as json within a string
    struct JsonStringVisitor;

    impl<'de> Visitor<'de> for JsonStringVisitor {
        type Value = isize;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a float or integer")
        }

        fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E> where E: serde::de::Error {
            Ok((v * 100.0).round() as isize)
        }

        fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E> where E: serde::de::Error {
            Ok(v as isize)
        }

        fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E> where E: serde::de::Error {
            Ok(v as isize)
        }
    }

    // use our visitor to deserialize an `ActualValue`
    deserializer.deserialize_any(JsonStringVisitor)
}

fn deserialize_summa3<'de, D>(deserializer: D) -> Result<Option<usize>, D::Error>
    where
        D: Deserializer<'de>,
{
    // define a visitor that deserializes
    // `ActualData` encoded as json within a string
    struct JsonStringVisitor;

    impl<'de> Visitor<'de> for JsonStringVisitor {
        type Value = Option<usize>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a float or integer or null")
        }

        fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E> where E: serde::de::Error {
            Ok(Some((v * 1000.0).round() as usize))
        }

        fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E> where E: serde::de::Error {
            Ok(Some(v as usize))
        }

        fn visit_unit<E>(self) -> Result<Self::Value, E> where E: serde::de::Error {
            Ok(None)
        }
    }

    // use our visitor to deserialize an `ActualValue`
    deserializer.deserialize_any(JsonStringVisitor)
}

impl FinanceOperation {
    pub fn apply(&self, changes: &mut HashMap<usize, FinanceChanges>, accounts: &Accounts,
                 subcategories: &HashMap<usize, Subcategory>) -> Result<(), Error> {
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

    fn handle_incc(&self, changes: &mut HashMap<usize, FinanceChanges>,
                   accounts: &Accounts) -> Result<(), Error> {
        get_account_changes(changes, self.account).handle_income(self.summa)?;
        // cash account for corresponding currency code
        let cash_account = accounts.get_cash_account(self.account)?;
        get_account_changes(changes, cash_account).handle_expenditure(self.summa)
    }

    fn handle_expc(&self, changes: &mut HashMap<usize, FinanceChanges>, accounts: &Accounts) -> Result<(), Error> {
        get_account_changes(changes, self.account).handle_expenditure(self.summa)?;
        // cash account for corresponding currency code
        let cash_account = accounts.get_cash_account(self.account)?;
        get_account_changes(changes, cash_account).handle_income(self.summa)
    }

    fn handle_exch(&self, changes: &mut HashMap<usize, FinanceChanges>) -> Result<(), Error> {
        if let Some(a) = &self.amount {
            return self.handle_trfr_with_summa(changes, (*a as isize) / 10)
        }
        Ok(())
    }

    fn handle_trfr(&self, changes: &mut HashMap<usize, FinanceChanges>) -> Result<(), Error> {
        self.handle_trfr_with_summa(changes, self.summa)
    }

    fn handle_trfr_with_summa(&self, changes: &mut HashMap<usize, FinanceChanges>, summa: isize) -> Result<(), Error> {
        get_account_changes(changes, self.account).handle_expenditure(summa)?;
        if let Some(p) = self.parameters.0.get(&"SECA".to_string()) {
            if let UsizeValue(v) = p {
                get_account_changes(changes, *v).handle_income(self.summa)?;
            }
        }
        Ok(())
    }
}

fn get_account_changes(changes: &mut HashMap<usize, FinanceChanges>, account: usize) -> &mut FinanceChanges {
    changes.entry(account).or_insert(FinanceChanges::new(0))
}

#[derive(Deserialize)]
struct FinOpParameterJson {
    #[serde(alias = "NumericValue", alias = "numericValue")]
    numeric_value: Option<usize>,
    #[serde(alias = "StringValue", alias = "stringValue")]
    string_value: Option<String>,
    #[serde(alias = "DateValue", alias = "dateValue")]
    date_value: Option<Vec<usize>>,
    #[serde(alias = "PropertyCode", alias = "propertyCode")]
    code: String
}

pub enum FinOpParameterValue {
    UsizeValue(usize),
    StringValue(String),
    DateValue(Vec<usize>)
}
