use crate::core::data_source::DataSource;
use crate::core::time_series_data::DatedSource;
use crate::db::DBConfiguration;
use crate::entities::accounts::Account;
use crate::entities::finance_operations::FinanceRecord;
use crate::entities::subcategories::{Category, Subcategory};

pub struct BinaryDBConfiguration {
    aes_key: [u8; 32]
}

impl BinaryDBConfiguration {
    pub fn new(aes_key: [u8; 32]) -> BinaryDBConfiguration {
        BinaryDBConfiguration{aes_key}
    }
}

impl DBConfiguration for BinaryDBConfiguration {
    fn get_accounts_source(&self) -> Box<dyn DataSource<Vec<Account>>> {
        todo!()
    }

    fn get_categories_source(&self) -> Box<dyn DataSource<Vec<Category>>> {
        todo!()
    }

    fn get_subcategories_source(&self) -> Box<dyn DataSource<Vec<Subcategory>>> {
        todo!()
    }

    fn get_main_data_source(&self) -> Box<dyn DatedSource<FinanceRecord>> {
        todo!()
    }
}