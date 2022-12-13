use crate::transformer::blank::BlankTransformer;
use crate::transformer::credit_card::CreditCardTransformer;
use crate::transformer::email::EmailTransformer;
use crate::transformer::first_name::FirstNameTransformer;
use crate::transformer::hstore_attr::HstoreAttrTransformer;
use crate::transformer::json_attrs::JsonAttrTransformer;
use crate::transformer::keep_first_char::KeepFirstCharTransformer;
use crate::transformer::phone_number::PhoneNumberTransformer;
use crate::transformer::mobile_number::MobileNumberTransformer;
use crate::transformer::random::RandomTransformer;
use crate::transformer::redacted::RedactedTransformer;
use crate::transformer::transient::TransientTransformer;
use crate::types::Column;

pub mod credit_card;
pub mod email;
pub mod first_name;
pub mod keep_first_char;
pub mod phone_number;
pub mod mobile_number;
pub mod random;
pub mod redacted;
pub mod transient;
pub mod blank;
pub mod hstore_attr;
pub mod json_attrs;

pub fn transformers() -> Vec<Box<dyn Transformer>> {
    vec![
        Box::new(EmailTransformer::default()),
        Box::new(FirstNameTransformer::default()),
        Box::new(PhoneNumberTransformer::default()),
        Box::new(MobileNumberTransformer::default()),
        Box::new(RandomTransformer::default()),
        Box::new(KeepFirstCharTransformer::default()),
        Box::new(TransientTransformer::default()),
        Box::new(CreditCardTransformer::default()),
        Box::new(RedactedTransformer::default()),
        Box::new(BlankTransformer::default()),
        Box::new(HstoreAttrTransformer::default()),
        Box::new(JsonAttrTransformer::default()),
    ]
}

/// Trait to implement to create a custom Transformer.
pub trait Transformer {
    fn id(&self) -> &str;
    fn description(&self) -> &str;
    fn database_name(&self) -> &str;
    fn table_name(&self) -> &str;
    fn column_name(&self) -> &str;
    fn quoted_table_name(&self) -> String {
        let table_name = self.table_name();

        if table_name.to_lowercase() != table_name {
            return format!("\"{}\"", table_name);
        }

        String::from(table_name)
    }

    fn database_and_table_name(&self) -> String {
        format!("{}.{}", self.database_name(), self.table_name())
    }

    fn database_and_table_and_column_name(&self) -> String {
        format!(
            "{}.{}.{}",
            self.database_name(),
            self.table_name(),
            self.column_name()
        )
    }

    fn database_and_quoted_table_and_column_name(&self) -> String {
        format!(
            "{}.{}.{}",
            self.database_name(),
            self.quoted_table_name(),
            self.column_name()
        )
    }

    fn table_and_column_name(&self) -> String {
        format!(
            "{}.{}",
            self.table_name(),
            self.column_name()
        )
    }

    fn transform(&self, column: Column) -> Column;
}
