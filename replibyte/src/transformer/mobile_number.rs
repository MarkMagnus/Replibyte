use serde::{Deserialize, Serialize};
use crate::transformer::Transformer;
use crate::types::Column;
use fake::faker::number::en::NumberWithFormat;
use fake::Fake;

mod MobileFormats {
    pub const NUMBER_FORMAT_6: &'static str = " ### ###";
    pub const NUMBER_FORMAT_7: &'static str = " ### ####";
    pub const NUMBER_FORMAT_8: &'static str = " #### ####";
    pub const NUMBER_FORMAT_9: &'static str = " ### ### ###";
    pub const NUMBER_FORMAT_10: &'static str = " ### ### ####";
    pub const NUMBER_FORMAT_11: &'static str = " ### #### ####";
    pub const NUMBER_FORMAT_DEFAULT: &'static str = " #### ####";
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Copy)]
pub struct MobileNumberOptions {
    pub country_code: u8,
    pub length: u8,
}

impl MobileNumberOptions {
    pub fn new<S>(
        country_code: S,
        length: S,
    ) -> Self
        where S: Into<u8> {
        MobileNumberOptions {
            country_code: country_code.into(),
            length: length.into(),
        }
    }
}

impl Default for MobileNumberOptions {
    fn default() -> Self {
        MobileNumberOptions {
            country_code: 1,
            length: 11,
        }
    }
}

/// This struct is dedicated to replacing a string by an email address.
pub struct MobileNumberTransformer {
    database_name: String,
    table_name: String,
    column_name: String,
    options: MobileNumberOptions,
}

impl MobileNumberTransformer {
    pub fn new<S>(
        database_name: S,
        table_name: S,
        column_name: S,
        options: MobileNumberOptions,
    ) -> Self
        where
            S: Into<String>,
    {
        MobileNumberTransformer {
            database_name: database_name.into(),
            table_name: table_name.into(),
            column_name: column_name.into(),
            options,
        }
    }
}

impl Default for MobileNumberTransformer {
    fn default() -> Self {
        MobileNumberTransformer {
            database_name: String::default(),
            table_name: String::default(),
            column_name: String::default(),
            options: MobileNumberOptions::default(),
        }
    }
}

impl Transformer for MobileNumberTransformer {
    fn id(&self) -> &str {
        "mobile-number"
    }

    fn description(&self) -> &str {
        "Generate a mobile number (string only)."
    }

    fn database_name(&self) -> &str {
        self.database_name.as_str()
    }

    fn table_name(&self) -> &str {
        self.table_name.as_str()
    }

    fn column_name(&self) -> &str {
        self.column_name.as_str()
    }

    fn transform(&self, column: Column) -> Column {
        let country_code = self.options.country_code;
        let prefix = country_code.to_string();
        let tail_length = self.options.length as usize - prefix.len() as usize;
        match column {
            Column::StringValue(column_name, _) => {
                let mobile : String = match tail_length {
                    6 => NumberWithFormat(MobileFormats::NUMBER_FORMAT_6).fake(),
                    7 => NumberWithFormat(MobileFormats::NUMBER_FORMAT_7).fake(),
                    8 => NumberWithFormat(MobileFormats::NUMBER_FORMAT_8).fake(),
                    9 => NumberWithFormat(MobileFormats::NUMBER_FORMAT_9).fake(),
                    10 => NumberWithFormat(MobileFormats::NUMBER_FORMAT_10).fake(),
                    11 => NumberWithFormat(MobileFormats::NUMBER_FORMAT_11).fake(),
                    _ => NumberWithFormat(MobileFormats::NUMBER_FORMAT_DEFAULT).fake(),
                };
                Column::StringValue(column_name, prefix + &mobile)
            }
            column => column,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{transformer, transformer::Transformer, types::Column};

    use super::{MobileNumberTransformer, MobileNumberOptions};

    fn get_us_transformer() -> MobileNumberTransformer {
        MobileNumberTransformer::new("github", "users", "mobile_number",
                                     MobileNumberOptions::default())
    }

    #[test]
    fn transform_string_with_us_cell_number() {
        let transformer = get_us_transformer();
        assert_transformer(&transformer)
    }


    fn get_au_transformer() -> MobileNumberTransformer {
        MobileNumberTransformer::new("github", "user", "mobile_number",
                                     MobileNumberOptions::new(61, 11))
    }

    #[test]
    fn transform_string_with_au_mobile_number() {
        let transformer = get_au_transformer();
        assert_transformer(&transformer)
    }

    fn assert_transformer(transformer: &dyn Transformer) {
        let column = Column::StringValue("mobile_number".to_string(), "+123456789".to_string());
        let transformed_column = transformer.transform(column);
        let transformed_value = transformed_column.string_value().unwrap();

        println!("fake mobile {}", transformed_value.to_string());

        assert!(!transformed_value.is_empty());
        assert_ne!(transformed_value, "+123456789".to_string());
    }
}
