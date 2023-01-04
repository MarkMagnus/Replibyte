use bson::doc;
use serde::{Deserialize, Serialize};
use crate::config::TransformerTypeConfig;
use crate::transformer::{Transformer, transformers};
use crate::types::Column;
use crate::source::hstore::Hstore;

pub struct HstoreAttrTransformer {
    database_name: String,
    table_name: String,
    column_name: String,
    options: HstoreAttrOptions,
}

impl Default for HstoreAttrTransformer {
    fn default() -> Self {
        HstoreAttrTransformer {
            database_name: String::default(),
            table_name: String::default(),
            column_name: String::default(),
            options: HstoreAttrOptions::default(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct HstoreAttrOption {
    pub attribute: String,
    #[serde(flatten)]
    pub transformer_type_config: TransformerTypeConfig,
}


impl Default for HstoreAttrOption {
    fn default() -> Self {
        HstoreAttrOption {
            attribute: String::default(),
            transformer_type_config: TransformerTypeConfig::Blank,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct HstoreAttrOptions {
    pub transformers: Vec<HstoreAttrOption>,
}

impl Default for HstoreAttrOptions {
    fn default() -> Self {
        HstoreAttrOptions {
            transformers: vec![HstoreAttrOption::default()]
        }
    }
}

impl HstoreAttrTransformer {
    pub fn new<S>(database_name: S, table_name: S, column_name: S, options: HstoreAttrOptions) -> Self
        where
            S: Into<String>,
    {
        HstoreAttrTransformer {
            table_name: table_name.into(),
            column_name: column_name.into(),
            database_name: database_name.into(),
            options: options,
        }
    }
}

impl Transformer for HstoreAttrTransformer {
    fn id(&self) -> &str {
        "blank"
    }

    fn description(&self) -> &str { "change hstore key values using appropriate transformers" }

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
        let mut transformed = false;
        let fallback_return_value = column.clone();
        match column {
            Column::StringValue(column_name, value) => {
                let mut hstore_key_values = Hstore::from_hstore(value);
                for hstore_attr_option in self.options.transformers.iter() {
                    let attribute_key_str = hstore_attr_option.attribute.as_str();
                    let attribute_key = attribute_key_str.to_string();

                    if hstore_key_values.contains_key(&attribute_key) {
                        let required_transformer = hstore_attr_option.transformer_type_config.transformer(self.database_name(), self.table_name(), attribute_key_str);

                        let attribute_value = match hstore_key_values.get(&attribute_key) {
                            Some(v) => v,
                            None => ""
                        };

                        let attribute_column = Column::StringValue(attribute_key_str.to_string(), attribute_value.to_string());
                        match required_transformer.transform(attribute_column) {
                            Column::StringValue(_, new_value) => {
                                hstore_key_values.insert(attribute_key, new_value);
                                transformed = true;
                            }
                            _ => println!("cannot transform {}", column_name)
                        };
                    }
                }
                let c: Column = match transformed {
                    true => {
                        let new_value = Hstore::to_hstore(&hstore_key_values);
                        println!("transformed {:?}", &hstore_key_values);
                        Column::StringValue(column_name, new_value)
                    }
                    false => fallback_return_value
                };
                c
            }
            column => column
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use crate::{transformer, transformer::Transformer, types::Column};
    use crate::config::TransformerTypeConfig;
    use crate::source::hstore::Hstore;
    use crate::transformer::first_name::FirstNameTransformer;
    use crate::transformer::hstore_attr::{HstoreAttrOption, HstoreAttrOptions};
    use crate::transformer::mobile_number::MobileNumberOptions;

    use super::{HstoreAttrTransformer};

    fn change_mobile_transformer() -> HstoreAttrOption {
        let options = MobileNumberOptions { length: 11, country_code: 1 };
        HstoreAttrOption {
            attribute: "mobile".to_string(),
            transformer_type_config: TransformerTypeConfig::MobileNumber(Option::from(options)),
        }
    }

    fn mask_name_transformer() -> HstoreAttrOption {
        HstoreAttrOption {
            attribute: "first_name".to_string(),
            transformer_type_config: TransformerTypeConfig::FirstName,
        }
    }

    fn with_no_options() -> HstoreAttrOptions {
        HstoreAttrOptions { transformers: vec![] }
    }

    fn with_ill_fitting_options() -> HstoreAttrOptions {
        HstoreAttrOptions { transformers: vec![mask_name_transformer()] }
    }

    fn with_options() -> HstoreAttrOptions {
        HstoreAttrOptions { transformers: vec![change_mobile_transformer()] }
    }

    fn get_hstore_column() -> Column {
        let column_name = "merge_attributes".to_string();
        let column_value = r#"1"=>"5", "email"=>"joe1,hotpants@gmail.com", "mobile"=>"61 466 333 222", "id"=>"1234""#.to_string();
        Column::StringValue(column_name, column_value)
    }

    fn get_transformer(options: HstoreAttrOptions) -> HstoreAttrTransformer {
        HstoreAttrTransformer::new("github", "user", "merge_attributes", options)
    }

    #[test]
    fn test_transformation_with_no_options_no_changes_made() {
        let transformer = get_transformer(with_no_options());
        let column = get_hstore_column();
        let expected = column.string_value().unwrap().to_string();

        let transformed_column = transformer.transform(column);

        let actual = transformed_column.string_value().unwrap().to_string();
        println!("expected {}", expected);
        println!("actual {}", actual);
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_transformation_with_ill_filling_options_no_changes_made() {
        let transformer = get_transformer(with_ill_fitting_options());
        let column = get_hstore_column();
        let expected = column.string_value().unwrap().to_string();

        let transformed_column = transformer.transform(column);

        let actual = transformed_column.string_value().unwrap().to_string();
        println!("expected {}", expected);
        println!("actual {}", actual);
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_transformation_with_options_to_change() {
        let transformer = get_transformer(with_options());
        let column = get_hstore_column();
        let expected = column.string_value().unwrap().to_string();

        let transformed_column = transformer.transform(column);

        let actual = transformed_column.string_value().unwrap().to_string();
        println!("expected {}", expected);
        println!("actual {}", actual);
        assert_ne!(expected, actual);

        let expected_key_values = Hstore::from_hstore(expected);
        let expected_email = expected_key_values.get("email").unwrap().to_string();
        let expected_mobile = expected_key_values.get("mobile").unwrap().to_string();
        let expected_id = expected_key_values.get("id").unwrap().to_string();
        let expected_one = expected_key_values.get("1").unwrap().to_string();

        let actual_key_values = Hstore::from_hstore(actual);
        let actual_email = actual_key_values.get("email").unwrap().to_string();
        let actual_mobile = actual_key_values.get("mobile").unwrap().to_string();
        let actual_id = actual_key_values.get("id").unwrap().to_string();
        let actual_one = actual_key_values.get("1").unwrap().to_string();

        assert_eq!(expected_email, actual_email);
        assert_ne!(expected_mobile, actual_mobile);
        assert_eq!(expected_id, actual_id);
        assert_eq!(expected_one, actual_one);
    }
}