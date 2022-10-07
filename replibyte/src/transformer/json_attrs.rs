use bson::doc;
use serde::{Deserialize, Serialize};
use crate::config::TransformerTypeConfig;
use crate::transformer::{Transformer};
use crate::types::Column;
use crate::source::json::Json;

pub struct JsonAttrTransformer {
    database_name: String,
    table_name: String,
    column_name: String,
    options: JsonAttrOptions,
}

impl Default for JsonAttrTransformer {
    fn default() -> Self {
        JsonAttrTransformer {
            database_name: String::default(),
            table_name: String::default(),
            column_name: String::default(),
            options: JsonAttrOptions::default(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct JsonAttrOption {
    pub attribute: String,
    #[serde(flatten)]
    pub transformer_type_config: TransformerTypeConfig,
}


impl Default for JsonAttrOption {
    fn default() -> Self {
        JsonAttrOption {
            attribute: String::default(),
            transformer_type_config: TransformerTypeConfig::Blank,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct JsonAttrOptions {
    pub transformers: Vec<JsonAttrOption>,
}

impl Default for JsonAttrOptions {
    fn default() -> Self {
        JsonAttrOptions {
            transformers: vec![JsonAttrOption::default()]
        }
    }
}

impl JsonAttrTransformer {
    pub fn new<S>(database_name: S, table_name: S, column_name: S, options: JsonAttrOptions) -> Self
        where
            S: Into<String>,
    {
        JsonAttrTransformer {
            table_name: table_name.into(),
            column_name: column_name.into(),
            database_name: database_name.into(),
            options,
        }
    }
}

impl Transformer for JsonAttrTransformer {
    fn id(&self) -> &str {
        "blank"
    }

    fn description(&self) -> &str { "change json key values using appropriate transformers" }

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
                let mut json_key_values = Json::from_json(value);
                for json_attr_option in self.options.transformers.iter() {
                    let attribute_key_str = json_attr_option.attribute.as_str();
                    let attribute_key = attribute_key_str.to_string();

                    if json_key_values.contains_key(&attribute_key) {
                        let required_transformer = json_attr_option.transformer_type_config.transformer(self.database_name(), self.table_name(), attribute_key_str);

                        let attribute_value = match json_key_values.get(&attribute_key) {
                            Some(v) => v,
                            None => ""
                        };

                        let attribute_column = Column::StringValue(attribute_key_str.to_string(), attribute_value.to_string());
                        match required_transformer.transform(attribute_column) {
                            Column::StringValue(_, new_value) => {
                                json_key_values.insert(attribute_key, new_value);
                                transformed = true;
                            }
                            _ => println!("cannot transform {}", column_name)
                        };
                    }
                }
                let c: Column = match transformed {
                    true => {
                        let new_value = Json::to_json(&json_key_values);
                        println!("transformed {:?}", &json_key_values);
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
    use crate::{transformer::Transformer, types::Column};
    use crate::config::TransformerTypeConfig;
    use crate::source::json::Json;
    use crate::transformer::json_attrs::{JsonAttrOption, JsonAttrOptions};

    use crate::transformer::mobile_number::MobileNumberOptions;

    use super::{JsonAttrTransformer};

    fn change_mobile_transformer() -> JsonAttrOption {
        let options = MobileNumberOptions { length: 11, country_code: 1 };
        JsonAttrOption {
            attribute: "mobile".to_string(),
            transformer_type_config: TransformerTypeConfig::MobileNumber(Option::from(options)),
        }
    }

    fn mask_name_transformer() -> JsonAttrOption {
        JsonAttrOption {
            attribute: "first_name".to_string(),
            transformer_type_config: TransformerTypeConfig::FirstName,
        }
    }

    fn with_no_options() -> JsonAttrOptions {
        JsonAttrOptions { transformers: vec![] }
    }

    fn with_ill_fitting_options() -> JsonAttrOptions {
        JsonAttrOptions { transformers: vec![mask_name_transformer()] }
    }

    fn with_options() -> JsonAttrOptions {
        JsonAttrOptions { transformers: vec![change_mobile_transformer()] }
    }

    fn get_json_column() -> Column {
        let column_name = "merge_attributes".to_string();
        let column_value = r#"{"1": "5", "email": "joe1,hotpants@gmail.com\", "mobile": "61 466 333 222", "id": "1234"}"#.to_string();
        Column::StringValue(column_name, column_value)
    }

    fn get_transformer(options: JsonAttrOptions) -> JsonAttrTransformer {
        JsonAttrTransformer::new("github", "user", "merge_attributes", options)
    }

    #[test]
    fn test_transformation_with_no_options_no_changes_made() {
        let transformer = get_transformer(with_no_options());
        let column = get_json_column();
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
        let column = get_json_column();
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
        let column = get_json_column();
        let expected = column.string_value().unwrap().to_string();

        let transformed_column = transformer.transform(column);

        let actual = transformed_column.string_value().unwrap().to_string();
        println!("expected {}", expected);
        println!("actual {}", actual);
        assert_ne!(expected, actual);

        let expected_key_values = Json::from_json(expected);
        let expected_email = expected_key_values.get("email").unwrap().to_string();
        let expected_mobile = expected_key_values.get("mobile").unwrap().to_string();
        let expected_id = expected_key_values.get("id").unwrap().to_string();
        let expected_one = expected_key_values.get("1").unwrap().to_string();

        let actual_key_values = Json::from_json(actual);
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