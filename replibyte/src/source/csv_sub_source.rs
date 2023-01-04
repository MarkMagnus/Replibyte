use std::collections::{HashMap};
use crate::config::{DbColumnConfig};
use crate::types::{Column};
use csv::{IntoInnerError, Reader, StringRecord, Writer, WriterBuilder};
use crate::transformer::Transformer;

pub struct CsvSubSource<'a> {
    pub csv: String,
    pub columns: Vec<DbColumnConfig>,
    pub transformers: HashMap<String, &'a Box<dyn Transformer>>,
}

impl CsvSubSource<'_> {
    pub fn new(csv: String, columns: Vec<DbColumnConfig>, transformers: HashMap<String, &Box<dyn Transformer>>) -> CsvSubSource {
        let mut sorted_columns = columns.clone();
        sorted_columns.sort_by(|a,b| a.ordinal.cmp(&b.ordinal));
        CsvSubSource { csv, columns: sorted_columns, transformers }
    }

    pub fn to_map(&self, record: StringRecord) -> HashMap<String, Column> {
        let mut row_columns: HashMap<String, Column> = HashMap::new();

        for column_config in self.columns.iter() {
            let column_name = column_config.column.to_string();
            let column_data_type = column_config.data_type.as_str();
            let ordinal = (column_config.ordinal - 1) as usize;
            let record_str = record.get(ordinal).unwrap().to_string();

            match column_data_type {
                "smallint" | "integer" | "bigint" | "decimal" | "numeric" | "real" | "double precision" | "smallserial" | "serial" | "bigserial" => {
                    let number_value: i128 = record_str.parse().unwrap();
                    let number_name = column_name.clone();
                    row_columns.insert(column_name, Column::NumberValue(number_name, number_value));
                }
                "boolean" => {
                    let boolean_value: bool = record_str.parse().unwrap();
                    let boolean_name = column_name.clone();
                    row_columns.insert(column_name, Column::BooleanValue(boolean_name, boolean_value));
                }
                "float" | "money" => {
                    let float_value: f64 = record_str.parse().unwrap();
                    let float_name = column_name.clone();
                    row_columns.insert(column_name, Column::FloatNumberValue(float_name, float_value));
                }
                _ => {
                    let string_value = record.get(ordinal).unwrap().to_string();
                    let string_name = column_name.clone();
                    row_columns.insert(column_name, Column::StringValue(string_name, string_value));
                }
            };
        }

        row_columns
    }

    pub fn transform(&self, mut row_columns: HashMap<String, Column>) -> Vec<String> {
        for (attribute, transformer) in self.transformers.iter() {
            let old = row_columns.get(attribute).unwrap().clone();
            let new = transformer.transform(old);
            row_columns.insert(attribute.to_string(), new);
        }

        let mut transformed: Vec<String> = Vec::with_capacity(row_columns.len());

        for column_config in self.columns.iter() {
            let column_name = column_config.column.as_str();
            let column = row_columns.get(column_name).unwrap();
            let position = (column_config.ordinal + 1) as usize;
            match column {
                Column::BooleanValue(_k, v) => {
                    transformed.push(v.to_string());
                }
                Column::FloatNumberValue(_k, v) => {
                    transformed.push(v.to_string());
                }
                Column::NumberValue(_k, v) => {
                    transformed.push( v.to_string());
                }
                Column::StringValue(_k, v) => {
                    transformed.push(v.to_string());
                }
                Column::CharValue(_k, v) => {
                    transformed.push(v.to_string());
                }
                Column::None(_k) => {
                    transformed.push("".to_string());
                }
            }
        }

        transformed
    }

    pub fn to_csv(&self, transformed: Vec<String>) -> String {
        let mut wtr = WriterBuilder::new().has_headers(false).delimiter(b'\t').from_writer(vec![]);

        wtr.write_record(transformed);

        let output: Result<Vec<u8>, IntoInnerError<Writer<Vec<u8>>>> = wtr.into_inner();
        let csv = match output {
            Ok(csv) => {
                String::from_utf8(csv).unwrap()
            }
            Err(e) => {
                println!("failed to extract from writer {}", e);
                "".to_string()
            }
        };
        csv
    }

    pub fn reader(&self) -> Reader<&[u8]> {
        let csv_buffer = self.csv.as_bytes();

        csv::ReaderBuilder::new()
            .has_headers(false).delimiter(b'\t').double_quote(false)
            .escape(Some(b'\\')).flexible(true).from_reader(csv_buffer)

    }

    pub fn process(&self) -> String {
        let mut lines: Vec<String> = Vec::new();

        for result in self.reader().records() {
            let record = result.unwrap();
            let row_columns = self.to_map(record);
            let transformed = self.transform(row_columns);

            let line = self.to_csv(transformed).to_string();
            lines.push(line);
        }

        lines.join("")
    }
}


#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use csv::StringRecord;
    use crate::config::DbColumnConfig;
    use crate::config::TransformerTypeConfig::MobileNumber;
    use crate::source::csv_sub_source::CsvSubSource;
    use crate::transformer::email::EmailTransformer;
    use crate::transformer::mobile_number::{MobileNumberOptions, MobileNumberTransformer};
    use crate::transformer::Transformer;

    fn get_origin_csv() -> String {
        "Bob\tJoe\tbob.joe@gmail.com\t61444222333".to_string()
    }

    fn get_config_columns() -> Vec<DbColumnConfig> {
        let config_column = vec![
            DbColumnConfig::new("first_name".to_string(), "varchar".to_string(), 1),
            DbColumnConfig::new("last_name".to_string(), "varchar".to_string(), 2),
            DbColumnConfig::new("email".to_string(), "varchar".to_string(), 3),
            DbColumnConfig::new("mobile_number".to_string(), "varchar".to_string(), 4),
        ];
        Vec::from(config_column)
    }

    fn get_empty_transformers<'a>() -> HashMap<String, &'a Box<dyn Transformer>> {
        let mut transformers: HashMap<String, &'a Box<dyn Transformer>> = HashMap::new();
        transformers
    }

    fn get_csv_sub_source<'a>(transformers : HashMap<String, &'a Box<dyn Transformer>>) -> CsvSubSource<'a>{
        let csv = get_origin_csv();
        let columns = get_config_columns();
        let transformers = transformers;
        CsvSubSource::new(csv, columns, transformers)
    }

    fn get_last_record(sub_source: &CsvSubSource) -> StringRecord {
        sub_source.reader().records().last().unwrap().unwrap()
    }

    #[test]
    fn should_reader_read() {
        let sub_source = get_csv_sub_source(get_empty_transformers());
        let record = get_last_record(&sub_source);
        assert_eq!("Bob", record.get(0).unwrap());
        assert_eq!("Joe", record.get(1).unwrap());
        assert_eq!("bob.joe@gmail.com", record.get(2).unwrap());
        assert_eq!("61444222333", record.get(3).unwrap())
    }

    #[test]
    fn should_goto_map() {
        let sub_source = get_csv_sub_source(get_empty_transformers());
        let record = get_last_record(&sub_source);
        let column_map = sub_source.to_map(record);

        let first_name_column = column_map.get("first_name").unwrap();
        let last_name_column = column_map.get("last_name").unwrap();
        let email_column = column_map.get("email").unwrap();
        let mobile_column = column_map.get("mobile_number").unwrap();

        assert_eq!(first_name_column.string_value().unwrap(), "Bob".to_string());
        assert_eq!(last_name_column.string_value().unwrap(), "Joe".to_string());
        assert_eq!(email_column.string_value().unwrap(), "bob.joe@gmail.com".to_string());
        assert_eq!(mobile_column.string_value().unwrap(), "61444222333".to_string());
    }

    #[test]
    fn should_transform<'a>() {

        let email_transformer = EmailTransformer::new("public".to_string(), "contacts".to_string(), "email".to_string());
        let mobile_transformer = MobileNumberTransformer::new("public".to_string(), "contact".to_string(), "mobile_number".to_string(), MobileNumberOptions::default());

        let mut transformers: HashMap<String, &Box<dyn Transformer>> = HashMap::new();
        let boxed_email_transformer: Box<dyn Transformer> = Box::new(email_transformer);
        let boxed_mobile_transformer: Box<dyn Transformer> = Box::new(mobile_transformer);
        transformers.insert("email".to_string(), &boxed_email_transformer);
        transformers.insert("mobile_number".to_string(), &boxed_mobile_transformer);

        let sub_source = get_csv_sub_source(transformers);
        let record = get_last_record(&sub_source);
        let column_map = sub_source.to_map(record);

        let transformed = sub_source.transform(column_map);

        println!("transformed {:?}", transformed);

        let first_name = transformed.get(0).unwrap();
        let last_name = transformed.get(1).unwrap();
        let email = transformed.get(2).unwrap();
        let mobile_number = transformed.get(3).unwrap();

        assert_eq!(first_name, "Bob");
        assert_eq!(last_name, "Joe");
        assert_ne!(email, "bob.joe@gmail.com");
        assert_ne!(mobile_number, "61444222333");
    }

    #[test]
    fn process_should_not_error() {
        let sub_source = get_csv_sub_source(get_empty_transformers());
        let transformed_csv = sub_source.process();
        println!("processed csv {}", transformed_csv);
        assert_ne!(transformed_csv, get_origin_csv());
    }

    #[test]
    fn should_goto_csv() {
        let sub_source = get_csv_sub_source(get_empty_transformers());
        let to_output = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let output = sub_source.to_csv(to_output);
        assert_eq!(output, "a\tb\tc\n");

    }

}




