use std::io::{Error, ErrorKind};
use crate::config::{DatabaseSubsetConfig, OnlyTablesConfig, DbTableConfig, SourceConfig};
use crate::transformer::Transformer;

pub struct SourceOptions<'a> {
    pub transformers: &'a Vec<Box<dyn Transformer>>,
    pub skip_config: &'a Vec<DbTableConfig>,
    pub database_subset: &'a Option<Vec<DatabaseSubsetConfig>>,
    pub only_tables: &'a Vec<OnlyTablesConfig>,
}

impl SourceOptions<'_> {
    
    pub(crate) fn new<'a>(
        config: &'a SourceConfig,
        empty_config: &'a Vec<DbTableConfig>,
        default_config: &'a Vec<OnlyTablesConfig>,
        transformers: &'a mut Vec<Box<dyn Transformer>>
    ) -> Result<SourceOptions, Error> {
        let mut new_transformers = SourceOptions::new_transformers(config);
        transformers.append(&mut new_transformers);
        let skip_config = SourceOptions::new_skip_config(config, empty_config);
        let only_tables= SourceOptions::new_only_tables_config(config, default_config);

        match SourceOptions::check_tables_config(&skip_config, &only_tables) {
            Ok(_) => {
                let options : SourceOptions = SourceOptions {
                    transformers,
                    skip_config,
                    database_subset: &config.database_subset,
                    only_tables,
                };
                Ok(options)
            },
            Err(e) => Err(e)
        }

    }

    fn new_transformers(config: &SourceConfig) -> Vec<Box<dyn Transformer>> {
        let transformers = match &config.transformers {
            Some(transformers) => transformers
                .iter()
                .flat_map(|transformer| {
                    transformer.columns.iter().map(|column| {
                        column.transformer.transformer(
                            transformer.database.as_str(),
                            transformer.table.as_str(),
                            column.name.as_str(),
                        )
                    })
                })
                .collect::<Vec<_>>(),
            None => vec![],
        };
        transformers
    }

    fn new_skip_config<'a>(config: &'a SourceConfig, default: &'a Vec<DbTableConfig>) -> &'a Vec<DbTableConfig> {
        let skip_config = match &config.skip {
            Some(config) => config,
            None => default,
        };
        skip_config
    }

    fn new_only_tables_config<'a>(config: &'a SourceConfig, empty_config: &'a Vec<OnlyTablesConfig>) -> &'a Vec<OnlyTablesConfig> {
        let only_tables_config = match &config.only_tables {
            Some(config) => config,
            None => empty_config,
        };
        only_tables_config
    }

    fn check_tables_config(skip_config: &Vec<DbTableConfig>, only_tables_config: &Vec<OnlyTablesConfig>) -> Result<(), Error> {
        for only_table in only_tables_config {
            for skip in skip_config {
                if only_table.database == skip.database && only_table.table == skip.table {
                    let error= Error::new(
                        ErrorKind::Other,
                        format!(
                            "Table \"{}.{}\" cannot be both in \"only_table\" and in \"skip_table\" at the same time",
                            only_table.database,
                            only_table.table
                        )
                    );
                    return Err(error)
                }
            }
        }
        Ok(())
    }

}

#[cfg(test)]
mod tests {
    use crate::config::{DatabaseSubsetConfigStrategy, DatabaseSubsetConfigStrategyForeignKey, DbTableConfig, OnlyTablesConfig, SourceConfig, TransformerTypeConfig};
    use crate::source::source_options::SourceOptions;
    use crate::transformer::mobile_number::MobileNumberOptions;
    use crate::transformer::Transformer;

    fn get_source_config_yaml() -> String {
r#"
connection_uri: postgres://root:password@localhost:5432/root
skip:
  - database: public
    table: states
  - database: public
    table: order_details
transformers:
  - database: public
    table: employees
    columns:
      - name: first_name
        transformer_name: first-name
      - name: last_name
        transformer_name: random
      - name: mobile
        transformer_name: mobile-number
        transformer_options:
          country_code: 1
          length: 10
only_tables: # optional - dumps only specified tables.
  - database: public
    table: orders
  - database: public
    table: customers
database_subset:
  - database: public
    table: customers
    strategy_name: foreign-key
    strategy_options:
      condition: merchant_id in (1980, 1672, 1823)


"#.to_string()
    }

    #[test]
    fn parse_source_config() {
        let source_options_yaml = get_source_config_yaml();
        let config : SourceConfig  = serde_yaml::from_str(&source_options_yaml).unwrap();

        println!("parsed {:?}", &config);

        // connection uri
        assert_eq!(config.connection_uri.unwrap(), "postgres://root:password@localhost:5432/root".to_string());

        // skip tables
        let skip_config = config.skip.unwrap();
        let skip_table_1 = skip_config.get(0).unwrap();
        assert_eq!(skip_table_1.database, "public");
        assert_eq!(skip_table_1.table, "states");
        let skip_table_2 = skip_config.get(1).unwrap();
        assert_eq!(skip_table_2.database, "public");
        assert_eq!(skip_table_2.table, "order_details");

        // only tables
        let only_tables = config.only_tables.unwrap();
        let only_table_1 = only_tables.get(0).unwrap();
        assert_eq!(only_table_1.database, "public");
        assert_eq!(only_table_1.table, "orders");
        let only_table_2 = only_tables.get(1).unwrap();
        assert_eq!(only_table_2.database, "public");
        assert_eq!(only_table_2.table, "customers");

        // transformers
        let transformers_config = config.transformers.unwrap();
        let transformer_config = transformers_config.last().unwrap();
        assert_eq!(transformer_config.database, "public");
        assert_eq!(transformer_config.table, "employees");
        let columns = &transformer_config.columns;
        let column_1 = columns.get(0).unwrap();
        assert_eq!(column_1.name, "first_name");
        assert_eq!(column_1.transformer, TransformerTypeConfig::FirstName);
        let column_2 = columns.get(1).unwrap();
        assert_eq!(column_2.name, "last_name");
        assert_eq!(column_2.transformer, TransformerTypeConfig::Random);
        let column_3 = columns.get(2).unwrap();
        assert_eq!(column_3.name, "mobile");
        assert_eq!(column_3.transformer, TransformerTypeConfig::MobileNumber(Some(MobileNumberOptions{country_code: 1, length: 10})));

        // subset
        let subsets = config.database_subset.unwrap();
        let subset_1 = subsets.get(0).unwrap();
        assert_eq!(subset_1.database, "public");
        assert_eq!(subset_1.table, "customers");
        match &subset_1.strategy {
            DatabaseSubsetConfigStrategy::ForeignKey(c) => {
                assert_eq!(c.condition, String::from("merchant_id in (1980, 1672, 1823)"));
            },
            _ => {
                println!("failed to extracte strategy");
                assert!(false);
            }
        }
    }


    #[test]
    fn parse_and_generate_source_config() {

        let source_config_yaml = get_source_config_yaml();
        let config : SourceConfig  = serde_yaml::from_str(&source_config_yaml).unwrap();

        let empty_config: Vec<DbTableConfig> = vec![];
        let default_config: Vec<OnlyTablesConfig> = vec![];
        let mut transformers : Vec<Box<dyn Transformer>> = vec![];

        match SourceOptions::new(&config, &empty_config, &default_config, &mut transformers) {
            Ok(o) => {
                println!("some thing went right");
                let last_transformer = o.transformers.last().unwrap();
                assert_eq!(last_transformer.database_name(), "public");
                assert_eq!(last_transformer.table_name(), "employees");
                assert_eq!(last_transformer.column_name(), "mobile");

                let last_skip_config = o.skip_config.last().unwrap();
                assert_eq!(last_skip_config.table, "order_details");
                assert_eq!(last_skip_config.database, "public");

            },
            Err(e) => {
                println!("some thing went horrendously wrong {}", e);
                assert!(false)
            }
        };


    }


}