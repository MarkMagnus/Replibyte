use std::collections::HashMap;
use std::io::{BufReader, Error, ErrorKind, Read};
use std::process::{Command, Stdio};
use sorted_vec::SortedVec;
use dump_parser::utils::{list_sql_copy_csv_from_dump_reader, list_sql_queries_from_dump_reader, ListQueryResult};
use crate::config::{DatabaseSubsetConfigStrategy, DbColumnConfig, DbTableConfig, SourceConfig};
use crate::connector::Connector;
use crate::DatabaseSubsetConfig;
use crate::source::csv_sub_source::CsvSubSource;
use crate::source::Source;
use crate::transformer::Transformer;
use crate::types::{OriginalQuery, Query};
use crate::utils::{binary_exists, wait_for_command};
use super::SourceOptions;

use mockall_double::double;

#[double]
use postgres_schema::QueryStruct;
use crate::source::postgres_schema::postgres_schema;

pub struct Postgres<'a> {
    pub(crate) connection_uri: &'a str,
    host: &'a str,
    port: u16,
    database: &'a str,
    username: &'a str,
    password: &'a str,
}

impl<'a> Postgres<'a> {
    pub fn new(
        connection_uri: &'a str,
        host: &'a str,
        port: u16,
        database: &'a str,
        username: &'a str,
        password: &'a str,
    ) -> Self {
        Postgres {
            connection_uri,
            host,
            port,
            database,
            username,
            password,
        }
    }
}

/// require both pg_dump and psql to continue
/// for data
/// psql -Atx <connection string>  -c "\copy <query> to stdout with (delimiter E'\t', FORMAT csv, QUOTE E'T' );"
/// for structure
/// pg_dump -d <connection string> <options>
impl<'a> Connector for Postgres<'a> {
    fn init(&mut self) -> Result<(), Error> {
        pg_dump_exists().and_then(|_n| pg_dump_exists())
    }
}

fn pg_dump_exists() -> Result<(), Error> {
    binary_exists("pg_dump")
}

fn psql_exists() -> Result<(), Error> {
    binary_exists("psql")
}

fn get_dump_args(options: &SourceOptions, postgres: &Postgres) -> Vec<String> {
    let mut dump_args = vec![
        "--no-owner",       // skip restoration of object ownership
        "-d",
        postgres.connection_uri,
        "--schema-only",
    ];

    let only_tables_args: Vec<String> = options
        .only_tables
        .iter()
        .map(|cfg| format!("--table={}.{}", cfg.database, cfg.table))
        .collect();

    let mut only_tables_args: Vec<&str> = only_tables_args
        .iter()
        .map(String::as_str)
        .collect();

    dump_args.append(&mut only_tables_args);
    let a: Vec<String> = dump_args.into_iter().map(|s| s.to_string()).collect();
    a
}

fn dump_database_schema<F: FnMut(OriginalQuery, Query)>(options: &SourceOptions, postgres: &Postgres, query_callback: &mut F) -> Result<(), Error> {
    let dump_args = get_dump_args(options, postgres);
    let mut process = Command::new("pg_dump")
        .args(dump_args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;

    let stdout = process
        .stdout
        .take()
        .ok_or_else(|| Error::new(ErrorKind::Other, "Could not capture standard output."))?;

    let reader = BufReader::new(stdout);
    read_schema(reader, query_callback);

    wait_for_command(&mut process)
}

///psql -Atx <connection string>  -c "\copy (<query>) to stdout with ( delimiter E'\t', FORMAT csv, QUOTE E'T' );"
fn get_copy_args(subset_config: &DatabaseSubsetConfig, connection_uri: &str) -> Vec<String> {
    let mut copy_args = vec![
        "-Atx",
        connection_uri,
        "-c",
    ];

    let query: String = match &subset_config.strategy {
        DatabaseSubsetConfigStrategy::None => {
            let a = format!("select * from {}.{}",
                            subset_config.database, subset_config.table);
            a
        }
        DatabaseSubsetConfigStrategy::ForeignKey(fks) => {
            let a = format!("select * from {}.{} where {}",
                            subset_config.database, subset_config.table, fks.condition);
            a
        }
        DatabaseSubsetConfigStrategy::Random(rs) => {
            let a = format!("select * from {}.{} tablesample system({}) order by random()",
                            subset_config.database, subset_config.table, rs.percent);
            a
        }
    };
    let command: String = format!("\\copy ({}) to stdout with (delimiter E'\\t', FORMAT csv, QUOTE E'T');", query);
    copy_args.push(&command);
    let a: Vec<String> = copy_args.into_iter().map(|s| s.to_string()).collect();
    a
}

fn dump_database_data<F: FnMut(OriginalQuery, Query)>(options: &SourceOptions, postgres: &Postgres, query_callback: &mut F) -> Result<(), Error> {
    let query_struct = QueryStruct::new(String::from(postgres.connection_uri));
    for subset_config in database_tables_subset_config(options, &query_struct) {
        match dump_table_data(subset_config, options, &query_struct, query_callback) {
            Err(e) => return Err(e),
            _ => {}
        }
    };
    Ok(())
}

/*
COPY public.contact(email, mobile_number, fields, cache, first_name, last_name) from stdin (delimiter E'\t', FORMAT csv, QUOTE E'T');
joe.blogs@gmail.com	61466343749	{"1": "2", "3": "4", "a": "2", "email": "joe.blogs@gmail.com"}	"1"=>"2", "3"=>"4", "a"=>"2", "\"email\""=>"\"joe.blogs@gmail.com\""	mark	magnus
\.
*/

fn generate_sql_copy_template(subset_config: &DatabaseSubsetConfig, columns: &SortedVec<DbColumnConfig>) -> String {
    let ord_column_names: Vec<String> = columns.iter().map(|dbcc| dbcc.column.to_string()).collect();
    let ord_column_names_str = ord_column_names.join(",");
    let template = format!("\\COPY {}.{} ({}) FROM stdin (delimiter E'\t', FORMAT csv, QUOTE E'T');",
                            subset_config.database, subset_config.table, ord_column_names_str);
    template
}

fn dump_table_data<F: FnMut(OriginalQuery, Query)>(
    subset_config: DatabaseSubsetConfig,
    options: &SourceOptions,
    query_struct: &QueryStruct,
    query_callback: &mut F
) -> Result<(), Error> {
    let copy_args = get_copy_args(&subset_config, &query_struct.connection_uri());
    let columns = query_struct.database_columns(subset_config.table_config());

    let mut process = Command::new("psql")
        .args(copy_args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;

    let stdout = process
        .stdout
        .take()
        .ok_or_else(|| Error::new(ErrorKind::Other, "Could not capture standard output."))?;

    let reader = BufReader::new(stdout);
    read_table_data(reader, options, subset_config, query_callback, columns);

    wait_for_command(&mut process)
}

fn subset_tables(options: &SourceOptions) -> Vec<DbTableConfig> {
    let tables: Vec<DbTableConfig> = match options.database_subset {
        Some(subset) => {
            let t: Vec<DbTableConfig> = subset
                .iter()
                .map(|config|
                    DbTableConfig::new(config.database.to_string(), config.table.to_string())
                ).collect();
            t
        }
        None => Vec::new()
    };
    tables
}

fn database_tables_subset_config(options: &SourceOptions, query_struct: &QueryStruct) -> Vec<DatabaseSubsetConfig> {
    let mut table_subset_config: Vec<DatabaseSubsetConfig> = vec![];
    let subset_tables = subset_tables(options);
    for table in query_struct.database_tables() {
        // unless specified in subset or skipping that table then don't generate default subset config
        // limit table config ignores skip tables configuration
        let limit_table_config = options.only_tables.len() > 1;
        if limit_table_config {
            if !subset_tables.contains(&table) && options.only_tables.contains(&table.only_config()) {
                let subset_config = DatabaseSubsetConfig::new(table.database.to_string(), table.table.to_string());
                table_subset_config.push(subset_config);
            }
        } else {
            if !subset_tables.contains(&table) && !options.skip_config.contains(&table) {
                let subset_config = DatabaseSubsetConfig::new(table.database.to_string(), table.table.to_string());
                table_subset_config.push(subset_config);
            }
        }

    }
    match options.database_subset {
        Some(subsets) => table_subset_config.append(&mut subsets.clone()),
        None => println!("not subsets present")
    }
    table_subset_config
}

impl<'a> Source for Postgres<'a> {
    fn read<F: FnMut(OriginalQuery, Query)>(
        &self,
        options: SourceOptions,
        mut query_callback: F,
    ) -> Result<(), Error> {

        // use pg_dump to capture the schema
        // use copy via psql to capture the data
        match dump_database_schema(&options, &self, &mut query_callback) {
            Ok(_) =>
                match dump_database_data(&options, &self, &mut query_callback) {
                    Err(e) => Err(e),
                    _ => Ok(())
                }
            Err(e) =>
                Err(e)
        }
    }
}

/// schema sourced from pg_dump, and thus lots of query
/// no transformations required, output can be read verbatim
pub fn read_schema<R: Read, F: FnMut(OriginalQuery, Query)>(reader: BufReader<R>, query_callback: &mut F) {
    list_sql_queries_from_dump_reader(reader, |query| {
        //queries.push(query.to_string());
        unmodified_callback(query.to_string(), query_callback);
        ListQueryResult::Continue
    }).unwrap();
}

pub fn unmodified_callback<F: FnMut(OriginalQuery, Query)>(query: String, query_callback: &mut F) {
    query_callback(
        Query(query.as_bytes().to_vec()),
        Query(query.as_bytes().to_vec()),
    );
}

pub fn modified_callback<F: FnMut(OriginalQuery, Query)>(a_query: String, b_query: String, query_callback: &mut F) {
    query_callback(
        Query(a_query.as_bytes().to_vec()),
        Query(b_query.as_bytes().to_vec()),
    );
}

/// table data is csv formatted data, produced by psql calling the copy command
/// batching is possible here
pub fn read_table_data<R: Read, F: FnMut(OriginalQuery, Query)>(
    reader: BufReader<R>,
    options: &SourceOptions,
    subset_config: DatabaseSubsetConfig,
    query_callback: &mut F,
    columns: SortedVec<DbColumnConfig>,
) {
    let sql_copy_template = generate_sql_copy_template(&subset_config, &columns);

    let _ = list_sql_copy_csv_from_dump_reader(reader, 1000, |csv_rows| {
        let query = format!("{}\n{}\n\\.\n", sql_copy_template, csv_rows);

        match get_applicable_transformers(subset_config.table_config(), options) {
            Some(transformers) => {
                let transformed_csv_rows: String = transform_csv(csv_rows.to_string(), &columns, transformers);
                let transformed_query = format!("{}\n{}\n\\.", sql_copy_template, transformed_csv_rows);
                modified_callback(query.clone(), transformed_query, query_callback)
            }
            None => unmodified_callback(query.clone(), query_callback)
        };
        ListQueryResult::Continue
    });
}

pub fn get_applicable_transformers<'a>(table: DbTableConfig, options: &SourceOptions<'a>) -> Option<HashMap<String, &'a Box<dyn Transformer>>> {
    // create a map variable with Transformer by column_name
    let mut transformers: HashMap<String, &Box<dyn Transformer>> = HashMap::new();
    for transformer in options.transformers {
        if transformer.table_name() == table.table && transformer.database_name() == table.database {
            let _ = transformers.insert(
                transformer.column_name().to_string(),
                transformer,
            );
        }
    }
    if transformers.len() == 0 {
        None
    } else {
        Some(transformers)
    }
}

pub fn transform_csv(csv: String, columns: &SortedVec<DbColumnConfig>, transformers: HashMap<String, &Box<dyn Transformer>>) -> String {
    let csv = CsvSubSource::new(csv, columns.to_vec(), transformers).process();
    csv
}

#[cfg(test)]
mod tests {
    use sorted_vec::SortedVec;
    use crate::config::{DbColumnConfig, DbTableConfig, OnlyTablesConfig, SourceConfig};
    use crate::source::postgres::{database_tables_subset_config, generate_sql_copy_template, get_applicable_transformers, get_copy_args, get_dump_args, Postgres, subset_tables};
    use crate::source::SourceOptions;
    use crate::transformer::Transformer;
    use crate::config::DatabaseSubsetConfigStrategy::ForeignKey;

    use super::*;
    use mockall_double::double;


    fn get_postgres() -> Postgres<'static> {
        Postgres::new("postgres://root:password@localhost:5432/root",
                      "localhost",
                      5432,
                      "root",
                      "root",
                      "password"
        )
    }

    fn get_source_yaml() -> String {
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
    fn should_collect_subset_tables() {
        let source_options_yaml = get_source_yaml();
        let config: SourceConfig = serde_yaml::from_str(&source_options_yaml).unwrap();
        let empty_config: Vec<DbTableConfig> = vec![];
        let default_config: Vec<OnlyTablesConfig> = vec![];
        let mut transformers: Vec<Box<dyn Transformer>> = vec![];

        let options = SourceOptions::new(&config, &empty_config, &default_config, &mut transformers).unwrap();

        let tables = subset_tables(&options);

        println!("tables {:?}", tables);

        let table = tables.last().unwrap();
        assert!(table.table == "customers");
        assert!(table.database == "public");
    }

    #[test]
    fn should_collect_subset_config(){
        let source_options_yaml = get_source_yaml();
        let config: SourceConfig = serde_yaml::from_str(&source_options_yaml).unwrap();
        let empty_config: Vec<DbTableConfig> = vec![];
        let default_config: Vec<OnlyTablesConfig> = vec![];
        let mut transformers: Vec<Box<dyn Transformer>> = vec![];

        let options = SourceOptions::new(&config, &empty_config, &default_config, &mut transformers).unwrap();

        let postgres = get_postgres();

        let mut query_struct_mock = QueryStruct::default();
        query_struct_mock.expect_database_tables().returning( ||
            vec![
                DbTableConfig::new(String::from("public"), String::from("customers")),
                DbTableConfig::new(String::from("public"), String::from("orders")),
                DbTableConfig::new(String::from("public"), String::from("unrequires")),
            ] as Vec<DbTableConfig>
            );

        let subset_configs = database_tables_subset_config(&options, &query_struct_mock);

        println!("config {:?}", subset_configs);

        assert!(subset_configs.len() == 2);

        let no_subset_config = subset_configs.first().unwrap();
        assert!(&no_subset_config.database == "public");
        assert!(&no_subset_config.table == "orders");
        let no_strategy = &no_subset_config.strategy;
        match no_strategy {
            DatabaseSubsetConfigStrategy::None => {},
            _ => {
                println!("incorrect no strategy found");
                assert!(false);
            }
        }

        let subset_config = subset_configs.last().unwrap();
        assert!(&subset_config.database == "public");
        assert!(&subset_config.table == "customers");
        let strategy = &subset_config.strategy;
        match strategy {
            ForeignKey(strategy_config) => {
                assert!(strategy_config.condition == "merchant_id in (1980, 1672, 1823)");
            },
            _ => {
                println!("incorrect strategy found");
                assert!(false);
            }
        }
    }

    #[test]
    fn should_generate_sql_copy_template() {
        let source_options_yaml = get_source_yaml();
        let config: SourceConfig = serde_yaml::from_str(&source_options_yaml).unwrap();
        let database_subset= config.database_subset;
        let subset_configs = database_subset.unwrap();
        let subset_config = subset_configs.last().unwrap();

        let raw_columns: Vec<DbColumnConfig> = vec![
            DbColumnConfig::new(String::from("id"), String::from("integer"), 1),
            DbColumnConfig::new(String::from("merchant_id"), String::from("integer"), 2),
            DbColumnConfig::new(String::from("email"), String::from("USER-DEFINED"), 3),
            DbColumnConfig::new(String::from("mobile_number"), String::from("character varchar"), 4),
            DbColumnConfig::new(String::from("unsubscribed"), String::from("boolean"), 5),
            DbColumnConfig::new(String::from("values"), String::from("jsonb"), 6),
            DbColumnConfig::new(String::from("validated"), String::from("boolean"), 7),
            DbColumnConfig::new(String::from("created_at"), String::from("timestamp without time zone"), 8),
        ];
        let columns: SortedVec<DbColumnConfig> = SortedVec::from(raw_columns);

        let actual_sql = generate_sql_copy_template(subset_config, &columns);

        println!("actual sql {}", actual_sql);
        let expected_sql = "\\COPY public.customers (id,merchant_id,email,mobile_number,unsubscribed,values,validated,created_at) FROM stdin (delimiter E'\t', FORMAT csv, QUOTE E'T');".to_string();
        println!("expected sql {}", expected_sql);
        assert!(actual_sql == expected_sql);
    }

    #[test]
    fn should_assemble_get_dump_args() {
        let source_options_yaml = get_source_yaml();
        let config: SourceConfig = serde_yaml::from_str(&source_options_yaml).unwrap();
        let empty_config: Vec<DbTableConfig> = vec![];
        let default_config: Vec<OnlyTablesConfig> = vec![];
        let mut transformers: Vec<Box<dyn Transformer>> = vec![];

        let options = SourceOptions::new(&config, &empty_config, &default_config, &mut transformers).unwrap();

        let postgres = get_postgres();

        let args = get_dump_args(&options, &postgres);
        println!("dump args {:?}", args);

        let a1 = args.get(0).unwrap();
        let a2 = args.get(1).unwrap();
        let a3 = args.get(2).unwrap();
        let a4 = args.get(3).unwrap();
        let a5 = args.get(4).unwrap();
        let a6 = args.get(5).unwrap();

        assert!(a1 == "--no-owner");
        assert!(a2 == "-d");
        assert!(a3 == "postgres://root:password@localhost:5432/root");
        assert!(a4 == "--schema-only");
        assert!(a5 == "--table=public.orders");
        assert!(a6 == "--table=public.customers");
    }

    #[test]
    fn should_assemble_get_copy_args(){
        let source_options_yaml = get_source_yaml();
        let config: SourceConfig = serde_yaml::from_str(&source_options_yaml).unwrap();
        let database_subset= config.database_subset;
        let subset_configs = database_subset.unwrap();
        let subset_config = subset_configs.last().unwrap();

        let connection_uri = "postgres://root:password@localhost:5432/root";
        let args = get_copy_args(subset_config, connection_uri);
        println!("copy args {:?}", args);

        let a1 = args.get(0).unwrap();
        let a2 = args.get(1).unwrap();
        let a3 = args.get(2).unwrap();
        let a4 = args.get(3).unwrap();

        assert!(a1 == "-Atx");
        assert!(a2 == "postgres://root:password@localhost:5432/root");
        assert!(a3 == "-c");

        let expect_query = "\\copy (select * from public.customers where merchant_id in (1980, 1672, 1823)) to stdout with (delimiter E'\\t', FORMAT csv, QUOTE E'T');";
        assert_eq!(a4, expect_query);
    }

    #[test]
    fn should_extract_applicable_transformers() {

        let source_options_yaml = get_source_yaml();
        let config: SourceConfig = serde_yaml::from_str(&source_options_yaml).unwrap();

        let empty_config: Vec<DbTableConfig> = vec![];
        let default_config: Vec<OnlyTablesConfig> = vec![];
        let mut transformers: Vec<Box<dyn Transformer>> = vec![];

        let options = SourceOptions::new(&config, &empty_config, &default_config, &mut transformers).unwrap();
        let postgres = get_postgres();
        let table_config = DbTableConfig::new(String::from("public"), String::from("employees"));

        let applicable_transformers = get_applicable_transformers(table_config, &options).unwrap();

        let first_name_transformer = applicable_transformers.get("first_name");
        let last_name_transformer = applicable_transformers.get("last_name");
        let mobile_transformer = applicable_transformers.get("mobile");
        let email_transformer = applicable_transformers.get("email");

        assert!(matches!(first_name_transformer, Some {..}));
        assert!(matches!(last_name_transformer, Some{..}));
        assert!(matches!(mobile_transformer, Some{..}));
        assert!(matches!(email_transformer, None{..}));
    }
}
