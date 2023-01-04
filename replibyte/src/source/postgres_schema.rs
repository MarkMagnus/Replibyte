pub mod postgres_schema {
    use std::ffi::CString;
    use postgres::{Client, NoTls};
    use sorted_vec::SortedVec;
    use crate::config::{DbColumnConfig, DbTableConfig};
    use crate::source::postgres::Postgres;

    #[cfg(test)]
    use mockall::automock;

    pub struct QueryStruct {
        connection_uri: String
    }

    impl Default for QueryStruct {
        fn default() -> Self {
            QueryStruct { connection_uri: String::default() }
        }
    }

    #[cfg_attr(test, automock)]
    impl QueryStruct {

        pub fn new(connection_uri: String) -> Self {
            Self { connection_uri }
        }

        pub fn connection_uri(&self) -> String {
            self.connection_uri.clone()
        }

        /// only public tables are included automatically
        pub fn database_tables(&self) -> Vec<DbTableConfig> {
            let mut table_names: Vec<DbTableConfig> = vec![];
            match Client::connect(self.connection_uri.as_str(), NoTls) {
                Ok(mut client) => {
                    let query = "SELECT table_name FROM information_schema.tables where table_schema = 'public' and table_type = 'BASE TABLE';";
                    for row in client.query(query, &[]).unwrap() {
                        let table_name: &str = row.get(0);
                        table_names.push(DbTableConfig::new("public".to_string(), table_name.to_string()));
                    }
                    client.close();
                }
                Err(e) => {
                    println!("Failed to connect to {}", self.connection_uri);
                    println!("Connection failed on {:?}", e);
                }
            }
            table_names
        }

        pub fn database_columns(&self, table: DbTableConfig) -> SortedVec<DbColumnConfig> {
            let mut column_names: Vec<DbColumnConfig> = vec![];
            match Client::connect(self.connection_uri.as_str(), NoTls) {
                Ok(mut client) => {
                    let query = "select column_name, data_type, ordinal_position from information_schema.columns where table_schema = $1 and table_name = $2 order by ordinal_position;";
                    for row in client.query(query, &[&table.database, &table.table]).unwrap() {
                        let column_name: &str = row.get(0);
                        let data_type: &str = row.get(1);
                        let ordinal_position: i32 = row.get(2);
                        let column_config = DbColumnConfig::new(
                            column_name.to_string(),
                            data_type.to_string(),
                            ordinal_position
                        );
                        column_names.push(column_config);
                    }
                    client.close();
                }
                Err(e) => {
                    println!("Failed to connect to {}", self.connection_uri);
                    println!("Connection failed on {:?}", e);
                }
            }
            SortedVec::from(column_names.clone())
        }
    }
}
