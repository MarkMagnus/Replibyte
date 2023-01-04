use std::any::Any;
use std::cmp::Ordering;
use crate::transformer::credit_card::CreditCardTransformer;
use crate::transformer::email::EmailTransformer;
use crate::transformer::first_name::FirstNameTransformer;
use crate::transformer::keep_first_char::KeepFirstCharTransformer;
use crate::transformer::phone_number::PhoneNumberTransformer;
use crate::transformer::mobile_number::{MobileNumberOptions, MobileNumberTransformer};
use crate::transformer::random::RandomTransformer;
use crate::transformer::redacted::{RedactedTransformer, RedactedTransformerOptions};
use crate::transformer::blank::BlankTransformer;
use crate::transformer::hstore_attr::{HstoreAttrTransformer, HstoreAttrOptions};
use crate::transformer::transient::TransientTransformer;
use crate::transformer::Transformer;
use percent_encoding::percent_decode_str;
use serde;
use serde::{Deserialize, Serialize};
use std::io::{Error, ErrorKind};
use sorted_vec::SortedVec;
use url::Url;
use crate::transformer::json_attrs::{JsonAttrOptions, JsonAttrTransformer};
use crate::types::Column;

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct Config {
    // pub bind: Ipv4Addr,
    // pub port: u16,
    pub source: Option<SourceConfig>,
    pub datastore: DatastoreConfig,
    pub destination: Option<DestinationConfig>,
    pub encryption_key: Option<String>,
}

pub enum ConnectorConfig<'a> {
    Source(&'a SourceConfig),
    Destination(&'a DestinationConfig),
}

impl Config {
    pub fn connector(&self) -> Result<ConnectorConfig, Error> {
        if let Some(source) = &self.source {
            return Ok(ConnectorConfig::Source(source));
        }

        if let Some(destination) = &self.destination {
            return Ok(ConnectorConfig::Destination(destination));
        }

        Err(Error::new(
            ErrorKind::Other,
            "<source> or <destination> is mandatory",
        ))
    }

    pub fn encryption_key(&self) -> Result<Option<String>, Error> {
        match &self.encryption_key {
            Some(key) => substitute_env_var(key.as_str()).map(|x| Some(x)),
            None => Ok(None),
        }
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub enum DatastoreConfig {
    #[serde(rename = "aws")]
    AWS(DatastoreAwsS3Config),
    #[serde(rename = "gcp")]
    GCP(DatastoreGcpCloudStorageConfig),
    #[serde(rename = "local_disk")]
    LocalDisk(DatastoreLocalDiskConfig),
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct DatastoreAwsS3Config {
    // At the moment we do support only S3 as B,
    // in a near future we'll need to make it generic
    pub bucket: String,
    pub region: Option<String>,
    pub profile: Option<String>,
    pub credentials: Option<AwsCredentials>,
    pub endpoint: Option<Endpoint>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct AwsCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

impl DatastoreAwsS3Config {
    /// decode and return the bucket value
    pub fn bucket(&self) -> Result<String, Error> {
        substitute_env_var(self.bucket.as_str())
    }

    /// decode and return the region value
    pub fn region(&self) -> Result<Option<String>, Error> {
        self.region
            .as_ref()
            .map(|region| substitute_env_var(region))
            .transpose()
    }

    /// decode and return profile value
    pub fn profile(&self) -> Result<Option<String>, Error> {
        self.profile
            .as_ref()
            .map(|profile| substitute_env_var(profile))
            .transpose()
    }

    /// decode and return the credentials
    pub fn credentials(&self) -> Result<Option<AwsCredentials>, Error> {
        if let Some(credentials) = &self.credentials {
            let session_token = if let Some(session_token) = &credentials.session_token {
                Some(substitute_env_var(session_token)?)
            } else {
                None
            };

            Ok(Some(AwsCredentials {
                access_key_id: substitute_env_var(&credentials.access_key_id)?,
                secret_access_key: substitute_env_var(&credentials.secret_access_key)?,
                session_token,
            }))
        } else {
            Ok(None)
        }
    }

    /// decode and return the endpoint value
    pub fn endpoint(&self) -> Result<Endpoint, Error> {
        if let Some(endpoint) = &self.endpoint {
            match endpoint {
                Endpoint::Custom(url) => match substitute_env_var(url.as_str()) {
                    Ok(substituted_url) => Ok(Endpoint::Custom(substituted_url)),
                    Err(err) => Err(err),
                },
                _ => Ok(Endpoint::Default),
            }
        } else {
            Ok(Endpoint::Default)
        }
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct DatastoreGcpCloudStorageConfig {
    pub bucket: String,
    pub region: String,
    pub access_key: String,
    pub secret: String,
    pub endpoint: Option<Endpoint>,
}

impl DatastoreGcpCloudStorageConfig {
    /// decode and return the bucket value
    pub fn bucket(&self) -> Result<String, Error> {
        substitute_env_var(self.bucket.as_str())
    }

    /// decode and return the region value
    pub fn region(&self) -> Result<String, Error> {
        substitute_env_var(self.region.as_str())
    }

    /// decode and return the access_key value
    pub fn access_key(&self) -> Result<String, Error> {
        substitute_env_var(self.access_key.as_str())
    }

    /// decode and return the secret value
    pub fn secret(&self) -> Result<String, Error> {
        substitute_env_var(self.secret.as_str())
    }

    /// decode and return the endpoint value
    pub fn endpoint(&self) -> Result<Endpoint, Error> {
        if let Some(endpoint) = &self.endpoint {
            match endpoint {
                Endpoint::Custom(url) => match substitute_env_var(url.as_str()) {
                    Ok(substituted_url) => Ok(Endpoint::Custom(substituted_url)),
                    Err(err) => Err(err),
                },
                _ => Ok(Endpoint::Default),
            }
        } else {
            Ok(Endpoint::Default)
        }
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct DatastoreLocalDiskConfig {
    pub dir: String,
}

impl DatastoreLocalDiskConfig {
    /// decode and return the directory value
    pub fn dir(&self) -> Result<String, Error> {
        substitute_env_var(self.dir.as_str())
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct SourceConfig {
    pub connection_uri: Option<String>,
    pub compression: Option<bool>,
    pub transformers: Option<Vec<TransformerConfig>>,
    pub skip: Option<Vec<DbTableConfig>>,
    pub database_subset: Option<Vec<DatabaseSubsetConfig>>,
    pub only_tables: Option<Vec<OnlyTablesConfig>>,
}

impl SourceConfig {
    pub fn connection_uri(&self) -> Result<ConnectionUri, Error> {
        match &self.connection_uri {
            Some(connection_uri) => parse_connection_uri(connection_uri.as_str()),
            None => Err(Error::new(
                ErrorKind::Other,
                format!("missing <source.connection_uri> in the configuration file"),
            )),
        }
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct DestinationConfig {
    pub connection_uri: String,
    pub wipe_database: Option<bool>,
}

impl DestinationConfig {
    pub fn connection_uri(&self) -> Result<ConnectionUri, Error> {
        parse_connection_uri(self.connection_uri.as_str())
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DbTableConfig {
    pub database: String,
    pub table: String,
}

impl PartialEq for DbTableConfig {
    fn eq(&self, other: &Self) -> bool {
        self.table == other.table && self.database == other.database
    }
}

impl DbTableConfig {
    pub(crate) fn new(database: String, table: String) -> Self {
        DbTableConfig {
            database,
            table,
        }
    }

    pub(crate) fn only_config(&self) -> OnlyTablesConfig {
        OnlyTablesConfig {
            database: self.database.clone(),
            table: self.table.clone(),
        }
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone, Eq)]
pub struct DbColumnConfig {
    pub column: String,
    pub data_type: String,
    pub ordinal: i32,
}

impl DbColumnConfig {

    pub(crate) fn new(column: String, data_type: String, ordinal: i32) -> Self {
        DbColumnConfig {
            column,
            data_type,
            ordinal,
        }
    }

}

impl PartialOrd<Self> for DbColumnConfig {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DbColumnConfig {
    fn cmp(&self, other: &Self) -> Ordering {
        self.ordinal.cmp(&other.ordinal)
    }
}

pub struct RowConfig {
   pub column_names: Vec<String>,
   pub data_types: Vec<String>,
   pub ordinals: Vec<i32>, 
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct DatabaseSubsetConfig {
    pub database: String,
    pub table: String,
    #[serde(flatten)]
    pub strategy: DatabaseSubsetConfigStrategy,
    // copy the entire table - not affected by the subset algorithm
    pub passthrough_tables: Option<Vec<String>>,
}

impl DatabaseSubsetConfig {
    pub(crate) fn new(database: String, table: String) -> Self {
        DatabaseSubsetConfig {
            database,
            table,
            strategy: DatabaseSubsetConfigStrategy::None,
            passthrough_tables: None
        }
    }
    pub fn table_config(&self) -> DbTableConfig {
        DbTableConfig { database: self.database.clone(), table: self.table.clone() }
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct OnlyTablesConfig {
    pub database: String,
    pub table: String,
}

impl OnlyTablesConfig {
    pub fn config(&self) -> DbTableConfig {
        DbTableConfig {
            database: self.database.clone(),
            table: self.table.clone(),
        }
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
#[serde(rename_all = "kebab-case")]
#[serde(tag = "strategy_name", content = "strategy_options")]
pub enum DatabaseSubsetConfigStrategy {
    Random(DatabaseSubsetConfigStrategyRandom),
    ForeignKey(DatabaseSubsetConfigStrategyForeignKey),
    None
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Copy)]
pub struct DatabaseSubsetConfigStrategyRandom {
    pub percent: u8,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct DatabaseSubsetConfigStrategyForeignKey {
    pub condition: String,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct TransformerConfig {
    pub database: String,
    pub table: String,
    pub columns: Vec<ColumnConfig>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct ColumnConfig {
    pub name: String,

    #[serde(flatten)]
    pub transformer: TransformerTypeConfig,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
#[serde(rename_all = "kebab-case")]
#[serde(tag = "transformer_name", content = "transformer_options")]
pub enum TransformerTypeConfig {
    Random,
    RandomDate,
    FirstName,
    Email,
    KeepFirstChar,
    PhoneNumber,
    MobileNumber(Option<MobileNumberOptions>),
    CreditCard,
    Redacted(Option<RedactedTransformerOptions>),
    Transient,
    Blank,
    HstoreAttr(HstoreAttrOptions),
    JsonAttr(JsonAttrOptions),
}

impl TransformerTypeConfig {
    pub fn transformer(
        &self,
        database_name: &str,
        table_name: &str,
        column_name: &str,
    ) -> Box<dyn Transformer> {
        let transformer: Box<dyn Transformer> = match self {
            TransformerTypeConfig::Random => Box::new(RandomTransformer::new(
                database_name,
                table_name,
                column_name,
            )),
            TransformerTypeConfig::Blank => Box::new(BlankTransformer::new(
                database_name,
                table_name,
                column_name,
            )),
            TransformerTypeConfig::FirstName => Box::new(FirstNameTransformer::new(
                database_name,
                table_name,
                column_name,
            )),
            TransformerTypeConfig::Email => Box::new(EmailTransformer::new(
                database_name,
                table_name,
                column_name,
            )),
            TransformerTypeConfig::KeepFirstChar => Box::new(KeepFirstCharTransformer::new(
                database_name,
                table_name,
                column_name,
            )),
            TransformerTypeConfig::PhoneNumber => Box::new(PhoneNumberTransformer::new(
                database_name,
                table_name,
                column_name,
            )),
            TransformerTypeConfig::MobileNumber(options) => {

                let options = match options {
                    Some(options) => *options,
                    None => MobileNumberOptions::default(),
                };

                Box::new(MobileNumberTransformer::new(
                    database_name,
                    table_name,
                    column_name,
                    options,
                ))
            },
            TransformerTypeConfig::HstoreAttr(options) => {
                Box::new(HstoreAttrTransformer::new(
                    database_name,
                    table_name,
                    column_name,
                    options.clone(),
                ))
            },
            TransformerTypeConfig::JsonAttr(options) => {
                Box::new(JsonAttrTransformer::new(
                    database_name,
                    table_name,
                    column_name,
                    options.clone(),
                ))
            },
            TransformerTypeConfig::RandomDate => todo!(),
            TransformerTypeConfig::CreditCard => Box::new(CreditCardTransformer::new(
                database_name,
                table_name,
                column_name,
            )),
            TransformerTypeConfig::Redacted(options) => {
                let options = match options {
                    Some(options) => *options,
                    None => RedactedTransformerOptions::default(),
                };
                Box::new(RedactedTransformer::new(
                    database_name,
                    table_name,
                    column_name,
                    options,
                ))
            }
            TransformerTypeConfig::Transient => Box::new(TransientTransformer::new(
                database_name,
                table_name,
                column_name,
            )),
        };

        transformer
    }
}

type Host = String;
type Port = u16;
type Username = String;
type Password = String;
type Database = String;
type Uri = String;

#[derive(Debug, PartialEq, Clone)]
pub enum ConnectionUri {
    Postgres(Uri, Host, Port, Username, Password, Database),
    Mysql(Host, Port, Username, Password, Database)
}

fn get_host(url: &Url) -> Result<String, Error> {
    match url.host() {
        Some(host) => Ok(host.to_string()),
        None => Err(Error::new(
            ErrorKind::Other,
            "missing <host> property from connection uri",
        )),
    }
}

fn get_port(url: &Url, default_port: u16) -> Result<u16, Error> {
    match url.port() {
        Some(port) if port < 1 => Err(Error::new(
            ErrorKind::Other,
            "<port> from connection uri can't be lower than 0",
        )),
        Some(port) => Ok(port),
        None => Ok(default_port),
    }
}

fn get_username(url: &Url) -> Result<String, Error> {
    match url.username() {
        username if username != "" => Ok(percent_decode_str(&username)
            .decode_utf8_lossy()
            .to_string()),
        _ => Err(Error::new(
            ErrorKind::Other,
            "missing <username> property from connection uri",
        )),
    }
}

fn get_password(url: &Url) -> Result<String, Error> {
    match url.password() {
        Some(password) => Ok(password.to_string()),
        None => Ok(String::new()), // no password
    }
}

fn get_database(url: &Url, default: Option<&str>) -> Result<String, Error> {
    let path = url.path().to_string();
    let database = path.split("/").collect::<Vec<&str>>();

    if database.is_empty() {
        return match default {
            Some(default) => Ok(default.to_string()),
            None => Err(Error::new(
                ErrorKind::Other,
                "missing <database> property from connection uri",
            )),
        };
    }

    let database = match database.get(1) {
        Some(database) => *database,
        None => {
            return match default {
                Some(default) => Ok(default.to_string()),
                None => Err(Error::new(
                    ErrorKind::Other,
                    "missing <database> property from connection uri",
                )),
            };
        }
    };

    Ok(database.to_string())
}

fn parse_connection_uri(uri: &str) -> Result<ConnectionUri, Error> {
    let uri = substitute_env_var(uri)?;

    let url = match Url::parse(uri.as_str()) {
        Ok(url) => url,
        Err(err) => return Err(Error::new(ErrorKind::Other, format!("{:?}", err))),
    };

    let connection_uri = match url.scheme() {
        scheme if scheme.to_lowercase() == "postgres" || scheme.to_lowercase() == "postgresql" => {
            ConnectionUri::Postgres(
                (&url.as_str()).parse().unwrap(),
                get_host(&url)?,
                get_port(&url, 5432)?,
                get_username(&url)?,
                get_password(&url)?,
                get_database(&url, Some("public"))?,
            )
        }
        scheme if scheme.to_lowercase() == "mysql" => ConnectionUri::Mysql(
            get_host(&url)?,
            get_port(&url, 3306)?,
            get_username(&url)?,
            get_password(&url)?,
            get_database(&url, None)?,
        ),
        scheme => {
            return Err(Error::new(
                ErrorKind::Other,
                format!("'{}' not supported", scheme),
            ));
        }
    };

    Ok(connection_uri)
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub enum Endpoint {
    #[serde(rename = "default")]
    Default,
    #[serde(rename = "custom")]
    Custom(String),
}

/// take as input $KEY_ENV_VAR and convert it into a real value if the env var does exist
/// otherwise return an Error
fn substitute_env_var(env_var: &str) -> Result<String, Error> {
    match env_var {
        "" => Ok(String::new()),
        env_var if env_var.starts_with("$") && env_var.len() > 1 => {
            let key = &env_var[1..env_var.len()];
            match std::env::var(key) {
                Ok(value) => Ok(value),
                Err(_) => Err(Error::new(
                    ErrorKind::Other,
                    format!("environment variable '{}' is missing", key),
                )),
            }
        }
        x => Ok(x.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use crate::config::{parse_connection_uri, substitute_env_var, ConnectionUri};

    #[test]
    fn substitute_env_variables() {
        assert!(substitute_env_var("$DOES_NOT_EXIST").is_err());
        assert_eq!(substitute_env_var("").unwrap(), "".to_string());
        assert_eq!(substitute_env_var("toto").unwrap(), "toto".to_string());

        std::env::set_var("THIS_IS_A_TEST", "here is my value");
        assert_eq!(
            substitute_env_var("$THIS_IS_A_TEST").unwrap(),
            "here is my value"
        );
    }

    #[test]
    fn parse_postgres_connection_uri() {
        assert!(parse_connection_uri("postgres://root:password@localhost:5432/db").is_ok());
        assert!(parse_connection_uri("postgres://root:@localhost:5432/db").is_ok());
        assert!(parse_connection_uri("postgres://root:password@localhost:5432").is_ok());
        assert!(parse_connection_uri("postgres://root:password@localhost").is_ok());
        assert!(parse_connection_uri("postgres://root:password").is_err());

        assert!(parse_connection_uri("postgresql://root:password@localhost:5432/db").is_ok());
        assert!(parse_connection_uri("postgresql://root:@localhost:5432/db").is_ok());
        assert!(parse_connection_uri("postgresql://root:password@localhost:5432").is_ok());
        assert!(parse_connection_uri("postgresql://root:password@localhost").is_ok());
        assert!(parse_connection_uri("postgresql://root:password").is_err());
    }

    #[test]
    fn parse_mysql_connection_uri() {
        assert!(parse_connection_uri("mysql://root:password@localhost:3306/db").is_ok());
        assert!(parse_connection_uri("mysql://root:@localhost:3306/db").is_ok());
        assert!(parse_connection_uri("mysql://root:password@localhost/db").is_ok());
        assert!(parse_connection_uri("mysql://root:password@localhost").is_err());
        assert!(parse_connection_uri("mysql://root:password").is_err());
    }

    #[test]
    fn parse_mysql_connection_uri_with_db() {
        assert_eq!(
            parse_connection_uri("mysql://root:password@localhost:3306/db").unwrap(),
            ConnectionUri::Mysql(
                "localhost".to_string(),
                3306,
                "root".to_string(),
                "password".to_string(),
                "db".to_string()
            ),
        );

        assert_eq!(
            parse_connection_uri("mysql://root:password@localhost/db").unwrap(),
            ConnectionUri::Mysql(
                "localhost".to_string(),
                3306,
                "root".to_string(),
                "password".to_string(),
                "db".to_string()
            ),
        );
    }

    #[test]
    fn parse_postgres_connection_uri_with_db() {
        assert_eq!(
            parse_connection_uri("postgres://root:password@localhost:5432/db").unwrap(),
            ConnectionUri::Postgres(
                "postgres://root:password@localhost:5432/db".to_string(),
                "localhost".to_string(),
                5432,
                "root".to_string(),
                "password".to_string(),
                "db".to_string(),
            ),
        )
    }

    #[test]
    fn parse_postgres_connection_uri_with_username_with_special_chars_db() {
        assert_eq!(
            parse_connection_uri("postgres://root@azure:password@localhost:5432/db").unwrap(),
            ConnectionUri::Postgres(
                "postgres://root%40azure:password@localhost:5432/db".to_string(),
                "localhost".to_string(),
                5432,
                "root@azure".to_string(),
                "password".to_string(),
                "db".to_string(),
            ),
        )
    }

}
