use std::io::{Error};
use lazy_static::lazy_static;
use regex::Regex;

use crate::connector::Connector;
use crate::source::source_options::SourceOptions;
use crate::types::{OriginalQuery, Query};

pub mod mysql;
pub mod mysql_stdin;
pub mod postgres;
pub mod postgres_schema;
pub mod postgres_stdin;
pub mod hstore;
pub mod csv_sub_source;
pub mod source_options;
pub mod json;

pub trait Source: Connector {
    fn read<F: FnMut(OriginalQuery, Query)>(
        &self,
        options: SourceOptions,
        query_callback: F,
    ) -> Result<(), Error>;
}

pub fn clean_preceding_quotes(s: String) -> String {
    lazy_static! {
        static ref PRECEDING_QUOTES_RE: Regex = Regex::new(r#"^""#).unwrap();
    }
    PRECEDING_QUOTES_RE.replace_all(&s, "").to_string()
}

pub fn clean_trailing_quotes(s: String) -> String {
    lazy_static! {
        static ref TRAILING_QUOTES_RE: Regex = Regex::new(r#""$"#).unwrap();
    }
    TRAILING_QUOTES_RE.replace_all(&s, "").to_string()
}

pub fn clean_quotes(s: String) -> String {
    clean_trailing_quotes(clean_preceding_quotes(s))
}
