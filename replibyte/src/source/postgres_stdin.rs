// use std::io::{stdin, BufReader, Error};
//
// use crate::connector::Connector;
// use crate::source::postgres::{read_and_transform};
// use crate::types::{OriginalQuery, Query};
// use crate::Source;
// use crate::source::source_options::SourceOptions;
//
// /// Source Postgres dump from STDIN
// pub struct PostgresStdin {}
//
// impl PostgresStdin {
//     pub fn new() -> Self {
//         PostgresStdin {}
//     }
// }
//
// impl Default for PostgresStdin {
//     fn default() -> Self {
//         PostgresStdin {}
//     }
// }
//
// impl Connector for PostgresStdin {
//     fn init(&mut self) -> Result<(), Error> {
//         Ok(())
//     }
// }
//
// impl Source for PostgresStdin {
//     fn read<F: FnMut(OriginalQuery, Query)>(
//         &self,
//         options: SourceOptions,
//         query_callback: F,
//     ) -> Result<(), Error> {
//
//         let reader = BufReader::new(stdin());
//         read_and_transform(reader, &options);
//
//         Ok(())
//     }
// }
