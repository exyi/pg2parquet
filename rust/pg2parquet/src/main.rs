#![allow(unused_imports)]
#![allow(dead_code)]
use std::{sync::Arc, path::PathBuf};

use clap::Parser;

mod postgresutils;
mod myfrom;
mod level_index;
mod parquetinfo;
mod playground;
mod column_appender;
mod column_pg_copier;
mod parquet_row_writer;
mod postgres_cloner;
mod pg_custom_types;
mod datatypes;


#[derive(Parser, Debug, Clone)]
#[command(name = "pg2parquet")]
#[command(bin_name = "pg2parquet")]
enum CliCommand {
    /// Dumps something from a parquet file
    ParquetInfo(ParquetInfoArgs),
    PlaygroundCreateSomething(PlaygroundCreateSomethingArgs),
    Export(ExportArgs)
}

#[derive(clap::Args, Debug, Clone)]
struct ExportArgs {
    output_file: PathBuf,
    query: Option<String>,
}

#[derive(clap::Args, Debug, Clone)]
// #[command(author, version, about, long_about = None)]
struct ParquetInfoArgs {
    parquet_file: PathBuf,
    // #[arg(long)]
    // manifest_path: Option<std::path::PathBuf>,
}

#[derive(clap::Args, Debug, Clone)]
struct PlaygroundCreateSomethingArgs {
    parquet_file: PathBuf,
}

fn handle_result<T, TErr: ToString>(r: Result<T, TErr>) -> T {
    match r {
        Ok(v) => v,
        Err(e) => {
            let args = CliCommand::try_parse();
            eprintln!("Error occured while executing command {:?}", args.ok());
            eprintln!();
            eprintln!("{}", e.to_string());
            std::process::exit(1);
        }
    }
}

fn main() {
    let args = CliCommand::parse();

    match args {
        CliCommand::ParquetInfo(args) => {
            eprintln!("parquet file: {:?}", args.parquet_file);
            parquetinfo::print_parquet_info(&args.parquet_file);
        },
        CliCommand::PlaygroundCreateSomething(args) => {
            eprintln!("parquet file: {:?}", args.parquet_file);
            playground::create_something(&args.parquet_file);
        },
        CliCommand::Export(args) => {
            // eprintln!("query: {:?}", args.query);
            let props =
                parquet::file::properties::WriterProperties::builder()
                    .set_compression(parquet::basic::Compression::SNAPPY)
                .build();
            let props = Arc::new(props);
    
            let settings = postgres_cloner::default_settings();
            let result = postgres_cloner::execute_copy(&args.query.unwrap(), &args.output_file, props, &settings);
            let stats = handle_result(result);

            eprintln!("Wrote {} rows, {} bytes of raw data in {} groups", stats.rows, stats.bytes, stats.groups);
        }
    }
}
