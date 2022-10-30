#![allow(unused_imports)]
#![allow(dead_code)]
use std::{sync::Arc, path::PathBuf, process};

use clap::{Parser, ValueEnum, Command};
use postgres_cloner::{SchemaSettingsMacaddrHandling, SchemaSettingsJsonHandling};

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
    #[command(arg_required_else_help = true)]
    ParquetInfo(ParquetInfoArgs),
    #[command(arg_required_else_help = true)]
    PlaygroundCreateSomething(PlaygroundCreateSomethingArgs),
    /// Exports a PostgreSQL table or query to a Parquet file
    #[command(arg_required_else_help = true)]
    Export(ExportArgs)
}

#[derive(clap::Args, Debug, Clone)]
struct ExportArgs {
    /// Path to the output file. If the file exists, it will be overwritten.
    #[arg(long, short = 'o')]
    output_file: PathBuf,
    /// SQL query to execute. Exclusive with --table
    #[arg(long, short = 'q')]
    query: Option<String>,
    /// Which table should be exported. Exclusive with --query
    #[arg(long, short = 't')]
    table: Option<String>,
    /// Compression applied on the output file. Default: zstd, change to Snappy or None if it's too slow
    #[arg(long, hide_short_help = true)]
    compression: Option<ParquetCompression>,
    #[command(flatten)]
    postgres: PostgresConnArgs,
    #[command(flatten)]
    schema_settings: SchemaSettingsArgs,
}

#[derive(clap::Args, Debug, Clone)]
pub struct PostgresConnArgs {
    /// Database server host
    #[arg(short='H', long)]
    host: String,
    /// Database user name. If not specified, PGUSER environment variable is used.
    #[arg(short='U', long)]
    user: Option<String>,
    #[arg(short='d', long)]
    dbname: String,
    #[arg(short='p', long)]
    port: Option<u16>,
    /// Password to use for the connection. It is recommended to use the PGPASSWORD environment variable instead, since process arguments are visible to other users on the system.
    #[arg(long)]
    password: Option<String>,
}

#[derive(clap::Args, Debug, Clone)]
pub struct SchemaSettingsArgs {
    #[arg(long, hide_short_help = true, default_value = "string")]
    macaddr_handling: SchemaSettingsMacaddrHandling,
    #[arg(long, hide_short_help = true, default_value = "string")]
	json_handling: SchemaSettingsJsonHandling,
    /// How many decimal digits after the decimal point are stored in the Parquet file
    #[arg(long, hide_short_help = true, default_value_t = 18)]
	decimal_scale: i32,
    /// How many decimal digits are allowed in numeric/DECIMAL column. By default 38, the largest value which fits in 128 bits.
    #[arg(long, hide_short_help = true, default_value_t = 38)]
	decimal_precision: u32,
}


#[derive(ValueEnum, Debug, Clone)]
enum ParquetCompression { None, Snappy, Gzip, Lzo, Brotli, Lz4, Zstd }

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
            process::exit(1);
        }
    }
}

fn perform_export(args: ExportArgs) {
    if args.query.is_some() && args.table.is_some() {
        eprintln!("Either query or table must be specified, but not both");
        process::exit(1);
    }
    if args.query.is_none() && args.table.is_none() {
        eprintln!("Either query or table must be specified");
        process::exit(1);
    }

    let compression = match args.compression {
        None => parquet::basic::Compression::ZSTD,
        Some(ParquetCompression::Brotli) => parquet::basic::Compression::BROTLI,
        Some(ParquetCompression::Gzip) => parquet::basic::Compression::GZIP,
        Some(ParquetCompression::Lzo) => parquet::basic::Compression::LZO,
        Some(ParquetCompression::Lz4) => parquet::basic::Compression::LZ4,
        Some(ParquetCompression::Snappy) => parquet::basic::Compression::SNAPPY,
        Some(ParquetCompression::Zstd) => parquet::basic::Compression::ZSTD,
        Some(ParquetCompression::None) => parquet::basic::Compression::UNCOMPRESSED
    };
    let props =
        parquet::file::properties::WriterProperties::builder()
            .set_compression(compression)
            .set_created_by("pg2parquet".to_owned())
        .build();
    let props = Arc::new(props);

    let settings = postgres_cloner::default_settings();
    let query = args.query.unwrap_or_else(|| {
        format!("SELECT * FROM {}", args.table.unwrap())
    });
    let result = postgres_cloner::execute_copy(&args.postgres, &query, &args.output_file, props, &settings);
    let stats = handle_result(result);

    eprintln!("Wrote {} rows, {} bytes of raw data in {} groups", stats.rows, stats.bytes, stats.groups);
}

fn parse_args() -> CliCommand {
    CliCommand::parse()
}

fn main() {
    let args = parse_args();

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
            perform_export(args);
        }
    }
}
