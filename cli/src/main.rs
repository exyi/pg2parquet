#![allow(unused_imports)]
#![allow(dead_code)]
use std::{sync::Arc, path::PathBuf, process};

use clap::{Parser, ValueEnum, Command};
use parquet::basic::{ZstdLevel, BrotliLevel, GzipLevel};
use postgres_cloner::{SchemaSettingsMacaddrHandling, SchemaSettingsJsonHandling, SchemaSettingsEnumHandling, SchemaSettingsIntervalHandling};

mod postgresutils;
mod myfrom;
mod level_index;
mod parquetinfo;
mod playground;
mod parquet_writer;
mod postgres_cloner;
mod pg_custom_types;
mod datatypes;
mod appenders;

#[cfg(not(any(target_family = "windows", target_arch = "riscv64")))]
use jemallocator::Jemalloc;

use crate::postgres_cloner::SchemaSettings;

#[cfg(not(any(target_family = "windows", target_arch = "riscv64")))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;


#[derive(Parser, Debug, Clone)]
#[command(name = "pg2parquet")]
#[command(bin_name = "pg2parquet")]
enum CliCommand {
    /// Dumps something from a parquet file
    #[command(arg_required_else_help = true, hide = true)]
    ParquetInfo(ParquetInfoArgs),
    #[command(arg_required_else_help = true, hide = true)]
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
    /// Compression level of the output file compressor. Only relevant for zstd, brotli and gzip. Default: 3
    #[arg(long, hide_short_help = true)]
    compression_level: Option<i32>,
    #[command(flatten)]
    postgres: PostgresConnArgs,
    #[command(flatten)]
    schema_settings: SchemaSettingsArgs,
}

#[derive(clap::ValueEnum, Debug, Clone)]
enum SslMode {
    /// Do not use TLS.
    Disable,
    /// Attempt to connect with TLS but allow sessions without (default behavior compiled with SSL support).
    Prefer,
    /// Require the use of TLS.
    Require,
}

#[derive(clap::Args, Clone)]
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
    /// Controls whether to use SSL/TLS to connect to the server.
    #[arg(long="sslmode", alias="tlsmode", alias="ssl-mode", alias="tls-mode")]
    sslmode: Option<SslMode>,
}

impl std::fmt::Debug for PostgresConnArgs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let password = self.password.as_ref().map(|_| "********");
        f.debug_struct("PostgresConnArgs").field("host", &self.host).field("user", &self.user).field("dbname", &self.dbname).field("port", &self.port).field("password", &password).field("sslmode", &self.sslmode).finish()
    }
}

#[derive(clap::Args, Debug, Clone)]
pub struct SchemaSettingsArgs {
    /// How to handle `macaddr` columns
    #[arg(long, hide_short_help = true, default_value = "text")]
    macaddr_handling: SchemaSettingsMacaddrHandling,
    /// How to handle `json` and `jsonb` columns
    #[arg(long, hide_short_help = true, default_value = "text")]
	json_handling: SchemaSettingsJsonHandling,
    /// How to handle enum (Enumerated Type) columns 
    #[arg(long, hide_short_help = true, default_value = "text")]
    enum_handling: SchemaSettingsEnumHandling,
    /// How to handle `interval` columns
    #[arg(long, hide_short_help = true, default_value = "interval")]
    interval_handling: SchemaSettingsIntervalHandling,
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
            match args.ok() {
                Some(a) => eprintln!("Error occured while executing command {:#?}", a),
                None => eprintln!("Error occured while executing an unparsable command"),
            };
            eprintln!();
            eprintln!("{}", e.to_string());
            process::exit(1);
        }
    }
}

fn get_compression(args: &ExportArgs) -> Result<parquet::basic::Compression, parquet::errors::ParquetError> {
    let lvl = args.compression_level;
    let level_not_supported =
        if lvl.is_some() {
            Err(parquet::errors::ParquetError::General(format!(
                "Compression algorithm {:?} does not allow setting --compression-level option",
                args.compression.as_ref().unwrap()
            )))
        } else {
            Ok(())
        };
    let compression = match args.compression {
        None => parquet::basic::Compression::ZSTD(ZstdLevel::try_new(lvl.unwrap_or(3))?),
        Some(ParquetCompression::Brotli) => parquet::basic::Compression::BROTLI(BrotliLevel::try_new(lvl.unwrap_or(3) as u32)?),
        Some(ParquetCompression::Gzip) => parquet::basic::Compression::GZIP(GzipLevel::try_new(lvl.unwrap_or(3) as u32)?),
        Some(ParquetCompression::Zstd) => parquet::basic::Compression::ZSTD(ZstdLevel::try_new(lvl.unwrap_or(3))?),
        Some(ParquetCompression::Lzo) => { level_not_supported?; parquet::basic::Compression::LZO }
        Some(ParquetCompression::Lz4) => { level_not_supported?; parquet::basic::Compression::LZ4 }
        Some(ParquetCompression::Snappy) => { level_not_supported?; parquet::basic::Compression::SNAPPY }
        Some(ParquetCompression::None) => { level_not_supported?; parquet::basic::Compression::UNCOMPRESSED }
    };
    Ok(compression)
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

    let compression = get_compression(&args).unwrap_or_else(|e| {
        eprintln!("Invalid combination of compression and compression_level: {}", e);
        process::exit(1);
    });

    let props =
        parquet::file::properties::WriterProperties::builder()
            .set_compression(compression)
            .set_created_by(format!("pg2parquet version {}, using {}", env!("CARGO_PKG_VERSION"), parquet::file::properties::DEFAULT_CREATED_BY))
        .build();
    let props = Arc::new(props);

    let settings = SchemaSettings {
        macaddr_handling: args.schema_settings.macaddr_handling,
        json_handling: args.schema_settings.json_handling,
        enum_handling: args.schema_settings.enum_handling,
        interval_handling: args.schema_settings.interval_handling,
        decimal_scale: args.schema_settings.decimal_scale,
        decimal_precision: args.schema_settings.decimal_precision,
    };
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
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |x| {
        default_hook(x);
        eprintln!();
        eprintln!("pg2parquet probably should not crash in this way, could you please report a bug at https://github.com/exyi/pg2parquet/issues/new (ideally with the backtrace and some info on what you did)?");
    }));
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
