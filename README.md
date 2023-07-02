# PostgreSQL -> Parquet

Simple tool for exporting PostgreSQL tables into parquet, with support for more esoteric Postgres features than just `int` and `text`.

## Installation

### Download Binary from Github

Download the binary from [Github Actions](https://github.com/exyi/pg2parquet/actions/workflows/build.yml?query=branch%3Amain) artifacts (click on the latest run, scroll to the bottom, choose your system).

### Using Nix flakes

If you use Nix, this command will install the latest pg2parquet version. It compiles it from sources, so the installation will take some time.

```
nix shell github:exyi/pg2parquet
```

Then use the `pg2parquet` in the new shell. Note that you might need to add `--extra-experimental-features 'nix-command flakes'` argument to the nix invocation.

### Using Cargo

```
cargo install pg2parquet
```

### From Sources

Install Rust and Cargo. Clone the repo.

```bash
cd cli
env RUSTFLAGS="-C target-cpu=native" cargo build --release
```

It should finish in few minutes (~10 CPU minutes). Take the `target/release/pg2parquet` file, delete rest of the target directory (it takes quite a bit of disk space). You can optionally `strip` the binary, but you'll get poor stack trace if it crashes.

## Basic usage

```
pg2parquet export --host localhost.for.example --dbname my_database --output-file output.parquet -t the_table_to_export
```

Alternatively, you can export result of a SQL query

```
pg2parquet export --host localhost.for.example --dbname my_database --output-file output.parquet -q 'select column_a, column_b::text from another_table'
```

You can also use environment variables `$PGPASSWORD` and `$PGUSER`

## Supported types

* **Basic SQL types**: `text`, `char`, `varchar` and friends, all kinds of `int`s, `bool`, floating point numbers, `timestamp`, `timestamptz`, `date`, `time`, `uuid`
* **Decimal numeric types**
	* `numeric` will have fixed precision according to the `--decimal-scale` and `--decimal-precision` parameters
	* `money` is always a 64-bit decimal with 2 decimal places
* **`json` and `jsonb`**: by default serialized as a text field with the JSON. `--json-handling` option allows setting parquet metadata that the column is JSON, but the feature is not widely supported, thus it's disabled by default.
* **`xml`**: serialized as text
* **`macaddr` and `inet`**: by default written out in text representation. It's possible to serialize macaddr as bytes or Int64 using `--macaddr-handling` option.
* **`bit` and `varbit`**: represented as text of `0` and `1`
* **[Enums](https://www.postgresql.org/docs/current/datatype-enum.html)**
	* By default serialized as text, use `--enum-handling int` to serialize them as integers
* **[Ranges](https://www.postgresql.org/docs/current/rangetypes.html)**
	- Serialized as `struct { lower: T, upper: T, lower_inclusive: bool, upper_inclusive: bool, is_empty: bool }`
* **[Arrays](https://www.postgresql.org/docs/current/arrays.html)**
	- Serialized as parquet repeated fields
	- Always serialized as single-dimensional arrays, and information about starting index is dropped
	- `NULL` is serialized as empty array (Parquet does not support NULL array without nesting it into another struct)
* **[Composite Types](https://www.postgresql.org/docs/current/rowtypes.html)**
	- Serialized as Parquet struct type

## Known Limitations (and workarounds)

* Not all PostgreSQL types are supported
	* Workaround: Convert it to text (or other supported type) on PostgreSQL side `--query 'select weird_type_column::text from my_table'`
	* Please [submit an issue](https://github.com/exyi/pg2parquet/issues/new)
* I need the file in slightly different format (rename columns, ...)
	* Workaround 1: Use the `--query` parameter to shape the resulting schema
	* Workaround 2: Use DuckDB or Spark to postprocess the parquet file
		- DuckDB `COPY (SELECT my_col as myCol, ... FROM 'export.parquet') TO 'export2.parquet' (FORMAT PARQUET);`


## Options

**`> pg2parquet export --help`**

```
Exports a PostgreSQL table or query to a Parquet file

Usage: pg2parquet export [OPTIONS] --output-file <OUTPUT_FILE> --host <HOST> --dbname <DBNAME>

Options:
  -o, --output-file <OUTPUT_FILE>
          Path to the output file. If the file exists, it will be overwritten

  -q, --query <QUERY>
          SQL query to execute. Exclusive with --table

  -t, --table <TABLE>
          Which table should be exported. Exclusive with --query

      --compression <COMPRESSION>
          Compression applied on the output file. Default: zstd, change to Snappy or None if it's too slow
          
          [possible values: none, snappy, gzip, lzo, brotli, lz4, zstd]

  -H, --host <HOST>
          Database server host

  -U, --user <USER>
          Database user name. If not specified, PGUSER environment variable is used

  -d, --dbname <DBNAME>
          

  -p, --port <PORT>
          

      --password <PASSWORD>
          Password to use for the connection. It is recommended to use the PGPASSWORD environment variable instead, since process arguments are visible to other users on the system

      --macaddr-handling <MACADDR_HANDLING>
          How to handle `macaddr` columns
          
          [default: text]

          Possible values:
          - text:       MAC address is converted to a string
          - byte-array: MAC is stored as fixed byte array of length 6
          - int64:      MAC is stored in Int64 (lowest 6 bytes)

      --json-handling <JSON_HANDLING>
          How to handle `json` and `jsonb` columns
          
          [default: text]

          Possible values:
          - text-marked-as-json:
            JSON is stored as a Parquet JSON type. This is essentially the same as text, but with a different ConvertedType, so it may not be supported in all tools
          - text:
            JSON is stored as a UTF8 text

      --enum-handling <ENUM_HANDLING>
          How to handle enum (Enumerated Type) columns
          
          [default: text]

          Possible values:
          - text:
            Enum is stored as the postgres enum name
          - int:
            Enum is stored as an 32-bit integer (zero-based index of the value in the enum definition)

      --decimal-scale <DECIMAL_SCALE>
          How many decimal digits after the decimal point are stored in the Parquet file
          
          [default: 18]

      --decimal-precision <DECIMAL_PRECISION>
          How many decimal digits are allowed in numeric/DECIMAL column. By default 38, the largest value which fits in 128 bits
          
          [default: 38]

  -h, --help
          Print help information (use `-h` for a summary)
```
