# Postgres -> Parquet

Simple tool for exporting PostgreSQL tables into parquet, with support for more esoteric Postgres features than just `int` and `text`.

## Installation

(create an issue if you want me to make this simpler)

Install .NET Core. Clone the repo.

## Run

```
dotnet run --project cli/Pg2Parquet.CLI -c Release -- export <args>
```

You'll need to specify how to connect to the database, the --help message will tell you.
You can also use environment variables like PGPASSWORD, PGPASSFILE, see https://www.npgsql.org/doc/connection-string-parameters.html for more details about the DB connection.

## Problems


Problem is that the dotnet-parquet is buggy as hell and I don't know how to fix it, so support for composite types is pretty broken.
It will produce invalid Parquet file, but most tools can load it... so I guess you can then convert it using Spark to a valid Parquet.

Solution is, of course, to rewrite it in Rust 😂

