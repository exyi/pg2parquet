PG wire protocol notes: https://www.npgsql.org/doc/dev/type-representations.html?q=wire

Parquet Repetition/Definition Levels: https://blog.twitter.com/engineering/en_us/a/2013/dremel-made-simple-with-parquet


## Rust postgres types

https://crates.io/crates/pg_bigdecimal

multi-dim array https://docs.rs/postgres_array/latest/postgres_array/struct.Array.html


## import tricks


* Populating a Database: https://www.postgresql.org/docs/current/populate.html
* new table - parallel insert into pg2parquet_tmp_XX, then rename table
  - CREATE UNLOGGED TABLE ?maybe?
  - 
* old table
  - transactional switch
  - PREPARE TRANSACTION


* http://ossc-db.github.io/pg_bulkload/index.html
* https://pgloader.readthedocs.io/en/latest/
