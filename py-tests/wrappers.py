import sys, os, re, math, subprocess
from typing import List, Tuple, Union
import psycopg
from psycopg import sql
import pyarrow
import pyarrow.parquet
import polars
import duckdb
import atexit

env_prefix = "PG2PARQUET_TEST_"

pg2parquet_binary = os.getenv(env_prefix + "BIN", None)
pg2parquet_host = os.getenv(env_prefix + "DB_HOST", "localhost")
pg2parquet_port = os.getenv(env_prefix + "DB_PORT", "5432")
pg2parquet_dbname = os.getenv(env_prefix + "DB_NAME", "pg2parquet_test")
pg2parquet_user = os.getenv(env_prefix + "DB_USER", os.getenv("PGUSER", None))
pg2parquet_password = os.getenv(env_prefix + "DB_PASSWORD", os.getenv("PGPASSWORD", None))
output_directory = os.getenv(env_prefix + "OUTDIR", f"/tmp/pg2parquet-test-{os.getpid()}")
os.makedirs(output_directory, exist_ok=True)

if output_directory.startswith("/tmp/"):
    atexit.register(lambda: subprocess.run(["rm", "-rf", output_directory]))

if pg2parquet_binary is not None and not os.path.exists(pg2parquet_binary):
    print(f"pg2parquet binary {pg2parquet_binary} does not exist")
    sys.exit(1)

if pg2parquet_binary is None or pg2parquet_user is None or pg2parquet_password is None:
    print("Please set the following environment variables:")
    print(f"   {env_prefix}BIN{' ✅' if pg2parquet_binary else ' ❌'} - path to the pg2parquet binary to test")
    print(f"   {env_prefix}DB_HOST{'' if pg2parquet_host == 'localhost' else ' ✅' if pg2parquet_host else ' ❌'} - hostname of the database to connect to (default: localhost)")
    print(f"   {env_prefix}DB_PORT{'' if pg2parquet_port == '5432' else ' ✅' if pg2parquet_port else ' ❌'} - port of the database to connect to (default: 5432)")
    print(f"   {env_prefix}DB_NAME{'' if pg2parquet_dbname == 'pg2parquet_test' else ' ✅' if pg2parquet_dbname else ' ❌'} - which database to use for testing. If the db does not exists, attempts to create it (default: pg2parquet_test)")
    print(f"   {env_prefix}DB_USER{' ✅' if pg2parquet_user else ' ❌'} - username to connect to the database with")
    print(f"   {env_prefix}DB_PASSWORD{' ✅' if pg2parquet_password else ' ❌'} - password to connect to the database with")
    sys.exit(1)

def pg_connect(dbname = pg2parquet_dbname):
    return psycopg.connect(
        host=pg2parquet_host,
        port=pg2parquet_port,
        user=pg2parquet_user,
        password=pg2parquet_password,
        dbname=dbname
    )

def ensure_new_db(dbname):
    conn = pg_connect("postgres")
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(sql.SQL("SELECT 1 FROM pg_database WHERE datname={}").format(sql.Literal(dbname)))
            if cur.fetchone() is not None:
                cur.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(dbname)))
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
    finally:
        conn.close()

    conn = pg_connect(dbname)
    try:

        for ext in ["citext", "postgis", "vector"]:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(sql.SQL("CREATE EXTENSION IF NOT EXISTS {}").format(sql.Identifier(ext)))
    finally:
        conn.close()

ensure_new_db(pg2parquet_dbname)

def run_sql(*commands: Union[str, sql.SQL, sql.Composed]):
    with pg_connect() as conn:
        with conn.cursor() as cur:
            for command in commands:
                if isinstance(command, str):
                    cur.execute(str_to_sql(command))
                else:
                    cur.execute(command)
            conn.commit()

def run_pg2parquet(args: list[str]):
    r = subprocess.run([ pg2parquet_binary, *args ], env={
        "PGPASSWORD": pg2parquet_password,
        "RUST_BACKTRACE": "1"
    }, capture_output=True)
    if r.returncode != 0:
        print(f"pg2parquet exited with code {r.returncode}. Stdout:")
        print(r.stdout.decode("utf-8"))
        print("Stderr:")
        print(r.stderr.decode("utf-8"))
        raise Exception(f"pg2parquet exited with code {r.returncode}")
    return r


def run_export(name, query = None, options = []) -> str:
    outfile = os.path.join(output_directory, name + ".parquet")
    if query is not None:
        query_opt = ["--query", query]
    else:
        query_opt = ["--table", name]
    args = [
        "export",
        "--host", pg2parquet_host,
        "--port", pg2parquet_port,
        "--user", pg2parquet_user,
        "--dbname", pg2parquet_dbname,
        *query_opt,
        "--output-file", outfile,
        *options
    ]

    run_pg2parquet(args)

    return outfile

def run_export_table(name, table, sort_column, options = []) -> str:
    return run_export(name, f"SELECT * FROM {table} ORDER BY {sort_column} NULLS LAST", options=options)

def str_to_sql(s: str) -> sql.SQL:
    return sql.SQL(s) # type: ignore

def create_and_export(name, sort_column, schema, inserts, options=[]):
    run_sql(
        f"DROP TABLE IF EXISTS {name}",
        f"CREATE TABLE {name} ({schema})",
        f"INSERT INTO {name} VALUES {inserts}"
    )
    return run_export_table(name, name, sort_column, options=options)
