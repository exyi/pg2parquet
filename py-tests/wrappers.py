import sys, os, re, math, subprocess
from typing import List, Tuple
import psycopg2, psycopg2.extensions
import pyarrow
import pyarrow.parquet
import polars
import duckdb
import atexit

env_prefix = "PG2PARQUET_TEST_"

pg2parquet_binary = os.getenv(env_prefix + "BIN", None)
pg2parquet_host = os.getenv(env_prefix + "DB_HOST", "localhost")
pg2parquet_port = os.getenv(env_prefix + "DB_PORT", "5432")
pg2parquet_dbname = os.getenv(env_prefix + "DB_NAME", "5432")
pg2parquet_user = os.getenv(env_prefix + "DB_USER", os.getenv("PGUSER", None))
pg2parquet_password = os.getenv(env_prefix + "DB_PASSWORD", os.getenv("PGPASSWORD", None))
output_directory = os.getenv(env_prefix + "OUTDIR", f"/tmp/pg2parquet-test-{os.getpid()}")
os.makedirs(output_directory, exist_ok=True)

if output_directory.startswith("/tmp/"):
    atexit.register(lambda: subprocess.run(["rm", "-rf", output_directory]))

if pg2parquet_binary is None or pg2parquet_user is None or pg2parquet_password is None:
    print("Please set the following environment variables:")
    print(f"   {env_prefix}BIN - path to the pg2parquet binary to test")
    print(f"   {env_prefix}DB_HOST - hostname of the database to connect to")
    print(f"   {env_prefix}DB_PORT - port of the database to connect to")
    print(f"   {env_prefix}DB_NAME - which database to use for testing. If the db does not exists, attempts to create it")
    print(f"   {env_prefix}DB_USER - username to connect to the database with")
    print(f"   {env_prefix}DB_PASSWORD - password to connect to the database with")
    sys.exit(1)

def pg_connect(dbname = pg2parquet_dbname):
    return psycopg2.connect(
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
            cur.execute(f"SELECT 1 FROM pg_database WHERE datname='{dbname}'")
            if cur.fetchone() is not None:
                cur.execute(f"DROP DATABASE {dbname}")
            cur.execute(f"CREATE DATABASE {dbname}")
    finally:
        conn.close()

    conn = pg_connect(dbname)
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS citext")
    finally:
        conn.close()

ensure_new_db(pg2parquet_dbname)

def run_sql(*commands: str):
    with pg_connect() as conn:
        with conn.cursor() as cur:
            for command in commands:
                cur.execute(command)
            conn.commit()

def run_export(name, query = None, options = []) -> pyarrow.Table:
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
    r = subprocess.run([ pg2parquet_binary, *args ], env={
        "PGPASSWORD": pg2parquet_password,
    }, capture_output=True)
    if r.returncode != 0:
        print(f"pg2parquet exited with code {r.returncode}. Stdout:")
        print(r.stdout.decode("utf-8"))
        print("Stderr:")
        print(r.stderr.decode("utf-8"))
        raise Exception(f"pg2parquet exited with code {r.returncode}")

    return outfile

def create_and_export(name, sort_column, schema, inserts, options=[]):
    run_sql(
        f"DROP TABLE IF EXISTS {name}",
        f"CREATE TABLE {name} ({schema})",
        f"INSERT INTO {name} VALUES {inserts}"
    )
    return run_export(name, f"SELECT * FROM {name} ORDER BY {sort_column} NULLS LAST", options=options)
