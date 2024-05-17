import datetime
from decimal import Decimal
import math
import uuid
import wrappers
import unittest
import duckdb
import polars as pl
import pandas as pd


class TestBasic(unittest.TestCase):
    def test_simple_table(self):
        wrappers.run_sql(
            "CREATE TABLE simple1 (id int, name text)",
            "INSERT INTO simple1 VALUES (1, 'hello'), (2, 'world'), (3, 'foo'), (4, 'bar')"
        )
        file = wrappers.run_export("simple1", query="select * from simple1 order by id")
        duckdb_table = duckdb.read_parquet(file).fetchall()
        self.assertEqual(duckdb_table, [
            (1, "hello"),
            (2, "world"),
            (3, "foo"),
            (4, "bar")
        ])

        polars_df = pl.read_parquet(file)
        self.assertEqual(polars_df.schema, { "id": pl.Int32, "name": pl.Utf8 })
        self.assertEqual(list(polars_df["id"]), [1, 2, 3, 4])
        self.assertEqual(list(polars_df["name"]), ["hello", "world", "foo", "bar"])

    def test_nulls(self):
        file = wrappers.create_and_export(
            "simple2", "id",
            "id int, name text, b bool",
            "(1, NULL, NULL), (2, 'b', true), (NULL, 'foo', NULL), (4, 'bar', true)"
        )
        duckdb_table = duckdb.read_parquet(file).fetchall()
        self.assertEqual(duckdb_table, [
            (1, None, None),
            (2, "b", True),
            (4, "bar", True),
            (None, "foo", None)
        ])

    def test_textish_types(self):
        file = wrappers.create_and_export(
            "texttish_types", "id",
            "id text, name name, citext citext, json json, jsonb jsonb, char char(10), varchar varchar(10)",
            "('id', 'name', 'CiTeXt', '{\"json\": true}', '{\"jsonb\": true}', 'char', 'varchar'), ('id2', NULL, NULL, NULL, NULL, NULL, NULL)"
        )
        duckdb_table = duckdb.read_parquet(file).fetchall()
        self.assertEqual(duckdb_table, [
            ("id", "name", "CiTeXt", "{\"json\": true}", "{\"jsonb\": true}", "char      ", "varchar"),
            ("id2", None, None, None, None, None, None)
        ])

    def test_integer_types(self):
        file = wrappers.create_and_export(
            "integer_types", "id",
            "id smallint, id2 int, id3 bigint, id4 serial, id5 bigserial, id6 oid",
            "(1, 2, 3, 4, 5, 6), (NULL, NULL, NULL, 1, 1, NULL)"
        )
        duckdb_table = duckdb.read_parquet(file).fetchall()
        self.assertEqual(duckdb_table, [
            (1, 2, 3, 4, 5, 6),
            (None, None, None, 1, 1, None)
        ])
        polars_df = pl.read_parquet(file)
        self.assertEqual(polars_df.schema, {
            "id": pl.Int16,
            "id2": pl.Int32,
            "id3": pl.Int64,
            "id4": pl.Int32,
            "id5": pl.Int64,
            "id6": pl.UInt32
        })
    def test_floats(self):
        file = wrappers.create_and_export(
            "float_types", "id",
            "id int, f32 real, f64 double precision",
            "(1, 1.1, 2.2), (2, 'NaN'::real, 'NaN'::double precision), (3, 'inf'::real, 'inf'::double precision), (4, '-inf'::real, '-inf'::double precision), (5, '-0'::real, '-0'::double precision)"
        )

        duckdb_table = duckdb.read_parquet(file).fetchall()
        self.assertEqual(duckdb_table[0], (1, 1.100000023841858, 2.2))
        self.assertEqual(duckdb_table[1][0], 2)
        self.assertTrue(math.isnan(duckdb_table[1][1]))
        self.assertTrue(math.isnan(duckdb_table[1][2]))
        self.assertEqual(duckdb_table[2], (3, float('inf'), float('inf')))
        self.assertEqual(duckdb_table[3], (4, float('-inf'), float('-inf')))
        self.assertEqual(duckdb_table[4], (5, float('-0'), float('-0')))
        self.assertEqual(str(duckdb_table[4][1]), '-0.0')
        self.assertEqual(str(duckdb_table[4][2]), '-0.0')

        polars_df = pl.read_parquet(file)
        self.assertEqual(polars_df.schema, {
            "id": pl.Int32,
            "f32": pl.Float32,
            "f64": pl.Float64
        })
        self.assertEqual(list(polars_df["f32"].cast(str)), ['1.1', 'NaN', 'inf', '-inf', '-0.0'])
        self.assertEqual(list(polars_df["f64"].cast(str)), ['2.2', 'NaN', 'inf', '-inf', '-0.0'])

    def test_numeric(self):
        file = wrappers.create_and_export(
            "numeric_types", "id",
            "id int, normal numeric(10, 5), high_precision numeric(140, 100)",
            "(1, 1000.0001, 1.00000000000000000000000000000000000000000001), (2, 'NaN', 'NaN')"
        )
        duckdb_table = duckdb.read_parquet(file).fetchall()
        self.assertEqual(duckdb_table, [
            (1, Decimal('1000.000100000000000000'), Decimal('1.000000000000000000')),
            (2,  None, None ) # parquet doesn't support NaN, so NULL it is
        ])

        # polars_df = pl.read_parquet(file)
        # print(polars_df)
        # self.assertEqual(polars_df.schema, {
        #     "id": pl.Int32,
        #     "normal": pl.Binary,
        #     "high_precision": pl.Binary
        # })

        file2 = wrappers.run_export("numeric_types_higher_prec", "select * from numeric_types order by id", options=["--decimal-precision=76", "--decimal-scale=50"])
        # only PyArrow supports precision=76
        pd_df = pd.read_parquet(file2, engine="pyarrow")
        self.assertEqual(list(pd_df["normal"]), [Decimal('1000.000100000000000000'), None])
        self.assertEqual(list(pd_df["high_precision"]), [Decimal('1.00000000000000000000000000000000000000000001'), None])

    def test_numeric_i32(self):
        file = wrappers.create_and_export(
            "numeric_types", "id",
            "id int, normal numeric(10, 5), high_precision numeric(140, 100)",
            "(1, 1000.0001, 1.00000000000000000000000000000000000000000001), (2, 'NaN', 'NaN')",
            options=["--decimal-precision=9", "--decimal-scale=4"]
        )
        duckdb_table = duckdb.read_parquet(file).fetchall()
        self.assertEqual(duckdb_table, [
            (1, Decimal('1000.000100000000000000'), Decimal('1.000000000000000000')),
            (2,  None, None )
        ])

    def test_numeric_i64(self):
        file = wrappers.create_and_export(
            "numeric_types", "id",
            "id int, normal numeric(10, 5), high_precision numeric(140, 100)",
            "(1, 1000.0001, 1.00000000000000000000000000000000000000000001), (2, 'NaN', 'NaN')",
            options=["--decimal-precision=18", "--decimal-scale=9"]
        )
        duckdb_table = duckdb.read_parquet(file).fetchall()
        self.assertEqual(duckdb_table, [
            (1, Decimal('1000.000100000000000000'), Decimal('1.000000000000000000')),
            (2,  None, None )
        ])
    def test_numeric_f64(self):
        file = wrappers.create_and_export(
            "numeric_types", "id",
            "id int, normal numeric(10, 5), high_precision numeric(140, 100)",
            "(1, 1000.0001, 1.00000000000000000000000000000000000000000001), (2, 'NaN', 'NaN')",
            options=["--numeric-handling=double"]
        )
        duckdb_table = duckdb.read_parquet(file).fetchall()
        self.assertEqual(duckdb_table[0], (1, 1000.0001, 1))
        self.assertTrue(math.isnan(duckdb_table[1][1]))
        self.assertTrue(math.isnan(duckdb_table[1][2]))
        schema = pl.read_parquet(file).schema
        self.assertEqual(schema["normal"], pl.Float64)
        self.assertEqual(schema["high_precision"], pl.Float64)

    def test_numeric_f32(self):
        file = wrappers.create_and_export(
            "numeric_types", "id",
            "id int, normal numeric(10, 5), high_precision numeric(140, 100)",
            "(1, 1000.0001, 1.00000000000000000000000000000000000000000001), (2, 'NaN', 'NaN')",
            options=["--numeric-handling=float32"]
        )
        duckdb_table = duckdb.read_parquet(file).fetchall()
        self.assertEqual(duckdb_table[0], (1, 1000.0001220703125, 1))
        self.assertTrue(math.isnan(duckdb_table[1][1]))
        self.assertTrue(math.isnan(duckdb_table[1][2]))
        schema = pl.read_parquet(file).schema
        self.assertEqual(schema["normal"], pl.Float32)
        self.assertEqual(schema["high_precision"], pl.Float32)
    def test_numeric_string(self):
        file = wrappers.create_and_export(
            "numeric_types", "id",
            "id int, normal numeric(10, 5), high_precision numeric(140, 100)",
            "(1, 1000.0001, 1.00000000000000000000000000000000000000000001), (2, 'NaN', 'NaN')",
            options=["--numeric-handling=string"]
        )
        duckdb_table = duckdb.read_parquet(file).fetchall()
        self.assertEqual(duckdb_table, [
            (1, '1000.00010', '1.0000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000000'),
            (2, 'NaN', 'NaN' )
        ])

    def test_bytes(self):
        file = wrappers.create_and_export(
            "bytes_types", "id",
            "id int, bytes bytea, onebyte \"char\"",
            "(1, 'foo'::bytea, 'x'), (2, '\\x0001', 0::\"char\"), (3, NULL, NULL)"
        )
        duckdb_table = duckdb.read_parquet(file).fetchall()
        self.assertEqual(duckdb_table, [
            (1, b'foo', ord('x')),
            (2, b'\x00\x01', 0),
            (3, None, None)
        ])
        polars_df = pl.read_parquet(file)
        self.assertEqual(polars_df.schema, {
            "id": pl.Int32,
            "bytes": pl.Binary,
            "onebyte": pl.UInt8
        })
    def test_uuid(self):
        file = wrappers.create_and_export(
            "uuid_types", "id",
            "id int, uuid uuid",
            "(1, '0000000a-000b-000c-000d-e00000000001'::uuid), (2, NULL)"
        )
        duckdb_table = duckdb.read_parquet(file).fetchall()
        self.assertEqual(duckdb_table, [
            (1, uuid.UUID('0000000a-000b-000c-000d-e00000000001')),
            (2, None)
        ])

    def test_dates(self):
        file = wrappers.create_and_export(
            "date_types", "id",
            "id int, date date, time time, timestamp timestamp, timestamp_tz timestamptz",
            "(1, '2000-01-01', '12:34:56', '2000-01-01 12:34:56', '2000-01-01 12:34:56'), (2, NULL, NULL, NULL, NULL)"
        )

        duckdb_table = duckdb.read_parquet(file).fetchall()
        self.assertEqual(duckdb_table, [
            (1, datetime.date(2000, 1, 1), datetime.time(12, 34, 56), datetime.datetime(2000, 1, 1, 12, 34, 56), datetime.datetime(2000, 1, 1, 12, 34, 56, tzinfo=datetime.timezone.utc).astimezone()),
            (2, None, None, None, None)
        ])

    def test_interval(self):
        file = wrappers.create_and_export(
            "interval_types", "id",
            "id int, interval interval",
            "(1, '1 year 2 months 1 days 40 hours 5 mins 6 secs 1 microsecond'), (2, NULL)"
        )

        duckdb_table = duckdb.read_parquet(file).fetchall()
        secperhour = 60 * 60
        self.assertEqual(duckdb_table, [
            (1, datetime.timedelta(days=422, seconds=16 * secperhour + 5 * 60 + 6)),
            #                        ^ Python doesn't support months, so duckdb convert it to days
            (2, None)
        ])
        duckdb_tostringed = duckdb.sql(f"select id, interval::string from '{file}'").fetchall()
        self.assertEqual(duckdb_tostringed, [
            (1, '1 year 2 months 2 days 16:05:06'),
            (2, None)
        ])
    def test_interval_struct(self):
        file = wrappers.run_export(
            "interval_types_struct",
            "select * from interval_types order by id",
            options=["--interval-handling=struct"]
        )

        duckdb_table = duckdb.read_parquet(file).fetchall()
        secperhour = 60 * 60
        theobject = { "months": 14, "days": 1, "microseconds": 1000000 * (secperhour * 40 + 5 * 60 + 6) + 1 }
        self.assertEqual(duckdb_table, [
            (1, theobject ),
            (2, None)
        ])
        
        pd_df = pd.read_parquet(file)
        self.assertEqual(pd_df["interval"][0], theobject)

        pl_df = pl.read_parquet(file)
        self.assertEqual(pl_df["interval"][0], theobject)

    def test_bits(self):
        file = wrappers.create_and_export(
            "bits_types", "id",
            "id int, bits bit(6), varbits varbit(6), bool bool",
            "(1, B'101000', B'101', true), (2, NULL, NULL, NULL)"
        )

        duckdb_table = duckdb.read_parquet(file).fetchall()
        self.assertEqual(duckdb_table, [
            (1, "101000", "101", True),
            (2, None, None, None)
        ])

        polars_df = pl.read_parquet(file)
        self.assertEqual(polars_df.schema, {
            "id": pl.Int32,
            "bits": pl.Utf8,
            "bool": pl.Boolean,
            "varbits": pl.Utf8
        })

    

