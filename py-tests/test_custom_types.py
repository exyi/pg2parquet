import datetime
from decimal import Decimal
import math
import uuid
import wrappers
import unittest
import duckdb
import polars as pl
import pandas as pd

wrappers.run_sql(
    """CREATE TYPE weekday AS ENUM ('monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday');""",
    """CREATE TYPE weekday_range AS RANGE (subtype = weekday);""",
)

class TestBasic(unittest.TestCase):
    def test_enums_text(self):
        file = wrappers.create_and_export(
            "custom_enums", "id",
            "id int, a weekday, b weekday[]",
            """(1, 'monday', ARRAY['monday'::weekday, 'tuesday']),
               (2, NULL, NULL),
               (3, 'sunday', ARRAY['sunday'::weekday, NULL, 'monday'])
            """
        )
        duckdb_table = duckdb.read_parquet(file).fetchall()
        self.assertEqual(duckdb_table[0], (1, "monday", ["monday", "tuesday"]))
        self.assertEqual(duckdb_table[1], (2, None, None))
        self.assertEqual(duckdb_table[2], (3, "sunday", ["sunday", None, "monday"]))

        polars_df = pl.read_parquet(file)
        self.assertEqual(polars_df.schema, {
            "id": pl.Int32,
            "a": pl.Binary, # enums not supported?
            "b": pl.List(pl.Binary),
        })
        self.assertEqual(polars_df["id"].to_list(), [1, 2, 3])
        self.assertEqual(polars_df["a"].to_list(), [b"monday", None, b"sunday"])
        self.assertEqual(polars_df["b"].to_list(), [[b"monday", b"tuesday"], None, [b"sunday", None, b"monday"]])

    def test_enums_plaintext(self):
        file = wrappers.create_and_export(
            "custom_enums", "id",
            "id int, a weekday, b weekday[]",
            """(1, 'monday', ARRAY['monday'::weekday, 'tuesday']),
               (2, NULL, NULL),
               (3, 'sunday', ARRAY['sunday'::weekday, NULL, 'monday'])
            """,
            options=["--enum-handling=plain-text"]
        )
        duckdb_table = duckdb.read_parquet(file).fetchall()
        self.assertEqual(duckdb_table[0], (1, "monday", ["monday", "tuesday"]))
        self.assertEqual(duckdb_table[1], (2, None, None))
        self.assertEqual(duckdb_table[2], (3, "sunday", ["sunday", None, "monday"]))

        polars_df = pl.read_parquet(file)
        self.assertEqual(polars_df.schema, {
            "id": pl.Int32,
            "a": pl.Utf8,
            "b": pl.List(pl.Utf8),
        })
        self.assertEqual(polars_df["id"].to_list(), [1, 2, 3])
        self.assertEqual(polars_df["a"].to_list(), ["monday", None, "sunday"])
        self.assertEqual(polars_df["b"].to_list(), [["monday", "tuesday"], None, ["sunday", None, "monday"]])

    def test_enums_int(self):
        file = wrappers.create_and_export(
            "custom_enums", "id",
            "id int, a weekday, b weekday[]",
            """(1, 'monday', ARRAY['monday'::weekday, 'tuesday']),
               (2, NULL, NULL),
               (3, 'sunday', ARRAY['sunday'::weekday, NULL, 'monday'])
            """,
            options=["--enum-handling=int"]
        )
        duckdb_table = duckdb.read_parquet(file).fetchall()
        self.assertEqual(duckdb_table[0], (1, 1, [1, 2]))
        self.assertEqual(duckdb_table[1], (2, None, None))
        self.assertEqual(duckdb_table[2], (3, 7, [7, None, 1]))

        polars_df = pl.read_parquet(file)
        self.assertEqual(polars_df.schema, {
            "id": pl.Int32,
            "a": pl.Int32,
            "b": pl.List(pl.Int32),
        })
        self.assertEqual(polars_df["id"].to_list(), [1, 2, 3])
        self.assertEqual(polars_df["a"].to_list(), [1, None, 7])
        self.assertEqual(polars_df["b"].to_list(), [[1, 2], None, [7, None, 1]])

    def test_enum_ranges(self):
        self.maxDiff = None
        file = wrappers.create_and_export(
            "custom_enum_ranges", "id",
            "id int, a weekday, b weekday_range, c weekday_range[]",
            """
                (1, 'monday', '[monday,tuesday]', ARRAY['[monday,tuesday)'::weekday_range, '[tuesday,thursday)']),
                (2, NULL, NULL, NULL),
                (3, 'tuesday', '[tuesday,tuesday]', ARRAY[NULL::weekday_range]),
                (4, 'wednesday', '(,)', NULL),
                (5, 'thursday', 'empty', NULL)
            """,
            options=["--enum-handling=plain-text"]
        )
        def r(low, up, low_inc=True, up_inc=False, is_empty=False):
            return {'lower': low, 'upper': up, 'lower_inclusive': low_inc, 'upper_inclusive': up_inc, 'is_empty': is_empty}

        duckdb_table = duckdb.read_parquet(file).fetchall()
        self.assertEqual(duckdb_table[0],
            (1, "monday", r("monday", "tuesday", up_inc=True), [r("monday", "tuesday"), r("tuesday", "thursday")])
        )
        self.assertEqual(duckdb_table[1], (2, None, None, None))
        self.assertEqual(duckdb_table[2], (3, "tuesday", r("tuesday", "tuesday", up_inc=True), [None]))
        self.assertEqual(duckdb_table[3], (4, "wednesday", r(None, None, low_inc=False), None))
        self.assertEqual(duckdb_table[4], (5, "thursday", r(None, None, low_inc=False, is_empty=True), None))

        pl_df = pl.read_parquet(file)
        range_struct = pl.Struct({
            "lower": pl.Utf8,
            "upper": pl.Utf8,
            "lower_inclusive": pl.Boolean,
            "upper_inclusive": pl.Boolean,
            "is_empty": pl.Boolean,
        })
        self.assertEqual(pl_df.schema, {
            "id": pl.Int32,
            "a": pl.Utf8,
            "b": range_struct,
            "c": pl.List(range_struct)
        })

