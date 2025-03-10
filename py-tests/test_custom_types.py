import datetime
from decimal import Decimal
import math
import uuid

import numpy as np
import wrappers
import unittest
import duckdb
import polars as pl
import pandas as pd

wrappers.run_sql(
    """CREATE TYPE weekday AS ENUM ('monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday');""",
    """CREATE TYPE weekday_range AS RANGE (subtype = weekday);""",
    """CREATE TYPE chain_id AS (pdbid char(4), model int, chain char(1));""",
)

class TestBasic(unittest.TestCase):
    def test_enums_text(self):
        file = wrappers.create_and_export(
            "custom_enums_text", "id",
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
            "custom_enums_plaintext", "id",
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
            "custom_enums_int", "id",
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

    def test_composite_type_simple(self):
        self.maxDiff = None
        file = wrappers.create_and_export(
            "custom_composite_type_simple", "id",
            "id int, a chain_id, b chain_id[]",
            """(1, ROW('1ehz', 1, 'A'), '{}'),
               (2, NULL, NULL),
               (3, '(,,)', ARRAY[NULL::chain_id, '(1ehz,1,A)', '(,,A)'])
            """)
        duckdb_table = duckdb.read_parquet(file).fetchall()
        self.assertEqual(duckdb_table[0], (1, {'pdbid': '1ehz', 'model': 1, 'chain': 'A'}, []))
        self.assertEqual(duckdb_table[1], (2, None, None))
        self.assertEqual(duckdb_table[2], (3, {'chain': None, 'model': None, 'pdbid': None}, [None, {'pdbid': '1ehz', 'model': 1, 'chain': 'A'}, {'pdbid': None, 'model': None, 'chain': 'A'}]))

        pl_df = pl.read_parquet(file)
        self.assertEqual(pl_df.schema, {
            "id": pl.Int32,
            "a": pl.Struct({
                "pdbid": pl.Utf8,
                "model": pl.Int32,
                "chain": pl.Utf8,
            }),
            "b": pl.List(pl.Struct({
                "pdbid": pl.Utf8,
                "model": pl.Int32,
                "chain": pl.Utf8,
            }))
        })
        self.assertEqual(pl_df["id"].to_list(), [1, 2, 3])
        self.assertEqual(pl_df["a"].to_list(), [
            {'pdbid': '1ehz', 'model': 1, 'chain': 'A'},
            None,
            {'pdbid': None, 'model': None, 'chain': None}
        ])
        self.assertEqual(pl_df["b"].to_list(), [
            [],
            None,
            [ None, {'pdbid': '1ehz', 'model': 1, 'chain': 'A'}, {'pdbid': None, 'model': None, 'chain': 'A'}]
        ])

        pd_df = pd.read_parquet(file)
        pd_rows = [*pd_df.itertuples()]
        self.assertEqual(tuple(pd_rows[0])[:-1], (0, 1, {'pdbid': '1ehz', 'model': 1.0, 'chain': 'A'}))
        self.assertEqual(list(tuple(pd_rows[0])[-1]), [])
        self.assertEqual(tuple(pd_rows[1]), (1, 2, None, None))
        self.assertEqual(tuple(pd_rows[2])[:-1], (2, 3, {'pdbid': None, 'model': None, 'chain': None}))
        self.assertEqual(list(tuple(pd_rows[2])[-1]), [None, {'pdbid': '1ehz', 'model': 1.0, 'chain': 'A'}, {'pdbid': None, 'model': None, 'chain': 'A'}])


