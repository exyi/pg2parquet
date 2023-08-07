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
    def test_arrays(self):
        file = wrappers.create_and_export(
            "simple_arrays", "id",
            "id int, a text[], b int[], c bytea[], d numeric(10, 2)[]",
            """(1, Array['a', 'b'], Array[1, 2], Array['\\x01'::bytea, '\\x02'::bytea], Array[1.1, 2.2]),
               (2, NULL, NULL, NULL, NULL),
               (3, Array[NULL, 'a', NULL, 'b'], Array[]::int[], Array[NULL::bytea], Array[1.1, 'NaN'::numeric, 1000010])
            """
        )
        duckdb_table = duckdb.read_parquet(file).fetchall()
        self.assertEqual(duckdb_table[0], (1, ["a", "b"], [1, 2], [ b"\x01", b"\x02" ], [Decimal("1.10"), Decimal("2.20")]))
        self.assertEqual(duckdb_table[1], (2, None, None, None, None))
        self.assertEqual(duckdb_table[2], (3, [None, "a", None, "b"], [], [None], [Decimal("1.10"), None, Decimal("1000010.000000000000000000")]))

        polars_df = pl.read_parquet(file)
        self.assertEqual(polars_df.schema, {
            "id": pl.Int32,
            "a": pl.List(pl.Utf8),
            "b": pl.List(pl.Int32),
            "c": pl.List(pl.Binary),
            "d": pl.List(pl.Binary)
        })
        self.assertEqual(polars_df["id"].to_list(), [1, 2, 3])
        self.assertEqual(polars_df["a"].to_list(), [["a", "b"], None, [None, "a", None, "b"]])
        self.assertEqual(polars_df["b"].to_list(), [[1, 2], None, []])
        self.assertEqual(polars_df["c"].to_list(), [[b"\x01", b"\x02"], None, [None]])
        self.assertEqual(polars_df["d"].to_list()[1], None)
        self.assertEqual(polars_df["d"].to_list()[2][1], None)

        pd_df = pd.read_parquet(file)
        self.assertEqual(pd_df["a"][0][0], "a")
        self.assertEqual(pd_df["a"][0][1], "b")
        self.assertEqual(pd_df["a"][1], None)
        self.assertEqual(pd_df["a"][2][0], None)
        self.assertEqual(pd_df["a"][2][1], "a")
        self.assertEqual(pd_df["a"][2][2], None)
        self.assertEqual(pd_df["a"][2][3], "b")

        self.assertEqual(len(pd_df["b"][2]), 0)

        self.assertEqual(pd_df["d"][0][0], Decimal("1.1"))
        self.assertEqual(pd_df["d"][2][0], Decimal("1.1"))
        self.assertEqual(pd_df["d"][2][1], None)

