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
            """,
            options=["--numeric-handling=decimal"]
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

    def test_ranges(self):
        self.maxDiff = None
        file = wrappers.create_and_export(
            "arrays_ranges", "id",
            "id int, simple_range int4range, rint_array int4range[], rnum_array numrange[], rts_array tsrange[]",
            """
                (1, '[1,2)', ARRAY['[1,2)'::int4range,'[2,3)'], ARRAY['[1.1,2.2)'::numrange,'[2.2,3.3)'], ARRAY['[2020-01-01 00:00:00,2020-01-02 00:00:00)'::tsrange,'[2020-01-02 00:00:00,2020-01-03 00:00:00)'::tsrange]),
                (2, NULL, NULL, NULL, NULL),
                (3, '(,2]', ARRAY[NULL::int4range, '(2,)', 'empty'], ARRAY['(1.1,)'::numrange,'(,)'], ARRAY[]::tsrange[])
            """,
            options=["--numeric-handling=decimal"]
        )
        def r(low, up, low_inc=True, up_inc=False, is_empty=False):
            return {'lower': low, 'upper': up, 'lower_inclusive': low_inc, 'upper_inclusive': up_inc, 'is_empty': is_empty}
        duckdb_table = duckdb.read_parquet(file).fetchall()
        self.assertEqual(duckdb_table[0],
            (1, r(1, 2),
                [r(1, 2), r(2, 3)],
                [r(Decimal('1.1'), Decimal('2.2')), r(Decimal('2.2'), Decimal('3.3'))],
                [r(datetime.datetime(2020, 1, 1, 0, 0), datetime.datetime(2020, 1, 2, 0, 0)), r(datetime.datetime(2020, 1, 2, 0, 0), datetime.datetime(2020, 1, 3, 0, 0))])
        )
        self.assertEqual(duckdb_table[1], (2, None, None, None, None))
        self.assertEqual(duckdb_table[2],
            (3, r(None, 3, low_inc=False),
                [None, r(3, None), r(None, None, is_empty=True, low_inc=False)],
                [r(Decimal('1.1'), None, low_inc=False, up_inc=False), r(None, None, low_inc=False)],
                [])
        )

        pl_df = pl.read_parquet(file)
        self.assertEqual(pl_df["id"].to_list(), [1, 2, 3])
        # TODO: polars is probably buggy with NULL struct
        # self.assertEqual(pl_df["simple_range"].to_list(), [ r[1] for r in duckdb_table ])
        # self.assertEqual(pl_df["rint_"].to_list(), [ r[2] for r in duckdb_table ])


    def test_multidim(self):
        self.maxDiff = None
        plain_file = wrappers.create_and_export(
            "arrays_multidim", "id",
            "id int, a int[], b text[]",
            """
                (1, ARRAY[[1,2],[3,4],[NULL,5]], ARRAY[[NULL,'b'],['c',NULL]]),
                (2, NULL, NULL),
                (3, ARRAY[]::int[], ARRAY[[[]]]::text[]),
                (4, ARRAY[[[[1]]]], '{{{a}}}'::text[]),
                (5, '[-2:0]={1,2,3}'::int[], '[-1:0][4:5]={{a,b},{c,d}}'::text[])
            """
        )

        expected_ids = [1, 2, 3, 4, 5]
        expected_a = [ [ 1, 2, 3, 4, None, 5 ], None, [], [1], [1, 2, 3] ]
        expected_b = [ [ None, "b", "c", None ], None, [], ["a"], ["a", "b", "c", "d"] ]
        plain_df = pl.read_parquet(plain_file)
        self.assertEqual(plain_df["id"].to_list(), expected_ids)
        self.assertEqual(plain_df["a"].to_list(), expected_a)
        self.assertEqual(plain_df["b"].to_list(), expected_b)

        dims_file = wrappers.run_export_table("arrays_multidim_dims", "arrays_multidim", "id", options=["--array-handling=dims"])
        dims_df = pl.read_parquet(dims_file)
        self.assertEqual(dims_df["id"].to_list(), expected_ids)
        self.assertEqual(dims_df["a"].struct.field("data").to_list(), expected_a)
        self.assertEqual(dims_df["b"].struct.field("data").to_list(), expected_b)
        self.assertEqual(dims_df["a"].struct.field("dims").to_list(), [ [3, 2], None, [], [1, 1, 1, 1], [3] ])
        self.assertEqual(dims_df["b"].struct.field("dims").to_list(), [ [2, 2], None, [], [1, 1, 1], [2, 2] ])

        dims_lb_file = wrappers.run_export_table("arrays_multidim_dims_lb", "arrays_multidim", "id", options=["--array-handling=dims+lb"])
        dims_lb_df = pl.read_parquet(dims_lb_file)
        self.assertEqual(dims_lb_df["id"].to_list(), expected_ids)
        self.assertEqual(dims_lb_df["a"].struct.field("data").to_list(), expected_a)
        self.assertEqual(dims_lb_df["b"].struct.field("data").to_list(), expected_b)
        self.assertEqual(dims_lb_df["a"].struct.field("dims").to_list(), [ [3, 2], None, [], [1, 1, 1, 1], [3] ])
        self.assertEqual(dims_lb_df["a"].struct.field("lower_bound").to_list(), [ [1, 1], None, [], [1, 1, 1, 1], [-2] ])
        self.assertEqual(dims_lb_df["b"].struct.field("lower_bound").to_list(), [ [1, 1], None, [], [1, 1, 1], [-1, 4] ])

