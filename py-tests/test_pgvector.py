import datetime
from decimal import Decimal
import math
import uuid
import wrappers
import unittest
import duckdb
import json
import polars as pl
import pandas as pd
import numpy as np


class TestBasic(unittest.TestCase):
    def test_basic_vectors(self):
        file = wrappers.create_and_export(
            "vector_simple", "id",
            "id int, a vector(5), b halfvec(5), c sparsevec(5), d bit(5)",
            """(1, '[1.0001,2,3,4,100000]', '[6.0e-8,9.0e-8,3,4,5]', '{1:-1,5:5.25}/5', '10001'),
               (2, NULL, NULL, NULL, NULL)
            """
        )
        duckdb_table = duckdb.read_parquet(file).fetchall()
        pandas_df = pd.read_parquet(file, engine="pyarrow")
        polars_df = pl.read_parquet(file)

        self.assertEqual(duckdb_table[0][0], 1)
        self.assertEqual(duckdb_table[0][1], [1.000100016593933, 2.0, 3.0, 4.0, 100000.0])
        self.assertEqual(duckdb_table[0][2], [5.960464477539063e-08, 1.1920928955078125e-07, 3.0, 4.0, 5.0])
        self.assertEqual(duckdb_table[0][3], {1:-1.0,5:5.25})
        self.assertEqual(duckdb_table[0][4], '10001')
        self.assertEqual(duckdb_table[1], (2, None, None, None, None))

        self.assertEqual(polars_df.schema, {
            "id": pl.Int32,
            "a": pl.List(pl.Float32),
            "b": pl.List(pl.Float32),
            "c": pl.List(pl.Struct({"key": pl.UInt32, "value": pl.Float32})),
            "d": pl.String
        })
        self.assertEqual(polars_df["id"].to_list(), [1, 2])
        self.assertEqual(polars_df["a"].to_list(), [[1.000100016593933,2.0, 3.0, 4.0, 100000.0], None])
        self.assertEqual(polars_df["b"].to_list(), [[5.960464477539063e-08, 1.1920928955078125e-07, 3.0, 4.0, 5.0], None])
        self.assertEqual(polars_df["c"].to_list(), [[{'key': 1, 'value': -1.0}, {'key': 5, 'value': 5.25}], None])
        self.assertEqual(polars_df["d"].to_list(), [ '10001', None ])


    def test_vector_arrays(self):
        file = wrappers.create_and_export(
            "vector_arrays", "id",
            "id int, a vector(2)[], b halfvec(2)[], c sparsevec(10)[], d bit(5)[]",
            """(1, array['[1,2]'::vector], array['[1,2]'::halfvec], array['{4:1,8:2}/10'::sparsevec], array['10001'::bit(5)]),
               (2, NULL, NULL, NULL, NULL),
               (3, '{}'::vector[], '{}'::halfvec[], '{}'::sparsevec[], '{}'::bit[]),
               (4, '{NULL}'::vector[], '{NULL}'::halfvec[], '{NULL}'::sparsevec[], '{NULL}'::bit[]),
               (5, '{NULL,NULL}'::vector[], '{NULL,NULL}'::halfvec[], '{NULL,NULL}'::sparsevec[], '{NULL,NULL}'::bit[]),
               (6, array['[0,0]'::vector], array['[0,0]'::halfvec], array['{}/10'::sparsevec,'{}/10'::sparsevec,'{}/10'::sparsevec], array['00000'::bit(5)]),
               (7, array[NULL,'[1,2]'::vector,'[1,2]'::vector,'[1,2]'::vector,NULL,'[1,2]'::vector,NULL], array['[1,2]'::halfvec,NULL,'[1,2]'::halfvec,NULL], array[NULL,'{4:1,8:2}/10'::sparsevec,NULL,'{4:1,8:2}/10'::sparsevec,NULL], NULL)
            """
        )
        duckdb_table = duckdb.read_parquet(file).fetchall()
        polars_df = pl.read_parquet(file)
        pandas_df = pd.read_parquet(file, engine="pyarrow")

        self.assertEqual(polars_df.schema, {
            "id": pl.Int32,
            "a": pl.List(pl.List(pl.Float32)),
            "b": pl.List(pl.List(pl.Float32)),
            "c": pl.List(pl.List(pl.Struct({"key": pl.UInt32, "value": pl.Float32}))),
            "d": pl.List(pl.String)
        })

        self.assertEqual(duckdb_table[0], (1, [[1.0,2.0]], [[1.0,2.0]], [{4:1.0,8:2.0}], ['10001']))
        self.assertEqual(duckdb_table[1], (2, None, None, None, None))
        self.assertEqual(duckdb_table[2], (3, [], [], [], []))
        self.assertEqual(duckdb_table[3], (4, [None], [None], [None], [None]))
        self.assertEqual(duckdb_table[4], (5, [None,None], [None,None], [None,None], [None,None]))
        self.assertEqual(duckdb_table[5], (6, [[0.0,0.0]], [[0.0,0.0]], [{}, {}, {}], ['00000']))
        self.assertEqual(duckdb_table[6], (7, [None,[1.0,2.0],[1.0,2.0],[1.0,2.0],None,[1.0,2.0],None], [[1.0,2.0],None,[1.0,2.0],None], [None,{4:1.0,8:2.0},None,{4:1.0,8:2.0},None], None))

        polars_rows = list(polars_df.iter_rows())
        self.assertEqual(polars_rows[0], (1, [[1.0,2.0]], [[1.0,2.0]], [[{"key":4, "value": 1.0}, {"key":8, "value": 2.0}]], ['10001']))
        self.assertEqual(polars_rows[1], (2, None, None, None, None))
        self.assertEqual(polars_rows[2], (3, [], [], [], []))
        self.assertEqual(polars_rows[3], (4, [None], [None], [None], [None]))
        self.assertEqual(polars_rows[4], (5, [None,None], [None,None], [None,None], [None,None]))
        self.assertEqual(polars_rows[5], (6, [[0.0,0.0]], [[0.0,0.0]], [[], [], []], ['00000']))
        self.assertEqual(polars_rows[6], (7, [None,[1.0,2.0],[1.0,2.0],[1.0,2.0],None,[1.0,2.0],None], [[1.0,2.0],None,[1.0,2.0],None], [None,[{"key":4, "value": 1.0}, {"key":8, "value": 2.0}],None,[{"key":4, "value": 1.0}, {"key":8, "value": 2.0}],None], None))

        pandas_obj = json.loads(pandas_df.to_json(orient='records'))
        self.assertEqual(pandas_obj[0], {"id": 1, "a": [[1.0,2.0]], "b": [[1.0,2.0]], "c": [[[4, 1.0], [8, 2.0]]], "d": ['10001']})
        self.assertEqual(pandas_obj[1], {"id": 2, "a": None, "b": None, "c": None, "d": None})
        self.assertEqual(pandas_obj[2], {"id": 3, "a": [], "b": [], "c": [], "d": []})
        self.assertEqual(pandas_obj[3], {"id": 4, "a": [None], "b": [None], "c": [None], "d": [None]})
        self.assertEqual(pandas_obj[4], {"id": 5, "a": [None,None], "b": [None,None], "c": [None,None], "d": [None,None]})
        self.assertEqual(pandas_obj[5], {"id": 6, "a": [[0.0,0.0]], "b": [[0.0,0.0]], "c": [[], [], []], "d": ['00000']})
        self.assertEqual(pandas_obj[6], {"id": 7, "a": [None,[1.0,2.0],[1.0,2.0],[1.0,2.0],None,[1.0,2.0],None], "b": [[1.0,2.0],None,[1.0,2.0],None], "c": [None,[[4, 1.0], [8, 2.0]],None,[[4, 1.0], [8, 2.0]],None], "d": None})


    def test_float16_vectors(self):
        file = wrappers.create_and_export(
            "vector_float16", "id",
            "id int, a halfvec(7), b halfvec(2)[]",
            """(1, '[1.0001,0,1.001,-4,10000,10001,-10000]', array['[1,2]'::halfvec, '[3,4]'::halfvec,'[6.0e-8,9.0e-8]'::halfvec]),
               (2, NULL, NULL),
               (3, NULL, array[NULL,'[1,2]'::halfvec,NULL,NULL,NULL,'[1,2]'::halfvec,NULL])
            """,
            ["--float16-handling=float32"]
        )
        duckdb_table = duckdb.read_parquet(file).fetchall()
        self.assertEqual(duckdb_table[0], (1, [1.0,0.0,1.0009765625,-4.0,10000.0,10000.0,-10000.0], [[1.0,2.0],[3.0,4.0],[5.960464477539063e-8, 1.1920928955078125e-7]]))
        self.assertEqual(duckdb_table[1], (2, None, None))
        self.assertEqual(duckdb_table[2], (3, None, [None, [1.0, 2.0], None, None, None, [1.0, 2.0], None]))

        f16_file = wrappers.run_export_table("vector_float16_f16", "vector_float16", "id", ["--float16-handling=float16"])

        f16_table = pd.read_parquet(f16_file, engine="pyarrow") # pandas/pyarrow is the only library which currently supports it
        print(f16_file)
        print(f16_table)
        self.assertEqual(json.loads(f16_table["a"].to_json()), {"0": [1.0,0.0,1.0009765625,-4.0,10000.0,10000.0,-10000.0], "1": None,"2": None })
        self.assertEqual(json.loads(f16_table["b"].to_json()), {
            "0": [[1.0,2.0], [3.0,4.0],[5.96e-8, 1.192e-7]], # pandas is just losing precision in to_json (if only they had to_python...)
            "1": None,
            "2": [ None, [1.0,2.0], None, None, None, [1.0,2.0], None ]
        })


    def test_nested_composites(self):
        wrappers.run_sql(
            """CREATE TYPE cc_vector_wrapper AS (single vector, half halfvec);""",
            """CREATE TYPE cc_vector_array AS (wrappers cc_vector_wrapper[], halfarr halfvec, sparse sparsevec);""",
            """CREATE TYPE cc_vector_lvl3 AS (arr cc_vector_array[], notarray cc_vector_wrapper);""",
        )

        file = wrappers.create_and_export(
            "vector_nested_composites", "id",
            "id int, a cc_vector_wrapper[], b cc_vector_array, c cc_vector_array[], d cc_vector_lvl3[]",
            """(1, NULL, NULL, NULL, NULL),
               (2, '{}', row('{}'::cc_vector_wrapper[], NULL::halfvec, NULL::sparsevec), '{}', '{}'),
               (3, array[NULL,row('[1,2]', '[5,6]')::cc_vector_wrapper,NULL,row('[1,2]', '[5,6]')::cc_vector_wrapper,NULL], NULL,NULL,NULL),

               (4, NULL, row(array[row('[1,2]', '[5,6]')::cc_vector_wrapper,NULL,row('[1,2,3,4,5,6,7,8]', NULL)::cc_vector_wrapper], '[1,2]'::halfvec, '{4:1,8:2}/10'::sparsevec), NULL, NULL),

               (5, NULL, NULL, array[row(array[row('[1,2]', '[5,6]')::cc_vector_wrapper,NULL::cc_vector_wrapper,row('[1,2,3,4,5,6,7,8]', NULL)::cc_vector_wrapper], '[1,2]'::halfvec, '{4:1,8:2}/10'::sparsevec)::cc_vector_array, row(NULL,NULL,NULL)::cc_vector_array, row('{}'::cc_vector_wrapper[],NULL,NULL)::cc_vector_array], NULL),

               (6, NULL, NULL, NULL, array[
                  row(
                    array[
                        NULL::cc_vector_array,
                        row(array[row('[1,2]', '[5,6]')::cc_vector_wrapper,NULL,row('[1,2,3,4,5,6,7,8]', NULL)::cc_vector_wrapper], '[1,2]'::halfvec, '{4:1,8:2}/10'::sparsevec)::cc_vector_array,
                        NULL::cc_vector_array
                    ],
                    row(NULL, '[1,2]')::cc_vector_wrapper
                  )::cc_vector_lvl3,
                  row('{}'::cc_vector_array[], row(NULL, NULL)::cc_vector_wrapper)::cc_vector_lvl3,
                  row(NULL, NULL)::cc_vector_lvl3,
                  NULL
               ])
            """
        )

        duckdb_table = duckdb.read_parquet(file).fetchall()
        polars_df = pl.read_parquet(file)
        pandas_df = pd.read_parquet(file, engine="pyarrow")

        for x in duckdb_table: print(x)

        self.assertEqual(duckdb_table[0], (1, None, None, None, None))
        self.assertEqual(duckdb_table[1], (2, [], {'halfarr': None, 'sparse': None, 'wrappers': []}, [], []))
        self.assertEqual(duckdb_table[2], (3, [None, {'single': [1.0, 2.0], 'half': [5.0, 6.0]}, None, {'single': [1.0, 2.0], 'half': [5.0, 6.0]}, None], None, None, None))
        self.assertEqual(duckdb_table[3], (4, None, {'wrappers': [{'single': [1.0, 2.0], 'half': [5.0, 6.0]}, None, {'single': [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0], 'half': None}], 'halfarr': [1.0, 2.0], 'sparse': {4: 1.0, 8: 2.0}}, None, None))
        self.assertEqual(duckdb_table[4], (5, None, None, [{'wrappers': [{'single': [1.0, 2.0], 'half': [5.0, 6.0]}, None, {'single': [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0], 'half': None}], 'halfarr': [1.0, 2.0], 'sparse': {4: 1.0, 8: 2.0}}, {'wrappers': None, 'halfarr': None, 'sparse': None}, {'wrappers': [], 'halfarr': None, 'sparse': None}], None))
        self.assertEqual(duckdb_table[5], (6, None, None, None, [{'arr': [None, {'wrappers': [{'single': [1.0, 2.0], 'half': [5.0, 6.0]}, None, {'single': [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0], 'half': None}], 'halfarr': [1.0, 2.0], 'sparse': {4: 1.0, 8: 2.0}}, None], 'notarray': {'single': None, 'half': [1.0, 2.0]}}, {'arr': [], 'notarray': {'single': None, 'half': None}}, {'arr': None, 'notarray': None}, None]))

