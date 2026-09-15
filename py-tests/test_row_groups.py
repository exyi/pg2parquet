import os
from pathlib import Path
import subprocess
import sys
import time
import unittest

import pyarrow.parquet as pq

import wrappers


class TestRowGroupSplitting(unittest.TestCase):
    @unittest.skipUnless(sys.platform == "linux", "Thread counts require Linux /proc")
    def test_threads_option_limits_writer_threads(self) -> None:
        # The payload makes PostgreSQL stream rows before finishing the query;
        # the delay lets us observe workers while the next batch is being read.
        columns = ", ".join(f"n AS c{i}" for i in range(8))
        outfile = os.path.join(wrappers.output_directory, "thread_limit.parquet")
        command = [
            wrappers.pg2parquet_binary, "export", *wrappers.export_connection_args,
            "--query", f"SELECT {columns}, repeat('x', 4096) AS payload, "
                       "pg_sleep(0.02)::text AS pause FROM generate_series(1, 64) n",
            "--output-file", outfile, "--threads", "1",
            "--row-group-row-limit", "2", "--quiet",
        ]
        max_threads = 0
        with subprocess.Popen(command, env={**os.environ, "PGPASSWORD": wrappers.pg2parquet_password},
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE) as process:
            try:
                deadline = time.monotonic() + 30
                while process.poll() is None and time.monotonic() < deadline:
                    try:
                        status = Path(f"/proc/{process.pid}/status").read_text()
                    except FileNotFoundError:
                        break
                    for line in status.splitlines():
                        if line.startswith("Threads:"):
                            max_threads = max(max_threads, int(line.split()[1]))
                    time.sleep(0.01)
                stdout, stderr = process.communicate(timeout=1)
            finally:
                if process.poll() is None:
                    process.kill()
                    process.communicate()
        self.assertEqual(process.returncode, 0, (stdout, stderr))
        self.assertEqual(pq.ParquetFile(outfile).metadata.num_rows, 64)
        self.assertGreater(max_threads, 0)
        # Allow the main PostgreSQL reader plus one Parquet writer worker.
        self.assertLessEqual(max_threads, 2, "--threads=1 must bound writer concurrency")

    def test_nested_types_across_batches_and_empty_results(self) -> None:
        query = """
            SELECT CASE n % 3 WHEN 0 THEN NULL::int[]
                             WHEN 1 THEN ARRAY[1, NULL, 3]
                             ELSE '{}'::int[] END AS items,
                   CASE n % 3 WHEN 0 THEN NULL::int4range
                             WHEN 1 THEN '[1,4)'::int4range
                             ELSE 'empty'::int4range END AS span
            FROM generate_series(0, 8) n
        """
        for mode in ["plain", "dimensions", "dimensions+lowerbound"]:
            with self.subTest(mode=mode):
                options = ["--array-handling", mode, "--row-group-row-limit", "2"]
                outfile = wrappers.run_export(f"nested_batches_{mode}", query, options)
                parquet = pq.ParquetFile(outfile)
                items = [None, [1, None, 3], []]
                if mode != "plain":
                    items = [None, {"data": items[1], "dims": [3]}, {"data": [], "dims": []}]
                    if mode == "dimensions+lowerbound":
                        items[1]["lower_bound"] = [1]
                        items[2]["lower_bound"] = []
                spans = [None,
                         dict(lower=1, upper=4, lower_inclusive=True, upper_inclusive=False, is_empty=False),
                         dict(lower=None, upper=None, lower_inclusive=False, upper_inclusive=False, is_empty=True)]
                self.assertEqual(parquet.read().to_pydict(), {"items": items * 3, "span": spans * 3})
                self.assertEqual([parquet.metadata.row_group(i).num_rows
                                  for i in range(parquet.num_row_groups)], [2, 2, 2, 2, 1])
                empty = wrappers.run_export(f"nested_empty_{mode}", f"SELECT * FROM ({query}) q WHERE false", options)
                empty_parquet = pq.ParquetFile(empty)
                self.assertEqual(empty_parquet.metadata.num_rows, 0)
                self.assertEqual(empty_parquet.schema, parquet.schema)

    def test_splits_at_the_first_row_or_byte_limit(self) -> None:
        table = "row_group_splitting"
        wrappers.run_sql(
            f"DROP TABLE IF EXISTS {table}",
            f"CREATE TABLE {table} (id int NOT NULL, payload text NOT NULL)",
            f"INSERT INTO {table} SELECT n, repeat('x', 100) FROM generate_series(1, 1000) AS n",
        )

        outfile = wrappers.run_export_table(
            "row_group_splitting_output",
            table,
            "id",
            options=[
                "--row-group-row-limit", "128",
                "--row-group-byte-limit", "8192",
            ],
        )

        parquet = pq.ParquetFile(outfile)
        row_counts = [
            parquet.metadata.row_group(i).num_rows
            for i in range(parquet.metadata.num_row_groups)
        ]

        # Each row accounts for 4 bytes of int storage and 104 bytes of
        # binary storage, so the byte limit wins after 76 rows.
        self.assertEqual(row_counts, [76] * 13 + [12])
        self.assertEqual(sum(row_counts), 1000)
