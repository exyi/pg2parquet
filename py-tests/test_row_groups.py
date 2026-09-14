import unittest

import pyarrow.parquet as pq

import wrappers


class TestRowGroupSplitting(unittest.TestCase):
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
