import wrappers
import unittest

class TestCLI(unittest.TestCase):
    def test_prints_version(self) -> None:
        p = wrappers.run_pg2parquet(["--version"])
        out = p.stdout.decode("utf-8").strip()
        self.assertTrue(out.startswith("pg2parquet 0."))
        self.assertEqual(1, len(out.splitlines()))

    def test_help_global(self) -> None:
        p = wrappers.run_pg2parquet(["--help"])
        out = p.stdout.decode("utf-8").strip()
        self.assertIn("export", out)
        self.assertIn("Exports a PostgreSQL table or query to a Parquet file", out)
        self.assertIn("--version", out)

    def test_help_export(self) -> None:
        p = wrappers.run_pg2parquet(["export", "--help"])
        out = p.stdout.decode("utf-8").strip()
        self.assertIn("--output-file", out)
        self.assertIn("--table", out)

    def test_h_export(self) -> None:
        p = wrappers.run_pg2parquet(["export", "-h"])
        out = p.stdout.decode("utf-8").strip()
        self.assertIn("--output-file", out)
        self.assertIn("--table", out)
        self.assertLess(len(out.splitlines()), 40) # short help would better fit on a screen
