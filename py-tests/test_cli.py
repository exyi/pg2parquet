import os, subprocess
import duckdb
import wrappers
import unittest

class TestCLI(unittest.TestCase):
    def setUp(self):
        """Create a test table for connection string tests"""
        # Clean up any leftover tables from previous test runs (in case tearDown wasn't called)
        wrappers.run_sql("DROP TABLE IF EXISTS connection_test")

        # Create fresh test table
        wrappers.run_sql(
            "CREATE TABLE connection_test (id int, message text)",
            "INSERT INTO connection_test VALUES (1, 'hello'), (2, 'world')"
        )

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

    def _build_connection_string(self, **overrides):
        """Build a PostgreSQL connection string from test environment variables"""
        host = overrides.get('host', wrappers.pg2parquet_host)
        port = overrides.get('port', wrappers.pg2parquet_port)
        dbname = overrides.get('dbname', wrappers.pg2parquet_dbname)
        user = overrides.get('user', wrappers.pg2parquet_user)
        password = overrides.get('password', wrappers.pg2parquet_password)

        return f"postgres://{user}:{password}@{host}:{port}/{dbname}"

    def _run_export_with_connection_string(self, connection_string=None, use_env_var=False, extra_args=None):
        """Helper to run pg2parquet export with connection string"""
        outfile = os.path.join(wrappers.output_directory, "connection_test.parquet")

        args = [
            wrappers.pg2parquet_binary,
            "export",
            "--table", "connection_test",
            "--output-file", outfile
        ]

        if extra_args:
            args.extend(extra_args)

        env = {}
        if use_env_var and connection_string:
            env["DATABASE_URL"] = connection_string
            # When using DATABASE_URL, we don't need to add --connection
        elif connection_string:
            args.extend(["--connection", connection_string])

        # Don't set PGPASSWORD when using connection string to avoid conflicts
        if not connection_string:
            env["PGPASSWORD"] = wrappers.pg2parquet_password

        result = subprocess.run(args, env={**os.environ, **env}, capture_output=True)
        return result, outfile

    def _verify_export_success(self, result, outfile, expected_data=None):
        """Helper to verify that export was successful and data matches expectations"""
        self.assertEqual(result.returncode, 0,
                        f"Export failed. Stderr: {result.stderr.decode('utf-8')}")

        # Verify the exported data
        data = duckdb.read_parquet(outfile).fetchall()

        if expected_data is None:
            # Default expectation: connection_test table data
            expected_data = [(1, 'hello'), (2, 'world')]
            self.assertEqual(len(data), 2)
            self.assertEqual(sorted(data), expected_data)
        else:
            # Custom expectation
            self.assertEqual(data, expected_data)

    def test_connection_string_flag_long(self):
        """Test --connection flag with full connection string"""
        connection_string = self._build_connection_string()
        result, outfile = self._run_export_with_connection_string(connection_string)

        self._verify_export_success(result, outfile)

    def test_connection_string_flag_short(self):
        """Test -c flag with full connection string"""
        connection_string = self._build_connection_string()
        result, outfile = self._run_export_with_connection_string(connection_string)
        self._verify_export_success(result, outfile)

    def test_database_url_env_var(self):
        """Test DATABASE_URL environment variable"""
        connection_string = self._build_connection_string()
        result, outfile = self._run_export_with_connection_string(
            connection_string, use_env_var=True)
        self._verify_export_success(result, outfile)

    def test_connection_string_vs_individual_params_conflict(self):
        """Test that connection string conflicts with individual connection parameters"""
        connection_string = self._build_connection_string()

        outfile = os.path.join(wrappers.output_directory, "connection_test_conflict.parquet")

        # Try to use both connection string and individual host parameter
        args = [
            wrappers.pg2parquet_binary,
            "export",
            "-c", connection_string,
            "-H", "localhost",  # This should cause a conflict
            "-t", "connection_test",
            "-o", outfile
        ]

        result = subprocess.run(args, capture_output=True)

        # Should fail due to conflicting arguments
        self.assertNotEqual(result.returncode, 0)
        stderr_text = result.stderr.decode('utf-8').lower()
        self.assertTrue("cannot be used with" in stderr_text or "conflicts" in stderr_text)

    def test_invalid_connection_string(self):
        """Test behavior with invalid connection string"""
        invalid_connection_string = "postgres://invalid_user:wrong_pass@host.invalid:5432/invalid_db"

        result, _ = self._run_export_with_connection_string(invalid_connection_string)
        self.assertNotEqual(result.returncode, 0)

        stderr_text = result.stderr.decode('utf-8')
        self.assertIn("connection failed: error connecting to server", stderr_text.lower())

    def test_connection_string_with_ssl_options(self):
        """Test connection string with SSL/TLS options"""
        # Build connection string with SSL options
        base_connection_string = self._build_connection_string()
        connection_string = f"{base_connection_string}?sslmode=prefer"

        result, outfile = self._run_export_with_connection_string(connection_string)

        self._verify_export_success(result, outfile)
        """Test connection string with query export instead of table export"""
        connection_string = self._build_connection_string()

        outfile = os.path.join(wrappers.output_directory, "connection_test_query.parquet")

        args = [
            wrappers.pg2parquet_binary,
            "export",
            "--connection", connection_string,
            "--query", "SELECT id * 2 as doubled_id, UPPER(message) as upper_message FROM connection_test ORDER BY id",
            "--output-file", outfile
        ]

        result = subprocess.run(args, capture_output=True)

        expected_data = [(2, 'HELLO'), (4, 'WORLD')]
        self._verify_export_success(result, outfile, expected_data)

    def test_connection_string_takes_precedence_over_pgpassword(self):
        """Test that connection string takes precedence over PGPASSWORD environment variable"""
        connection_string = self._build_connection_string()

        outfile = os.path.join(wrappers.output_directory, "connection_test_precedence.parquet")

        args = [
            wrappers.pg2parquet_binary,
            "export",
            "--connection", connection_string,
            "--table", "connection_test",
            "--output-file", outfile
        ]

        # Set wrong PGPASSWORD to ensure connection string password is used
        env = {"PGPASSWORD": "wrong_password"}

        result = subprocess.run(args, env=env, capture_output=True)

        self._verify_export_success(result, outfile)
        """Test that help text shows connection string options"""
        if wrappers.pg2parquet_binary is None:
            self.skipTest("pg2parquet binary not available")

    def test_connection_url_help_text(self):
        result = subprocess.run([wrappers.pg2parquet_binary, "export", "--help"], capture_output=True)

        self.assertEqual(result.returncode, 0)
        help_text = result.stdout.decode('utf-8')

        # Check that connection string option is documented
        self.assertIn("--connection", help_text)
        self.assertIn("-c", help_text)
        self.assertIn("DATABASE_URL", help_text)
        self.assertIn("postgres://", help_text)
