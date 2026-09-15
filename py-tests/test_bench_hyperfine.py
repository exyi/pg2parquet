import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile
import unittest


class TestBenchmarkCommand(unittest.TestCase):
    def test_output_directory_query_and_cpu_affinity_are_preserved(self):
        query = r'''SELECT E'\\' AS "backslash", '$HOME' AS "literal"'''
        with tempfile.TemporaryDirectory(prefix="pg2parquet bench ") as directory:
            temp = Path(directory)
            args_file = temp / "hyperfine-args.json"
            fake_bin = temp / "bin"
            fake_bin.mkdir()
            hyperfine = fake_bin / "hyperfine"
            hyperfine.write_text(
                "#!/usr/bin/env python3\n"
                "import json, os, pathlib, sys\n"
                "args = sys.argv[1:]\n"
                "pathlib.Path(os.environ['HYPERFINE_ARGS_FILE']).write_text(json.dumps(args))\n"
                "output = args[args.index('--export-json') + 1]\n"
                "pathlib.Path(output).write_text(json.dumps({'results': []}))\n"
            )
            hyperfine.chmod(0o755)

            env = os.environ.copy()
            env["PATH"] = os.pathsep.join([str(fake_bin), env["PATH"]])
            env["HYPERFINE_ARGS_FILE"] = str(args_file)
            result = subprocess.run(
                [sys.executable, Path(__file__).with_name("bench_hyperfine.py"),
                 "--query", query, "--out-dir", directory],
                env=env,
                capture_output=True,
                text=True,
            )
            self.assertEqual(result.returncode, 0, result.stderr)

            args = json.loads(args_file.read_text())
            output = str(temp / "out.parquet")
            self.assertEqual(shlex.split(args[args.index("--prepare") + 1]), ["rm", "-f", output])
            commands = [shlex.split(args[i + 2]) for i, arg in enumerate(args) if arg == "-n"]
            self.assertIn(len(commands), (2, 4))
            for command in commands:
                self.assertEqual(command[command.index("--output-file") + 1], output)
                self.assertEqual(command[command.index("--query") + 1], query)
