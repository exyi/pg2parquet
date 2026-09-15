#!/usr/bin/env python3
"""
Run pg2parquet benchmarks with hyperfine under different performance settings.

Scenarios:
- available cores, zstd compression
- available cores, no compression
- pinned to one available core, zstd compression
- pinned to one available core, no compression

Usage examples:
    python py-tests/bench_hyperfine.py --table public.my_table -o /tmp/bench
    python py-tests/bench_hyperfine.py --query "SELECT * FROM t" --runs 1

Notes:
- Requires `hyperfine` in PATH. Install from https://github.com/sharkdp/hyperfine
- Uses py-tests/wrappers.py infra for connection and pg2parquet binary.
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import shutil
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Iterable, List, Optional, Tuple, Sequence, Any, Callable
import random
import uuid as _uuid
from decimal import Decimal
import time
from datetime import timezone, timedelta
from psycopg.types.range import Range
from psycopg.types.numeric import Int4
from psycopg.types.range import register_range, RangeInfo
import psycopg.types.range as range_types
from psycopg import sql
from psycopg.types.json import Json
import mimesis.schema, mimesis
import tempfile
import json
import math
import platform
import datetime
import psycopg
from psycopg.types.enum import register_enum, EnumInfo
from psycopg.types.composite import register_composite, CompositeInfo
from wrappers import (
    pg2parquet_binary,
    export_connection_args,
    pg2parquet_password,
    pg_connect,
)

# Shared mimesis Field for schema-based value generation
F = mimesis.schema.Field()
G = mimesis.Generic()


def which_or_die(cmd: str) -> str:
    path = shutil.which(cmd)
    if path:
        return path
    print(f"Required command not found: {cmd}", file=sys.stderr)
    sys.exit(2)

def _sanitize_ident(name: str) -> str:
    safe = [ch if (ch.isalnum() or ch == "_") else "_" for ch in name]
    res = "".join(safe).lower()
    return res or "bench_table"

def _maybe_null(val: Callable[[], Any], p: float = 0.7):
    return None if random.random() < p else val()


def _copy_binary(conn: psycopg.Connection, table_ident: str, schema_cols: List[Tuple[str, str]], rows_iter: Iterable[Sequence[Any]], batch_size: int = 1000) -> None:
    with conn.cursor() as cur:
        tbl_ident = sql.Identifier(*table_ident.split('.', 1))

        cols_sql = sql.SQL(", ").join(
            sql.SQL("{} {}").format(
                sql.Identifier(col_name),
                sql.SQL(col_type),  # type: ignore
            )
            for col_name, col_type in schema_cols
        )
        create_stmt = sql.SQL("CREATE TABLE {} ({})").format(tbl_ident, cols_sql)
        cur.execute(create_stmt)

        col_names = [n for n, _ in schema_cols]

        cols_ident = sql.SQL(',').join(sql.Identifier(c) for c in col_names)
        copy_stmt = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT binary)").format(tbl_ident, cols_ident)

        cur.execute(
            """
            SELECT a.atttypid::int
            FROM pg_attribute a
            WHERE a.attrelid = %s::regclass AND a.attnum > 0 AND NOT a.attisdropped
            ORDER BY a.attnum
            """,
            (table_ident,),
        )
        type_oids = [row[0] for row in cur.fetchall()]

        with cur.copy(copy_stmt) as cp:
            cp.set_types(type_oids)
            for row in rows_iter:
                cp.write_row(row)


def _generate_rows(profile: str, rows: int, table_ident: str, *, G=G, enum_cls: Any | None = None):
    if profile == "primitives":
        schema_cols = [
            ("i_small", "smallint"),
            ("i_int", "integer"),
            ("i_big", "bigint"),
            ("num_val", "numeric(20,5)"),
            ("f4", "real"),
            ("f8", "double precision"),
            ("b", "boolean"),
            ("u", "uuid"),
            ("d", "date"),
            ("t", "time without time zone"),
            ("ts", "timestamp without time zone"),
            ("tz", "timestamp with time zone"),
            ("n_nullable", "integer"),
            ("s_nullable", "text"),
        ]

        def it_prim():
            for _ in range(rows):
                yield [
                    G.numeric.integer_number(-32000, 32000),
                    G.numeric.integer_number(-2_000_000_000, 2_000_000_000),
                    G.numeric.integer_number(-9_000_000_000, 9_000_000_000),
                    Decimal(G.numeric.integer_number(-1_000_000, 1_000_000)) / Decimal(100),
                    G.numeric.float_number(-1e6, 1e6),
                    G.numeric.float_number(-1e6, 1e6),
                    random.choice([True, False]),
                    G.cryptographic.uuid_object(),
                    G.datetime.date(),
                    G.datetime.time(),
                    G.datetime.datetime(),
                    G.datetime.datetime().replace(tzinfo=timezone.utc),
                    _maybe_null(lambda: random.randint(-1_000_000, 1_000_000), 0.6),
                    _maybe_null(G.text.sentence, 0.7),
                ]

        return schema_cols, it_prim()

    elif profile == "longstr":
        schema_cols = [("s1", "text"), ("s2", "text"), ("s3", "text")]

        def it_long():
            for _ in range(rows):
                yield [
                    G.text.text(),
                    G.text.text(quantity=100),
                    G.text.sentence(),
                ]

        return schema_cols, it_long()

    elif profile == "shortstr":
        schema_cols = [("s1", "text"), ("s2", "text"), ("s3", "text"), ("s4", "text")]

        def it_short():
            for _ in range(rows):
                yield [
                    G.text.word(),
                    G.text.color(),
                    G.text.emoji(),
                    _maybe_null(G.text.sentence, 0.9),
                ]

        return schema_cols, it_short()

    elif profile == "nested":
        base_name = _sanitize_ident(table_ident.split('.')[-1])
        enum_type = f"{base_name}_status"
        addr_t = f"{base_name}_addr"
        contact_t = f"{base_name}_contact"
        meta_t = f"{base_name}_meta"
        profile_t = f"{base_name}_profile"
        schema_cols = [
            ("status", enum_type),
            ("ints", "integer[]"),
            ("uuids", "uuid[]"),
            ("tags", "text[]"),
            ("r_int", "int4range"),
            ("r_num", "numrange"),
            ("r_tstz", "tstzrange"),
            ("c_addr", addr_t),
            ("c_contact", contact_t),
            ("c_meta", meta_t),
            ("c_profile", profile_t),
        ]

        def it_nested():
            for _ in range(rows):
                a = sorted(random.sample(range(0, 10_000), k=random.randint(0, 8)))
                uu = [_uuid.uuid4() for _ in range(random.randint(0, 5))]
                tags = [F("text.word") for _ in range(random.randint(0, 6))]

                i1 = random.randint(-1000, 1000)
                i2 = i1 + random.randint(0, 100)
                b_left = random.choice([True, False])
                b_right = random.choice([True, False])
                bounds = ("[" if b_left else "(") + ("]" if b_right else ")")
                # For binary COPY some psycopg versions require subtype wrapper objects
                r_int = Range(Int4(i1), Int4(i2), bounds=bounds)

                n1 = Decimal(random.randint(-10000, 10000)) / Decimal(10)
                n2 = n1 + (Decimal(random.randint(0, 1000)) / Decimal(10))
                r_num = Range(n1, n2, bounds=bounds)

                t1 = G.datetime.datetime().replace(tzinfo=timezone.utc)
                t2 = t1 + timedelta(seconds=random.randint(0, 360000))
                r_tstz = Range(t1, t2, bounds=bounds)

                home_addr = (
                    G.address.street_name(),
                    G.address.city(),
                    G.address.zip_code(),
                )
                contact = (
                    G.person.email(),
                    G.person.email(),
                )
                created_at = G.datetime.datetime().replace(tzinfo=timezone.utc)
                updated_at = created_at + timedelta(seconds=random.randint(0, 360000))
                meta = (
                    created_at,
                    updated_at,
                    random.choice(["new", "open", "closed", "archived"]),
                )
                profile_comp = (
                    home_addr,
                    home_addr,
                    contact,
                    meta,
                )

                status_val = random.choice(["new", "open", "closed", "archived"]) 
                status_out = enum_cls[status_val] if enum_cls is not None else status_val
                yield [
                    status_out,
                    a,
                    uu,
                    tags,
                    r_int,
                    r_num,
                    r_tstz,
                    home_addr,
                    contact,
                    meta,
                    profile_comp,
                ]

        return schema_cols, it_nested()
    else:
        raise SystemExit(f"Unknown profile: {profile}")


def generate_data(profile: str, rows: int, table: str) -> None:
    with pg_connect() as conn:
        with conn.cursor() as cur:
            tbl_ident = sql.Identifier(*table.split('.', 1))
            base_name = _sanitize_ident(table.split('.', 1)[-1])
            cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(tbl_ident))
            if profile == "nested":
                enum_type = f"{base_name}_status"
                addr_t = f"{base_name}_addr"
                contact_t = f"{base_name}_contact"
                meta_t = f"{base_name}_meta"
                profile_t = f"{base_name}_profile"
                cur.execute("SELECT 1 FROM pg_type WHERE typname = %s", (enum_type,))
                if cur.fetchone() is None:
                    cur.execute(
                        sql.SQL("CREATE TYPE {} AS ENUM ('new','open','closed','archived')").format(sql.Identifier(enum_type))
                    )
                einfo = EnumInfo.fetch(conn, enum_type)
                register_enum(einfo, conn, mapping=[(label, label) for label in getattr(einfo, 'labels', [])]) # pyright: ignore[reportArgumentType]
                # Composite types
                for tname, ddl in [
                    (addr_t, sql.SQL("CREATE TYPE {} AS (street text, city text, zip text)").format(sql.Identifier(addr_t))),
                    (contact_t, sql.SQL("CREATE TYPE {} AS (email text, phone text)").format(sql.Identifier(contact_t))),
                    (meta_t, sql.SQL("CREATE TYPE {} AS (created_at timestamptz, updated_at timestamptz, status {})").format(sql.Identifier(meta_t), sql.Identifier(enum_type))),
                    (profile_t, sql.SQL("CREATE TYPE {} AS (home {}, work {}, contact {}, meta {})").format(sql.Identifier(profile_t), sql.Identifier(addr_t), sql.Identifier(addr_t), sql.Identifier(contact_t), sql.Identifier(meta_t))),
                ]:
                    cur.execute("SELECT 1 FROM pg_type WHERE typname = %s", (tname,))
                    if cur.fetchone() is None:
                        cur.execute(ddl)
                    register_composite(CompositeInfo.fetch(conn, tname), conn) # pyright: ignore[reportArgumentType]
                range_types.register_default_adapters(conn)
        schema_cols, row_iter = _generate_rows(profile, rows, table)
        _copy_binary(conn, table, schema_cols, row_iter)
        conn.commit()

def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark pg2parquet with hyperfine")
    # Data selection
    g = parser.add_mutually_exclusive_group(required=True)
    g.add_argument("--table", help="Table to export, e.g. public.my_table")
    g.add_argument("--query", help="SQL query to export")
    # Outputs
    parser.add_argument("-o", "--out-dir", dest="out_dir", default=None, help="Directory to store parquet files and results")
    parser.add_argument("--runs", type=int, default=None, help="Hyperfine runs per scenario (default: auto)")
    parser.add_argument("--warmup", type=int, default=1, help="Hyperfine warmup runs per scenario (default: 0)")
    parser.add_argument("--json", dest="export_json", default=None, help="Path to write aggregated JSON results (default: <out-dir>/bench.json)")
    parser.add_argument("--compare", dest="compare_json", default=None, help="Path to a previous aggregated (or hyperfine) JSON to compare against")
    parser.add_argument("--show-output", action="store_true", help="Pass --show-output to hyperfine to print command stdout/stderr")
    # Data generation
    parser.add_argument(
        "--gen-profile",
        choices=["primitives", "longstr", "shortstr", "nested"],
        default=None,
        help="Generate test data table before benchmarking using the selected profile",
    )
    parser.add_argument("--gen-rows", type=int, default=10_000, help="Number of rows to generate when --gen-profile is used (default: 10000)")

    args = parser.parse_args()

    # Verify dependencies
    which_or_die("hyperfine")

    # Output dir
    timestamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    out_dir = Path(args.out_dir or tempfile.mkdtemp(prefix="pg2parquet-bench"))
    out_dir.mkdir(parents=True, exist_ok=True)

    # Optionally generate data
    if args.gen_profile:
        if args.query:
            print("--gen-profile cannot be combined with --query; use --table.", file=sys.stderr)
            sys.exit(2)
        target_table = args.table or f"bench_{args.gen_profile}"
        generate_data(profile=args.gen_profile, rows=args.gen_rows, table=target_table)
        args.table = target_table

    scenarios: list[tuple[str, list[str]]] = []  # (name, command string, outfile)
    outfile = str(out_dir / "out.parquet")

    def build_cmd(compression: str, query: str | None, table: str | None) -> list[str]:
        base = [pg2parquet_binary, "export", *export_connection_args, "--compression", compression, "--quiet", "--output-file", outfile]
        if query is not None:
            base += ["--query", query]
        else:
            base += ["--table", table or ""]
        return base

    scenarios.append(("zstd", build_cmd("zstd", args.query, args.table)))

    scenarios.append(("nocompress", build_cmd("none", args.query, args.table)))

    def run_hyperfine(cmds: list[tuple[str, list[str]]], file_suffix='') -> str:
        assert cmds
        hf_args: list[str] = [
            "hyperfine",
            "--warmup", str(args.warmup),
            "--prepare", shlex.join(["rm", "-f", outfile]),
            "--shell=none",
        ]
        if args.runs:
            hf_args.extend(["--runs", str(args.runs)])
        if args.show_output:
            hf_args.append("--show-output")
        out_json = out_dir / f"hyperfine{file_suffix}.json"
        hf_args += ["--export-json", str(out_json)]
        for name, cmd in cmds:
            hf_args += ["-n", name, shlex.join(cmd)]
        env = os.environ.copy()
        if pg2parquet_password:
            env["PGPASSWORD"] = pg2parquet_password
        try:
            subprocess.run(hf_args, check=True, env=env)
            return str(out_json)
        except subprocess.CalledProcessError as e:
            print(f"hyperfine failed with exit code {e.returncode}", file=sys.stderr)
            sys.exit(e.returncode)
    
    runs = [(f"norm_{n}", c) for (n, c) in scenarios]
    if shutil.which("taskset"):
        cpu = min(os.sched_getaffinity(0))
        runs.extend([(f"thread1_{n}", ["taskset", "-c", str(cpu), *c]) for (n, c) in scenarios])
    else:
        print("Warning: 'taskset' not found. Skipping pinned single-core scenarios.", file=sys.stderr)

    result_files = [ run_hyperfine(runs) ]

    # Aggregate JSON results
    def load_hf(path: str) -> dict:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    aggregated_results: list[dict] = []
    for tj in result_files:
        try:
            data = load_hf(tj)
            aggregated_results.extend(data.get("results", []))
        except Exception as e:
            print(f"Failed to load hyperfine JSON {tj}: {e}", file=sys.stderr)

    aggregated = {
        "generated_at": dt.datetime.now().isoformat(),
        "host": platform.node(),
        "scenarios": {name: cmd for name, cmd in scenarios},
        "results": aggregated_results,
    }

    agg_path = args.export_json or str(out_dir / f"bench-{datetime.datetime.now().replace(microsecond=0).isoformat()}.json")
    with open(agg_path, "w", encoding="utf-8") as f:
        json.dump(aggregated, f, indent=2)
    print(f"Aggregated results written to {agg_path}")

    # Optional: compare with previous run
    if args.compare_json:
        def normalize_results(obj: dict) -> dict[str, dict]:
            # Returns mapping: name/command -> result dict containing times
            res = {}
            results = obj.get("results", [])
            for r in results:
                key = r.get("command")  # with -n, this should be our scenario name
                if key:
                    res[key] = r
            return res

        def mann_whitney_u_p(times_a: list[float], times_b: list[float]) -> float:
            # Two-sided Mann–Whitney U test with normal approximation
            n1, n2 = len(times_a), len(times_b)
            if n1 == 0 or n2 == 0:
                return float("nan")
            # rank all values with average ranks for ties
            combined = [(x, 0) for x in times_a] + [(y, 1) for y in times_b]
            combined.sort(key=lambda t: t[0])
            ranks = [0.0] * (n1 + n2)
            i = 0
            while i < len(combined):
                j = i
                while j + 1 < len(combined) and combined[j + 1][0] == combined[i][0]:
                    j += 1
                avg_rank = (i + j + 2) / 2.0  # ranks are 1-based
                for k in range(i, j + 1):
                    ranks[k] = avg_rank
                i = j + 1
            R1 = sum(ranks[idx] for idx, (_, g) in enumerate(combined) if g == 0)
            U1 = R1 - n1 * (n1 + 1) / 2.0
            mu = n1 * n2 / 2.0
            sigma = math.sqrt(n1 * n2 * (n1 + n2 + 1) / 12.0)
            if sigma == 0:
                return float("nan")
            # continuity correction
            z = (U1 - mu - 0.5 * (1 if U1 > mu else -1 if U1 < mu else 0)) / sigma
            # two-sided p-value using normal approximation
            # Phi(z) = 0.5 * (1 + erf(z / sqrt(2)))
            p = 2.0 * (1.0 - 0.5 * (1.0 + math.erf(abs(z) / math.sqrt(2.0))))
            return p

        try:
            prev = load_hf(args.compare_json)
            curr = aggregated
            prev_map = normalize_results(prev)
            curr_map = normalize_results(curr)
            print("\nComparison vs previous run:")
            print("name, prev_mean_s, curr_mean_s, speedup, p_value")
            for name, r in curr_map.items():
                if name not in prev_map:
                    continue
                a = prev_map[name]
                b = r
                mean_a = a.get("mean")
                mean_b = b.get("mean")
                times_a = a.get("times") or []
                times_b = b.get("times") or []
                p = mann_whitney_u_p(times_a, times_b)
                speedup = (mean_a / mean_b) if (mean_a and mean_b) else float("nan")
                print(f"{name}, {mean_a:.6f}, {mean_b:.6f}, {speedup:.3f}x, p={p:.4g}")
        except Exception as e:
            print(f"Comparison failed: {e}", file=sys.stderr)


if __name__ == "__main__":
    main()
