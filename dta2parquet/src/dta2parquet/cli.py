"""Command line interface: ``dta2parquet convert|batch|inspect``."""

from __future__ import annotations

import argparse
import json
import os
import sys

from .convert import ConvertOptions, convert_file


def _shard(s: str) -> tuple[int, int]:
    try:
        k, n = (int(x) for x in s.split("/"))
    except ValueError:
        raise argparse.ArgumentTypeError("expected K/N, e.g. 2/5")
    if not (n >= 1 and 1 <= k <= n):
        raise argparse.ArgumentTypeError("need 1 <= K <= N")
    return k, n


def _add_convert_options(p: argparse.ArgumentParser) -> None:
    g = p.add_argument_group("conversion")
    g.add_argument("--compression", default="zstd",
                   choices=["zstd", "snappy", "gzip", "brotli", "lz4", "none"],
                   help="Parquet compression codec (default: zstd, like pq save)")
    g.add_argument("--compression-level", type=int, default=None)
    g.add_argument("--engine", default="polars", choices=["polars", "pyarrow"],
                   help="Parquet writer: polars (default; parallel, same writer as pq save) "
                        "or pyarrow (single-threaded)")
    g.add_argument("--chunk-mb", type=float, default=64,
                   help="MB of .dta data converted per chunk. Peak memory per file is "
                        "roughly (threads + 2) x 2-3 x this (default: 64)")
    g.add_argument("--row-group-rows", type=int, default=None,
                   help="fixed rows per Parquet row group (default: writer's choice)")
    g.add_argument("--threads", type=int, default=None,
                   help="threads per file (default: all cores / parallel files)")
    g.add_argument("--encoding", default=None,
                   help="string encoding of the .dta (default: utf-8 for Stata 14+ "
                        "files, latin-1 for older ones; e.g. cp1252)")
    g.add_argument("--no-statametadata", action="store_true",
                   help="do not embed the Stata metadata envelope")


def _opts(a, threads: int) -> ConvertOptions:
    return ConvertOptions(
        compression="none" if a.compression == "none" else a.compression,
        compression_level=a.compression_level,
        chunk_bytes=int(a.chunk_mb * 1024 * 1024),
        row_group_rows=a.row_group_rows,
        threads=max(1, threads),
        engine=a.engine,
        encoding=a.encoding,
        statametadata=not a.no_statametadata,
    )


def _limit_polars_threads(n: int) -> None:
    # must happen before polars is first imported (it is imported lazily)
    os.environ.setdefault("POLARS_MAX_THREADS", str(max(1, n)))


def cmd_convert(a) -> int:
    dst = a.output or os.path.splitext(a.input)[0] + ".parquet"
    if os.path.exists(dst) and not a.overwrite:
        print(f"{dst} exists (use --overwrite)", file=sys.stderr)
        return 1
    threads = a.threads or os.cpu_count() or 1
    _limit_polars_threads(threads)
    r = convert_file(a.input, dst, _opts(a, threads))
    mb = r.in_bytes / 2**20
    print(f"{r.src} -> {r.dst}: {r.rows:,} rows x {r.columns} cols, "
          f"{mb:,.1f} MB -> {r.out_bytes / 2**20:,.1f} MB in {r.seconds:.1f}s "
          f"({mb / max(r.seconds, 1e-9):,.0f} MB/s)")
    return 0


def cmd_batch(a) -> int:
    from .batch import plan_jobs, run_batch

    jobs = plan_jobs(a.inputs, a.output_dir, shard=a.shard, pattern=a.pattern)
    if not jobs:
        print("no input files found", file=sys.stderr)
        return 1
    cpus = os.cpu_count() or 1
    workers = a.jobs or max(1, min(len(jobs), cpus // 2 or 1))
    threads = a.threads or max(1, cpus // workers)
    _limit_polars_threads(threads)  # inherited by the (spawned) worker processes
    if a.dry_run:
        for j in jobs:
            print(f"{j.src} -> {j.dst}  ({j.size / 2**20:,.1f} MB)")
        print(f"{len(jobs)} files; would run {workers} parallel files x {threads} threads")
        return 0
    outcomes = run_batch(
        jobs,
        _opts(a, threads),
        workers=workers,
        claim=not a.no_claim,
        overwrite=a.overwrite,
        update=a.update,
        stale_after=a.stale_after * 60,
        heartbeat=min(30.0, a.stale_after * 60 / 4),
        log_path=a.log,
        quiet=a.quiet,
    )
    return 1 if any(o.status == "failed" for o in outcomes) else 0


def cmd_inspect(a) -> int:
    from .metadata import STATA_META_KEY

    if a.file.lower().endswith(".parquet"):
        import pyarrow.parquet as pq

        kv = pq.ParquetFile(a.file).metadata.metadata or {}
        raw = kv.get(STATA_META_KEY.encode())
        if raw is None:
            print("no Stata metadata in this file", file=sys.stderr)
            return 1
        env = json.loads(raw)
    else:
        from .convert import schema_for
        from .dta import open_dta

        with open_dta(a.file, encoding=a.encoding) as d:
            schema, _ = schema_for(d)
            print(f"# dta release {d.release}, {d.nobs:,} obs x {d.nvar} vars", file=sys.stderr)
            print(schema.to_string(show_schema_metadata=False), file=sys.stderr)
            env = json.loads(schema.metadata[STATA_META_KEY.encode()])
    print(json.dumps(env, indent=2, ensure_ascii=False))
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="dta2parquet",
        description="Fast Stata .dta -> Parquet conversion that keeps Stata metadata "
                    "(labels, value labels, notes, formats, types) in the format "
                    "the `pq` Stata package reads.",
    )
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("convert", help="convert one file")
    p.add_argument("input")
    p.add_argument("output", nargs="?", help="default: input with .parquet extension")
    p.add_argument("--overwrite", action="store_true")
    _add_convert_options(p)
    p.set_defaults(func=cmd_convert)

    p = sub.add_parser(
        "batch",
        help="convert many files, in parallel, optionally across machines",
        description="Convert many files. Run the identical command on several machines "
                    "sharing OUTPUT_DIR and they split the work via lock files; "
                    "re-run to resume. Or use --shard K/N for a static split.",
    )
    p.add_argument("inputs", nargs="+",
                   help="directories (searched recursively), files, globs, or @list.txt")
    p.add_argument("-o", "--output-dir", required=True)
    p.add_argument("-j", "--jobs", type=int, default=None,
                   help="files converted in parallel on this machine (default: cores/2)")
    p.add_argument("--shard", type=_shard, default=None, metavar="K/N",
                   help="only handle shard K of N (static split across machines)")
    p.add_argument("--no-claim", action="store_true",
                   help="do not use .lock files (only safe if each output dir has one writer)")
    p.add_argument("--stale-after", type=float, default=10.0, metavar="MIN",
                   help="take over locks without heartbeat for this many minutes (default: 10)")
    p.add_argument("--pattern", default="*.dta", help="file pattern inside directories")
    p.add_argument("--overwrite", action="store_true", help="redo files whose output exists")
    p.add_argument("--update", action="store_true",
                   help="redo files whose .dta is newer than their existing .parquet")
    p.add_argument("--log", default=None, help="append one JSON line per file to this file")
    p.add_argument("--dry-run", action="store_true", help="list the work and exit")
    p.add_argument("-q", "--quiet", action="store_true", help="only print failures and the summary")
    _add_convert_options(p)
    p.set_defaults(func=cmd_batch)

    p = sub.add_parser("inspect", help="print the Stata metadata envelope of a .parquet "
                                       "(or what would be written for a .dta)")
    p.add_argument("file")
    p.add_argument("--encoding", default=None)
    p.set_defaults(func=cmd_inspect)

    a = ap.parse_args(argv)
    return a.func(a)


if __name__ == "__main__":
    sys.exit(main())
