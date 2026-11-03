"""Batch conversion across processes and across machines.

Two ways to split work between machines, usable together:

* **Claiming (default)** - every machine runs the same command against the
  same inputs and the same (shared, e.g. NFS/SMB/Lustre) output directory.
  Before converting a file a worker atomically creates ``<out>.lock``
  (``O_CREAT|O_EXCL``); whoever creates it owns the file, everyone else
  skips it. A finished file is never redone, so you can start, stop and add
  machines at any time and simply re-run the command to resume. Locks are
  heart-beaten while converting; a lock whose heartbeat is older than
  ``--stale-after`` (a crashed/killed machine) is taken over.

* **Static sharding** (``--shard K/N``) - machine K of N only considers the
  files whose path hashes to K. Needs no shared filesystem for coordination
  (e.g. each machine writes to local disk and you copy results afterwards).
"""

from __future__ import annotations

import glob
import json
import multiprocessing
import os
import socket
import threading
import time
import zlib
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from dataclasses import asdict, dataclass

from .convert import ConvertOptions, ConvertResult, convert_file

HOST = socket.gethostname()


# ---------------------------------------------------------------- planning
@dataclass
class Job:
    src: str
    dst: str
    rel: str
    size: int


def expand_inputs(inputs: list[str], pattern: str = "*.dta") -> list[tuple[str, str]]:
    """(absolute path, relative output stem) for every input .dta file.

    Directories are searched recursively and keep their sub-folder layout;
    explicitly named files / globs map to their base name. ``@list.txt``
    reads one path per line.
    """
    found: list[tuple[str, str]] = []
    for item in inputs:
        if item.startswith("@"):
            with open(item[1:]) as fh:
                lines = [ln.strip() for ln in fh if ln.strip() and not ln.startswith("#")]
            found.extend(expand_inputs(lines, pattern))
        elif os.path.isdir(item):
            root = os.path.abspath(item)
            for p in sorted(glob.glob(os.path.join(root, "**", pattern), recursive=True)):
                if os.path.isfile(p):
                    found.append((p, os.path.relpath(p, root)))
        elif os.path.isfile(item):
            found.append((os.path.abspath(item), os.path.basename(item)))
        else:
            matches = sorted(glob.glob(item, recursive=True))
            if not matches:
                raise FileNotFoundError(item)
            found.extend((os.path.abspath(p), os.path.basename(p)) for p in matches if os.path.isfile(p))
    return found


def plan_jobs(inputs: list[str], out_dir: str, shard: tuple[int, int] | None = None, pattern: str = "*.dta") -> list[Job]:
    jobs: dict[str, Job] = {}
    for src, rel in expand_inputs(inputs, pattern):
        rel_out = os.path.splitext(rel)[0] + ".parquet"
        if shard is not None:
            k, n = shard
            # hash the *relative* path so every machine agrees regardless of
            # where the share is mounted
            if zlib.crc32(rel_out.replace(os.sep, "/").encode()) % n != k - 1:
                continue
        dst = os.path.join(out_dir, rel_out)
        if dst in jobs and jobs[dst].src != src:
            raise ValueError(f"two inputs map to the same output {dst}: {jobs[dst].src} and {src}")
        jobs[dst] = Job(src, dst, rel_out, os.path.getsize(src))
    # biggest first: the long poles start early, small files fill the gaps
    return sorted(jobs.values(), key=lambda j: (-j.size, j.rel))


# ---------------------------------------------------------------- locking
class Claim:
    """An exclusive, heart-beaten claim on one output file."""

    def __init__(self, dst: str, stale_after: float, heartbeat: float):
        self.lock = dst + ".lock"
        self.stale_after = stale_after
        self.heartbeat = heartbeat
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def _create(self) -> bool:
        try:
            fd = os.open(self.lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        except FileExistsError:
            return False
        with os.fdopen(fd, "w") as fh:
            json.dump({"host": HOST, "pid": os.getpid(), "started": time.time()}, fh)
        return True

    def acquire(self) -> bool:
        os.makedirs(os.path.dirname(os.path.abspath(self.lock)), exist_ok=True)
        if not self._create():
            try:
                age = time.time() - os.stat(self.lock).st_mtime
            except FileNotFoundError:
                age = None
            if age is not None and age < self.stale_after:
                return False
            # stale: move it aside atomically - only one taker can win
            try:
                os.rename(self.lock, f"{self.lock}.stale-{HOST}-{os.getpid()}")
                os.unlink(f"{self.lock}.stale-{HOST}-{os.getpid()}")
            except FileNotFoundError:
                pass
            if not self._create():
                return False
        self._thread = threading.Thread(target=self._beat, daemon=True)
        self._thread.start()
        return True

    def _beat(self):
        while not self._stop.wait(self.heartbeat):
            try:
                os.utime(self.lock)
            except FileNotFoundError:
                return

    def release(self):
        self._stop.set()
        try:
            os.unlink(self.lock)
        except FileNotFoundError:
            pass


# ---------------------------------------------------------------- workers
@dataclass
class Outcome:
    status: str  # done | skipped | busy | failed
    src: str
    dst: str
    host: str = HOST
    error: str | None = None
    result: dict | None = None


def _work(job: Job, opts: ConvertOptions, claim: bool, overwrite: bool, stale_after: float, heartbeat: float) -> Outcome:
    if os.path.exists(job.dst) and not overwrite:
        return Outcome("skipped", job.src, job.dst)
    c = Claim(job.dst, stale_after, heartbeat) if claim else None
    if c is not None and not c.acquire():
        return Outcome("busy", job.src, job.dst)
    try:
        # re-check: another machine may have finished it after our first look
        if os.path.exists(job.dst) and not overwrite:
            return Outcome("skipped", job.src, job.dst)
        res: ConvertResult = convert_file(job.src, job.dst, opts)
        return Outcome("done", job.src, job.dst, result=asdict(res))
    except Exception as e:  # report, keep going with other files
        return Outcome("failed", job.src, job.dst, error=f"{type(e).__name__}: {e}")
    finally:
        if c is not None:
            c.release()


def _fmt_bytes(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024 or unit == "TB":
            return f"{n:.1f} {unit}" if unit != "B" else f"{int(n)} B"
        n /= 1024
    return f"{n:.1f} TB"


def run_batch(
    jobs: list[Job],
    opts: ConvertOptions,
    workers: int,
    claim: bool = True,
    overwrite: bool = False,
    stale_after: float = 600.0,
    heartbeat: float = 30.0,
    log_path: str | None = None,
    quiet: bool = False,
) -> list[Outcome]:
    outcomes: list[Outcome] = []
    log = open(log_path, "a") if log_path else None
    t0 = time.perf_counter()
    total = len(jobs)

    def report(o: Outcome):
        outcomes.append(o)
        if log:
            log.write(json.dumps(asdict(o)) + "\n")
            log.flush()
        if quiet and o.status != "failed":
            return
        n = len(outcomes)
        if o.status == "done":
            r = o.result
            speed = r["in_bytes"] / max(r["seconds"], 1e-9)
            print(
                f"[{n}/{total}] done    {o.src} -> {o.dst}  "
                f"{r['rows']:,} rows x {r['columns']} cols, {_fmt_bytes(r['in_bytes'])} -> "
                f"{_fmt_bytes(r['out_bytes'])} in {r['seconds']:.1f}s ({_fmt_bytes(speed)}/s)",
                flush=True,
            )
        elif o.status == "failed":
            print(f"[{n}/{total}] FAILED  {o.src}: {o.error}", flush=True)
        else:
            print(f"[{n}/{total}] {o.status:<7} {o.src}", flush=True)

    args = (opts, claim, overwrite, stale_after, heartbeat)
    try:
        if workers <= 1:
            for j in jobs:
                report(_work(j, *args))
        else:
            # spawn (not fork): fresh interpreters, no inherited thread pools,
            # and POLARS_MAX_THREADS from the environment is honoured
            ctx = multiprocessing.get_context("spawn")
            with ProcessPoolExecutor(workers, mp_context=ctx) as pool:
                # submit lazily so claims are taken just before work starts,
                # leaving the remaining files to other machines meanwhile
                it = iter(jobs)
                running = set()
                for j in it:
                    running.add(pool.submit(_work, j, *args))
                    if len(running) >= workers:
                        break
                while running:
                    done, running = wait(running, return_when=FIRST_COMPLETED)
                    for f in done:
                        report(f.result())
                        nxt = next(it, None)
                        if nxt is not None:
                            running.add(pool.submit(_work, nxt, *args))
    finally:
        if log:
            log.close()

    counts = {s: sum(o.status == s for o in outcomes) for s in ("done", "skipped", "busy", "failed")}
    in_bytes = sum(o.result["in_bytes"] for o in outcomes if o.result)
    el = time.perf_counter() - t0
    print(
        f"{HOST}: {counts['done']} converted, {counts['skipped']} already done, "
        f"{counts['busy']} claimed by others, {counts['failed']} failed - "
        f"{_fmt_bytes(in_bytes)} in {el:.1f}s ({_fmt_bytes(in_bytes / max(el, 1e-9))}/s)",
        flush=True,
    )
    return outcomes
