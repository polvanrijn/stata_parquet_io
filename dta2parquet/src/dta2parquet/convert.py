"""Convert one .dta file to Parquet, streaming it row group by row group.

Column types mirror ``pq save`` (``src/mapping.rs`` / ``src/write.rs``):

    byte -> int8      int -> int16      long -> int32
    float -> float32  double -> float64
    str# / strL -> string (strL holding binary data -> binary)
    numeric with a date format (%td, %tw, %tm, ...)       -> date32
    numeric with a datetime format (%tc, %tC, ...)        -> timestamp[ms]
    numeric with a time-of-day format (contains hh:mm)   -> time64[ns]

Stata missing values (., .a, ..., .z) become Parquet nulls.
"""

from __future__ import annotations

import os
import socket
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from . import dta as D
from .metadata import (
    STATA_META_KEY,
    build_envelope,
    dumps,
    parquet_column_name,
    resolve_stata_type,
)

DAY_SHIFT = 3653  # days between 1960-01-01 (Stata) and 1970-01-01 (Unix)
MS_SHIFT = 315619200 * 1000  # same, in milliseconds

_NUMERIC = {
    "byte": ("i1", pa.int8(), D.BYTE_MAX),
    "int": ("i2", pa.int16(), D.INT_MAX),
    "long": ("i4", pa.int32(), D.LONG_MAX),
    "float": ("f4", pa.float32(), None),
    "double": ("f8", pa.float64(), None),
}


@dataclass
class ConvertOptions:
    compression: str = "zstd"
    compression_level: int | None = None
    chunk_bytes: int = 64 * 1024 * 1024  # raw .dta bytes converted per chunk
    row_group_rows: int | None = None  # fixed Parquet row-group size
    engine: str = "polars"  # "polars" (parallel writer) or "pyarrow"
    threads: int = 4  # threads converting chunks ahead of the writer
    encoding: str | None = None  # default: utf-8 (118+), latin-1 (<=117)
    statametadata: bool = True


@dataclass
class ConvertResult:
    src: str
    dst: str
    rows: int
    columns: int
    seconds: float
    in_bytes: int
    out_bytes: int


# ======================================================================
#                       column -> arrow conversion
# ======================================================================


def _strided(rows: np.ndarray, var: D.Variable, dtype: str) -> np.ndarray:
    """Zero-copy strided view of one numeric column of a (n, row_width) chunk."""
    return np.ndarray(
        shape=(rows.shape[0],),
        dtype=np.dtype(dtype),
        buffer=rows,
        offset=var.offset,
        strides=(rows.shape[1],),
    )


def _numeric(dta, rows, var, kind) -> pa.Array:
    code, atype, imax = _NUMERIC[var.stata_type]
    x = _strided(rows, var, dta.byteorder + code)
    x = x.astype(x.dtype.newbyteorder("="), copy=True)  # contiguous, native order
    if imax is not None:
        missing = x > imax
    elif var.stata_type == "float":
        missing = ~(x < D.FLOAT_MISSING)
    else:
        missing = ~(x < D.DOUBLE_MISSING)
    if not missing.any():
        missing = None

    if kind in ("byte", "int", "long", "float", "double"):
        return pa.array(x, type=atype, mask=missing)

    xf = x.astype(np.float64, copy=False)
    if missing is not None:
        xf = np.where(missing, 0.0, xf)
    if kind == "date":  # Rust: (value as i32) - 3653
        v = np.clip(np.trunc(xf), -(2**31), 2**31 - 1).astype(np.int32) - np.int32(DAY_SHIFT)
        return pa.array(v, type=pa.date32(), mask=missing)
    if kind == "datetime":  # Rust: (value - shift*1000) as i64, milliseconds
        v = np.trunc(xf - MS_SHIFT).astype(np.int64)
        return pa.array(v, type=pa.timestamp("ms"), mask=missing)
    if kind == "time":  # Rust: (value as i64) * 1_000_000, nanoseconds
        v = np.trunc(xf).astype(np.int64) * np.int64(1_000_000)
        return pa.array(v, type=pa.time64("ns"), mask=missing)
    raise AssertionError(kind)


# ---------------------------------------------------------------- strings
_TRANSCODE_CACHE: dict[str, tuple[np.ndarray, np.ndarray, np.ndarray]] = {}


def _transcode_table(enc: str):
    if enc not in _TRANSCODE_CACHE:
        parts = [bytes([b]).decode(enc, errors="replace").encode("utf-8") for b in range(256)]
        lens = np.array([len(p) for p in parts], dtype=np.int64)
        starts = np.concatenate([[0], np.cumsum(lens)[:-1]])
        flat = np.frombuffer(b"".join(parts), dtype=np.uint8)
        _TRANSCODE_CACHE[enc] = (flat, starts, lens)
    return _TRANSCODE_CACHE[enc]


def _is_utf8(enc: str) -> bool:
    return enc.lower().replace("-", "").replace("_", "") in ("utf8", "utf8sig")


def _to_utf8(data: np.ndarray, offsets: np.ndarray, enc: str):
    """Re-encode single-byte-encoded strings to UTF-8, fully vectorized."""
    if data.size == 0 or data.max() < 0x80:
        return data, offsets
    flat, starts, lens = _transcode_table(enc)
    out_len = lens[data]
    cum = np.concatenate([[0], np.cumsum(out_len)])
    total = int(cum[-1])
    idx = np.repeat(starts[data] - cum[:-1], out_len) + np.arange(total, dtype=np.int64)
    return flat[idx], cum[offsets]


def _string_array(data: np.ndarray, offsets: np.ndarray, n: int, enc: str) -> pa.Array:
    if _is_utf8(enc):
        arr = pa.LargeStringArray.from_buffers(n, pa.py_buffer(offsets), pa.py_buffer(data))
        try:
            arr.validate(full=True)
            return arr
        except pa.ArrowInvalid:
            # rare: invalid UTF-8 (e.g. a str# cut mid-character); replace it
            raw = data.tobytes()
            vals = [raw[a:b].decode("utf-8", errors="replace") for a, b in zip(offsets[:-1], offsets[1:])]
            return pa.array(vals, type=pa.large_string())
    try:
        data, offsets = _to_utf8(data, offsets, enc)
    except (LookupError, ValueError):  # multi-byte legacy encoding: slow path
        raw = data.tobytes()
        vals = [raw[a:b].decode(enc, errors="replace") for a, b in zip(offsets[:-1], offsets[1:])]
        return pa.array(vals, type=pa.large_string())
    return pa.LargeStringArray.from_buffers(n, pa.py_buffer(offsets), pa.py_buffer(data))


def _fixed_str(dta, rows, var) -> pa.Array:
    n = rows.shape[0]
    w = var.width
    # contiguous copy first: the elementwise passes below are ~3x faster on
    # contiguous memory than on the strided column slice
    cells = np.ascontiguousarray(rows[:, var.offset : var.offset + w])
    nul = cells == 0
    has_nul = nul.any(axis=1)
    lens = np.where(has_nul, nul.argmax(axis=1), w).astype(np.int64)
    offsets = np.zeros(n + 1, dtype=np.int64)
    np.cumsum(lens, out=offsets[1:])
    if int(offsets[-1]) == n * w:
        data = cells.reshape(-1)
    else:
        data = cells[np.arange(w) < lens[:, None]]
    return _string_array(data, offsets, n, dta.encoding)


def _strl(dta, rows, var, binary: bool) -> pa.Array:
    n = rows.shape[0]
    raw8 = rows[:, var.offset : var.offset + 8]
    keys = D.strl_refs_to_keys(raw8, dta.release, dta.byteorder)
    gkeys, gstarts, glens, _ = dta.strl_index()
    if len(gkeys):
        # (v, o) = (0, 0) - and anything unresolvable - is the empty string
        pos = np.minimum(np.searchsorted(gkeys, keys), len(gkeys) - 1)
        found = gkeys[pos] == keys
        starts = np.where(found, gstarts[pos], 0)
        lens = np.where(found, glens[pos], 0).astype(np.int64)
    else:
        starts = lens = np.zeros(n, dtype=np.int64)
    offsets = np.zeros(n + 1, dtype=np.int64)
    np.cumsum(lens, out=offsets[1:])
    mm = dta._mm
    data = np.frombuffer(
        b"".join([mm[s : s + l] for s, l in zip(starts.tolist(), lens.tolist()) if l]),
        dtype=np.uint8,
    )
    if binary:
        return pa.LargeBinaryArray.from_buffers(n, pa.py_buffer(offsets), pa.py_buffer(data))
    return _string_array(data, offsets, n, dta.encoding)


# ======================================================================
#                              plan & write
# ======================================================================


@dataclass
class _ColPlan:
    var: D.Variable
    name: str
    kind: str  # resolved stata type, or "binary"
    atype: pa.DataType


def _plan(dta: D.DtaFile) -> list[_ColPlan]:
    binary_vars = dta.binary_strl_vars() if any(v.stata_type == "strl" for v in dta.variables) else set()
    plans = []
    seen: set[str] = set()
    for var in dta.variables:
        name = parquet_column_name(dta, var)
        if name in seen:
            raise ValueError(f"{dta.path}: duplicate output column name {name!r}")
        seen.add(name)
        kind = resolve_stata_type(var)
        if kind in _NUMERIC:
            atype = _NUMERIC[kind][1]
        elif kind == "date":
            atype = pa.date32()
        elif kind == "datetime":
            atype = pa.timestamp("ms")
        elif kind == "time":
            atype = pa.time64("ns")
        elif kind == "strl" and (var.index + 1) in binary_vars:
            kind, atype = "binary", pa.large_binary()
        else:
            atype = pa.large_string()
        plans.append(_ColPlan(var, name, kind, atype))
    return plans


def _convert_column(dta, rows, p: _ColPlan) -> pa.Array:
    if p.kind == "string":
        return _fixed_str(dta, rows, p.var)
    if p.kind == "strl":
        return _strl(dta, rows, p.var, binary=False)
    if p.kind == "binary":
        return _strl(dta, rows, p.var, binary=True)
    return _numeric(dta, rows, p.var, p.kind)


def _convert_chunk(dta, plans, schema, start, stop) -> pa.Table:
    rows = dta.read_rows(start, stop)
    cols = [_convert_column(dta, rows, p) for p in plans]
    return pa.Table.from_arrays(cols, schema=schema)


def schema_for(dta: D.DtaFile, statametadata: bool = True) -> tuple[pa.Schema, list[_ColPlan]]:
    plans = _plan(dta)
    meta = None
    if statametadata:
        env = build_envelope(dta, [p.name for p in plans])
        meta = {STATA_META_KEY: dumps(env)}
    schema = pa.schema([pa.field(p.name, p.atype) for p in plans], metadata=meta)
    return schema, plans


def _rows_per_group(dta: D.DtaFile, opts: ConvertOptions) -> int:
    if opts.row_group_rows:
        return max(1, opts.row_group_rows)
    # bytes a row costs once converted: its fixed width plus, per strL
    # column, the average text it points to. That text lives outside the row
    # and is deduplicated in the file (many rows can share one stored copy),
    # but conversion materializes it for every row - so use the mean length
    # of a stored entry, not total stored bytes / rows.
    per_row = float(max(dta.row_width, 1))
    n_strl = sum(v.stata_type == "strl" for v in dta.variables)
    if n_strl:
        lens = dta.strl_index()[2]
        if len(lens):
            per_row += n_strl * float(lens.mean())
    return int(min(max(opts.chunk_bytes // per_row, 1), 4_000_000))


def _iter_tables(dta, plans, schema, bounds, threads):
    """Yield converted row chunks in order, converting ahead in a thread
    pool (numpy and Arrow release the GIL) while the consumer writes."""
    if threads <= 1 or len(bounds) <= 1:
        for s, e in bounds:
            yield _convert_chunk(dta, plans, schema, s, e)
        return
    with ThreadPoolExecutor(threads) as pool:
        futs = [pool.submit(_convert_chunk, dta, plans, schema, s, e) for s, e in bounds[: threads + 1]]
        nxt = len(futs)
        try:
            for i in range(len(bounds)):
                tbl = futs[i].result()
                futs[i] = None
                if nxt < len(bounds):
                    futs.append(pool.submit(_convert_chunk, dta, plans, schema, *bounds[nxt]))
                    nxt += 1
                yield tbl
                del tbl
        finally:
            for f in futs:
                if f is not None:
                    f.cancel()


def _write_pyarrow(path, schema, tables, opts: ConvertOptions) -> None:
    writer = pq.ParquetWriter(
        path,
        schema,
        compression=None if opts.compression in (None, "none") else opts.compression,
        compression_level=opts.compression_level,
        write_statistics=True,  # pq use relies on row-group statistics
    )
    try:
        wrote = False
        for tbl in tables:
            writer.write_table(tbl, row_group_size=tbl.num_rows)
            wrote = True
        if not wrote:
            writer.write_table(schema.empty_table())
    finally:
        writer.close()


def _write_polars(path, schema, tables, opts: ConvertOptions) -> None:
    """Stream chunks through polars' parallel Parquet sink - the same writer
    `pq save` uses, so the output is laid out like Stata-written files."""
    import polars as pl
    from polars.io.plugins import register_io_source

    empty = pl.from_arrow(schema.empty_table())

    def source(with_columns, predicate, n_rows, batch_size):
        wrote = False
        for tbl in tables:
            wrote = True
            yield pl.from_arrow(tbl, rechunk=False)
        if not wrote:
            yield empty

    lf = register_io_source(source, schema=empty.schema)
    meta = schema.metadata or {}
    lf.sink_parquet(
        path,
        compression="uncompressed" if opts.compression in (None, "none") else opts.compression,
        compression_level=opts.compression_level,
        statistics=True,  # pq use relies on row-group statistics
        row_group_size=opts.row_group_rows,
        metadata={k.decode(): v.decode() for k, v in meta.items()} or None,
    )


def convert_file(src: str | os.PathLike, dst: str | os.PathLike, opts: ConvertOptions | None = None) -> ConvertResult:
    """Convert `src` (.dta) into `dst` (.parquet), atomically.

    Data is written to a temporary file next to `dst` and renamed into place
    only once complete and verified, so a crashed or killed run never leaves
    a truncated `dst` behind (important when several machines share an
    output folder).
    """
    opts = opts or ConvertOptions()
    src, dst = os.fspath(src), os.fspath(dst)
    t0 = time.perf_counter()
    os.makedirs(os.path.dirname(os.path.abspath(dst)), exist_ok=True)
    tmp = f"{dst}.tmp-{socket.gethostname()}-{os.getpid()}"

    dta = D.open_dta(src, encoding=opts.encoding)
    try:
        schema, plans = schema_for(dta, opts.statametadata)
        step = _rows_per_group(dta, opts)
        bounds = [(s, min(s + step, dta.nobs)) for s in range(0, dta.nobs, step)]
        tables = _iter_tables(dta, plans, schema, bounds, opts.threads)
        if opts.engine == "polars":
            _write_polars(tmp, schema, tables, opts)
        elif opts.engine == "pyarrow":
            _write_pyarrow(tmp, schema, tables, opts)
        else:
            raise ValueError(f"unknown engine {opts.engine!r}")
        md = pq.ParquetFile(tmp).metadata
        if md.num_rows != dta.nobs or md.num_columns != dta.nvar:
            raise RuntimeError(
                f"verification failed: wrote {md.num_rows} rows x {md.num_columns} cols, "
                f"expected {dta.nobs} x {dta.nvar}"
            )
        os.replace(tmp, dst)
        nrows, ncols = dta.nobs, dta.nvar
    except BaseException:
        try:
            os.unlink(tmp)
        except FileNotFoundError:
            pass
        raise
    finally:
        dta.close()

    return ConvertResult(
        src=src,
        dst=dst,
        rows=nrows,
        columns=ncols,
        seconds=time.perf_counter() - t0,
        in_bytes=os.path.getsize(src),
        out_bytes=os.path.getsize(dst),
    )
