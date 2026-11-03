"""Independently check a converted Parquet file against its source .dta.

Everything is re-read from the .dta with **pyreadstat** (ReadStat, a C
library with no code in common with dta2parquet's reader), then compared
with what landed in the Parquet file:

* metadata envelope: dataset label, dataset notes, variable labels, display
  formats, storage types, value-label assignments and value-label
  definitions (including labels on extended missing values .a-.z);
* schema: every column present, in order, with the type ``pq save`` uses;
* data: rows sampled from the start, middle and end of the file, value by
  value (numbers exactly, dates/times via the same epoch arithmetic as
  ``pq save``, Stata missing values as nulls).

Not independently checkable (pyreadstat does not expose characteristics):
per-variable notes and the ``_pq_parquet_name`` rename characteristic.
Those are reported as "not verifiable" instead of being silently trusted.

Requires the optional ``verify`` extra (``pyreadstat``).
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, field

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from .metadata import STATA_META_KEY

DAY_SHIFT = 3653
MS_SHIFT = 315619200 * 1000

# readstat storage type -> Stata storage type
_RS_TYPES = {"int8": "byte", "int16": "int", "int32": "long", "float": "float", "double": "double"}


@dataclass
class Report:
    dta: str
    parquet: str
    checks: int = 0
    problems: list[str] = field(default_factory=list)
    unverifiable: list[str] = field(default_factory=list)
    rows_compared: int = 0

    @property
    def ok(self) -> bool:
        return not self.problems

    def expect(self, cond: bool, msg: str) -> None:
        self.checks += 1
        if not cond:
            self.problems.append(msg)

    def summary(self) -> str:
        head = "OK" if self.ok else f"FAILED ({len(self.problems)} problem(s))"
        lines = [f"{head}: {self.parquet}  [{self.checks} checks, {self.rows_compared:,} rows compared]"]
        lines += [f"  - {p}" for p in self.problems[:50]]
        if len(self.problems) > 50:
            lines.append(f"  ... and {len(self.problems) - 50} more")
        lines.append("  (text compared ignoring trailing spaces, which ReadStat strips)")
        if self.unverifiable:
            lines.append("  not independently verifiable: " + "; ".join(self.unverifiable))
        return "\n".join(lines)


def _t(x):
    """Normalize text for comparison with ReadStat, which strips trailing
    spaces from every string it reads (labels, notes, values). dta2parquet
    keeps them, exactly as stored in the .dta."""
    if isinstance(x, str):
        return x.rstrip(" ") or None
    if isinstance(x, list):
        return [_t(i) or "" for i in x]
    if isinstance(x, dict):
        return {k: _t(v) or "" for k, v in x.items()}
    return x


def _rs_code(code) -> str:
    """pyreadstat value-label key -> envelope code string."""
    if isinstance(code, str):  # tagged missing: 'a' .. 'z'
        return "." + code
    f = float(code)
    return str(int(f)) if f.is_integer() else repr(f)


def _expected_kind(rs_type: str, fmt: str) -> str:
    """Stata type pq save records, from readstat's storage type + format."""
    if rs_type == "string":
        return ""  # strings carry no stata_type in the schema
    st = _RS_TYPES[rs_type]
    if st == "byte":
        return "byte"
    f = (fmt or "").lower()
    if "hh:mm" in f:
        return "time"
    if f.startswith(("%tc", "%c", "%tn", "%n", "%tu", "%u")):
        return "datetime"
    if f.startswith(("%td", "%d", "%tw", "%tm", "%tq", "%th", "%ty", "%tb")):
        return "date"
    return st


_ARROW_FOR = {
    "byte": (pa.int8(),),
    "int": (pa.int16(),),
    "long": (pa.int32(),),
    "float": (pa.float32(),),
    "double": (pa.float64(),),
    "date": (pa.date32(),),
    "datetime": (pa.timestamp("ms"),),
    "time": (pa.time64("ns"),),
    "string": (pa.string(), pa.large_string(), pa.string_view()),
    "strl": (pa.string(), pa.large_string(), pa.string_view(), pa.binary(), pa.large_binary()),
}


def _check_metadata(rep: Report, meta, env: dict, colmap: dict[str, str]) -> None:
    rep.expect(env.get("version") == 1, f"envelope version is {env.get('version')!r}, expected 1")
    rep.expect(
        _t(env.get("dataset_label")) == _t(meta.file_label),
        f"dataset label: dta={meta.file_label!r} parquet={env.get('dataset_label')!r}",
    )
    rep.expect(
        _t(env.get("dataset_notes", [])) == _t(list(meta.notes or [])),
        f"dataset notes: dta={meta.notes!r} parquet={env.get('dataset_notes', [])!r}",
    )
    variables = env.get("variables", {})
    rep.expect(
        len(variables) == len(meta.column_names),
        f"envelope has {len(variables)} variables, dta has {len(meta.column_names)}",
    )
    used_labels = set()
    for name in meta.column_names:
        col = colmap.get(name, name)
        v = variables.get(col)
        if v is None:
            rep.expect(False, f"{name}: missing from the envelope")
            continue
        lab = meta.column_names_to_labels.get(name)
        rep.expect(_t(v.get("label")) == _t(lab), f"{name}: label dta={lab!r} parquet={v.get('label')!r}")
        fmt = meta.original_variable_types.get(name)
        rep.expect(v.get("format") == (fmt or None), f"{name}: format dta={fmt!r} parquet={v.get('format')!r}")
        exp = _expected_kind(meta.readstat_variable_types[name], fmt)
        got = v.get("stata_type")
        rep.expect(got == (exp or None), f"{name}: stata_type {got!r}, expected {exp or None!r}")
        vl = meta.variable_to_label.get(name)
        rep.expect(v.get("value_label") == vl, f"{name}: value label dta={vl!r} parquet={v.get('value_label')!r}")
        if vl:
            used_labels.add(vl)

    env_vls = env.get("value_labels", {})
    for vl in sorted(used_labels):
        want = {_rs_code(k): t for k, t in (meta.value_labels.get(vl) or {}).items()}
        got = env_vls.get(vl)
        if not want:  # attached but never defined: pq save writes no definition
            rep.expect(got is None, f"value label {vl!r}: undefined in dta but present in parquet")
            continue
        rep.expect(_t(got) == _t(want), f"value label {vl!r}: dta={want!r} parquet={got!r}")
        if got is not None:
            rep.expect(list(got) == sorted(got), f"value label {vl!r}: codes not sorted like pq save")
    extra = set(env_vls) - used_labels
    rep.expect(not extra, f"value labels not attached to any variable: {sorted(extra)}")

    var_notes = [n for n, v in variables.items() if v.get("notes")]
    rep.unverifiable.append(
        f"per-variable notes ({len(var_notes)} variable(s) carry notes)" if var_notes else "per-variable notes"
    )
    if any(k != v for k, v in colmap.items()):
        rep.unverifiable.append("_pq_parquet_name renames")


def _stata_numbers(col: pa.ChunkedArray, kind: str) -> tuple[np.ndarray, np.ndarray]:
    """Parquet column -> (float64 values on Stata's scale, null mask)."""
    null = col.is_null().to_numpy(zero_copy_only=False)
    if kind == "date":
        v = col.cast(pa.int32()).fill_null(0).to_numpy().astype(np.float64) + DAY_SHIFT
    elif kind == "datetime":
        v = col.cast(pa.int64()).fill_null(0).to_numpy().astype(np.float64) + MS_SHIFT
    elif kind == "time":
        v = col.cast(pa.int64()).fill_null(0).to_numpy().astype(np.float64) / 1e6
    else:
        v = col.cast(pa.float64()).fill_null(0).to_numpy()
    return v, null


def _check_rows(rep: Report, dta_path: str, pf: pq.ParquetFile, kinds: dict[str, str],
                colmap: dict[str, str], offset: int, limit: int, encoding: str | None) -> None:
    import pyreadstat

    ref, _ = pyreadstat.read_dta(
        dta_path, row_offset=offset, row_limit=limit, disable_datetime_conversion=True,
        user_missing=True, apply_value_formats=False, encoding=encoding,
    )
    if len(ref) == 0:
        return
    got = _read_rows(pf, offset, len(ref))
    for name in ref.columns:
        col = got.column(colmap.get(name, name))
        exp = ref[name]
        kind = kinds[name]
        if kind in ("string", "strl"):
            vals = [_t(x) or "" for x in col.to_pylist()]
            if pa.types.is_binary(col.type) or pa.types.is_large_binary(col.type):
                continue  # binary strL: pyreadstat can't return the raw bytes
            exp_vals = ["" if (e is None or (isinstance(e, float) and math.isnan(e))) else (_t(e) or "")
                        for e in exp]
            bad = [i for i, (a, b) in enumerate(zip(vals, exp_vals)) if a != b]
            rep.expect(not bad, f"{name}: {len(bad)} string value(s) differ, first at row "
                                f"{offset + bad[0] if bad else 0}: parquet={vals[bad[0]] if bad else ''!r} "
                                f"dta={exp_vals[bad[0]] if bad else ''!r}")
            continue
        # pyreadstat: numbers, NaN for '.', 'a'..'z' strings for extended missing
        e = exp.to_numpy(dtype=object)
        e_missing = np.array([not isinstance(x, (int, float, np.number)) or
                              (isinstance(x, float) and math.isnan(x)) for x in e])
        e_num = np.array([0.0 if m else float(x) for x, m in zip(e, e_missing)])
        v, null = _stata_numbers(col, kind)
        miss_bad = np.flatnonzero(null != e_missing)
        rep.expect(miss_bad.size == 0, f"{name}: missing/non-missing differs at {miss_bad.size} row(s), "
                                       f"first at row {offset + int(miss_bad[0]) if miss_bad.size else 0}")
        ok = ~null & ~e_missing
        if kind in ("date", "datetime", "time"):
            want = np.trunc(e_num[ok])  # pq save truncates fractional dates/times
            if kind == "time":
                want = np.trunc(e_num[ok])
        else:
            want = e_num[ok].astype(np.float32).astype(np.float64) if kind == "float" else e_num[ok]
        diff = np.flatnonzero(v[ok] != want)
        rep.expect(diff.size == 0, f"{name}: {diff.size} value(s) differ, first: parquet={v[ok][diff[0]] if diff.size else 0!r} "
                                   f"dta={want[diff[0]] if diff.size else 0!r}")
    rep.rows_compared += len(ref)


def _read_rows(pf: pq.ParquetFile, offset: int, n: int) -> pa.Table:
    """Rows [offset, offset+n) reading only the row groups that hold them."""
    md = pf.metadata
    groups, start, first_start = [], 0, None
    for i in range(md.num_row_groups):
        g = md.row_group(i).num_rows
        if start + g > offset and start < offset + n:
            groups.append(i)
            if first_start is None:
                first_start = start
        start += g
    t = pf.read_row_groups(groups) if groups else pf.schema_arrow.empty_table()
    return t.slice(offset - (first_start or 0), n)


def verify(dta_path: str, parquet_path: str, sample_rows: int = 20_000, encoding: str | None = None) -> Report:
    """Check `parquet_path` against `dta_path`. ``sample_rows=0`` compares every row."""
    import pyreadstat

    rep = Report(dta_path, parquet_path)
    pf = pq.ParquetFile(parquet_path)
    raw = (pf.metadata.metadata or {}).get(STATA_META_KEY.encode())
    rep.expect(raw is not None, f"no {STATA_META_KEY} metadata in the parquet footer")
    if raw is None:
        return rep
    env = json.loads(raw)

    _, meta = pyreadstat.read_dta(dta_path, metadataonly=True, encoding=encoding)

    # readstat -> parquet column names (renames via characteristics are
    # invisible to readstat; map them through our own parser)
    from .dta import open_dta
    from .metadata import parquet_column_name

    with open_dta(dta_path, encoding=encoding) as d:
        colmap = {v.name: parquet_column_name(d, v) for v in d.variables}
        nobs = d.nobs
        rep.expect(d.nobs == meta.number_rows, f"row count: readstat={meta.number_rows} dta2parquet={d.nobs}")

    _check_metadata(rep, meta, env, colmap)

    schema = pf.schema_arrow
    rep.expect(pf.metadata.num_rows == nobs, f"parquet has {pf.metadata.num_rows} rows, dta has {nobs}")
    rep.expect(
        schema.names == [colmap.get(n, n) for n in meta.column_names],
        f"columns: parquet={schema.names} dta={meta.column_names}",
    )
    kinds = {}
    for name in meta.column_names:
        col = colmap.get(name, name)
        # no stata_type = string column ("strl" accepts every string/binary type)
        kind = env.get("variables", {}).get(col, {}).get("stata_type") or "strl"
        kinds[name] = kind
        if col in schema.names:
            t = schema.field(col).type
            rep.expect(t in _ARROW_FOR.get(kind, ()), f"{col}: parquet type {t} does not match Stata type {kind}")

    if nobs:
        if not sample_rows or sample_rows >= nobs:
            windows = [(0, nobs)]
        else:
            k = max(1, sample_rows // 3)
            windows = [(0, k), (max(0, nobs // 2 - k // 2), k), (max(0, nobs - k), k)]
        for off, lim in windows:
            _check_rows(rep, dta_path, pf, kinds, colmap, off, lim, encoding)
    return rep
