"""Is the Stata metadata in the .dta really what ends up in the Parquet?

The reference is pyreadstat (ReadStat), an implementation independent of
dta2parquet's reader, via `dta2parquet.verify`. The verifier itself is
tested by tampering with converted files: it must notice every change.
"""

from __future__ import annotations

import json
import os
import struct
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

sys.path.insert(0, os.path.dirname(__file__))

from make_rich_dta import make_rich_dta  # noqa: E402

from dta2parquet import STATA_META_KEY, ConvertOptions, convert_file  # noqa: E402
from dta2parquet.verify import verify  # noqa: E402

KEY = STATA_META_KEY.encode()


@pytest.fixture(scope="module")
def rich(tmp_path_factory):
    d = tmp_path_factory.mktemp("rich")
    src = d / "rich.dta"
    make_rich_dta(str(src))
    out = d / "rich.parquet"
    convert_file(src, out, ConvertOptions(row_group_rows=1000))
    return str(src), str(out)


def _env(path):
    return json.loads(pq.ParquetFile(path).metadata.metadata[KEY])


def test_rich_metadata_verifies(rich):
    rep = verify(*rich, sample_rows=0)
    assert rep.ok, rep.summary()
    assert rep.rows_compared == 5000 and rep.checks > 100


def test_rich_metadata_content(rich):
    env = _env(rich[1])
    v = env["variables"]
    assert env["dataset_label"] == 'Rich metadata test: "quotes", ümlauts \\ backslash'
    assert v["name"]["label"] == "Name ☃"
    assert len(v["income"]["label"]) == 80 and v["income"]["label"].endswith(" ")  # kept verbatim
    assert "label" not in v["share"]  # empty label omitted, like pq save
    assert v["income"]["format"] == "%12.2fc"
    assert v["clock"] == {"label": "Time of day", "format": "%tcHH:MM:SS", "stata_type": "time"}
    assert v["month"]["stata_type"] == "date" and v["month"]["format"] == "%tm"
    # shared definitions, negative and large codes; codes sorted as strings
    assert env["value_labels"]["status1"] == {
        "-1": "Don't know", "-9": "Refused", "0": "Unemployed", "1": "Employed", "1000000000": "Huge code",
    }
    assert list(env["value_labels"]["status1"]) == ["-1", "-9", "0", "1", "1000000000"]
    assert env["value_labels"]["tagged4"] == {".a": "not asked", ".z": "skipped"}
    t = pq.read_table(rich[1])
    assert t.column("tagged").null_count == len({*range(0, 5000, 5), *range(0, 5000, 13)})


def _tampered(src_parquet, dst, env_fn=None, table_fn=None):
    t = pq.read_table(src_parquet)
    # polars-written files keep the key in the footer only, not in the Arrow
    # schema metadata - so start from the footer
    meta = {k: v for k, v in pq.ParquetFile(src_parquet).metadata.metadata.items() if k == KEY}
    if env_fn is not None:
        env = json.loads(meta[KEY])
        env_fn(env)
        if env is None:
            del meta[KEY]
        else:
            meta[KEY] = json.dumps(env, ensure_ascii=False, separators=(",", ":")).encode()
    if table_fn is not None:
        t = table_fn(t)
    pq.write_table(t.replace_schema_metadata(meta), dst)
    return dst


def _set_value(t, col, row, value):
    arr = t.column(col).to_pylist()
    arr[row] = value
    i = t.schema.get_field_index(col)
    return t.set_column(i, t.schema.field(i), pa.array(arr, t.schema.field(i).type))


TAMPERING = {
    "variable label": (lambda e: e["variables"]["region"].update(label="Region"), None, "region: label"),
    "value label text": (lambda e: e["value_labels"]["region0"].update({"2": "Oost"}), None, "value label 'region0'"),
    "value label code dropped": (lambda e: e["value_labels"]["status1"].pop("-9"), None, "value label 'status1'"),
    "missing-value label": (lambda e: e["value_labels"]["tagged4"].pop(".z"), None, "value label 'tagged4'"),
    "value label detached": (lambda e: e["variables"]["agree2"].pop("value_label"), None, "agree2: value label"),
    "format": (lambda e: e["variables"]["visit"].update(format="%tdDD/NN/CCYY"), None, "visit: format"),
    "stata type": (lambda e: e["variables"]["income"].update(stata_type="float"), None, "income: stata_type"),
    "dataset label": (lambda e: e.pop("dataset_label"), None, "dataset label"),
    "data value": (None, lambda t: _set_value(t, "income", 4321, 1.5), "income: 1 value(s) differ"),
    "date value": (None, lambda t: _set_value(t, "stamp", 9, None), "stamp: missing/non-missing"),
    "string value": (None, lambda t: _set_value(t, "name", 4999, "x"), "name: 1 string value(s) differ"),
}


@pytest.mark.parametrize("what", list(TAMPERING))
def test_verifier_catches_tampering(rich, what, tmp_path):
    env_fn, table_fn, expected = TAMPERING[what]
    bad = _tampered(rich[1], str(tmp_path / "bad.parquet"), env_fn, table_fn)
    rep = verify(rich[0], bad, sample_rows=0)
    assert not rep.ok
    assert any(expected in p for p in rep.problems), rep.problems


def test_verifier_catches_missing_metadata(rich, tmp_path):
    t = pq.read_table(rich[1])
    pq.write_table(t.replace_schema_metadata({}), tmp_path / "plain.parquet")
    rep = verify(rich[0], str(tmp_path / "plain.parquet"))
    assert not rep.ok and "no org.stata.pq.labels.v1" in rep.problems[0]


# ---------------------------------------------------------------- notes
def add_characteristics(path, chars):
    """Insert Stata characteristics (how notes are stored) into a 118 file.

    `chars` is a list of (owner, name, value); owner "_dta" for the dataset.
    Shifts the <map> offsets of every section after <characteristics>.
    """
    b = bytearray(open(path, "rb").read())
    assert b[28:31] == b"118" and b[52:55] == b"LSF"
    blob = b""
    for owner, name, value in chars:
        body = owner.encode().ljust(129, b"\0") + name.encode().ljust(129, b"\0") + value.encode() + b"\0"
        blob += b"<ch>" + struct.pack("<I", len(body)) + body + b"</ch>"
    pos = b.index(b"</characteristics>")
    m = b.index(b"<map>") + 5
    offsets = list(struct.unpack("<14Q", b[m : m + 112]))
    offsets = [o + len(blob) if o > pos else o for o in offsets]
    b[m : m + 112] = struct.pack("<14Q", *offsets)
    b[pos:pos] = blob
    open(path, "wb").write(bytes(b))


def test_variable_and_dataset_notes(tmp_path):
    import pandas as pd

    src = tmp_path / "n.dta"
    pd.DataFrame({"a": [1, 2], "b": [3.5, 4.5]}).to_stata(src, write_index=False, version=118)
    add_characteristics(src, [
        ("_dta", "note0", "2"), ("_dta", "note1", "Dataset note one"), ("_dta", "note2", "Note ☃ two"),
        ("a", "note0", "1"), ("a", "note1", 'Variable note with "quotes"'),
        ("b", "_pq_parquet_name", "b with a long, Parquet-only name"),
        ("b", "note0", "2"), ("b", "note1", "first"), ("b", "note2", "second"),
    ])
    out = tmp_path / "n.parquet"
    convert_file(src, out)
    env = _env(out)
    assert env["dataset_notes"] == ["Dataset note one", "Note ☃ two"]
    assert env["variables"]["a"]["notes"] == ['Variable note with "quotes"']
    # renamed column: metadata is keyed by the Parquet column name
    assert pq.read_table(out).column_names == ["a", "b with a long, Parquet-only name"]
    assert env["variables"]["b with a long, Parquet-only name"]["notes"] == ["first", "second"]
    # pyreadstat independently reads the dataset notes (and the injected file
    # is valid for it); variable notes and renames are flagged as unverifiable
    rep = verify(str(src), str(out), sample_rows=0)
    assert rep.ok, rep.summary()
    assert any("2 variable(s) carry notes" in u for u in rep.unverifiable)
    assert any("_pq_parquet_name" in u for u in rep.unverifiable)


# ------------------------------------------------------- schema conformance
STATA_TYPES = {"byte", "int", "long", "float", "double", "date", "time", "datetime"}
VAR_FIELDS = {"label": str, "value_label": str, "notes": list, "format": str, "stata_type": str}


def assert_conforms(env: dict, table: pa.Table):
    """The envelope schema from examples/python_stata_metadata.py, strictly:
    every field but version optional and omitted rather than empty."""
    assert set(env) <= {"version", "dataset_label", "dataset_notes", "variables", "value_labels"}
    assert env["version"] == 1
    for key in ("dataset_label", "dataset_notes", "variables", "value_labels"):
        if key in env:
            assert env[key] not in ("", [], {}, None), f"empty {key} must be omitted"
    if "dataset_label" in env:
        assert isinstance(env["dataset_label"], str)
    for n in env.get("dataset_notes", []):
        assert isinstance(n, str)
    vls = env.get("value_labels", {})
    for col, var in env.get("variables", {}).items():
        assert col in table.column_names, f"{col}: not a parquet column name"
        assert set(var) <= set(VAR_FIELDS), f"{col}: unknown fields {set(var) - set(VAR_FIELDS)}"
        for k, typ in VAR_FIELDS.items():
            if k in var:
                assert isinstance(var[k], typ) and var[k] not in ("", []), f"{col}.{k} empty or wrong type"
        assert all(isinstance(n, str) for n in var.get("notes", []))
        t = table.schema.field(col).type
        is_text = pa.types.is_string(t) or pa.types.is_large_string(t) or pa.types.is_binary(t) \
            or pa.types.is_large_binary(t)
        if is_text:
            assert "stata_type" not in var, f"{col}: string columns carry no stata_type"
            assert "value_label" not in var, f"{col}: value labels apply only to numeric columns"
        else:
            assert var.get("stata_type") in STATA_TYPES, f"{col}: stata_type {var.get('stata_type')!r}"
        if "value_label" in var:
            assert var["value_label"] in vls, f"{col}: value label {var['value_label']!r} not defined"
    for name, codes in vls.items():
        assert isinstance(name, str) and name and codes
        for code, text in codes.items():
            # codes are strings holding integers (or .a-.z / . for missing)
            assert isinstance(code, str) and isinstance(text, str)
            assert code.lstrip("-").isdigit() or code in {"."} | {"." + c for c in "abcdefghijklmnopqrstuvwxyz"}, code


def _example_reader():
    import importlib.util

    path = os.path.join(os.path.dirname(__file__), "..", "..", "examples", "python_stata_metadata.py")
    if not os.path.exists(path):
        return None
    spec = importlib.util.spec_from_file_location("python_stata_metadata", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.read_stata_metadata


def _all_conversions(tmp_path):
    import glob

    import pandas as pd

    srcs = sorted(glob.glob(os.path.join(os.path.dirname(__file__), "..", "..", "test_data", "*", "*.dta")))
    rich_src = tmp_path / "rich.dta"
    make_rich_dta(str(rich_src), n=200)
    srcs.append(str(rich_src))
    strl_src = tmp_path / "strl.dta"
    pd.DataFrame({"t": ["x" * 3000, "y"], "s": ["a", "b"]}).to_stata(
        strl_src, write_index=False, version=118, convert_strl=["t"])
    srcs.append(str(strl_src))
    notes_src = tmp_path / "notes.dta"
    pd.DataFrame({"a": [1, 2]}).to_stata(notes_src, write_index=False, version=118)
    add_characteristics(notes_src, [("_dta", "note0", "1"), ("_dta", "note1", "n"),
                                    ("a", "note0", "1"), ("a", "note1", "m")])
    srcs.append(str(notes_src))
    for i, src in enumerate(srcs):
        out = tmp_path / f"{i}.parquet"
        convert_file(src, out)
        yield src, str(out)


def test_every_output_conforms_to_the_schema(tmp_path):
    reader = _example_reader()
    n = 0
    for src, out in _all_conversions(tmp_path):
        env = _env(out)
        assert_conforms(env, pq.read_table(out))
        if reader is not None:  # the repo's own example reads it back identically
            assert reader(out) == env, src
        n += 1
    assert n >= 4
