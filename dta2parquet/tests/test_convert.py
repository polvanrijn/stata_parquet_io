"""Cross-check dta2parquet against pandas' Stata reader."""

from __future__ import annotations

import glob
import json
import os

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from dta2parquet import STATA_META_KEY, ConvertOptions, convert_file
from dta2parquet.batch import plan_jobs, run_batch
from dta2parquet.dta import DtaFormatError

HERE = os.path.dirname(__file__)
TEST_DATA = os.path.join(HERE, "..", "..", "test_data")
REPO_FILES = sorted(glob.glob(os.path.join(TEST_DATA, "*", "*.dta")))
# the sample .dta files live in the stata_parquet_io repo's test_data/
needs_repo_data = pytest.mark.skipif(not REPO_FILES, reason="repo test_data/ not available")


def envelope(path):
    return json.loads(pq.ParquetFile(path).metadata.metadata[STATA_META_KEY.encode()])


def assert_matches_pandas(dta_path, pq_path):
    with pd.io.stata.StataReader(dta_path, convert_dates=False, convert_categoricals=False) as rd:
        ref = rd.read()
        var_labels = rd.variable_labels()
        data_label = rd.data_label
    got = pq.read_table(pq_path)
    assert got.column_names == list(ref.columns)
    assert got.num_rows == len(ref)
    for name in ref.columns:
        exp = ref[name]
        col = got.column(name)
        t = col.type
        if pa.types.is_string(t) or pa.types.is_large_string(t):
            assert col.to_pylist() == exp.tolist(), name
        elif pa.types.is_date32(t):
            v = col.cast(pa.int32()).to_numpy(zero_copy_only=False)
            mask = exp.isna().to_numpy()
            np.testing.assert_array_equal(col.is_null().to_numpy(zero_copy_only=False), mask)
            np.testing.assert_array_equal(v[~mask] + 3653, np.trunc(exp.to_numpy()[~mask]))
        elif pa.types.is_timestamp(t):
            v = col.cast(pa.int64()).to_numpy(zero_copy_only=False)
            mask = exp.isna().to_numpy()
            np.testing.assert_array_equal(v[~mask] + 315619200000, np.trunc(exp.to_numpy()[~mask]))
        elif pa.types.is_time64(t):
            pass  # checked in test_pyreadstat_sample
        else:
            v = col.to_numpy(zero_copy_only=False).astype(float)
            np.testing.assert_array_equal(v, exp.to_numpy(dtype=float), err_msg=name)

    env = envelope(pq_path)
    assert env["version"] == 1
    assert env.get("dataset_label", "") == data_label
    for name, lab in var_labels.items():
        assert env["variables"][name].get("label", "") == lab
    return env


@pytest.mark.parametrize("engine", ["polars", "pyarrow"])
@pytest.mark.parametrize("path", REPO_FILES, ids=os.path.basename)
def test_repo_test_files(path, engine, tmp_path):
    out = tmp_path / "out.parquet"
    convert_file(path, out, ConvertOptions(engine=engine))
    assert_matches_pandas(path, str(out))


def _frame(n=1000, seed=0):
    rng = np.random.default_rng(seed)
    df = pd.DataFrame(
        {
            "b": rng.integers(-100, 100, n).astype(np.int8),
            "i": rng.integers(-30000, 30000, n).astype(np.int16),
            "l": rng.integers(-(2**31) + 30, 2**31 - 30, n).astype(np.int32),
            "f": rng.standard_normal(n).astype(np.float32),
            "d": rng.standard_normal(n),
            "s": [f"row {k} äöü €" if k % 7 else "" for k in range(n)],
            "dt": pd.to_datetime("2001-02-03") + pd.to_timedelta(rng.integers(0, 9000, n), unit="D"),
            "ts": pd.to_datetime("2001-02-03") + pd.to_timedelta(rng.integers(0, 10**9, n), unit="ms"),
            "lab": pd.Categorical(rng.choice(["low", "mid", "high"], n)),
        }
    )
    df.loc[::5, "f"] = np.nan
    df.loc[::3, "d"] = np.nan
    df.loc[::11, "dt"] = pd.NaT
    return df


@pytest.mark.parametrize("engine", ["polars", "pyarrow"])
@pytest.mark.parametrize("version", [114, 117, 118, 119])
@pytest.mark.parametrize("byteorder", ["<", ">"])
def test_pandas_written(version, byteorder, engine, tmp_path):
    df = _frame()
    if version < 118:  # no unicode before Stata 14
        df["s"] = df["s"].str.replace("€", "E")
    kw = {}
    # pandas' big-endian strL writer is inconsistent with itself (GSO headers
    # little-endian, references big-endian), so only test strLs little-endian
    with_strl = version >= 117 and byteorder == "<"
    if with_strl:
        # (pandas writes 117 strLs as UTF-8, unlike Stata 13 - keep them ASCII)
        tail = "é" if version >= 118 else "e"
        df["long_text"] = ["x" * (k % 3000) + tail for k in range(len(df))]
        kw["convert_strl"] = ["long_text"]
    src = tmp_path / f"v{version}.dta"
    df.to_stata(
        src,
        version=version,
        byteorder=byteorder,
        write_index=False,
        convert_dates={"dt": "td", "ts": "tc"},
        data_label="A test dataset",
        variable_labels={"d": "A double", "lab": "Labelled"},
        **kw,
    )
    out = tmp_path / "out.parquet"
    # tiny row groups + threads to exercise the chunked pipeline
    convert_file(src, out, ConvertOptions(row_group_rows=97, threads=3, engine=engine))
    env = assert_matches_pandas(str(src), str(out))

    pf = pq.ParquetFile(out)
    assert pf.metadata.num_row_groups == -(-len(df) // 97)
    assert pf.metadata.row_group(0).column(0).statistics.has_min_max
    t = pf.read()
    assert t.schema.field("b").type == pa.int8()
    assert t.schema.field("i").type == pa.int16()
    assert t.schema.field("l").type == pa.int32()
    assert t.schema.field("f").type == pa.float32()
    assert t.schema.field("dt").type == pa.date32()
    assert t.schema.field("ts").type == pa.timestamp("ms")
    assert t.column("dt").to_pylist() == [None if pd.isna(x) else x.date() for x in df["dt"]]
    assert t.column("ts").to_pylist() == [x.to_pydatetime() for x in df["ts"]]
    assert t.column("f").null_count == len(range(0, len(df), 5))

    assert env["variables"]["lab"]["value_label"] == "lab"
    codes = {str(i): c for i, c in enumerate(df["lab"].cat.categories)}
    assert env["value_labels"]["lab"] == dict(sorted(codes.items()))
    assert env["variables"]["dt"]["stata_type"] == "date"
    assert env["variables"]["ts"]["stata_type"] == "datetime"
    assert env["variables"]["s"]["stata_type"] == "string"
    if with_strl:
        assert env["variables"]["long_text"]["stata_type"] == "strl"
        assert t.column("long_text").to_pylist() == df["long_text"].tolist()


@needs_repo_data
def test_pyreadstat_sample(tmp_path):
    src = os.path.join(TEST_DATA, "pyreadstat", "sample.dta")
    out = tmp_path / "o.parquet"
    convert_file(src, out)
    t = pq.read_table(out)
    assert t.schema.field("mytime").type == pa.time64("ns")
    assert str(t.column("mytime")[1].as_py()) == "23:10:10"
    assert str(t.column("dtime")[0].as_py()) == "2018-05-06 10:10:10"


@needs_repo_data
def test_notes_and_missing_value_labels(tmp_path):
    out = tmp_path / "n.parquet"
    convert_file(os.path.join(TEST_DATA, "haven", "notes.dta"), out)
    env = envelope(out)
    assert env["dataset_label"] == "This is a test dataset."
    assert env["dataset_notes"] == ["Reference: https://github.com/hadley/haven/issues/186"]

    convert_file(os.path.join(TEST_DATA, "haven", "tagged-na-int.dta"), out)
    assert envelope(out)["value_labels"] == {"testlabel": {".a": "apple", ".z": ".zebra"}}
    assert pq.read_table(out).column("x").null_count == 3


@needs_repo_data
def test_envelope_serialization_matches_rust(tmp_path):
    """Key order must match serde's: struct order outside, sorted maps inside."""
    out = tmp_path / "t.parquet"
    convert_file(os.path.join(TEST_DATA, "haven", "types.dta"), out)
    raw = pq.ParquetFile(out).metadata.metadata[STATA_META_KEY.encode()].decode()
    assert raw.startswith('{"version":1,"variables":{"vbyte":{"format":"%9.0g","stata_type":"byte"}')
    assert ", " not in raw and ": " not in raw


def test_batch_claims_and_resume(tmp_path):
    src_dir = tmp_path / "in" / "sub"
    src_dir.mkdir(parents=True)
    for k in range(4):
        _frame(50, seed=k).to_stata(src_dir / f"f{k}.dta", write_index=False, version=118)
    out_dir = tmp_path / "out"
    jobs = plan_jobs([str(tmp_path / "in")], str(out_dir))
    assert sorted(j.rel for j in jobs) == [f"sub/f{k}.parquet" for k in range(4)]

    # a fresh lock held by "another machine" is respected
    (out_dir / "sub").mkdir(parents=True)
    (out_dir / "sub" / "f0.parquet.lock").write_text("{}")
    res = run_batch(jobs, ConvertOptions(threads=1), workers=2)
    assert sorted(o.status for o in res) == ["busy", "done", "done", "done"]

    # ...but a stale one is taken over; finished files are skipped
    os.utime(out_dir / "sub" / "f0.parquet.lock", (0, 0))
    res = run_batch(jobs, ConvertOptions(threads=1), workers=1, stale_after=60)
    assert sorted(o.status for o in res) == ["done", "skipped", "skipped", "skipped"]
    assert not list(out_dir.rglob("*.lock")) and not list(out_dir.rglob("*.tmp-*"))


def test_shards_partition_the_work(tmp_path):
    for k in range(20):
        (tmp_path / f"f{k}.dta").write_bytes(b"x")
    n = 3
    shards = [{j.rel for j in plan_jobs([str(tmp_path)], "o", shard=(k, n))} for k in range(1, n + 1)]
    assert sum(len(s) for s in shards) == 20
    assert set.union(*shards) == {f"f{k}.parquet" for k in range(20)}


@pytest.mark.parametrize("engine", ["polars", "pyarrow"])
def test_empty_dataset(engine, tmp_path):
    src = tmp_path / "e.dta"
    pd.DataFrame({"a": pd.Series([], dtype="int32"), "s": pd.Series([], dtype=object)}).to_stata(
        src, write_index=False, version=118
    )
    out = tmp_path / "e.parquet"
    convert_file(src, out, ConvertOptions(engine=engine))
    t = pq.read_table(out)
    assert t.num_rows == 0 and t.column_names == ["a", "s"]


def test_corrupt_file_leaves_nothing_behind(tmp_path):
    src = tmp_path / "bad.dta"
    _frame(100).to_stata(src, write_index=False, version=118)
    data = src.read_bytes()
    src.write_bytes(data[: len(data) // 2])  # truncated
    with pytest.raises(DtaFormatError, match="truncated"):
        convert_file(src, tmp_path / "bad.parquet")
    assert [p.name for p in tmp_path.iterdir()] == ["bad.dta"]


@pytest.mark.parametrize("engine", ["polars", "pyarrow"])
def test_error_mid_stream_leaves_nothing_behind(engine, tmp_path, monkeypatch):
    import dta2parquet.convert as C

    src = tmp_path / "f.dta"
    _frame(1000).to_stata(src, write_index=False, version=118)
    real, calls = C._convert_chunk, []

    def flaky(*a):
        calls.append(1)
        if len(calls) == 3:
            raise OSError("disk on fire")
        return real(*a)

    monkeypatch.setattr(C, "_convert_chunk", flaky)
    with pytest.raises(OSError, match="disk on fire"):
        convert_file(src, tmp_path / "f.parquet", ConvertOptions(row_group_rows=100, threads=2, engine=engine))
    assert [p.name for p in tmp_path.iterdir()] == ["f.dta"]


def test_stale_lock_takeover_races_and_cleanup(tmp_path):
    from dta2parquet.batch import Claim

    dst = str(tmp_path / "x.parquet")
    lock = dst + ".lock"
    with open(lock, "w") as fh:
        fh.write('{"host": "dead"}')
    os.utime(lock, (0, 0))
    a, b = Claim(dst, 60, 3600), Claim(dst, 60, 3600)
    assert a.acquire()  # took the stale lock over
    assert not b.acquire()  # a's lock is fresh
    with open(lock) as fh:
        owner = fh.read()

    # b decided "stale" based on the dead lock's content, but a replaced it
    # before b's rename: b must restore a's lock and back off
    real_open, calls = open, []

    def fake_read(path, *args, **kw):
        if path == lock and not calls:
            calls.append(1)
            import io
            return io.StringIO('{"host": "dead"}')
        return real_open(path, *args, **kw)

    import builtins
    b._fs_now = lambda: os.stat(lock).st_mtime + 10**6  # everything looks stale
    builtins.open, saved = fake_read, builtins.open
    try:
        assert not b._take_over()
    finally:
        builtins.open = saved
    with open(lock) as fh:
        assert fh.read() == owner
    a.release()
    assert not os.path.exists(lock)
    assert [p.name for p in tmp_path.iterdir()] == []


def test_batch_removes_leftover_temp_files(tmp_path):
    src = tmp_path / "in"
    src.mkdir()
    _frame(20).to_stata(src / "a.dta", write_index=False, version=118)
    out = tmp_path / "out"
    out.mkdir()
    (out / "a.parquet.tmp-deadhost-123").write_bytes(b"partial")
    res = run_batch(plan_jobs([str(src)], str(out)), ConvertOptions(threads=1), workers=1)
    assert [o.status for o in res] == ["done"]
    assert sorted(p.name for p in out.iterdir()) == ["a.parquet"]


def test_batch_update_redoes_newer_sources(tmp_path):
    src = tmp_path / "in"
    src.mkdir()
    _frame(20).to_stata(src / "a.dta", write_index=False, version=118)
    out = tmp_path / "out"
    jobs = plan_jobs([str(src)], str(out))
    assert [o.status for o in run_batch(jobs, ConvertOptions(threads=1), workers=1)] == ["done"]
    assert [o.status for o in run_batch(jobs, ConvertOptions(threads=1), workers=1, update=True)] == ["skipped"]
    future = os.stat(out / "a.parquet").st_mtime + 100
    os.utime(src / "a.dta", (future, future))
    assert [o.status for o in run_batch(jobs, ConvertOptions(threads=1), workers=1)] == ["skipped"]
    assert [o.status for o in run_batch(jobs, ConvertOptions(threads=1), workers=1, update=True)] == ["done"]


def test_chunk_size_accounts_for_strl_payload(tmp_path):
    from dta2parquet import convert as C
    from dta2parquet.dta import open_dta

    src = tmp_path / "s.dta"
    pd.DataFrame({"t": ["y" * 100_000] * 50}).to_stata(src, write_index=False, version=118, convert_strl=["t"])
    with open_dta(src) as d:
        # 8-byte refs alone would allow 64 MB / 8 = millions of rows per chunk
        assert C._rows_per_group(d, ConvertOptions(chunk_bytes=1_000_000)) == 9


def test_cli_batch_log_in_new_output_dir(tmp_path):
    from dta2parquet.cli import main

    src = tmp_path / "in"
    src.mkdir()
    _frame(20).to_stata(src / "a.dta", write_index=False, version=118)
    out = tmp_path / "out"
    assert main(["batch", str(src), "-o", str(out), "--log", str(out / "log.jsonl"), "-j", "1"]) == 0
    rec = json.loads((out / "log.jsonl").read_text())
    assert rec["status"] == "done" and rec["result"]["rows"] == 20
