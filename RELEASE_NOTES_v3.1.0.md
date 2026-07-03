# pq v3.1.0

## New: Stata variable metadata round-trips through Parquet

`pq save` now embeds per-variable Stata metadata into the Parquet file's
key/value metadata (key `stata.variable_metadata`), and `pq use` restores it:

- **Variable labels** (`label variable`)
- **Value labels** (name + full set of mappings)
- **Notes / comments** (`notes`)
- **Display formats** (`format`)
- **Original storage type** (`byte`/`int`/`long`/`float`/`double`/`str#`), so a
  numeric column that was compressed keeps its narrow type on reload instead of
  widening.

The metadata is written as a single JSON blob in the Parquet footer, so files
remain fully readable by pandas/polars/Arrow/Spark, which simply ignore the
extra key.

## Fixes since 3.0.7

- **Metadata now survives directory consolidation.** `consolidate` previously
  wrote the merged file with no key/value metadata, dropping all labels/notes.
  It now carries the embedded metadata across the rewrite.
- **Value labels are captured in full.** The previous implementation only saved
  mappings for values that actually appeared in the data, silently dropping
  labels for unused values. It now enumerates the entire value-label definition
  (via a Mata `st_vlload` helper).
- **Cleaned up corrupted `char … [pq_parquet_name]` statements** in the rename
  path (repeated `capture` tokens introduced by an earlier edit).
- **Fixed note text on read.** Notes were re-applied with `notes var: `"text"'`;
  because `notes` stores the rest of the line verbatim, the quote delimiters
  ended up inside the note. Notes now round-trip exactly.

## Python interoperability

The metadata is a plain JSON blob under the Parquet key `stata.variable_metadata`,
so you can produce Stata-ready Parquet from Python (and read it back):

- `examples/python_stata_metadata.py` — write/read the metadata with pyarrow.
- `examples/python_roundtrip.do` — Python-made parquet → `pq use` → `pq save`.

## Known limitations

- **Dataset-level** metadata (`label data`, `_dta` notes) is not yet
  round-tripped — only per-variable metadata.
- If a column was renamed on import (because its Parquet name was not a legal
  Stata name) and then re-saved, its metadata is keyed by the Stata name and may
  not re-attach on the next read.

## Build / verification

- Windows plugin cross-compiled from macOS via `cargo-xwin`
  (`x86_64-pc-windows-msvc`); verified as a PE32+ x64 DLL exporting
  `pginit`, `stata_call`, `_stata_`.
- See [BUILDING.md](BUILDING.md) for build instructions.
- A self-checking round-trip test is provided in
  [test_metadata_roundtrip.do](test_metadata_roundtrip.do).
