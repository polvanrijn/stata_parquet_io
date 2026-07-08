# pq v3.1.1

Patch release on top of v3.1.0.

## Fix: metadata is now visible to pyarrow / pandas / polars

`pq save` embedded `stata.variable_metadata` only as a **file-level** Parquet
key/value pair. Polars regenerates the `ARROW:schema` footer key from the frame
schema (which carries no metadata), so Arrow-based readers reconstruct the
schema from `ARROW:schema` and never saw the Stata metadata — e.g.
`pyarrow.parquet.read_schema().metadata` (and pandas / polars) returned nothing
for a Stata-written file, so `read_stata_metadata()` reported `{}`.

The metadata is now **also embedded inside the Arrow schema** (the `ARROW:schema`
IPC blob), so it survives the full `Python → pq use → pq save → Python` round
trip. The file-level key is still written, so `pq use` is unaffected. Applied to
all Parquet write paths (single file, hive partitions, and directory
consolidation).

Verify:

```python
import pyarrow.parquet as pq
print(pq.read_schema("stata_resaved.parquet").metadata)   # no longer None
```

## Build / packaging

- The build artifacts now include a ready-to-load `pq.plugin` for every platform
  (a universal Intel+ARM binary on macOS), so `workflow_dispatch` builds — not
  only tagged releases — can be dropped straight onto Stata's adopath. Tagged
  `v*` releases continue to ship the per-platform `pq-*.zip` packages.

## Note on loading a rebuilt plugin

Stata caches a plugin in memory for the whole session: `pq_register_plugin` only
loads the binary when one is not already loaded. After replacing `pq.plugin`,
**restart Stata** (or `capture program drop polars_parquet_plugin` before the
first `pq` call) so the new binary is actually used.
