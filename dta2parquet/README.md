# dta2parquet

Fast Stata `.dta` → Parquet conversion that keeps the Stata metadata:
variable labels, value labels, notes, display formats and storage types.

The metadata is embedded exactly the way `pq save, statametadata` from the
[`pq` Stata package](https://github.com/jrothbaum/stata_parquet_io) writes it
(the `org.stata.pq.labels.v1` key in the Parquet footer), so `pq use` in Stata
restores everything, and other tools can read it too.

* **Fast.** The `.dta` is memory-mapped and decoded column by column with
  numpy (no per-cell Python objects), and the Parquet is written by polars'
  parallel streaming writer, the same writer `pq save` uses. On a 4-core VM a
  650 MB file (5M rows × 29 columns) takes **2.9 s**. For comparison,
  `pandas.read_stata` + `to_parquet` takes 21.6 s and `pyreadstat` needs
  44.8 s just to read the file.
* **Bounded memory.** It streams in chunks (`--chunk-mb`, 128 MB of `.dta`
  by default), so files bigger than RAM are fine.
* **Scales out.** `batch` converts many files in parallel, and several
  machines can share the work (see below).
* **Safe.** Each output is written to a temp file, checked (row and column
  count) and atomically renamed, so you never get a half-written `.parquet`.
* Supports `.dta` formats 113–115 (Stata 8–12) and 117–119 (Stata 13+), both
  byte orders, `strL`s, and every date/time format.

## Install / run (uv)

```bash
# run from a checkout, without installing
cd dta2parquet
uv run dta2parquet convert input.dta output.parquet

# or install it as a tool on every machine
uv tool install ./dta2parquet            # from a checkout
uv tool install "git+https://github.com/polvanrijn/stata_parquet_io#subdirectory=dta2parquet"
dta2parquet --help
```

From Python:

```python
from dta2parquet import convert_file, ConvertOptions
convert_file("in.dta", "out.parquet", ConvertOptions(threads=8))
```

## Usage

```bash
# one file (default output: same name, .parquet)
dta2parquet convert big.dta
dta2parquet convert big.dta /data/parquet/big.parquet --compression-level 6

# many files: directories (recursive, keeps the sub-folder layout), globs,
# files, or @list.txt (one path per line)
dta2parquet batch /data/stata -o /data/parquet
dta2parquet batch "/data/stata/wave_*.dta" @more_files.txt -o /data/parquet -j 4 --threads 2
dta2parquet batch /data/stata -o /data/parquet --dry-run     # show the plan

# look at the metadata of a result (or of what a .dta would produce)
dta2parquet inspect /data/parquet/big.parquet
dta2parquet inspect big.dta
```

`-j` sets how many files run at once on this machine and `--threads` sets
the threads per file. By default `-j` is half the cores and `--threads`
splits the cores between the files. When you have a few huge files, use
fewer jobs with more threads. When you have many small ones, use more jobs.
Files are processed biggest first.

Already converted outputs are skipped, so an interrupted run resumes when
you re-run the same command. `--overwrite` redoes them. `--log run.jsonl`
appends one JSON record per file (host, timings, errors).

## Spreading the work over several computers

**Option 1: shared output folder (recommended).** Mount the input and output
folders on every machine (NFS, SMB, Lustre, ...) and start the **same
command** on each one:

```bash
# on machine 1, 2, 3, ... (e.g. via ssh, or as a SLURM array job)
uvx --from "git+https://github.com/polvanrijn/stata_parquet_io#subdirectory=dta2parquet" \
    dta2parquet batch /shared/stata -o /shared/parquet --log /shared/parquet/log.jsonl
```

Before a worker converts a file it creates `<output>.lock` atomically. Other
machines see the lock and move on to the next file, so every file is
converted once. Machines can join or leave at any time. While a worker
converts a file it keeps refreshing the lock. A lock that has not been
refreshed for `--stale-after` minutes (default 10) belongs to a crashed
machine, and the next run takes it over. Just re-run the command to pick up
anything that was left over.

**Option 2: static shards (no shared output needed).** Give each machine a
fixed slice of the files with `--shard K/N`:

```bash
dta2parquet batch /data/stata -o /local/out --shard 1/3   # machine 1
dta2parquet batch /data/stata -o /local/out --shard 2/3   # machine 2
dta2parquet batch /data/stata -o /local/out --shard 3/3   # machine 3
```

Shards are assigned by a hash of each file's relative path, so every machine
agrees on the split without talking to the others. Use `--shard K/N` with
`$SLURM_ARRAY_TASK_ID`, for example. You can combine both options.

## What gets converted, and how

Column types follow `pq save` exactly (`src/mapping.rs` in this repo):

| Stata                                         | Parquet / Arrow                           |
|-----------------------------------------------|-------------------------------------------|
| `byte` / `int` / `long`                       | int8 / int16 / int32                      |
| `float` / `double`                            | float32 / float64                         |
| `str#`, `strL`                                | string (a `strL` holding binary data → binary) |
| numeric with `%td %tw %tm %tq %th %ty %tb`    | date32                                    |
| numeric with `%tc %tC`                        | timestamp[ms]                             |
| numeric with a time-of-day format (`hh:mm`)   | time64[ns]                                |
| missing `.`, `.a` … `.z`                      | null                                      |

The metadata envelope matches what `pq save, statametadata` writes:

```json
{"version":1,"dataset_label":"…","dataset_notes":["…"],
 "variables":{"income":{"label":"Household income","value_label":"inc_lbl",
                        "notes":["…"],"format":"%10.0g","stata_type":"double"}},
 "value_labels":{"inc_lbl":{"1":"low","2":"high",".a":"refused"}}}
```

This includes the same key order and compact serialization, the
`_pq_parquet_name` characteristic and `{parquet_name:…}` label renames, and
value labels on extended missing values (`.a` … `.z`).

Things to know:

* Like `pq save`, `%tw %tm %tq %th %ty %tb` variables are written as dates
  (the Stata value shifted to the Unix epoch). `pq use` turns them back into
  the original values and formats. Other tools will show them as odd
  calendar dates, though.
* Extended missing values (`.a`–`.z`) become plain nulls, as in `pq save`.
* Strings in Stata 14+ files are UTF-8. Older files are read as latin-1; pass
  `--encoding cp1252` (or similar) if they used another code page.
* Value-label codes are written as plain integers (`"1000000000"`). Stata's
  own `pq save` formats them with Mata `strofreal`, which can switch to
  exponent notation for codes with very many digits.

## Development

```bash
cd dta2parquet
uv run pytest            # cross-checks against pandas' reader on many .dta variants
```

The package lives in `dta2parquet/` of the `stata_parquet_io` repo for now.
It has no dependency on the rest of the repo except the `test_data/` files
the tests read. To move it to its own repository, use
`git subtree split --prefix dta2parquet -b dta2parquet` and copy the few
`test_data/*/*.dta` files.
