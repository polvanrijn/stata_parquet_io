# dta2parquet

Fast Stata `.dta` → Parquet conversion that keeps the Stata metadata:
variable labels, value labels, notes, display formats and storage types.

The metadata is embedded exactly the way `pq save, statametadata` from the
[`pq` Stata package](https://github.com/jrothbaum/stata_parquet_io) writes it
(the `org.stata.pq.labels.v1` key in the Parquet footer), so `pq use` in Stata
restores everything, and other tools can read it too.

* **Fast.** The `.dta` is read in large sequential chunks and decoded column
  by column with numpy (no per-cell Python objects). The Parquet is written
  by polars' parallel streaming writer, the same writer `pq save` uses.
  Measured on a 4-core VM:

  | File                              | dta2parquet | pandas `read_stata` + `to_parquet` | pyreadstat (read only) |
  |-----------------------------------|-------------|------------------------------------|------------------------|
  | 650 MB (5M rows × 29 columns)     | 3–4 s       | 21.6 s                             | 44.8 s                 |
  | 2.6 GB (20M rows × 29 columns)    | 9 s         | not run                            | not run                |

* **Bounded memory.** It works through the file in chunks (`--chunk-mb`,
  64 MB by default), so files bigger than RAM are fine. Peak memory stays
  around 1 GB per file with the defaults and 4 threads: about 0.95 GB for the
  650 MB file and 1.1 GB for the 2.6 GB one. Lower `--chunk-mb` or
  `--threads` if you run many files at once on a small machine.
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
you re-run the same command. `--update` redoes a file only when its `.dta`
is newer than its `.parquet`, and `--overwrite` redoes everything.
`--log run.jsonl` appends one JSON record per file (host, timings, errors).

## Checking the results

`verify` re-reads each `.dta` with
[pyreadstat](https://github.com/Roche/pyreadstat), which is built on the
ReadStat C library and shares no code with this converter. It then compares
the result with the Parquet file:

* **Metadata:** dataset label and notes, variable labels, display formats,
  storage types, which value label each variable uses, and every value-label
  code and text (including labels on `.a`–`.z`).
* **Schema:** every column, in order, with the type `pq save` would use.
* **Data:** rows from the start, middle and end of the file (30,000 by
  default, `--all-rows` for every row), value by value, with Stata missing
  values as nulls.

```bash
uv run --extra verify dta2parquet verify big.dta big.parquet
uv run --extra verify dta2parquet verify /data/stata /data/parquet   # a whole batch
```

Text is compared ignoring trailing spaces, because ReadStat strips them.
The converter itself keeps them exactly as stored. pyreadstat can't read
per-variable notes or the `_pq_parquet_name` rename, so `verify` lists those
as "not independently verifiable" instead of passing them silently. The test
suite covers them by adding notes directly into a `.dta` file.

The verifier is itself tested: the suite deliberately damages converted
files (a changed label, a dropped value-label code, a different format, one
altered value, an extra null, missing metadata) and checks that each change
is reported.

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

The lock age is measured with the file server's clock, so machines whose
clocks disagree don't mistake each other's live locks for stale ones. When a
machine takes over a stale lock it also deletes the half-written temp file
the crashed machine left behind. Input is read in large sequential chunks,
which suits network filesystems.

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
uv run pytest   # cross-checks against pandas and pyreadstat on many .dta variants
```

The package lives in `dta2parquet/` of the `stata_parquet_io` repo for now.
It has no dependency on the rest of the repo except the `test_data/` files
the tests read. To move it to its own repository, use
`git subtree split --prefix dta2parquet -b dta2parquet` and copy the few
`test_data/*/*.dta` files.
