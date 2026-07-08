#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "polars",
#   "pyarrow",
# ]
# ///

import polars as pl

from python_stata_metadata import read_stata_metadata

old_wo_metadata_df = pl.read_parquet("python_made.parquet")
new_wo_metadata_df = pl.read_parquet("stata_resaved.parquet")
assert old_wo_metadata_df.equals(new_wo_metadata_df), "DataFrames are not identical"

assert read_stata_metadata("python_made.parquet") == read_stata_metadata("stata_resaved.parquet")
