#!/usr/bin/env python3
"""
Create a Parquet file from Python that carries Stata variable metadata
(variable labels, value labels, notes/comments, display formats, storage
types) so that `pq use` in Stata restores all of it.

The metadata is stored as a single JSON string under the Parquet key/value
metadata key ``stata.variable_metadata``.  This is exactly the format that
`pq save` writes, so files are interoperable in both directions:

    Python  --write-->  Parquet  --`pq use`-->  Stata  --`pq save`-->  Parquet

The JSON schema (all per-variable fields optional):

    {
      "version": 1,
      "variables": {
        "<column name>": {
          "label":            "<variable label>",
          "notes":            ["<note 1>", "<note 2>", ...],
          "comment":          "<first note; optional, kept for compatibility>",
          "format":           "<Stata display format, e.g. %9.2f>",
          "stata_type":       "byte|int|long|float|double|strN|strL",
          "value_label_name": "<name of the value label>",
          "value_labels":     [{"value": "0", "label": "Zero"}, ...]
        }
      }
    }

Notes:
  * ``value`` in ``value_labels`` is a STRING holding an integer; value labels
    apply only to numeric columns.  You may include values that never appear in
    the data -- they are preserved.
  * Use column names that are legal Stata variable names to avoid a rename on
    import (renamed columns may not re-attach their metadata).

Requires: pyarrow  (pip install pyarrow)
"""

import json
import pyarrow as pa
import pyarrow.parquet as pq

STATA_META_KEY = b"stata.variable_metadata"


def write_stata_parquet(path, table, variables, existing_metadata=None):
    """Write `table` to `path`, embedding `variables` as Stata metadata.

    table      : pyarrow.Table
    variables  : dict[str, dict] keyed by column name (see schema above)
    """
    blob = {"version": 1, "variables": variables}
    schema_meta = dict(existing_metadata or {})
    schema_meta[STATA_META_KEY] = json.dumps(blob).encode("utf-8")
    table = table.replace_schema_metadata(schema_meta)
    pq.write_table(table, path)


def read_stata_metadata(path):
    """Return the embedded Stata metadata dict, or {} if none is present."""
    meta = pq.read_schema(path).metadata or {}
    raw = meta.get(STATA_META_KEY)
    return json.loads(raw) if raw else {}


def _demo(path="python_made.parquet"):
    # ---- the data -------------------------------------------------------
    table = pa.table(
        {
            "grp":   pa.array([0, 1, 2, 0, 1, 2], pa.int8()),      # -> Stata byte
            "price": pa.array([1.5, 3, 4.5, 6, 7.5, 9], pa.float64()),
            "name":  pa.array([f"obs{i}" for i in range(1, 7)]),
        }
    )

    # ---- the metadata ---------------------------------------------------
    variables = {
        "grp": {
            "label": "Group indicator",
            "notes": ["first note on grp", "second note on grp"],
            "format": "%8.0g",
            "stata_type": "byte",
            "value_label_name": "grplbl",
            "value_labels": [
                {"value": "0", "label": "Zero"},
                {"value": "1", "label": "One"},
                {"value": "2", "label": "Two"},
                {"value": "3", "label": "Three (unused)"},  # kept even if absent
            ],
        },
        "price": {
            "label": "Unit price",
            "notes": ["price in local currency"],
            "format": "%9.2f",
            "stata_type": "double",
        },
        "name": {
            "label": "Observation name",
            "stata_type": "str12",
        },
    }

    write_stata_parquet(path, table, variables)
    print(f"wrote {path}")
    print(json.dumps(read_stata_metadata(path), indent=2))


if __name__ == "__main__":
    import sys
    _demo(*(sys.argv[1:2] or ["python_made.parquet"]))
