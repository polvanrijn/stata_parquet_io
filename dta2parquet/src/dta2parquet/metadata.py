"""Build the Stata metadata envelope the ``pq`` Stata package reads.

The envelope is one JSON string stored in the Parquet footer key/value
metadata under ``org.stata.pq.labels.v1``. Its content and serialization
mirror ``src/stata_metadata.rs`` (``metadata_from_macros``) and
``pq.ado`` (``pq save, statametadata``) exactly, so a file converted here is
byte-for-byte the same envelope Stata itself would have written:

* field order follows the Rust struct declaration order (serde), maps
  (``variables``, ``value_labels`` and each label's codes) are sorted by key
  like Rust's ``BTreeMap``; empty fields are omitted;
* compact separators, non-ASCII left unescaped (serde_json's output).
"""

from __future__ import annotations

import json
import re

from .dta import DtaFile, Variable

STATA_META_KEY = "org.stata.pq.labels.v1"
STATA_META_VERSION = 1

_PARQUET_NAME_LABEL = re.compile(r"^\{parquet_name:([^}]*)\}")


# ---------------------------------------------------------------- type logic
def _match_var_format(fmt: str) -> str | None:
    """Port of mapping.rs::match_var_format_stata."""
    f = fmt.lower()
    if "hh:mm:ss" in f or "hh:mm" in f:
        return "time"
    if f.startswith(("%tc", "%c", "%tn", "%n", "%tu", "%u")):
        return "datetime"
    if f.startswith(("%td", "%d", "%tw", "%tm", "%tq", "%th", "%ty", "%tb")):
        return "date"
    return None


def resolve_stata_type(var: Variable) -> str:
    """Port of mapping.rs::resolve_stata_type.

    Returns byte|int|long|float|double|date|time|datetime|string|strl.
    Note that ``byte`` never becomes a date, exactly as in ``pq save``.
    """
    t = var.stata_type
    if t == "str":
        return "string"
    if t == "strl":
        return "strl"
    if t == "byte":
        return "byte"
    return _match_var_format(var.format) or t


def parquet_column_name(dta: DtaFile, var: Variable) -> str:
    """Column name ``pq save`` would use (honours its two rename hooks)."""
    char_name = dta.characteristics.get(var.name, {}).get("_pq_parquet_name", "")
    if char_name:
        return char_name
    m = _PARQUET_NAME_LABEL.match(var.label)
    if m:
        return m.group(1)
    return var.name


def value_label_code(code: int) -> str:
    """Render a value-label code the way pq.ado does (Mata ``strofreal``).

    Labels on extended missing values are stored in the .dta as the integer
    codes 2147483621 (``.``) and 2147483622.. (``.a`` .. ``.z``).
    """
    if code == 2147483621:
        return "."
    if 2147483622 <= code <= 2147483647:
        return "." + chr(ord("a") + code - 2147483622)
    return str(code)


def _notes(chars: dict[str, str]) -> list[str]:
    try:
        n = int(chars.get("note0", "0") or 0)
    except ValueError:
        n = 0
    return [chars.get(f"note{k}", "") for k in range(1, n + 1)]


def build_envelope(dta: DtaFile, column_names: list[str]) -> dict:
    """Envelope for every variable of `dta` (written under `column_names`)."""
    env: dict = {"version": STATA_META_VERSION}
    if dta.data_label:
        env["dataset_label"] = dta.data_label
    ds_notes = _notes(dta.characteristics.get("_dta", {}))
    if ds_notes:
        env["dataset_notes"] = ds_notes

    variables: dict[str, dict] = {}
    value_labels: dict[str, dict[str, str]] = {}
    for var, col in zip(dta.variables, column_names):
        entry: dict = {}
        if var.label:
            entry["label"] = var.label
        if var.value_label:
            entry["value_label"] = var.value_label
            defn = dta.value_labels.get(var.value_label)
            if defn and var.value_label not in value_labels:
                value_labels[var.value_label] = {value_label_code(k): v for k, v in defn.items()}
        notes = _notes(dta.characteristics.get(var.name, {}))
        if notes:
            entry["notes"] = notes
        if var.format:
            entry["format"] = var.format
        entry["stata_type"] = resolve_stata_type(var)
        variables[col] = entry

    if variables:
        env["variables"] = dict(sorted(variables.items()))
    if value_labels:
        env["value_labels"] = {
            name: dict(sorted(codes.items())) for name, codes in sorted(value_labels.items())
        }
    return env


def dumps(envelope: dict) -> str:
    """Serialize like serde_json::to_string."""
    return json.dumps(envelope, ensure_ascii=False, separators=(",", ":"))
