"""Fast Stata .dta -> Parquet conversion preserving Stata metadata.

The metadata is embedded exactly like `pq save, statametadata` from the
`pq` Stata package does, so `pq use` restores variable labels, value
labels, notes, display formats and storage types.
"""

from .convert import ConvertOptions, ConvertResult, convert_file
from .metadata import STATA_META_KEY, build_envelope

__all__ = ["STATA_META_KEY", "ConvertOptions", "ConvertResult", "build_envelope", "convert_file"]
__version__ = "0.1.0"
