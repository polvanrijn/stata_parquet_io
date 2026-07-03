*!  python_roundtrip.do
*!  Completes the round trip:  Python-made parquet  ->  Stata  ->  parquet
*!
*!  Prereq: run  python examples/python_stata_metadata.py python_made.parquet
*!          then set PQDIR (folder with pq.ado/pq.plugin) and the file paths below.

clear all
set more off
version 16.0

local PQDIR   "`c(pwd)'"                 // folder with pq.ado + pq.plugin
local INFILE  "python_made.parquet"      // written by the Python example
local OUTFILE "stata_resaved.parquet"    // Stata will re-save here

adopath ++ "`PQDIR'"
capture program drop pq

*-------------------------------------------------------------------------------
* 1.  Load the Python-made parquet and confirm the metadata came through
*-------------------------------------------------------------------------------
pq use "`INFILE'", clear

describe
di as txt _n "grp variable label : " as res `"`: variable label grp'"'
di as txt    "grp value label    : " as res `"`: value label grp'"'
label list grplbl
di as txt "grp notes:"
notes grp

di as txt _n "{hline 50}"
di as res "Loaded from Python-made parquet OK"
di as txt "{hline 50}"

*-------------------------------------------------------------------------------
* 2.  Re-save from Stata -- metadata is written back into the new parquet
*-------------------------------------------------------------------------------
pq save "`OUTFILE'", replace
di as res _n "Re-saved to `OUTFILE'."
di as txt "Verify from Python with:"
di as txt `"    python -c "import json,examples.python_stata_metadata as m;"' ///
          `" print(json.dumps(m.read_stata_metadata('`OUTFILE''), indent=2))""'
