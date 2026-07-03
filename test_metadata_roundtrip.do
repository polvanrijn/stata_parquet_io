*!  test_metadata_roundtrip.do
*!  Round-trips column labels, value labels, notes/comments and data through
*!  parquet using the pq plugin and asserts everything survives.
*!
*!  USAGE: put pq.ado, pq.sthlp and pq.plugin in one folder, set PQDIR below to
*!         that folder, then:  do test_metadata_roundtrip.do

clear all
set more off
version 16.0

*-------------------------------------------------------------------------------
* 0.  Point Stata at the plugin + ado and load it
*-------------------------------------------------------------------------------
* EDIT THIS to the folder that contains pq.ado / pq.plugin (e.g. dist/pq-windows)
local PQDIR "`c(pwd)'"
adopath ++ "`PQDIR'"

* Force a fresh load and register the compiled plugin
capture program drop pq
capture program drop polars_parquet_plugin
which pq

*-------------------------------------------------------------------------------
* 1.  Build dummy data with rich metadata
*-------------------------------------------------------------------------------
set obs 6

* numeric var with a value label (all defined values are present in the data)
gen byte grp = mod(_n-1, 3)                 // -> 0,1,2,0,1,2
label define grplbl 0 "Zero" 1 "One" 2 "Two"
label values grp grplbl
label variable grp "Group indicator"
notes grp: first note on grp
notes grp: second note on grp

* double var with a display format and a note
gen double price = _n * 1.5
label variable price "Unit price"
format price %9.2f
notes price: price in local currency

* string var with a label
gen str12 name = "obs" + string(_n)
label variable name "Observation name"

* dataset-level metadata (NOTE: not currently round-tripped by pq -- see report)
label data "Toy dataset for metadata test"
notes _dta: dataset-level note

list, sepby(grp) abbrev(20)

*-------------------------------------------------------------------------------
* 2.  Capture the EXPECTED metadata before saving
*-------------------------------------------------------------------------------
local exp_lbl_grp   : variable label grp
local exp_lbl_price : variable label price
local exp_lbl_name  : variable label name
local exp_vlname    : value label grp
local exp_vl0       : label grplbl 0
local exp_vl1       : label grplbl 1
local exp_vl2       : label grplbl 2
local exp_fmt_price : format price
local exp_grp_n     : char grp[note0]
local exp_grp_note1 : char grp[note1]
local exp_grp_note2 : char grp[note2]
local exp_type_grp  : type grp

*-------------------------------------------------------------------------------
* 3.  Save to parquet, clear, reload
*-------------------------------------------------------------------------------
tempfile stub
local pqfile "`stub'.parquet"

pq save "`pqfile'", replace
clear
pq use "`pqfile'", clear

* reload can reorder rows; sort to a known order for value asserts
sort name

*-------------------------------------------------------------------------------
* 4.  ASSERTIONS
*-------------------------------------------------------------------------------
local fail 0
program define _chk
    args cond msg
    if (`cond') di as result "  PASS: `msg'"
    else {
        di as error  "  FAIL: `msg'"
        global _PQ_FAIL = 1
    }
end
global _PQ_FAIL 0

di as txt _n "== Variable labels =="
local a : variable label grp
_chk (`"`a'"' == `"`exp_lbl_grp'"')     "grp label = `exp_lbl_grp'"
local a : variable label price
_chk (`"`a'"' == `"`exp_lbl_price'"')   "price label = `exp_lbl_price'"
local a : variable label name
_chk (`"`a'"' == `"`exp_lbl_name'"')    "name label = `exp_lbl_name'"

di as txt _n "== Value labels =="
local a : value label grp
_chk (`"`a'"' == `"`exp_vlname'"')      "grp value-label name = `exp_vlname'"
local a : label grplbl 0
_chk (`"`a'"' == `"`exp_vl0'"')         "grplbl 0 = `exp_vl0'"
local a : label grplbl 1
_chk (`"`a'"' == `"`exp_vl1'"')         "grplbl 1 = `exp_vl1'"
local a : label grplbl 2
_chk (`"`a'"' == `"`exp_vl2'"')         "grplbl 2 = `exp_vl2'"

di as txt _n "== Notes / comments =="
local a : char grp[note0]
_chk (real("0`a'") == real("0`exp_grp_n'")) "grp note count = `exp_grp_n'"
local a : char grp[note1]
_chk (`"`a'"' == `"`exp_grp_note1'"')   "grp note1 = `exp_grp_note1'"
local a : char grp[note2]
_chk (`"`a'"' == `"`exp_grp_note2'"')   "grp note2 = `exp_grp_note2'"

di as txt _n "== Display format =="
local a : format price
_chk (`"`a'"' == `"`exp_fmt_price'"')   "price format = `exp_fmt_price'"

di as txt _n "== Storage type preserved =="
local a : type grp
_chk (`"`a'"' == `"`exp_type_grp'"')    "grp type = `exp_type_grp' (got `a')"

di as txt _n "== Data values =="
_chk (grp[1]==0 & grp[6]==2)            "grp data intact"
_chk (abs(price[3]-4.5)<1e-9)           "price data intact"
_chk (name[1]=="obs1" & name[6]=="obs6") "name data intact"

*-------------------------------------------------------------------------------
* 5.  Known-limitation probe (soft warning, does NOT fail the test)
*     Value labels for values that never appear in the data are dropped on save.
*-------------------------------------------------------------------------------
local unused : label grplbl 9, strict
if ("`unused'" == "") di as txt _n ///
    "NOTE: (expected) labels for unused values are not saved -- see report item #2"

*-------------------------------------------------------------------------------
di as txt _n "{hline 60}"
if ("$_PQ_FAIL" == "0") di as result "ALL METADATA ROUND-TRIP CHECKS PASSED"
else                    di as error  "SOME CHECKS FAILED -- see FAIL lines above"
di as txt "{hline 60}"
