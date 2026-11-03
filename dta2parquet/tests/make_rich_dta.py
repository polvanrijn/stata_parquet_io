"""A .dta with rich, awkward metadata, written by pyreadstat (ReadStat) so
that neither the writer nor the reference reader is dta2parquet's code.

Note: pyreadstat's writer drops integer codes from a value label that also
labels extended missing values (it writes only .a/.z for `tagged` below);
the raw file confirms it, so the converted envelope rightly has only those.
"""

import numpy as np
import pandas as pd
import pyreadstat


def make_rich_dta(out, n=5000):
    rng = np.random.default_rng(1)
    df = pd.DataFrame({
        "region": rng.integers(1, 5, n).astype(float),
        "status": rng.choice([-9, -1, 0, 1, 1_000_000_000], n).astype(float),
        "agree1": rng.integers(1, 6, n).astype(float),
        "agree2": rng.integers(1, 6, n).astype(float),
        "income": rng.lognormal(10, 1, n),
        "share": rng.random(n),
        "visit": rng.integers(0, 20000, n).astype(float),     # %td
        "stamp": rng.integers(0, 10**12, n).astype(float),     # %tc
        "month": rng.integers(0, 800, n).astype(float),        # %tm
        "clock": rng.integers(0, 86_400_000, n).astype(float), # %tcHH:MM:SS
        "name": rng.choice(["Ünïcödé ☃", 'quote " and \\ backslash', "tab\there", "", "plain"], n),
        "tagged": rng.integers(1, 4, n).astype(float),
    })
    df.loc[::7, "income"] = np.nan
    df.loc[::11, "visit"] = np.nan
    # extended missing values in 'tagged' (.a / .z)
    df["tagged"] = df["tagged"].astype(object)
    df.loc[::5, "tagged"] = "a"
    df.loc[::13, "tagged"] = "z"
    long_label = ("Ä long label with ümlauts and a \"quote\" " * 10)[:80]
    pyreadstat.write_dta(
        df, out, version=15,
        file_label='Rich metadata test: "quotes", ümlauts \\ backslash',
        column_labels={
            "region": "Region of residence", "status": "Employment status (negative codes)",
            "agree1": "I like Parquet", "agree2": "I like Stata", "income": long_label,
            "share": "", "visit": "Date of visit", "stamp": "Timestamp", "month": "Month",
            "clock": "Time of day", "name": "Name ☃", "tagged": "Has tagged missings",
        },
        variable_value_labels={
            "region": {1: "North", 2: "East", 3: "South", 4: "West"},
            "status": {-9: "Refused", -1: "Don't know", 0: "Unemployed", 1: "Employed",
                       1_000_000_000: "Huge code"},
            "agree1": {1: "Strongly disagree", 2: "Disagree", 3: "Neutral", 4: "Agree", 5: "Strongly agree"},
            "agree2": {1: "Strongly disagree", 2: "Disagree", 3: "Neutral", 4: "Agree", 5: "Strongly agree"},
            "tagged": {1: "one", 2: "two", 3: "three", "a": "not asked", "z": "skipped"},
        },
        missing_user_values={"tagged": ["a", "z"]},
        variable_format={"income": "%12.2fc", "share": "%6.3f", "visit": "%td", "stamp": "%tc",
                         "month": "%tm", "clock": "%tcHH:MM:SS", "name": "%-20s", "region": "%8.0g"},
    )
