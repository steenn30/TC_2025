#!/usr/bin/env python3
import sys, io
import pandas as pd
import numpy as np

def main():
    raw = sys.stdin.read()
    if not raw.strip():
        return

    # Read TSV or CSV (detect via regex separator)
    df = pd.read_csv(io.StringIO(raw), sep=r'\s*,\s*|\t+', engine='python')

    # Expected columns (case-insensitive match)
    cols = {c.lower(): c for c in df.columns}
    try:
        age_col = cols.get('age')
        spend_col = cols.get('spending score (1-100)')
        if age_col is None or spend_col is None:
            # Try a friendlier error for unexpected headers
            raise KeyError(f"Missing Age or Spending Score columns. Got: {list(df.columns)}")
    except Exception as e:
        # Fail quietly in mapper to avoid killing tasks; optionally print to stderr
        print(f"Mapper header error: {e}", file=sys.stderr)
        return

    # Keep age >= 18
    df = df[pd.to_numeric(df[age_col], errors='coerce').notna()]
    df = df[df[age_col].astype(float) >= 18]

    if df.empty:
        return

    ages = df[age_col].astype(int).to_numpy()
    scores = pd.to_numeric(df[spend_col], errors='coerce').fillna(0.0).to_numpy()

    # Bucket: 18–28, 29–39, 40–49, ...
    starts = 18 + ((ages - 18) // 11) * 11
    ends = starts + 10
    buckets = np.char.add(starts.astype(str), np.char.add("-", ends.astype(str)))

    # Emit key-value: bucket \t sum,count (we'll use reducer as combiner-safe aggregator)
    for b, s in zip(buckets, scores):
        # Use NumPy formatting to ensure consistent numeric text
        print(f"{b}\t{np.format_float_positional(s, trim='-')},1")

if __name__ == "__main__":
    main()
