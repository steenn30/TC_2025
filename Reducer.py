#!/usr/bin/env python3
import sys
import numpy as np
from collections import defaultdict

def main():
    sums = defaultdict(float)
    counts = defaultdict(int)

    for line in sys.stdin:
        line = line.strip()
        if not line or "\t" not in line:
            continue
        key, val = line.split("\t", 1)
        try:
            s_txt, c_txt = val.split(",", 1)
            s = float(s_txt)
            c = int(c_txt)
        except ValueError:
            continue

        # aggregate
        sums[key] += s
        counts[key] += c

    # Output averages; sorted buckets look nicer
    for key in sorted(sums.keys(), key=lambda k: int(k.split("-", 1)[0])):
        avg = np.divide(sums[key], counts[key])  # NumPy division
        print(f"{key}\t{avg}")

if __name__ == "__main__":
    main()
