"""
recompute_qlike.py — fix bugged QLIKE values in existing experiment results
without rerunning anything.

Reads every *_results.csv under results/experiments/, recomputes QLIKE from
stored y_true / y_pred, overwrites the qlike column, and re-saves.
"""

import ast
import glob
import os

import numpy as np
import pandas as pd


def correct_qlike(y_log_true, y_log_pred):
    """Patton QLIKE in log-RV space (numerically stable)."""
    delta = np.asarray(y_log_true) - np.asarray(y_log_pred)
    per_obs = np.exp(delta) - delta - 1
    return float(np.mean(per_obs))


def main():
    paths = glob.glob("results/experiments/*/*/*_results.csv")
    paths = [p for p in paths if "all_results" not in os.path.basename(p)]

    print(f"Found {len(paths)} experiment result files")
    fixed = 0

    for path in paths:
        df = pd.read_csv(path)
        if "y_true" not in df.columns or "y_pred" not in df.columns:
            continue
        new_q = []
        for _, row in df.iterrows():
            try:
                y_true = ast.literal_eval(row["y_true"])
                y_pred = ast.literal_eval(row["y_pred"])
                new_q.append(correct_qlike(y_true, y_pred))
            except Exception:
                new_q.append(np.nan)
        df["qlike"] = np.round(new_q, 6)
        df.to_csv(path, index=False)
        fixed += 1

    # Rebuild the combined file
    combined_paths = [
        p for p in glob.glob("results/experiments/*/*/*_results.csv")
        if "all_results" not in os.path.basename(p)
    ]
    combined = pd.concat([
        pd.read_csv(p, parse_dates=["date_start", "date_end"])
        for p in combined_paths
    ], ignore_index=True)
    combined.to_csv("results/experiments/all_results.csv", index=False)
    print(f"Fixed {fixed} files, rebuilt all_results.csv")


if __name__ == "__main__":
    main()