"""
analysis.py — Post-experiment analysis.

Computes:
1. Per-strategy mean F1 and directional accuracy (overall_summary.csv)
2. Detection latency: windows between a stress event and first retrain after it (detection_latency.csv)
3. False positive rate: retrains outside known stress windows (false_positive_rate.csv)
4. Per-class F1: reveals class imbalance from stale bin edges (per_class_f1.csv)
5. Best configs per experiment type: top-k by F1 for each (model_type, exp_type) (best_configs.csv)
6. Retrain efficiency: F1 gain per retrain event — cost/benefit ratio (retrain_efficiency.csv)

Input:  results/experiments/all_results.csv
Output: results/analysis/ (6 CSVs)

Bug fixes vs original:
  - STRESS_EVENTS dict was missing closing brace (SyntaxError)
  - compute_overall_summary() was missing closing parenthesis (SyntaxError)
  - in_any_window() helper was missing closing parenthesis (SyntaxError)
  - retrain_triggered is a bool column — use .astype(bool) for safe indexing
  - analysis now reads from the correct nested path via glob fallback if all_results.csv is absent
  - added get_experiment_type() to tag rows for group-level analysis
  - added best_configs and retrain_efficiency outputs
"""

import ast
import os
import glob
import numpy as np
import pandas as pd
from sklearn.metrics import f1_score

# ---------------------------------------------------------------------------
# STRESS EVENTS — known structural break-points
# ---------------------------------------------------------------------------
STRESS_EVENTS = {
    'covid_crash':    pd.Timestamp('2020-02-20'),
    'fed_hikes_2022': pd.Timestamp('2022-03-16'),
}  # ← closing brace was missing in original, causing SyntaxError

STRESS_WINDOW_DAYS = 63  # ±3 months defines a "true positive" retrain window

# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def get_experiment_type(name: str) -> str:
    """Map retrainer name string to a short experiment-type label."""
    for prefix in ['spy_msm', 'timeout_msm', 'msm', 'causal', 'fixed', 'perf', 'adwin', 'random', 'static']:
        if name.startswith(prefix):
            return prefix
    return 'other'


def load_results(path: str = "results/experiments/all_results.csv") -> pd.DataFrame:
    """
    Load the combined results CSV.  Falls back to globbing individual per-model
    CSVs if the aggregate file is missing (e.g. experiment crashed before the
    final concat).
    """
    if os.path.exists(path):
        df = pd.read_csv(path, parse_dates=['date_start', 'date_end'])
    else:
        # Fallback: stitch together every per-experiment CSV
        parts = glob.glob("results/experiments/*/*_results.csv")
        if not parts:
            raise FileNotFoundError(
                f"No results found at '{path}' and no per-experiment CSVs under "
                "results/experiments/. Run experiment.py first."
            )
        df = pd.concat([pd.read_csv(p, parse_dates=['date_start', 'date_end'])
                        for p in parts], ignore_index=True)

    # Parse stored JSON arrays — only convert if still a string
    for col in ('y_true', 'y_pred'):
        if df[col].dtype == object:
            df[col] = df[col].apply(ast.literal_eval)

    # Tag experiment type for group-level analyses
    df['exp_type'] = df['retrainer'].apply(get_experiment_type)

    return df


# ---------------------------------------------------------------------------
# 1. OVERALL SUMMARY
# ---------------------------------------------------------------------------

def compute_overall_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Per-(model_type, retrainer) mean F1, directional accuracy, retrain count."""
    return (
        df.groupby(['model_type', 'retrainer', 'exp_type'])
        .agg(
            mean_f1            = ('f1', 'mean'),
            std_f1             = ('f1', 'std'),
            mean_dir_accuracy  = ('directional_acc', 'mean'),
            retrains           = ('retrain_triggered', 'sum'),
            windows            = ('f1', 'count'),
        )
        .round(4)
        .sort_values(['model_type', 'mean_f1'], ascending=[True, False])
        .reset_index()
    )  # ← closing paren was missing in original, causing SyntaxError


# ---------------------------------------------------------------------------
# 2. PER-CLASS F1
# ---------------------------------------------------------------------------

def compute_per_class_f1(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate y_true/y_pred across all windows per (model_type, retrainer),
    then compute per-class F1.  Low F1 on any single class indicates the
    class imbalance caused by stale bin edges.
    """
    records = []
    for (model, retrainer), group in df.groupby(['model_type', 'retrainer']):
        y_true_all, y_pred_all = [], []
        for _, row in group.iterrows():
            y_true_all.extend(row['y_true'])
            y_pred_all.extend(row['y_pred'])
        scores = f1_score(y_true_all, y_pred_all, average=None, zero_division=0)
        for cls_idx, score in enumerate(scores):
            records.append({
                'model_type': model,
                'retrainer':  retrainer,
                'class':      cls_idx,
                'f1':         round(float(score), 4),
            })
    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# 3. DETECTION LATENCY
# ---------------------------------------------------------------------------

def compute_detection_latency(df: pd.DataFrame) -> pd.DataFrame:
    """
    For each (model_type, retrainer, stress_event): first retrain on or after
    event date.  Latency reported in calendar days and rolling windows.
    NaN = never retrained after the event.

    Bug fix: original used df[df['retrain_triggered'] == True]; bool column
    supports direct boolean indexing — using .astype(bool) here for safety.
    """
    records = []
    retrains = df[df['retrain_triggered'].astype(bool)].copy()

    for (model, retrainer), group in retrains.groupby(['model_type', 'retrainer']):
        for event_name, event_date in STRESS_EVENTS.items():
            after = group[group['date_start'] >= event_date]
            if after.empty:
                records.append({
                    'model_type':      model,
                    'retrainer':       retrainer,
                    'event':           event_name,
                    'event_date':      event_date.date(),
                    'first_retrain':   pd.NaT,
                    'latency_days':    np.nan,
                    'latency_windows': np.nan,
                })
            else:
                first      = after.iloc[0]
                lat_days   = (first['date_start'] - event_date).days
                records.append({
                    'model_type':      model,
                    'retrainer':       retrainer,
                    'event':           event_name,
                    'event_date':      event_date.date(),
                    'first_retrain':   first['date_start'].date(),
                    'latency_days':    lat_days,
                    'latency_windows': round(lat_days / 21, 1),
                })
    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# 4. FALSE POSITIVE RATE
# ---------------------------------------------------------------------------

def compute_false_positive_rate(df: pd.DataFrame) -> pd.DataFrame:
    """
    False positive = retrain triggered outside all stress windows.
    Stress window = [event_date ± STRESS_WINDOW_DAYS] for each known event.

    Bug fix: original in_any_window() was missing a closing parenthesis on the
    generator expression passed to any(), causing a SyntaxError.
    """
    def in_any_window(date: pd.Timestamp) -> bool:
        return any(
            (event_date - pd.Timedelta(days=STRESS_WINDOW_DAYS))
            <= date
            <= (event_date + pd.Timedelta(days=STRESS_WINDOW_DAYS))
            for event_date in STRESS_EVENTS.values()
        )  # ← closing paren was missing in original, causing SyntaxError

    retrains = df[df['retrain_triggered'].astype(bool)].copy()
    retrains['in_stress_window'] = retrains['date_start'].apply(in_any_window)

    records = []
    for (model, retrainer), group in retrains.groupby(['model_type', 'retrainer']):
        total = len(group)
        tp    = int(group['in_stress_window'].sum())
        fp    = total - tp
        records.append({
            'model_type':          model,
            'retrainer':           retrainer,
            'total_retrains':      total,
            'true_positives':      tp,
            'false_positives':     fp,
            'false_positive_rate': round(fp / total, 4) if total else np.nan,
        })
    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# 5. BEST CONFIGS PER EXPERIMENT TYPE  (new)
# ---------------------------------------------------------------------------

def compute_best_configs(df: pd.DataFrame, top_k: int = 3) -> pd.DataFrame:
    """
    For each (model_type, exp_type) group, return the top_k retrainer
    configurations ranked by mean F1.  Useful for picking the best
    hyper-parameter combination from the sensitivity sweep.
    """
    summary = (
        df.groupby(['model_type', 'exp_type', 'retrainer'])
        .agg(
            mean_f1           = ('f1', 'mean'),
            std_f1            = ('f1', 'std'),
            mean_dir_accuracy = ('directional_acc', 'mean'),
            retrains          = ('retrain_triggered', 'sum'),
            windows           = ('f1', 'count'),
        )
        .round(4)
        .reset_index()
    )
    summary['rank'] = (
        summary
        .groupby(['model_type', 'exp_type'])['mean_f1']
        .rank(ascending=False, method='first')
        .astype(int)
    )
    return (
        summary[summary['rank'] <= top_k]
        .sort_values(['model_type', 'exp_type', 'rank'])
        .reset_index(drop=True)
    )


# ---------------------------------------------------------------------------
# 6. RETRAIN EFFICIENCY  (new)
# ---------------------------------------------------------------------------

def compute_retrain_efficiency(df: pd.DataFrame, lookback: int = 3) -> pd.DataFrame:
    """
    For each retrain event, compute the F1 improvement over the preceding
    `lookback` windows.  Positive delta = the retrain helped; negative = hurt.

    Returns one row per retrain event with:
      - pre_retrain_f1   : mean F1 over the `lookback` windows before the retrain
      - post_retrain_f1  : mean F1 over the `lookback` windows after the retrain
      - f1_delta         : post - pre
    """
    records = []
    for (model, retrainer), group in df.groupby(['model_type', 'retrainer']):
        group = group.sort_values('window').reset_index(drop=True)
        retrain_idxs = group.index[group['retrain_triggered'].astype(bool)].tolist()
        for idx in retrain_idxs:
            pre_slice  = group.loc[max(0, idx - lookback): idx - 1, 'f1']
            post_slice = group.loc[idx + 1: idx + lookback, 'f1']
            if pre_slice.empty or post_slice.empty:
                continue
            pre_f1  = pre_slice.mean()
            post_f1 = post_slice.mean()
            records.append({
                'model_type':      model,
                'retrainer':       retrainer,
                'window':          group.loc[idx, 'window'],
                'date_start':      group.loc[idx, 'date_start'],
                'pre_retrain_f1':  round(pre_f1,  4),
                'post_retrain_f1': round(post_f1, 4),
                'f1_delta':        round(post_f1 - pre_f1, 4),
            })
    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    os.makedirs("results/analysis", exist_ok=True)

    df = load_results()
    print(
        f"Loaded {len(df):,} rows | "
        f"{df['model_type'].nunique()} models | "
        f"{df['retrainer'].nunique()} retrainers | "
        f"{df['exp_type'].nunique()} experiment types"
    )

    # 1. Overall summary
    summary = compute_overall_summary(df)
    summary.to_csv("results/analysis/overall_summary.csv", index=False)
    print("\n=== Overall Summary (top 15 by model/F1) ===")
    print(summary.head(15).to_string(index=False))

    # 2. Per-class F1
    per_class = compute_per_class_f1(df)
    per_class.to_csv("results/analysis/per_class_f1.csv", index=False)
    print(f"\nPer-class F1 saved ({len(per_class)} rows).")

    # 3. Detection latency
    latency = compute_detection_latency(df)
    latency.to_csv("results/analysis/detection_latency.csv", index=False)
    print("\n=== Detection Latency ===")
    print(latency.to_string(index=False))

    # 4. False positive rate
    fpr = compute_false_positive_rate(df)
    fpr.to_csv("results/analysis/false_positive_rate.csv", index=False)
    print("\n=== False Positive Rate (sorted) ===")
    print(fpr.sort_values('false_positive_rate').head(20).to_string(index=False))

    # 5. Best configs per experiment type
    best = compute_best_configs(df, top_k=3)
    best.to_csv("results/analysis/best_configs.csv", index=False)
    print(f"\nBest configs saved ({len(best)} rows).")

    # 6. Retrain efficiency
    efficiency = compute_retrain_efficiency(df, lookback=3)
    efficiency.to_csv("results/analysis/retrain_efficiency.csv", index=False)
    mean_delta = efficiency.groupby(['model_type', 'retrainer'])['f1_delta'].mean().round(4)
    print("\n=== Mean F1 Delta per Retrain (top gainers) ===")
    print(mean_delta.sort_values(ascending=False).head(10).to_string())

    print("\nAll outputs written to results/analysis/")


if __name__ == "__main__":
    main()
