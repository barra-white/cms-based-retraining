"""
analysis.py — Post-experiment analysis.

Computes:
  1. Per-strategy mean F1 and directional accuracy
  2. Detection latency: windows between a stress event and first retrain after it
  3. False positive rate: retrains outside known stress windows
  4. Per-class F1: reveals class imbalance from stale bin edges

Input:  results/experiments/all_results.csv
Output: results/analysis/  (4 CSVs)
"""

import ast
import os
import numpy as np
import pandas as pd
from sklearn.metrics import f1_score

STRESS_EVENTS = {
    'covid_crash':    pd.Timestamp('2020-02-20'),
    'fed_hikes_2022': pd.Timestamp('2022-03-16'),
}
STRESS_WINDOW_DAYS = 63   # ±3 months defines a "true positive" retrain window

def load_results(path="results/experiments/all_results.csv"):
    df = pd.read_csv(path, parse_dates=['date_start', 'date_end'])
    df['y_true'] = df['y_true'].apply(ast.literal_eval)
    df['y_pred'] = df['y_pred'].apply(ast.literal_eval)
    return df

def compute_per_class_f1(df):
    """
    Aggregate y_true/y_pred across all windows per (model, retrainer),
    then compute per-class F1. Low F1 on any single class indicates the
    class imbalance that was caused by stale bin edges in the original code.
    """
    records = []
    for (model, retrainer), group in df.groupby(['model_type', 'retrainer']):
        y_true_all, y_pred_all = [], []
        for _, row in group.iterrows():
            y_true_all.extend(row['y_true'])
            y_pred_all.extend(row['y_pred'])
        for cls_idx, score in enumerate(
            f1_score(y_true_all, y_pred_all, average=None, zero_division=0)
        ):
            records.append({'model_type': model, 'retrainer': retrainer,
                            'class': cls_idx, 'f1': round(score, 4)})
    return pd.DataFrame(records)

def compute_detection_latency(df):
    """
    For each (model, retrainer, stress_event): first retrain after event date.
    Latency in calendar days and windows. NaN = never retrained after event.
    Note: event dates are proxies for true structural break points.
    """
    records = []
    retrains = df[df['retrain_triggered'] == True].copy()
    for (model, retrainer), group in retrains.groupby(['model_type', 'retrainer']):
        for event_name, event_date in STRESS_EVENTS.items():
            after = group[group['date_start'] >= event_date]
            if after.empty:
                records.append({'model_type': model, 'retrainer': retrainer,
                                'event': event_name, 'event_date': event_date.date(),
                                'first_retrain': pd.NaT,
                                'latency_days': np.nan, 'latency_windows': np.nan})
            else:
                first = after.iloc[0]
                lat_days = (first['date_start'] - event_date).days
                records.append({'model_type': model, 'retrainer': retrainer,
                                'event': event_name, 'event_date': event_date.date(),
                                'first_retrain': first['date_start'],
                                'latency_days': lat_days,
                                'latency_windows': round(lat_days / 21, 1)})
    return pd.DataFrame(records)

def compute_false_positive_rate(df):
    """
    False positive = retrain outside all stress windows.
    Stress window = [event_date ± STRESS_WINDOW_DAYS] for each known event.
    """
    def in_any_window(date):
        return any(
            (event_date - pd.Timedelta(days=STRESS_WINDOW_DAYS)) <= date <=
            (event_date + pd.Timedelta(days=STRESS_WINDOW_DAYS))
            for event_date in STRESS_EVENTS.values()
        )

    retrains = df[df['retrain_triggered'] == True].copy()
    retrains['in_stress_window'] = retrains['date_start'].apply(in_any_window)
    records = []
    for (model, retrainer), group in retrains.groupby(['model_type', 'retrainer']):
        total = len(group)
        tp    = group['in_stress_window'].sum()
        fp    = total - tp
        records.append({'model_type': model, 'retrainer': retrainer,
                        'total_retrains': total, 'true_positives': tp,
                        'false_positives': fp,
                        'false_positive_rate': round(fp / total, 4) if total else np.nan})
    return pd.DataFrame(records)

def compute_overall_summary(df):
    return (
        df.groupby(['model_type', 'retrainer'])
        .agg(mean_f1=('f1','mean'), std_f1=('f1','std'),
             mean_dir_accuracy=('directional_acc','mean'),
             retrains=('retrain_triggered','sum'), windows=('f1','count'))
        .round(4)
        .sort_values(['model_type','mean_f1'], ascending=[True, False])
        .reset_index()
    )

def main():
    os.makedirs("results/analysis", exist_ok=True)
    df = load_results()
    print(f"Loaded {len(df)} rows | "
          f"{df['model_type'].nunique()} models | "
          f"{df['retrainer'].nunique()} retrainers")

    summary = compute_overall_summary(df)
    summary.to_csv("results/analysis/overall_summary.csv", index=False)
    print(summary.to_string(index=False))

    per_class = compute_per_class_f1(df)
    per_class.to_csv("results/analysis/per_class_f1.csv", index=False)

    latency = compute_detection_latency(df)
    latency.to_csv("results/analysis/detection_latency.csv", index=False)
    print(latency.to_string(index=False))

    fpr = compute_false_positive_rate(df)
    fpr.to_csv("results/analysis/false_positive_rate.csv", index=False)
    print(fpr.sort_values('false_positive_rate').to_string(index=False))

if __name__ == "__main__":
    main()