'''
analysis.py — Post-experiment analysis pipeline.

Run after experiment.py completes. Produces CSVs to results/analysis/.

Usage:
    cd src/retraining
    python analysis.py
'''

import ast
import glob
import json
import os
import re

import numpy as np
import pandas as pd
from scipy.stats import friedmanchisquare
from sklearn.metrics import f1_score
import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config as cfg

STRESS_EVENTS          = cfg.STRESS_EVENTS
STRESS_WINDOW_DAYS     = cfg.STRESS_WINDOW_DAYS
EXP_TYPE_PREFIXES      = cfg.EXP_TYPE_PREFIXES
MSM_TYPES              = cfg.MSM_TYPES
BASELINE_TYPES         = cfg.BASELINE_TYPES
SIGNAL_DRIVEN          = cfg.SIGNAL_DRIVEN
get_experiment_type    = cfg.get_experiment_type
in_stress_window       = cfg.in_stress_window

ROBUSTNESS_WINDOW_DAYS = [45, 60, 90, 120]  # local to analysis.py, keep here


# ── LOADING ──

def load_results(path='results/experiments/all_results.csv'):
    partial = os.path.join(os.path.dirname(path), 'all_results_partial.csv')

    if os.path.exists(path):
        df = pd.read_csv(path, parse_dates=['date_start', 'date_end'])
    elif os.path.exists(partial):
        print(f'Warning: using partial results from {partial}')
        df = pd.read_csv(partial, parse_dates=['date_start', 'date_end'])
    else:
        parts = glob.glob('results/experiments/*/*/*_results.csv')
        parts = [p for p in parts if os.path.basename(p) not in
                 ('all_results.csv', 'all_results_partial.csv')]
        if not parts:
            raise FileNotFoundError("No results found. Run experiment.py first.")
        df = pd.concat(
            [pd.read_csv(p, parse_dates=['date_start', 'date_end']) for p in parts],
            ignore_index=True,
        )

    # Parse stored JSON columns
    for col in ('y_true', 'y_pred'):
        if col in df.columns and df[col].dtype == object:
            df[col] = df[col].apply(ast.literal_eval)

    # Ensure boolean columns — CSV round-trip turns True/False into strings.
    # str.astype(bool) makes 'False' → True (non-empty string is truthy).
    # Must map explicitly.
    for col in ('retrain_triggered', 'signal_fired', 'cooldown_active'):
        if col in df.columns:
            df[col] = (
                df[col].astype(str).str.strip().str.lower()
                .map({'true': True, 'false': False, '1': True, '0': False,
                      '1.0': True, '0.0': False})
                .fillna(False)
            )

    df['exp_type'] = df['retrainer'].apply(get_experiment_type)
    return df


# ── 1. OVERALL SUMMARY ──

def overall_summary(df):
    return (
        df.groupby(['model_type', 'retrainer', 'exp_type'])
        .agg(
            mean_f1           = ('f1', 'mean'),
            std_f1            = ('f1', 'std'),
            median_f1         = ('f1', 'median'),
            mean_dir_acc      = ('directional_acc', 'mean'),
            retrains          = ('retrain_triggered', 'sum'),
            signals_fired     = ('signal_fired', 'sum'),
            cooldowns_hit     = ('cooldown_active', 'sum'),
            windows           = ('f1', 'count'),
        )
        .round(4)
        .sort_values(['model_type', 'mean_f1'], ascending=[True, False])
        .reset_index()
    )


# ── 2. PER-CLASS F1 ──

def per_class_f1(df):
    CLASS_NAMES = {0: 'down', 1: 'neutral', 2: 'up'}
    records = []
    for (model, retrainer), grp in df.groupby(['model_type', 'retrainer']):
        y_true = sum(grp['y_true'].tolist(), [])
        y_pred = sum(grp['y_pred'].tolist(), [])
        scores = f1_score(y_true, y_pred, average=None, zero_division=0)
        for cls, score in enumerate(scores):
            records.append({
                'model_type': model, 'retrainer': retrainer,
                'exp_type': get_experiment_type(retrainer),
                'class': cls, 'class_name': CLASS_NAMES.get(cls, str(cls)),
                'f1': round(float(score), 4),
            })
    return pd.DataFrame(records)


# ── 3. DETECTION LATENCY (signal-driven only) ──

def detection_latency(df):
    records = []
    sig_df   = df[df['exp_type'].isin(SIGNAL_DRIVEN)].copy()
    retrains = sig_df[sig_df['retrain_triggered']].copy()

    for (model, retrainer), grp in retrains.groupby(['model_type', 'retrainer']):
        exp = get_experiment_type(retrainer)
        for event_name, event_date in STRESS_EVENTS.items():
            after = grp[grp['date_start'] >= event_date]
            if after.empty:
                records.append({
                    'model_type': model, 'retrainer': retrainer, 'exp_type': exp,
                    'event': event_name, 'event_date': event_date.date(),
                    'first_retrain': pd.NaT, 'latency_days': np.nan,
                    'latency_windows': np.nan, 'detected': False,
                })
            else:
                first = after.iloc[0]
                lat   = (first['date_start'] - event_date).days
                records.append({
                    'model_type': model, 'retrainer': retrainer, 'exp_type': exp,
                    'event': event_name, 'event_date': event_date.date(),
                    'first_retrain': first['date_start'].date(),
                    'latency_days': lat, 'latency_windows': round(lat / 21, 1),
                    'detected': True,
                })
    return pd.DataFrame(records)


# ── 4. FALSE POSITIVE RATE ──

def false_positive_rate(df):
    retrains = df[df['retrain_triggered']].copy()
    if retrains.empty:
        return pd.DataFrame(columns=[
            'model_type', 'retrainer', 'exp_type', 'total_retrains',
            'true_positives', 'false_positives', 'fpr', 'precision'])
    retrains['in_stress'] = retrains['date_start'].apply(in_stress_window)
    records = []
    for (model, retrainer), grp in retrains.groupby(['model_type', 'retrainer']):
        total = len(grp)
        tp    = int(grp['in_stress'].sum())
        fp    = total - tp
        records.append({
            'model_type': model, 'retrainer': retrainer,
            'exp_type': get_experiment_type(retrainer),
            'total_retrains': total, 'true_positives': tp, 'false_positives': fp,
            'fpr': round(fp / total, 4) if total else np.nan,
            'precision': round(tp / total, 4) if total else np.nan,
        })
    return pd.DataFrame(records)


# ── 5. BEST CONFIGS ──

def best_configs(df, top_k=3):
    summary = (
        df.groupby(['model_type', 'exp_type', 'retrainer'])
        .agg(mean_f1=('f1', 'mean'), std_f1=('f1', 'std'),
             mean_dir_acc=('directional_acc', 'mean'),
             retrains=('retrain_triggered', 'sum'), windows=('f1', 'count'))
        .round(4).reset_index()
    )
    summary['rank'] = (
        summary.groupby(['model_type', 'exp_type'])['mean_f1']
        .rank(ascending=False, method='first').astype(int)
    )
    return (
        summary[summary['rank'] <= top_k]
        .sort_values(['model_type', 'exp_type', 'rank'])
        .reset_index(drop=True)
    )


# ── 6. RETRAIN EFFICIENCY ──

def retrain_efficiency(df, n_windows=3):
    records = []
    for (model, retrainer), grp in df.groupby(['model_type', 'retrainer']):
        grp = grp.sort_values('window').reset_index(drop=True)
        exp = get_experiment_type(retrainer)
        retrain_idxs = grp.index[grp['retrain_triggered']].tolist()
        gains = []
        for idx in retrain_idxs:
            pre  = grp.iloc[max(0, idx - n_windows):idx]['f1'].mean()
            post = grp.iloc[idx + 1: idx + 1 + n_windows]['f1'].mean()
            if not (np.isnan(pre) or np.isnan(post)):
                gains.append(post - pre)
        n_pos = sum(1 for g in gains if g > 0)
        records.append({
            'model_type': model, 'retrainer': retrainer, 'exp_type': exp,
            'n_retrains': len(retrain_idxs),
            'mean_f1_gain': round(np.mean(gains), 4) if gains else np.nan,
            'positive_retrains': n_pos,
            'negative_retrains': len(gains) - n_pos,
            'pct_positive': round(n_pos / len(gains), 3) if gains else np.nan,
        })
    return pd.DataFrame(records).sort_values(
        ['model_type', 'mean_f1_gain'], ascending=[True, False]
    ).reset_index(drop=True)


# ── 7. COOLDOWN ANALYSIS ──

def cooldown_analysis(df):
    records = []
    for (model, retrainer), grp in df.groupby(['model_type', 'retrainer']):
        signals    = int(grp['signal_fired'].sum())
        retrains   = int(grp['retrain_triggered'].sum())
        suppressed = signals - retrains
        records.append({
            'model_type': model, 'retrainer': retrainer,
            'exp_type': get_experiment_type(retrainer),
            'signals_fired': signals, 'retrains_triggered': retrains,
            'cooldown_suppressed': suppressed,
            'suppression_rate': round(suppressed / signals, 4) if signals else np.nan,
        })
    return pd.DataFrame(records)


# ── 8. STRESS PERIOD F1 ──

def stress_period_f1(df):
    data = df.copy()
    data['regime'] = data['date_start'].apply(
        lambda d: 'stress' if in_stress_window(d) else 'calm'
    )
    agg = (
        data.groupby(['model_type', 'retrainer', 'exp_type', 'regime'])
        .agg(mean_f1=('f1', 'mean'), std_f1=('f1', 'std'), n=('f1', 'count'))
        .round(4).reset_index()
    )
    pivot = agg.pivot_table(
        index=['model_type', 'retrainer', 'exp_type'],
        columns='regime', values=['mean_f1', 'n'],
    )
    pivot.columns = ['_'.join(c).strip() for c in pivot.columns]
    if 'mean_f1_stress' in pivot.columns and 'mean_f1_calm' in pivot.columns:
        pivot['f1_stress_minus_calm'] = (
            pivot['mean_f1_stress'] - pivot['mean_f1_calm']
        ).round(4)
    return (
        pivot.reset_index()
        .sort_values(['model_type', 'f1_stress_minus_calm'], ascending=[True, False])
    )


# ── 9. SENSITIVITY SUMMARY ──

def sensitivity_summary(df):
    sub = df[df['exp_type'].isin(MSM_TYPES)].copy()
    if sub.empty:
        return pd.DataFrame()

    def _parse(name):
        t1 = re.search(r'tau_1_([\d.]+)', name)
        t2 = re.search(r'tau_2_([\d.]+)', name)
        lb = re.search(r'lb_(\d+)', name)
        return {
            'tau_1':    float(t1.group(1)) if t1 else np.nan,
            'tau_2':    float(t2.group(1)) if t2 else np.nan,
            'lookback': int(lb.group(1))   if lb else np.nan,
        }

    params = sub['retrainer'].apply(_parse).apply(pd.Series)
    sub = pd.concat([sub.reset_index(drop=True), params], axis=1)

    return (
        sub.groupby(['model_type', 'exp_type', 'tau_1', 'tau_2', 'lookback'])
        .agg(mean_f1=('f1', 'mean'), std_f1=('f1', 'std'),
             mean_dir_acc=('directional_acc', 'mean'),
             retrains=('retrain_triggered', 'sum'))
        .round(4)
        .sort_values(['model_type', 'exp_type', 'mean_f1'], ascending=[True, True, False])
        .reset_index()
    )


# ── 10. CAUSAL FEATURE USAGE ──

def causal_feature_usage(df):
    causal = df[(df['exp_type'] == 'causal') & df['active_features'].notna()].copy()
    if causal.empty:
        return pd.DataFrame()

    records = []
    for model, grp in causal.groupby('model_type'):
        total = len(grp)
        counts = {}
        for _, row in grp.iterrows():
            try:
                feats = json.loads(row['active_features']) if isinstance(row['active_features'], str) else row['active_features']
                for f in feats:
                    counts[f] = counts.get(f, 0) + 1
            except (json.JSONDecodeError, TypeError):
                continue
        for feat, count in counts.items():
            records.append({
                'model_type': model, 'feature': feat,
                'selection_count': count,
                'selection_rate': round(count / total, 4),
                'total_windows': total,
            })

    return pd.DataFrame(records).sort_values(
        ['model_type', 'selection_count'], ascending=[True, False]
    ).reset_index(drop=True)


# ── 11. FRIEDMAN RANKS ──

def friedman_ranks(df):
    rank_records = []
    test_records = []

    for model, grp in df.groupby('model_type'):
        pivot = grp.pivot_table(index='date_start', columns='retrainer', values='f1')
        n_before = len(pivot)
        pivot = pivot.dropna()
        if n_before != len(pivot):
            print(f'  [friedman/{model}] dropped {n_before - len(pivot)} incomplete windows')

        if pivot.empty or pivot.shape[1] < 2:
            continue

        ranked = pivot.rank(axis=1, ascending=False, method='average')
        for retrainer, mean_rank in ranked.mean().sort_values().items():
            rank_records.append({
                'model_type': model, 'retrainer': retrainer,
                'exp_type': get_experiment_type(retrainer),
                'mean_rank': round(mean_rank, 4), 'n_windows': len(pivot),
            })

        if pivot.shape[1] >= 3:
            arrays = [pivot[col].values for col in pivot.columns]
            stat, p = friedmanchisquare(*arrays)
            test_records.append({
                'model_type': model, 'n_retrainers': len(arrays),
                'n_windows': len(pivot),
                'chi2_stat': round(stat, 4), 'p_value': round(p, 6),
                'significant': p < 0.05,
            })

    ranks_df = pd.DataFrame(rank_records).sort_values(['model_type', 'mean_rank']).reset_index(drop=True)
    test_df  = pd.DataFrame(test_records)
    return ranks_df, test_df


# ── 12. REGIME RETRAIN RATE ──

def regime_retrain_rate(df):
    data = df.copy()
    data['regime'] = data['date_start'].apply(
        lambda d: 'stress' if in_stress_window(d) else 'calm'
    )
    rate = (
        data.groupby(['model_type', 'retrainer', 'exp_type', 'regime'])
        .agg(retrains=('retrain_triggered', 'sum'), windows=('retrain_triggered', 'count'))
        .reset_index()
    )
    rate['retrain_rate'] = (rate['retrains'] / rate['windows']).round(4)

    pivot = rate.pivot_table(
        index=['model_type', 'retrainer', 'exp_type'],
        columns='regime', values='retrain_rate',
    ).reset_index()
    pivot.columns.name = None

    if 'stress' in pivot.columns:
        pivot = pivot.rename(columns={'stress': 'rate_stress'})
    if 'calm' in pivot.columns:
        pivot = pivot.rename(columns={'calm': 'rate_calm'})

    if 'rate_stress' in pivot.columns and 'rate_calm' in pivot.columns:
        pivot['stress_calm_ratio'] = (
            pivot['rate_stress'] / pivot['rate_calm'].replace(0, np.nan)
        ).round(4)
        pivot['likely_inverted'] = pivot['stress_calm_ratio'] < 1.0

    return pivot.sort_values(
        ['model_type', 'stress_calm_ratio'] if 'stress_calm_ratio' in pivot.columns else ['model_type'],
        ascending=[True, False] if 'stress_calm_ratio' in pivot.columns else [True],
    ).reset_index(drop=True)


# ── 13. STRESS WINDOW ROBUSTNESS ──

def stress_window_robustness(df):
    records = []
    for window_days in ROBUSTNESS_WINDOW_DAYS:
        def _in(date, wd=window_days):
            return any(
                (ev - pd.Timedelta(days=wd)) <= date <= (ev + pd.Timedelta(days=wd))
                for ev in STRESS_EVENTS.values()
            )

        data = df.copy()
        data['regime'] = data['date_start'].apply(lambda d: 'stress' if _in(d) else 'calm')

        pivot = (
            data.groupby(['model_type', 'retrainer', 'exp_type', 'regime'])
            .agg(mean_f1=('f1', 'mean')).reset_index()
            .pivot_table(index=['model_type', 'retrainer', 'exp_type'],
                         columns='regime', values='mean_f1').reset_index()
        )
        pivot.columns.name = None
        if 'stress' in pivot.columns and 'calm' in pivot.columns:
            pivot['f1_stress_minus_calm'] = (pivot['stress'] - pivot['calm']).round(4)
        pivot['stress_window_days'] = window_days
        records.append(pivot)

    full = pd.concat(records, ignore_index=True)

    summary_rows = []
    for (model, exp), grp in full.groupby(['model_type', 'exp_type']):
        if exp not in MSM_TYPES:
            continue
        for wd in ROBUSTNESS_WINDOW_DAYS:
            msm_row = grp[grp['stress_window_days'] == wd]
            static_rows = full[
                (full['model_type'] == model) & (full['exp_type'] == 'static')
                & (full['stress_window_days'] == wd)
            ]
            if msm_row.empty or static_rows.empty:
                continue
            if 'f1_stress_minus_calm' not in msm_row.columns or 'f1_stress_minus_calm' not in static_rows.columns:
                continue
            summary_rows.append({
                'model_type': model, 'exp_type': exp,
                'stress_window_days': wd,
                'msm_delta':    round(msm_row['f1_stress_minus_calm'].max(), 4),
                'static_delta': round(static_rows['f1_stress_minus_calm'].max(), 4),
                'msm_beats_static': bool(msm_row['f1_stress_minus_calm'].max() > static_rows['f1_stress_minus_calm'].max()),
            })

    return full, pd.DataFrame(summary_rows)


# ── MAIN ──

def main():
    out = 'results/analysis'
    os.makedirs(out, exist_ok=True)

    df = load_results()
    print(f'Loaded {len(df):,} rows | {df["model_type"].nunique()} models | '
          f'{df["retrainer"].nunique()} retrainers')

    def _save(data, name):
        data.to_csv(f'{out}/{name}', index=False)
        print(f'  saved {name} ({len(data)} rows)')

    # 1
    _save(overall_summary(df), 'overall_summary.csv')

    # 2
    _save(per_class_f1(df), 'per_class_f1.csv')

    # 3
    _save(detection_latency(df), 'detection_latency.csv')

    # 4
    _save(false_positive_rate(df), 'false_positive_rate.csv')

    # 5
    _save(best_configs(df), 'best_configs.csv')

    # 6
    _save(retrain_efficiency(df), 'retrain_efficiency.csv')

    # 7
    _save(cooldown_analysis(df), 'cooldown_analysis.csv')

    # 8
    _save(stress_period_f1(df), 'stress_period_f1.csv')

    # 9
    sens = sensitivity_summary(df)
    if not sens.empty:
        _save(sens, 'sensitivity_summary.csv')
    else:
        print('  sensitivity_summary.csv skipped (no MSM configs)')

    # 10
    _save(causal_feature_usage(df), 'causal_feature_usage.csv')

    # 11
    ranks_df, test_df = friedman_ranks(df)
    _save(ranks_df, 'friedman_ranks.csv')
    if not test_df.empty:
        _save(test_df, 'friedman_test.csv')

    # 12
    _save(regime_retrain_rate(df), 'regime_retrain_rate.csv')

    # 13
    rob_full, rob_summary = stress_window_robustness(df)
    _save(rob_full, 'stress_window_robustness.csv')
    if not rob_summary.empty:
        _save(rob_summary, 'stress_window_robustness_summary.csv')

    print(f'\nAll outputs in {out}/')


if __name__ == '__main__':
    main()
