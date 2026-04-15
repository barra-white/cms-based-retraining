'''
analysis.py — Post-experiment analysis.

Computes and saves to results/analysis/:
    1.  overall_summary.csv      — per-(model, retrainer) mean F1, dir-acc, retrain count
    2.  per_class_f1.csv         — per-class F1 to surface class imbalance from stale bins
    3.  detection_latency.csv    — calendar-day / window latency to first retrain after each stress event
    4.  false_positive_rate.csv  — fraction of retrains outside known stress windows
    5.  best_configs.csv         — top-3 configs per (model_type, exp_type) by mean F1
    6.  retrain_efficiency.csv   — pre/post F1 delta per retrain event
    7.  cooldown_analysis.csv    — how often cooldown suppresses a valid signal, by strategy
    8.  stress_period_f1.csv     — F1 split into stress vs. calm market regimes
    9.  sensitivity_summary.csv  — mean F1 vs tau_1, tau_2, lookback for MSM-family retrainers
    10. causal_feature_usage.csv — most frequently selected features by CausalFeatureRetrainer
    11. friedman_ranks.csv       — per-window strategy ranks for Friedman / Nemenyi test
    12. regime_retrain_rate.csv  — retrains-per-window split by stress vs. calm regime

Input:  results/experiments/<model_name>/<retraining_type>/*_results.csv
        (or results/experiments/all_results.csv if the aggregate exists)
Output: results/analysis/ (12 CSVs + friedman_test.csv)
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


# ----- STRESS EVENTS ----- #
# Add new events here — all downstream analyses pick them up automatically.
# Set STRESS_WINDOW_DAYS to define how wide a ±window counts as a true positive.

STRESS_EVENTS = {
    'covid_crash':    pd.Timestamp('2020-02-20'),
    'fed_hikes_2022': pd.Timestamp('2022-03-16'),
}
STRESS_WINDOW_DAYS = 60  # ±3 calendar months = one financial quarter


# ----- EXPERIMENT TYPE PREFIXES ----- #
# Order matters — longer prefixes must come first to avoid 'msm' matching 'spy_msm'

EXP_TYPE_PREFIXES = [
    'spy_msm', 'timeout_msm', 'msm', 'causal',
    'fixed', 'perf', 'adwin', 'random', 'static',
]


# ----- HELPERS ----- #

def get_experiment_type(name):
    '''Map a retrainer name string to a short experiment-type label.'''
    for prefix in EXP_TYPE_PREFIXES:
        if name.startswith(prefix):
            return prefix
    return 'other'


def in_any_window(date):
    '''Return True if date falls inside any known stress window.'''
    return any(
        (event_date - pd.Timedelta(days=STRESS_WINDOW_DAYS))
        <= date
        <= (event_date + pd.Timedelta(days=STRESS_WINDOW_DAYS))
        for event_date in STRESS_EVENTS.values()
    )


# ----- LOADING ----- #

def load_results(path='results/experiments/all_results.csv'):
    '''
    Load the combined results CSV.

    Falls back to globbing all per-experiment CSVs under the nested
    results/experiments/<model_name>/<retraining_type>/ structure
    if the aggregate file is missing (e.g. experiment.py crashed before
    the final concat, or results were never merged).

    Partial-run safety: also checks for all_results_partial.csv so
    analysis can be run mid-experiment without waiting for completion.
    '''
    partial_path = os.path.join(os.path.dirname(path), 'all_results_partial.csv')

    if os.path.exists(path):
        df = pd.read_csv(path, parse_dates=['date_start', 'date_end'])
    elif os.path.exists(partial_path):
        print(f'Warning: using partial results from {partial_path}')
        df = pd.read_csv(partial_path, parse_dates=['date_start', 'date_end'])
    else:
        parts = glob.glob('results/experiments/*/*/*_results.csv')
        # explicitly exclude both aggregate files — all_results_partial.csv is a
        # crash-recovery artefact and may contain only a subset of models
        parts = [p for p in parts if os.path.basename(p) not in
                 ('all_results.csv', 'all_results_partial.csv')]
        if not parts:
            raise FileNotFoundError(
                f"No results at '{path}' and no per-experiment CSVs found. "
                "Run experiment.py first."
            )
        df = pd.concat(
            [pd.read_csv(p, parse_dates=['date_start', 'date_end']) for p in parts],
            ignore_index=True,
        )

    # validate required columns are present
    required_cols = {'model_type', 'retrainer', 'f1', 'directional_acc',
                     'retrain_triggered', 'signal_fired', 'cooldown_active',
                     'date_start', 'window', 'y_true', 'y_pred'}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(
            f"Results DataFrame is missing expected columns: {missing}. "
            "Re-run experiment.py to regenerate results."
        )

    # parse stored JSON/list columns — only convert if still a string
    for col in ('y_true', 'y_pred'):
        if col in df.columns and df[col].dtype == object:
            df[col] = df[col].apply(ast.literal_eval)

    # ensure boolean columns are typed correctly after CSV round-trip
    for col in ('retrain_triggered', 'signal_fired', 'cooldown_active'):
        if col in df.columns:
            df[col] = df[col].astype(bool)

    df['exp_type'] = df['retrainer'].apply(get_experiment_type)
    return df


# ----- 1. OVERALL SUMMARY ----- #

def compute_overall_summary(df):
    '''
    Per-(model_type, retrainer) mean F1, directional accuracy, and retrain count.
    Also reports signals_fired and cooldowns_hit to diagnose cooldown behaviour.
    '''
    return (
        df.groupby(['model_type', 'retrainer', 'exp_type'])
        .agg(
            mean_f1           = ('f1', 'mean'),
            std_f1            = ('f1', 'std'),
            median_f1         = ('f1', 'median'),
            mean_dir_accuracy = ('directional_acc', 'mean'),
            retrains          = ('retrain_triggered', 'sum'),
            signals_fired     = ('signal_fired', 'sum'),
            cooldowns_hit     = ('cooldown_active', 'sum'),
            windows           = ('f1', 'count'),
        )
        .round(4)
        .sort_values(['model_type', 'mean_f1'], ascending=[True, False])
        .reset_index()
    )


# ----- 2. PER-CLASS F1 ----- #

def compute_per_class_f1(df):
    '''
    Aggregate y_true/y_pred across all windows per (model, retrainer),
    then compute per-class F1. Low F1 on any single class indicates class
    imbalance caused by stale bin edges not being refreshed often enough.
    Classes: 0 = down, 1 = neutral, 2 = up.
    '''
    CLASS_NAMES = {0: 'down', 1: 'neutral', 2: 'up'}
    records = []
    for (model, retrainer), group in df.groupby(['model_type', 'retrainer']):
        # vectorised: sum() on a list-of-lists flattens without iterrows
        y_true_all = sum(group['y_true'].tolist(), [])
        y_pred_all = sum(group['y_pred'].tolist(), [])
        scores = f1_score(y_true_all, y_pred_all, average=None, zero_division=0)
        for cls_idx, score in enumerate(scores):
            records.append({
                'model_type': model,
                'retrainer':  retrainer,
                'exp_type':   get_experiment_type(retrainer),
                'class':      cls_idx,
                'class_name': CLASS_NAMES.get(cls_idx, str(cls_idx)),
                'f1':         round(float(score), 4),
            })
    return pd.DataFrame(records)


# ----- 3. DETECTION LATENCY ----- #

def compute_detection_latency(df):
    '''
    For each (model_type, retrainer, stress_event): find the first retrain
    on or after the event date. Latency is reported in both calendar days
    and rolling windows (1 window ≈ 21 trading days).

    detected=False means the strategy never retrains after the event onset
    — a missed regime shift.
    '''
    records = []
    # boolean column already typed correctly by load_results()
    retrains = df[df['retrain_triggered']].copy()

    for (model, retrainer), group in retrains.groupby(['model_type', 'retrainer']):
        exp_type = get_experiment_type(retrainer)
        for event_name, event_date in STRESS_EVENTS.items():
            after = group[group['date_start'] >= event_date]
            if after.empty:
                records.append({
                    'model_type':      model,
                    'retrainer':       retrainer,
                    'exp_type':        exp_type,
                    'event':           event_name,
                    'event_date':      event_date.date(),
                    'first_retrain':   pd.NaT,
                    'latency_days':    np.nan,
                    'latency_windows': np.nan,
                    'detected':        False,
                })
            else:
                first    = after.iloc[0]
                lat_days = (first['date_start'] - event_date).days
                records.append({
                    'model_type':      model,
                    'retrainer':       retrainer,
                    'exp_type':        exp_type,
                    'event':           event_name,
                    'event_date':      event_date.date(),
                    'first_retrain':   first['date_start'].date(),
                    'latency_days':    lat_days,
                    'latency_windows': round(lat_days / 21, 1),
                    'detected':        True,
                })
    return pd.DataFrame(records)


# ----- 4. FALSE POSITIVE RATE ----- #

def compute_false_positive_rate(df):
    '''
    False positive = retrain triggered outside all known stress windows.
    Precision = TP / (TP + FP).

    A strategy that retrains constantly will have high TP but also high FP.
    The precision column penalises that — MSM should score higher precision
    than FixedSchedule or Random because it is selective.
    '''
    retrains = df[df['retrain_triggered']].copy()
    retrains['in_stress_window'] = retrains['date_start'].apply(in_any_window)

    records = []
    for (model, retrainer), group in retrains.groupby(['model_type', 'retrainer']):
        total = len(group)
        tp    = int(group['in_stress_window'].sum())
        fp    = total - tp
        records.append({
            'model_type':          model,
            'retrainer':           retrainer,
            'exp_type':            get_experiment_type(retrainer),
            'total_retrains':      total,
            'true_positives':      tp,
            'false_positives':     fp,
            'false_positive_rate': round(fp / total, 4) if total else np.nan,
            'precision':           round(tp / total, 4) if total else np.nan,
        })
    return pd.DataFrame(records)


# ----- 5. BEST CONFIGS ----- #

def compute_best_configs(df, top_k=3):
    '''
    For each (model_type, exp_type), the top_k retrainer configs ranked by
    mean F1. Directly shows which tau_1/tau_2/lookback combination won the
    MSM sensitivity sweep, and the best interval for FixedSchedule, etc.
    '''
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


# ----- 6. RETRAIN EFFICIENCY ----- #

def compute_retrain_efficiency(df, lookback=3):
    '''
    Per retrain event: mean F1 in the preceding `lookback` windows vs.
    the following `lookback` windows.
    Positive f1_delta = retrain improved performance.
    Negative f1_delta = retrain hurt (regime shifted immediately after).

    in_stress_window flags whether the retrain fell inside a known stress
    event — lets you compare efficiency of stress vs. non-stress retrains.
    '''
    records = []
    for (model, retrainer), group in df.groupby(['model_type', 'retrainer']):
        group = group.sort_values('window').reset_index(drop=True)
        retrain_idxs = group.index[group['retrain_triggered']].tolist()
        for idx in retrain_idxs:
            pre_slice  = group.iloc[max(0, idx - lookback): idx]['f1']
            post_slice = group.iloc[idx + 1: idx + 1 + lookback]['f1']
            if pre_slice.empty or post_slice.empty:
                continue
            pre_f1, post_f1 = pre_slice.mean(), post_slice.mean()
            records.append({
                'model_type':       model,
                'retrainer':        retrainer,
                'exp_type':         get_experiment_type(retrainer),
                'window':           int(group.iloc[idx]['window']),
                'date_start':       group.iloc[idx]['date_start'],
                'pre_retrain_f1':   round(pre_f1,  4),
                'post_retrain_f1':  round(post_f1, 4),
                'f1_delta':         round(post_f1 - pre_f1, 4),
                'in_stress_window': in_any_window(group.iloc[idx]['date_start']),
            })
    return pd.DataFrame(records)


# ----- 7. COOLDOWN ANALYSIS ----- #

def compute_cooldown_analysis(df):
    '''
    How often does the cooldown mechanism suppress a real signal?

    suppression_rate = (signals_fired - retrains_triggered) / signals_fired

    High suppression_rate → cooldown too long; many signals are being blocked.
    Near-zero suppression_rate → strategy rarely fires consecutive signals
    (threshold may be too conservative).

    This directly informs the cooldown hyperparameter choice in the thesis.
    '''
    records = []
    for (model, retrainer), group in df.groupby(['model_type', 'retrainer']):
        signals    = int(group['signal_fired'].sum())
        retrains   = int(group['retrain_triggered'].sum())
        suppressed = signals - retrains
        records.append({
            'model_type':          model,
            'retrainer':           retrainer,
            'exp_type':            get_experiment_type(retrainer),
            'signals_fired':       signals,
            'retrains_triggered':  retrains,
            'cooldown_suppressed': suppressed,
            'suppression_rate':    round(suppressed / signals, 4) if signals else np.nan,
        })
    return pd.DataFrame(records)


# ----- 8. STRESS PERIOD F1 ----- #

def compute_stress_period_f1(df):
    '''
    Split windows into stress / calm regimes and compare mean F1.

    f1_stress_minus_calm > 0 → strategy adapts faster than the market moves.
    f1_stress_minus_calm < 0 → strategy is being disrupted by the very events
    it is supposed to detect (common for strategies that retrain too late).

    This is one of the core thesis comparisons: MSM should show less F1
    degradation during stress periods than static or fixed-schedule baselines.
    '''
    data = df.copy()
    data['regime'] = data['date_start'].apply(
        lambda d: 'stress' if in_any_window(d) else 'calm'
    )
    result = (
        data.groupby(['model_type', 'retrainer', 'exp_type', 'regime'])
        .agg(mean_f1=('f1', 'mean'), std_f1=('f1', 'std'), n=('f1', 'count'))
        .round(4)
        .reset_index()
    )
    pivot = result.pivot_table(
        index=['model_type', 'retrainer', 'exp_type'],
        columns='regime',
        values=['mean_f1', 'n'],
    )
    pivot.columns = ['_'.join(c).strip() for c in pivot.columns]
    pivot['f1_stress_minus_calm'] = (
        pivot.get('mean_f1_stress', np.nan) - pivot.get('mean_f1_calm', np.nan)
    ).round(4)
    return (
        pivot.reset_index()
        .sort_values(['model_type', 'f1_stress_minus_calm'], ascending=[True, False])
    )


# ----- 9. SENSITIVITY SUMMARY ----- #

def compute_sensitivity_summary(df):
    '''
    For MSM, SPY-MSM, and timeout_msm retrainers: parse tau_1, tau_2, lookback
    from the retrainer name string and report mean F1 per
    (model_type, exp_type, tau_1, tau_2, lookback).

    CausalFeatureRetrainer is intentionally excluded — it has additional
    dimensions (spy_idx, all_features) that are not captured in the name
    and would conflate the sensitivity surface.

    Directly answers: which region of the parameter sweep performs best?
    Use best_configs.csv to find the winner; use this to see the full surface.
    '''
    # causal excluded deliberately — see docstring
    msm_types = {'msm', 'spy_msm', 'timeout_msm'}
    sub = df[df['exp_type'].isin(msm_types)].copy()
    if sub.empty:
        return pd.DataFrame()

    def _parse_params(name):
        t1 = re.search(r'tau_1_([\d.]+)', name)
        t2 = re.search(r'tau_2_([\d.]+)', name)
        lb = re.search(r'lb_(\d+)', name)
        return {
            'tau_1':    float(t1.group(1)) if t1 else np.nan,
            'tau_2':    float(t2.group(1)) if t2 else np.nan,
            'lookback': int(lb.group(1))   if lb else np.nan,
        }

    params = sub['retrainer'].apply(_parse_params).apply(pd.Series)
    sub = pd.concat([sub.reset_index(drop=True), params], axis=1)

    return (
        sub.groupby(['model_type', 'exp_type', 'tau_1', 'tau_2', 'lookback'])
        .agg(
            mean_f1           = ('f1', 'mean'),
            std_f1            = ('f1', 'std'),
            mean_dir_accuracy = ('directional_acc', 'mean'),
            retrains          = ('retrain_triggered', 'sum'),
        )
        .round(4)
        .sort_values(['model_type', 'exp_type', 'mean_f1'], ascending=[True, True, False])
        .reset_index()
    )


# ----- 10. CAUSAL FEATURE USAGE ----- #

def compute_causal_feature_usage(df):
    '''
    For CausalFeatureRetrainer rows, parse the JSON list in active_features
    and count how often each feature is selected across all windows.

    High selection_rate → structurally stable causal predictor of SPY.
    Low selection_rate  → regime-specific feature, appearing only during
    particular market conditions (e.g. VIX during stress periods).
    '''
    causal = df[(df['exp_type'] == 'causal') & df['active_features'].notna()].copy()
    if causal.empty:
        return pd.DataFrame(columns=[
            'model_type', 'feature', 'selection_count', 'selection_rate', 'total_windows'
        ])

    records = []
    for model, group in causal.groupby('model_type'):
        total_windows = len(group)
        feature_counts = {}
        for _, row in group.iterrows():
            try:
                feats = (
                    json.loads(row['active_features'])
                    if isinstance(row['active_features'], str)
                    else row['active_features']
                )
                for f in feats:
                    feature_counts[f] = feature_counts.get(f, 0) + 1
            except (json.JSONDecodeError, TypeError):
                continue
        for feat, count in feature_counts.items():
            records.append({
                'model_type':      model,
                'feature':         feat,
                'selection_count': count,
                'selection_rate':  round(count / total_windows, 4),
                'total_windows':   total_windows,
            })

    return (
        pd.DataFrame(records)
        .sort_values(['model_type', 'selection_count'], ascending=[True, False])
        .reset_index(drop=True)
    )


# ----- 11. FRIEDMAN RANKS ----- #

def compute_friedman_ranks(df):
    '''
    Rank all retrainers per window per model (1 = best F1), then average
    ranks across windows. Lower mean rank = better overall strategy.

    Also runs the Friedman chi-squared test (non-parametric equivalent of
    repeated-measures ANOVA) to test whether differences in ranks are
    statistically significant across the full set of strategies.

    This is the standard ML benchmarking methodology (Demsar 2006) and
    is the correct complement to the pairwise Wilcoxon tests in
    significance_test.py — it controls family-wise error rate.

    Outputs:
        friedman_ranks.csv     — mean rank per (model_type, retrainer)
        friedman_test.csv      — one row per model_type with chi2 stat and p-value
    '''
    rank_records = []
    test_records = []

    for model, group in df.groupby('model_type'):
        pivot = group.pivot_table(
            index='date_start',
            columns='retrainer',
            values='f1',
        )

        # log dropped windows so missing data is visible, not silent
        n_before = len(pivot)
        pivot = pivot.dropna()
        n_after  = len(pivot)
        if n_before != n_after:
            print(
                f'  [friedman/{model}] dropped {n_before - n_after} windows '
                f'({n_before - n_after}/{n_before}) due to incomplete retrainer coverage. '
                f'Check for crashed experiments.'
            )

        if pivot.empty or pivot.shape[1] < 2:
            continue

        # rank within each window: rank 1 = highest F1
        ranked = pivot.rank(axis=1, ascending=False, method='average')
        mean_ranks = ranked.mean().sort_values()

        for retrainer, mean_rank in mean_ranks.items():
            rank_records.append({
                'model_type':  model,
                'retrainer':   retrainer,
                'exp_type':    get_experiment_type(retrainer),
                'mean_rank':   round(mean_rank, 4),
                'n_windows':   len(pivot),
            })

        # friedman test — needs one array per retrainer
        arrays = [pivot[col].values for col in pivot.columns]
        if len(arrays) >= 3:
            stat, p = friedmanchisquare(*arrays)
            test_records.append({
                'model_type':   model,
                'n_retrainers': len(arrays),
                'n_windows':    len(pivot),
                'chi2_stat':    round(stat, 4),
                'p_value':      round(p, 6),
                'significant':  p < 0.05,
            })

    ranks_df = (
        pd.DataFrame(rank_records)
        .sort_values(['model_type', 'mean_rank'])
        .reset_index(drop=True)
    )
    test_df = pd.DataFrame(test_records)
    return ranks_df, test_df


# ----- 12. REGIME RETRAIN RATE ----- #

def compute_regime_retrain_rate(df):
    '''
    Retrains-per-window split by stress vs. calm regime.

    A well-calibrated MSM strategy should show a noticeably higher retrain
    rate during stress windows than during calm periods. A FixedSchedule or
    Random strategy will show similar rates in both — this is the key
    selectivity comparison for the thesis.

    retrain_rate_stress / retrain_rate_calm > 1 means the strategy is
    concentrating retrains during the periods that matter most.

    likely_inverted flags any strategy where stress_calm_ratio < 1.0,
    meaning it retrains more during calm than stress — a signal of
    misconfiguration (e.g. the SPY-MSM empty-graph bug).
    '''
    data = df.copy()
    data['regime'] = data['date_start'].apply(
        lambda d: 'stress' if in_any_window(d) else 'calm'
    )

    rate = (
        data.groupby(['model_type', 'retrainer', 'exp_type', 'regime'])
        .agg(
            retrains=('retrain_triggered', 'sum'),
            windows =('retrain_triggered', 'count'),
        )
        .reset_index()
    )
    rate['retrain_rate'] = (rate['retrains'] / rate['windows']).round(4)

    pivot = rate.pivot_table(
        index=['model_type', 'retrainer', 'exp_type'],
        columns='regime',
        values='retrain_rate',
    ).reset_index()
    pivot.columns.name = None

    rename = {}
    if 'stress' in pivot.columns:
        rename['stress'] = 'retrain_rate_stress'
    if 'calm' in pivot.columns:
        rename['calm'] = 'retrain_rate_calm'
    pivot = pivot.rename(columns=rename)

    if 'retrain_rate_stress' in pivot.columns and 'retrain_rate_calm' in pivot.columns:
        pivot['stress_calm_ratio'] = (
            pivot['retrain_rate_stress'] / pivot['retrain_rate_calm'].replace(0, np.nan)
        ).round(4)

        # flag strategies retraining more during calm than stress
        pivot['likely_inverted'] = pivot['stress_calm_ratio'] < 1.0

    return pivot.sort_values(
        ['model_type', 'stress_calm_ratio'] if 'stress_calm_ratio' in pivot.columns
        else ['model_type'],
        ascending=[True, False] if 'stress_calm_ratio' in pivot.columns else [True],
    ).reset_index(drop=True)


# ----- MAIN ----- #

def main():
    os.makedirs('results/analysis', exist_ok=True)

    df = load_results()
    print(
        f'Loaded {len(df):,} rows | '
        f'{df["model_type"].nunique()} models | '
        f'{df["retrainer"].nunique()} retrainers | '
        f'{df["exp_type"].nunique()} experiment types'
    )

    # 1. overall summary
    summary = compute_overall_summary(df)
    summary.to_csv('results/analysis/overall_summary.csv', index=False)
    print('\n=== Overall Summary (top 10) ===')
    print(summary.head(10).to_string(index=False))

    # 2. per-class F1
    per_class = compute_per_class_f1(df)
    per_class.to_csv('results/analysis/per_class_f1.csv', index=False)
    print(f'\nPer-class F1 saved ({len(per_class)} rows).')

    # 3. detection latency
    latency = compute_detection_latency(df)
    latency.to_csv('results/analysis/detection_latency.csv', index=False)
    print('\n=== Detection Latency ===')
    print(latency.to_string(index=False))

    # 4. false positive rate
    fpr = compute_false_positive_rate(df)
    fpr.to_csv('results/analysis/false_positive_rate.csv', index=False)
    print('\n=== False Positive Rate (lowest 10) ===')
    print(fpr.sort_values('false_positive_rate').head(10).to_string(index=False))

    # 5. best configs per experiment type
    best = compute_best_configs(df, top_k=3)
    best.to_csv('results/analysis/best_configs.csv', index=False)
    print(f'\nBest configs saved ({len(best)} rows).')

    # 6. retrain efficiency
    efficiency = compute_retrain_efficiency(df, lookback=3)
    efficiency.to_csv('results/analysis/retrain_efficiency.csv', index=False)
    mean_delta = (
        efficiency.groupby(['model_type', 'exp_type'])['f1_delta']
        .mean().round(4).sort_values(ascending=False)
    )
    print('\n=== Mean F1 Delta per Retrain by Experiment Type ===')
    print(mean_delta.to_string())

    # 7. cooldown analysis
    cooldown = compute_cooldown_analysis(df)
    cooldown.to_csv('results/analysis/cooldown_analysis.csv', index=False)
    print('\n=== Cooldown Suppression Rate (top 10) ===')
    print(
        cooldown.sort_values('suppression_rate', ascending=False)
        .head(10).to_string(index=False)
    )

    # 8. stress vs. calm F1
    stress_f1 = compute_stress_period_f1(df)
    stress_f1.to_csv('results/analysis/stress_period_f1.csv', index=False)
    print(f'\nStress-period F1 saved ({len(stress_f1)} rows).')
    if 'f1_stress_minus_calm' in stress_f1.columns:
        print('\n=== Top 10 Strategies by Stress F1 Advantage ===')
        cols = ['model_type', 'retrainer', 'exp_type',
                'mean_f1_stress', 'mean_f1_calm', 'f1_stress_minus_calm']
        cols = [c for c in cols if c in stress_f1.columns]
        print(stress_f1[cols].head(10).to_string(index=False))

    # 9. sensitivity summary (MSM-family, causal excluded)
    sensitivity = compute_sensitivity_summary(df)
    if not sensitivity.empty:
        sensitivity.to_csv('results/analysis/sensitivity_summary.csv', index=False)
        print(f'\nSensitivity summary saved ({len(sensitivity)} rows).')
    else:
        print('\nNo MSM-family configs found — sensitivity_summary.csv skipped.')

    # 10. causal feature usage
    feature_usage = compute_causal_feature_usage(df)
    feature_usage.to_csv('results/analysis/causal_feature_usage.csv', index=False)
    print(f'\nCausal feature usage saved ({len(feature_usage)} rows).')
    if not feature_usage.empty:
        print('\n=== Top 10 Most Selected Causal Features (xgboost) ===')
        top = feature_usage[feature_usage['model_type'] == 'xgboost'].head(10)
        print(top[['feature', 'selection_count', 'selection_rate']].to_string(index=False))

    # 11. friedman ranks + test
    ranks_df, test_df = compute_friedman_ranks(df)
    ranks_df.to_csv('results/analysis/friedman_ranks.csv', index=False)
    if not test_df.empty:
        test_df.to_csv('results/analysis/friedman_test.csv', index=False)
        print('\n=== Friedman Test Results ===')
        print(test_df.to_string(index=False))
    print('\n=== Mean Rank per Strategy (lower = better, xgboost) ===')
    xgb_ranks = ranks_df[ranks_df['model_type'] == 'xgboost']
    print(xgb_ranks[['retrainer', 'exp_type', 'mean_rank']].head(15).to_string(index=False))

    # 12. regime retrain rate
    regime_rate = compute_regime_retrain_rate(df)
    regime_rate.to_csv('results/analysis/regime_retrain_rate.csv', index=False)
    print(f'\nRegime retrain rate saved ({len(regime_rate)} rows).')
    if 'stress_calm_ratio' in regime_rate.columns:
        print('\n=== Top 10 Strategies by Stress/Calm Retrain Ratio ===')
        print(
            regime_rate[['model_type', 'retrainer', 'exp_type', 'retrain_rate_stress', 'retrain_rate_calm', 'stress_calm_ratio']]
            .head(10).to_string(index=False)
        )

    print('\nAll outputs written to results/analysis/')


if __name__ == '__main__':
    main()
