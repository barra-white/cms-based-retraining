'''
analysis.py — Post-experiment analysis pipeline (regression task).

Run after experiment.py completes. Produces CSVs to results/analysis/.

Primary metric: RMSE (lower = better). MAE and R² reported alongside.

Usage:
    python src/retraining/analysis.py

Key outputs (each answers a specific thesis claim):
    overall_summary.csv     — RQ2: which strategy wins on RMSE
    aggregate_metrics.csv   — RQ2: pooled regression metrics (rmse, mae, r2, pearson)
    detection_latency.csv   — RQ2: response time to stress events
    false_positive_rate.csv — RQ3: selectivity
    stress_period_rmse.csv  — RQ2: stress vs calm robustness
    sensitivity_summary.csv — RQ3: hyperparameter robustness
    msm_summary.csv         — RQ3: MSM distribution sanity
    ... and others
'''

import ast
import glob
import os
import re
import sys

import numpy as np
import pandas as pd
from scipy.stats import friedmanchisquare, pearsonr
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config as cfg

STRESS_EVENTS        = cfg.STRESS_EVENTS
STRESS_WINDOW_DAYS   = cfg.STRESS_WINDOW_DAYS
EXP_TYPE_PREFIXES    = cfg.EXP_TYPE_PREFIXES
MSM_TYPES            = cfg.MSM_TYPES
BASELINE_TYPES       = cfg.BASELINE_TYPES
SIGNAL_DRIVEN        = cfg.SIGNAL_DRIVEN
get_experiment_type  = cfg.get_experiment_type
in_stress_window     = cfg.in_stress_window

ROBUSTNESS_WINDOW_DAYS = [45, 60, 90, 120]


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

    for col in ('y_true', 'y_pred'):
        if col in df.columns and df[col].dtype == object:
            df[col] = df[col].apply(ast.literal_eval)

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
    out = (
        df.groupby(['model_type', 'retrainer', 'exp_type'])
        .agg(
            mean_rmse=('rmse', 'mean'),
            std_rmse=('rmse', 'std'),
            median_rmse=('rmse', 'median'),
            mean_mae=('mae', 'mean'),
            mean_r2=('r2', 'mean'),
            mean_qlike=('qlike', 'mean'),   # ← ADD
            std_qlike=('qlike', 'std'),     # ← ADD
            retrains=('retrain_triggered', 'sum'),
            signals_fired=('signal_fired', 'sum'),
            cooldowns_hit=('cooldown_active', 'sum'),
            windows=('rmse', 'count'),
        )
        .round(4)
        .sort_values(['model_type', 'mean_rmse'], ascending=[True, True])
        .reset_index()
    )
    out['rank_within_model'] = out.groupby('model_type')['mean_rmse'].rank(
        ascending=True, method='min'
    ).astype(int)
    return out


# ── 2. AGGREGATE REGRESSION METRICS (pooled y_true/y_pred) ──

def regression_metrics(df):
    records = []
    for (model, retrainer), grp in df.groupby(['model_type', 'retrainer']):
        y_true = np.asarray(sum(grp['y_true'].tolist(), []), dtype=float)
        y_pred = np.asarray(sum(grp['y_pred'].tolist(), []), dtype=float)

        if len(y_true) < 2 or np.var(y_true) == 0:
            rmse = mae = r2 = pearson_r = pearson_p = qlike = np.nan
        else:
            rmse  = float(np.sqrt(mean_squared_error(y_true, y_pred)))
            mae   = float(mean_absolute_error(y_true, y_pred))
            r2    = float(r2_score(y_true, y_pred))
            try:
                pearson_r, pearson_p = pearsonr(y_true, y_pred)
                pearson_r = float(pearson_r)
                pearson_p = float(pearson_p)
            except Exception:
                pearson_r = pearson_p = np.nan
            # Pooled QLIKE on log-RV arrays
            rv_true = np.exp(y_true)
            rv_pred = np.clip(np.exp(y_pred), 1e-10, None)
            qlike   = float(np.mean(rv_true / rv_pred - np.log(rv_true / rv_pred) - 1))

        records.append({
            'model_type':   model,
            'retrainer':    retrainer,
            'exp_type':     get_experiment_type(retrainer),
            'rmse':         round(rmse,     6) if not np.isnan(rmse)     else np.nan,
            'mae':          round(mae,      6) if not np.isnan(mae)      else np.nan,
            'r2':           round(r2,       6) if not np.isnan(r2)       else np.nan,
            'qlike':        round(qlike,    6) if not np.isnan(qlike)    else np.nan,   # ← ADD
            'pearson_r':    round(pearson_r,6) if not np.isnan(pearson_r)else np.nan,
            'pearson_p':    round(pearson_p,6) if not np.isnan(pearson_p)else np.nan,
            'n_points':     len(y_true),
        })

    return pd.DataFrame(records).sort_values(
        ['model_type', 'rmse'], ascending=[True, True]
    ).reset_index(drop=True)


# ── 3. DETECTION LATENCY ──

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
                lat = (first['date_start'] - event_date).days
                records.append({
                    'model_type': model, 'retrainer': retrainer, 'exp_type': exp,
                    'event': event_name, 'event_date': event_date.date(),
                    'first_retrain': first['date_start'].date(),
                    'latency_days': lat, 'latency_windows': round(lat / 21, 1),
                    'detected': True,
                })
    result = pd.DataFrame(records)
    if not result.empty and 'detected' in result.columns:
        for (model, event), grp in result[result['detected']].groupby(['model_type', 'event']):
            fastest_idx = grp['latency_windows'].idxmin()
            result.loc[fastest_idx, 'is_fastest_per_event'] = True
        if 'is_fastest_per_event' not in result.columns:
            result['is_fastest_per_event'] = False
        else:
            result['is_fastest_per_event'] = result['is_fastest_per_event'].fillna(False)
    return result


# ── 4. FALSE POSITIVE RATE ──

def false_positive_rate(df):
    retrains = df[df['retrain_triggered']].copy()
    if retrains.empty:
        return pd.DataFrame(columns=[
            'model_type', 'retrainer', 'exp_type', 'total_retrains',
            'true_positives', 'false_positives', 'fpr', 'precision',
        ])
    retrains['in_stress'] = retrains['date_start'].apply(in_stress_window)

    all_dates = df['date_start'].drop_duplicates()
    n_stress  = all_dates.apply(in_stress_window).sum()
    baseline_fpr = 1 - (n_stress / len(all_dates)) if len(all_dates) > 0 else np.nan

    records = []
    for (model, retrainer), grp in retrains.groupby(['model_type', 'retrainer']):
        total = len(grp)
        tp    = int(grp['in_stress'].sum())
        fp    = total - tp
        records.append({
            'model_type': model, 'retrainer': retrainer,
            'exp_type': get_experiment_type(retrainer),
            'total_retrains': total, 'true_positives': tp, 'false_positives': fp,
            'fpr':       round(fp / total, 4) if total else np.nan,
            'precision': round(tp / total, 4) if total else np.nan,
            'baseline_random_fpr': round(baseline_fpr, 4),
        })
    return pd.DataFrame(records)


# ── 5. BEST CONFIGS ──

def best_configs(df, top_k=3):
    summary = (
        df.groupby(['model_type', 'exp_type', 'retrainer'])
        .agg(mean_rmse=('rmse', 'mean'), std_rmse=('rmse', 'std'),
             mean_mae=('mae', 'mean'), mean_r2=('r2', 'mean'),
             retrains=('retrain_triggered', 'sum'), windows=('rmse', 'count'))
        .round(4).reset_index()
    )
    summary['rank'] = (
        summary.groupby(['model_type', 'exp_type'])['mean_rmse']
        .rank(ascending=True, method='first').astype(int)
    )
    return (
        summary[summary['rank'] <= top_k]
        .sort_values(['model_type', 'exp_type', 'rank'])
        .reset_index(drop=True)
    )


# ── 6. RETRAIN EFFICIENCY ──

def retrain_efficiency(df, n_windows=3):
    '''For each retrain event, compute RMSE change (pre_rmse - post_rmse).
    Positive = RMSE dropped after retrain = improvement.'''
    records = []
    for (model, retrainer), grp in df.groupby(['model_type', 'retrainer']):
        grp = grp.sort_values('window').reset_index(drop=True)
        exp = get_experiment_type(retrainer)
        retrain_idxs = grp.index[grp['retrain_triggered']].tolist()
        gains = []
        for idx in retrain_idxs:
            pre  = grp.iloc[max(0, idx - n_windows):idx]['rmse'].mean()
            post = grp.iloc[idx + 1: idx + 1 + n_windows]['rmse'].mean()
            if not (np.isnan(pre) or np.isnan(post)):
                gains.append(pre - post)   # positive => RMSE dropped
        n_pos = sum(1 for g in gains if g > 0)
        records.append({
            'model_type': model, 'retrainer': retrainer, 'exp_type': exp,
            'n_retrains': len(retrain_idxs),
            'mean_rmse_gain':    round(np.mean(gains), 4) if gains else np.nan,
            'positive_retrains': n_pos,
            'negative_retrains': len(gains) - n_pos,
            'pct_positive':      round(n_pos / len(gains), 3) if gains else np.nan,
        })
    return pd.DataFrame(records).sort_values(
        ['model_type', 'mean_rmse_gain'], ascending=[True, False]
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
            'signals_fired': signals,
            'retrains_triggered': retrains,
            'cooldown_suppressed': suppressed,
            'suppression_rate': round(suppressed / signals, 4) if signals else np.nan,
        })
    return pd.DataFrame(records)


# ── 8. STRESS vs CALM (RMSE) ──

def stress_period_rmse(df):
    '''
    NOTE: positive rmse_stress_minus_calm now means the retrainer is WORSE in
    stress (higher error). Flipped semantics vs classification; interpretation
    reversed wherever this column is consumed.
    '''
    data = df.copy()
    data['regime'] = data['date_start'].apply(
        lambda d: 'stress' if in_stress_window(d) else 'calm'
    )
    agg = (
        data.groupby(['model_type', 'retrainer', 'exp_type', 'regime'])
        .agg(mean_rmse=('rmse', 'mean'), std_rmse=('rmse', 'std'), n=('rmse', 'count'))
        .round(4).reset_index()
    )
    pivot = agg.pivot_table(
        index=['model_type', 'retrainer', 'exp_type'],
        columns='regime', values=['mean_rmse', 'std_rmse', 'n'],
    )
    pivot.columns = ['_'.join(c).strip() for c in pivot.columns]
    if 'mean_rmse_stress' in pivot.columns and 'mean_rmse_calm' in pivot.columns:
        pivot['rmse_stress_minus_calm'] = (
            pivot['mean_rmse_stress'] - pivot['mean_rmse_calm']
        ).round(4)
        if 'std_rmse_stress' in pivot.columns:
            noise = np.maximum(
                pivot.get('std_rmse_stress', 0).fillna(0),
                pivot.get('std_rmse_calm', 0).fillna(0),
            )
            pivot['likely_within_noise'] = (pivot['rmse_stress_minus_calm'].abs() < 0.5 * noise)
    return (
        pivot.reset_index()
        .sort_values(['model_type', 'rmse_stress_minus_calm'], ascending=[True, True])
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

    out = (
        sub.groupby(['model_type', 'exp_type', 'tau_1', 'tau_2', 'lookback'])
        .agg(mean_rmse=('rmse', 'mean'), std_rmse=('rmse', 'std'),
             mean_mae=('mae', 'mean'),
             retrains=('retrain_triggered', 'sum'))
        .round(4)
        .sort_values(['model_type', 'exp_type', 'mean_rmse'], ascending=[True, True, True])
        .reset_index()
    )

    def _add_sensitivity(grp):
        rmse_range = grp['mean_rmse'].max() - grp['mean_rmse'].min()
        rmse_mean  = grp['mean_rmse'].mean()
        grp['rmse_range_within_group'] = round(rmse_range, 4)
        grp['rmse_range_pct_of_mean']  = round(rmse_range / rmse_mean, 4) if rmse_mean > 0 else np.nan
        return grp
    out = out.groupby(['model_type', 'exp_type'], group_keys=False).apply(_add_sensitivity)
    return out


# ── 10. CAUSAL FEATURE USAGE ──

def causal_feature_usage(df):
    causal = df[(df['exp_type'] == 'causal') & df['active_features'].notna()].copy()
    if causal.empty:
        return pd.DataFrame()

    import json as _json
    records = []
    for model, grp in causal.groupby('model_type'):
        total = len(grp)
        counts = {}
        for _, row in grp.iterrows():
            try:
                feats = (_json.loads(row['active_features'])
                         if isinstance(row['active_features'], str)
                         else row['active_features'])
                for f in feats:
                    counts[f] = counts.get(f, 0) + 1
            except (_json.JSONDecodeError, TypeError):
                continue
        for feat, count in counts.items():
            records.append({
                'model_type': model, 'feature': feat,
                'selection_count': count,
                'selection_rate':  round(count / total, 4),
                'total_windows':   total,
            })

    return pd.DataFrame(records).sort_values(
        ['model_type', 'selection_count'], ascending=[True, False]
    ).reset_index(drop=True)


# ── 11. FRIEDMAN RANKS (on RMSE) ──

def friedman_ranks(df):
    rank_records = []
    test_records = []

    for model, grp in df.groupby('model_type'):
        pivot = grp.pivot_table(index='date_start', columns='retrainer', values='rmse')
        n_before = len(pivot)
        pivot = pivot.dropna()
        if n_before != len(pivot):
            print(f'  [friedman/{model}] dropped {n_before - len(pivot)} incomplete windows')

        if pivot.empty or pivot.shape[1] < 2:
            continue

        # Lower RMSE = better → rank ascending.
        ranked = pivot.rank(axis=1, ascending=True, method='average')
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

    sort_cols = ['model_type', 'stress_calm_ratio'] if 'stress_calm_ratio' in pivot.columns else ['model_type']
    sort_asc  = [True, False] if 'stress_calm_ratio' in pivot.columns else [True]
    return pivot.sort_values(sort_cols, ascending=sort_asc).reset_index(drop=True)


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
            .agg(mean_rmse=('rmse', 'mean')).reset_index()
            .pivot_table(index=['model_type', 'retrainer', 'exp_type'],
                         columns='regime', values='mean_rmse').reset_index()
        )
        pivot.columns.name = None
        if 'stress' in pivot.columns and 'calm' in pivot.columns:
            pivot['rmse_stress_minus_calm'] = (pivot['stress'] - pivot['calm']).round(4)
        pivot['stress_window_days'] = window_days
        records.append(pivot)

    full = pd.concat(records, ignore_index=True)

    # MSM beats static when its stress-minus-calm is LOWER (less degradation).
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
            if 'rmse_stress_minus_calm' not in msm_row.columns or 'rmse_stress_minus_calm' not in static_rows.columns:
                continue
            summary_rows.append({
                'model_type': model, 'exp_type': exp,
                'stress_window_days': wd,
                'msm_delta':    round(msm_row['rmse_stress_minus_calm'].min(), 4),
                'static_delta': round(static_rows['rmse_stress_minus_calm'].min(), 4),
                'msm_beats_static': bool(
                    msm_row['rmse_stress_minus_calm'].min() < static_rows['rmse_stress_minus_calm'].min()
                ),
            })

    return full, pd.DataFrame(summary_rows)


# ── 14. MSM SUMMARY ──

def msm_summary(df):
    if 'graph_msm' not in df.columns:
        return pd.DataFrame()
    records = []
    for (model, retrainer), grp in df.groupby(['model_type', 'retrainer']):
        msm = grp['graph_msm'].dropna()
        if len(msm) == 0:
            continue
        records.append({
            'model_type': model, 'retrainer': retrainer,
            'exp_type':   get_experiment_type(retrainer),
            'n_windows':  len(msm),
            'msm_mean':   round(msm.mean(), 4),
            'msm_median': round(msm.median(), 4),
            'msm_std':    round(msm.std(), 4),
            'msm_min':    round(msm.min(), 4),
            'msm_max':    round(msm.max(), 4),
            'msm_p25':    round(msm.quantile(0.25), 4),
            'msm_p75':    round(msm.quantile(0.75), 4),
            'windows_below_0.5': int((msm < 0.5).sum()),
            'windows_below_0.3': int((msm < 0.3).sum()),
        })
    return pd.DataFrame(records).sort_values(
        ['model_type', 'msm_mean'], ascending=[True, True]
    ).reset_index(drop=True)


# --- QLIKE SUMMARY (added as metric in retrainers.py) ---
def qlike_summary(df):
    """
    Per-window mean QLIKE per strategy, ranked ascending (lower = better).
    MSM vs baseline comparison mirrors overall_summary but on QLIKE.
    """
    if 'qlike' not in df.columns:
        return pd.DataFrame()
    out = (
        df.groupby(['model_type', 'retrainer', 'exp_type'])
        .agg(
            mean_qlike=('qlike', 'mean'),
            std_qlike=('qlike', 'std'),
            median_qlike=('qlike', 'median'),
            windows=('qlike', 'count'),
        )
        .round(6)
        .sort_values(['model_type', 'mean_qlike'], ascending=[True, True])
        .reset_index()
    )
    out['qlike_rank'] = out.groupby('model_type')['mean_qlike'].rank(
        ascending=True, method='min'
    ).astype(int)
    return out

def transition_period_rmse(df):
    """RMSE during ±10-day transition windows vs calm. This is where MSM
    advantage should appear, not in sustained-stress periods."""
    data = df.copy()
    data['regime'] = data['date_start'].apply(
        lambda d: 'transition' if cfg.in_transition_window(d) else 'calm'
    )
    agg = (
        data.groupby(['model_type', 'retrainer', 'exp_type', 'regime'])
        .agg(mean_rmse=('rmse', 'mean'), std_rmse=('rmse', 'std'), n=('rmse', 'count'))
        .round(4).reset_index()
    )
    pivot = agg.pivot_table(
        index=['model_type', 'retrainer', 'exp_type'],
        columns='regime', values=['mean_rmse', 'n'],
    )
    pivot.columns = ['_'.join(c).strip() for c in pivot.columns]
    if 'mean_rmse_transition' in pivot.columns and 'mean_rmse_calm' in pivot.columns:
        pivot['rmse_transition_minus_calm'] = (
            pivot['mean_rmse_transition'] - pivot['mean_rmse_calm']
        ).round(4)
    return pivot.reset_index().sort_values(
        ['model_type', 'rmse_transition_minus_calm'], ascending=[True, True]
    )

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

    _save(overall_summary(df), 'overall_summary.csv')
    _save(regression_metrics(df),   'aggregate_metrics.csv')
    _save(detection_latency(df),    'detection_latency.csv')
    _save(false_positive_rate(df),  'false_positive_rate.csv')
    _save(best_configs(df),         'best_configs.csv')
    _save(retrain_efficiency(df),   'retrain_efficiency.csv')
    _save(cooldown_analysis(df),    'cooldown_analysis.csv')
    _save(stress_period_rmse(df),   'stress_period_rmse.csv')
    _save(transition_period_rmse(df), 'transition_period_rmse.csv')
    _save(qlike_summary(df),         'qlike_summary.csv')

    sens = sensitivity_summary(df)
    if not sens.empty:
        _save(sens, 'sensitivity_summary.csv')

    _save(causal_feature_usage(df), 'causal_feature_usage.csv')

    ranks_df, test_df = friedman_ranks(df)
    _save(ranks_df, 'friedman_ranks.csv')
    if not test_df.empty:
        _save(test_df, 'friedman_test.csv')

    _save(regime_retrain_rate(df), 'regime_retrain_rate.csv')

    rob_full, rob_summary = stress_window_robustness(df)
    _save(rob_full, 'stress_window_robustness.csv')
    if not rob_summary.empty:
        _save(rob_summary, 'stress_window_robustness_summary.csv')

    _save(msm_summary(df), 'msm_summary.csv')

    print(f'\nAll outputs in {out}/')


if __name__ == '__main__':
    main()
