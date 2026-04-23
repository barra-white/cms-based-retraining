'''
significance_test.py — Pairwise Wilcoxon + Cohen's d + bootstrap CIs.

Regression task: all tests operate on per-window RMSE.

Sign conventions (important):
    mean_diff   = mean(a_rmse) - mean(b_rmse)   → negative = a better
    cohens_d    = mean(a - b) / std(a - b)      → negative = a better (lower RMSE)
    effect_sizes() compares best-MSM vs best-baseline; negative d ⇒ MSM better.

Run after analysis.py.

Outputs:
    results/analysis/wilcoxon_results.csv
    results/analysis/effect_size.csv
    results/analysis/rmse_bootstrap_ci.csv  (includes overlaps_with_best_baseline)
'''

import itertools
import os
import sys

import numpy as np
import pandas as pd
from scipy.stats import wilcoxon
from arch.bootstrap import StationaryBootstrap

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config as cfg
from analysis import load_results

MSM_TYPES           = cfg.MSM_TYPES
BASELINE_TYPES      = cfg.BASELINE_TYPES
get_experiment_type = cfg.get_experiment_type


def cohens_d(a, b):
    diff = a - b
    return float(diff.mean() / diff.std()) if diff.std() > 0 else 0.0


def effect_label(d):
    d = abs(d)
    if d < 0.2:  return 'negligible'
    if d < 0.5:  return 'small'
    if d < 0.8:  return 'medium'
    return 'large'


def pairwise_wilcoxon(df):
    records = []

    for model, model_df in df.groupby('model_type'):
        # Best per exp_type = lowest mean RMSE.
        best_per_type = (
            model_df.groupby(['exp_type', 'retrainer'])['rmse']
            .mean().reset_index()
            .sort_values('rmse', ascending=True)
            .drop_duplicates('exp_type')
        )
        best_names = best_per_type['retrainer'].tolist()

        pivot = model_df[model_df['retrainer'].isin(best_names)].pivot_table(
            index='date_start', columns='retrainer', values='rmse'
        ).dropna()

        if pivot.shape[1] < 2:
            continue

        pairs = list(itertools.combinations(pivot.columns, 2))
        p_values = []

        for a_name, b_name in pairs:
            a = pivot[a_name].values
            b = pivot[b_name].values
            try:
                stat, p = wilcoxon(a, b, alternative='two-sided', zero_method='pratt')
            except ValueError:
                stat, p = np.nan, 1.0

            d = cohens_d(a, b)
            exp_a = best_per_type[best_per_type['retrainer'] == a_name]['exp_type'].values[0]
            exp_b = best_per_type[best_per_type['retrainer'] == b_name]['exp_type'].values[0]

            records.append({
                'model_type': model,
                'strategy_a': a_name, 'strategy_b': b_name,
                'exp_type_a': exp_a,  'exp_type_b': exp_b,
                'mean_rmse_a': round(a.mean(), 4), 'mean_rmse_b': round(b.mean(), 4),
                'mean_diff':   round((a - b).mean(), 4),   # negative ⇒ a better
                'wilcoxon_stat': round(stat, 4) if not np.isnan(stat) else np.nan,
                'p_value': round(p, 6),
                'cohens_d': round(d, 4),
                'effect_label': effect_label(d),
                'n_windows': len(pivot),
            })
            p_values.append(p)

        if p_values:
            m = len(p_values)
            sorted_idx = np.argsort(p_values)
            corrected = np.ones(m)
            for rank, idx in enumerate(sorted_idx):
                corrected[idx] = min(1.0, p_values[idx] * (m - rank))
            for i in range(1, m):
                corrected[sorted_idx[i]] = max(
                    corrected[sorted_idx[i]],
                    corrected[sorted_idx[i - 1]],
                )
            start = len(records) - m
            for i in range(m):
                records[start + i]['p_value_corrected'] = round(corrected[i], 6)
                records[start + i]['significant'] = corrected[i] < 0.05

    return pd.DataFrame(records)


def effect_sizes(df):
    '''MSM vs best baseline, per model. Compares RMSE. Negative d ⇒ MSM better.'''
    records = []
    for model, model_df in df.groupby('model_type'):
        msm_sub = model_df[model_df['exp_type'].isin(MSM_TYPES)]
        if msm_sub.empty:
            continue
        best_msm = msm_sub.groupby('retrainer')['rmse'].mean().idxmin()

        for btype in BASELINE_TYPES:
            base_sub = model_df[model_df['exp_type'] == btype]
            if base_sub.empty:
                continue
            best_base = base_sub.groupby('retrainer')['rmse'].mean().idxmin()

            pivot = model_df[
                model_df['retrainer'].isin([best_msm, best_base])
            ].pivot_table(index='date_start', columns='retrainer', values='rmse').dropna()

            if pivot.shape[1] < 2 or best_msm not in pivot.columns or best_base not in pivot.columns:
                continue

            a = pivot[best_msm].values
            b = pivot[best_base].values
            d = cohens_d(a, b)
            try:
                _, p = wilcoxon(a, b, zero_method='pratt')
            except ValueError:
                p = 1.0

            records.append({
                'model_type': model,
                'strategy_a': best_msm, 'strategy_b': best_base,
                'exp_type_a': model_df[model_df['retrainer'] == best_msm]['exp_type'].iloc[0],
                'exp_type_b': btype,
                'mean_rmse_a': round(a.mean(), 4), 'mean_rmse_b': round(b.mean(), 4),
                'mean_diff':   round((a - b).mean(), 4),   # negative ⇒ MSM better
                'cohens_d':    round(d, 4), 'effect_label': effect_label(d),
                'significant': p < 0.05, 'p_value': round(p, 6),
                'n_windows':   len(pivot),
            })
    return pd.DataFrame(records)


def bootstrap_rmse_ci(df, n_bootstrap=1000, seed=42, block_length=10):
    """
    Stationary block bootstrap (Politis-Romano 1994) accounts for serial
    correlation in per-window RMSE. Standard iid bootstrap would produce
    over-narrow CIs on autocorrelated time series.
    """
    records = []

    for model, model_df in df.groupby('model_type'):
        pivot = model_df.pivot_table(
            index='date_end', columns='retrainer', values='rmse'    # was date_start
        ).sort_index().dropna()
        if pivot.empty:
            continue

        model_records = []
        for retrainer in pivot.columns:
            values = pivot[retrainer].values
            if len(values) < 10:
                continue

            # Stationary bootstrap on the ordered RMSE series
            bs = StationaryBootstrap(block_length, values, seed=seed)
            boot_means = np.array([x[0][0].mean() for x in bs.bootstrap(n_bootstrap)])

            model_records.append({
                'model_type': model,
                'retrainer':  retrainer,
                'exp_type':   get_experiment_type(retrainer),
                'mean_rmse':  round(values.mean(), 4),
                'ci_lower':   round(np.percentile(boot_means, 2.5), 4),
                'ci_upper':   round(np.percentile(boot_means, 97.5), 4),
                'n_windows':  len(values),
                'block_length': block_length,
            })

        # Best-baseline overlap flagging logic stays the same
        best_baseline_ci = None
        for r in model_records:
            if r['exp_type'] in BASELINE_TYPES:
                if best_baseline_ci is None or r['mean_rmse'] < best_baseline_ci['mean_rmse']:
                    best_baseline_ci = r
        if best_baseline_ci is not None:
            for r in model_records:
                overlap = (r['ci_lower'] <= best_baseline_ci['ci_upper']
                           and best_baseline_ci['ci_lower'] <= r['ci_upper'])
                r['overlaps_with_best_baseline'] = overlap
                r['best_baseline'] = best_baseline_ci['retrainer']

        records.extend(model_records)

    return pd.DataFrame(records).sort_values(
        ['model_type', 'mean_rmse'], ascending=[True, True]
    ).reset_index(drop=True)


def main():
    os.makedirs('results/analysis', exist_ok=True)
    df = load_results()

    print('Pairwise Wilcoxon tests (on per-window RMSE)...')
    w = pairwise_wilcoxon(df)
    w.to_csv('results/analysis/wilcoxon_results.csv', index=False)
    sig = w[w.get('significant', False) == True]
    print(f'  {len(w)} pairs, {len(sig)} significant after Holm-Bonferroni')

    print('\nEffect sizes (MSM vs baselines, on RMSE; negative d = MSM better)...')
    e = effect_sizes(df)
    e.to_csv('results/analysis/effect_size.csv', index=False)
    print(f'  {len(e)} comparisons saved')
    if not e.empty:
        print(e[['model_type', 'strategy_a', 'strategy_b',
                 'mean_diff', 'cohens_d', 'effect_label']].to_string(index=False))

    print('\nBootstrap RMSE confidence intervals...')
    ci = bootstrap_rmse_ci(df)
    ci.to_csv('results/analysis/rmse_bootstrap_ci.csv', index=False)
    print(f'  {len(ci)} retrainer CIs computed')


if __name__ == '__main__':
    main()
