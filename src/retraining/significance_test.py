'''
significance_test.py — Pairwise Wilcoxon + Cohen's d + bootstrap CIs.

Run after analysis.py.

Outputs:
    results/analysis/wilcoxon_results.csv
    results/analysis/effect_size.csv
    results/analysis/f1_bootstrap_ci.csv  (includes overlaps_with_best_baseline)
'''

import itertools
import os
import sys

import numpy as np
import pandas as pd
from scipy.stats import wilcoxon

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
        best_per_type = (
            model_df.groupby(['exp_type', 'retrainer'])['f1']
            .mean().reset_index()
            .sort_values('f1', ascending=False)
            .drop_duplicates('exp_type')
        )
        best_names = best_per_type['retrainer'].tolist()

        pivot = model_df[model_df['retrainer'].isin(best_names)].pivot_table(
            index='date_start', columns='retrainer', values='f1'
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
                'mean_f1_a':  round(a.mean(), 4), 'mean_f1_b': round(b.mean(), 4),
                'mean_diff':  round((a - b).mean(), 4),
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
    records = []
    for model, model_df in df.groupby('model_type'):
        msm_sub = model_df[model_df['exp_type'].isin(MSM_TYPES)]
        if msm_sub.empty:
            continue
        best_msm = msm_sub.groupby('retrainer')['f1'].mean().idxmax()

        for btype in BASELINE_TYPES:
            base_sub = model_df[model_df['exp_type'] == btype]
            if base_sub.empty:
                continue
            best_base = base_sub.groupby('retrainer')['f1'].mean().idxmax()

            pivot = model_df[
                model_df['retrainer'].isin([best_msm, best_base])
            ].pivot_table(index='date_start', columns='retrainer', values='f1').dropna()

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
                'mean_f1_a':  round(a.mean(), 4), 'mean_f1_b': round(b.mean(), 4),
                'mean_diff':  round((a - b).mean(), 4),
                'cohens_d':   round(d, 4), 'effect_label': effect_label(d),
                'significant': p < 0.05, 'p_value': round(p, 6),
                'n_windows':  len(pivot),
            })
    return pd.DataFrame(records)


def bootstrap_f1_ci(df, n_bootstrap=1000, seed=42):
    records = []
    rng = np.random.default_rng(seed)

    for model, model_df in df.groupby('model_type'):
        pivot = model_df.pivot_table(
            index='date_start', columns='retrainer', values='f1'
        ).dropna()
        if pivot.empty:
            continue

        model_records = []
        for retrainer in pivot.columns:
            values = pivot[retrainer].values
            if len(values) < 10:
                continue
            boot_means = []
            for _ in range(n_bootstrap):
                sample = values[rng.integers(0, len(values), size=len(values))]
                boot_means.append(sample.mean())
            model_records.append({
                'model_type': model,
                'retrainer':  retrainer,
                'exp_type':   get_experiment_type(retrainer),
                'mean_f1':    round(values.mean(), 4),
                'ci_lower':   round(np.percentile(boot_means, 2.5), 4),
                'ci_upper':   round(np.percentile(boot_means, 97.5), 4),
                'n_windows':  len(values),
            })

        # Compute ci-overlap-with-best-baseline
        best_baseline_ci = None
        for r in model_records:
            if r['exp_type'] in BASELINE_TYPES:
                if best_baseline_ci is None or r['mean_f1'] > best_baseline_ci['mean_f1']:
                    best_baseline_ci = r
        if best_baseline_ci is not None:
            for r in model_records:
                overlap = (r['ci_lower'] <= best_baseline_ci['ci_upper']
                           and best_baseline_ci['ci_lower'] <= r['ci_upper'])
                r['overlaps_with_best_baseline'] = overlap
                r['best_baseline'] = best_baseline_ci['retrainer']

        records.extend(model_records)

    return pd.DataFrame(records).sort_values(
        ['model_type', 'mean_f1'], ascending=[True, False]
    ).reset_index(drop=True)


def main():
    os.makedirs('results/analysis', exist_ok=True)
    df = load_results()

    print('Pairwise Wilcoxon tests...')
    w = pairwise_wilcoxon(df)
    w.to_csv('results/analysis/wilcoxon_results.csv', index=False)
    sig = w[w.get('significant', False) == True]
    print(f'  {len(w)} pairs, {len(sig)} significant after Holm-Bonferroni')

    print('\nEffect sizes (MSM vs baselines)...')
    e = effect_sizes(df)
    e.to_csv('results/analysis/effect_size.csv', index=False)
    print(f'  {len(e)} comparisons saved')
    if not e.empty:
        print(e[['model_type', 'strategy_a', 'strategy_b',
                 'mean_diff', 'cohens_d', 'effect_label']].to_string(index=False))

    print('\nBootstrap F1 confidence intervals...')
    ci = bootstrap_f1_ci(df)
    ci.to_csv('results/analysis/f1_bootstrap_ci.csv', index=False)
    print(f'  {len(ci)} retrainer CIs computed')


if __name__ == '__main__':
    main()