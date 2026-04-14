'''
significance_test.py — Pairwise statistical significance testing.

Run AFTER analysis.py. Reads from results/analysis/ and writes:
    results/analysis/wilcoxon_results.csv   — pairwise Wilcoxon signed-rank tests
    results/analysis/effect_size.csv        — Cohen's d effect size per pair

Purpose
-------
The Friedman test in analysis.py tells you that SOME strategy differs
significantly from the others (global null). This file tells you WHICH
pairs differ and by how much.

For the thesis, the key comparisons are:
    MSM best config  vs  Static
    MSM best config  vs  Random
    MSM best config  vs  FixedSchedule best config
    SPY-MSM best     vs  MSM best

The Wilcoxon signed-rank test is used because:
    - F1 scores per window are paired (same test windows for all strategies)
    - F1 distributions are non-normal (skewed, bounded [0,1])
    - Sample sizes (~100-200 windows) are moderate, not large enough for CLT

Multiple comparison correction: Holm-Bonferroni is applied across all pairs
per model_type to control family-wise error rate.

Usage
-----
    python significance_test.py

Or import and call run() from a notebook for interactive use.
'''

import os
import itertools

import numpy as np
import pandas as pd
from scipy.stats import wilcoxon

from analysis import load_results, get_experiment_type, compute_best_configs


# ----- CONFIG ----- #

# Strategies to always include in pairwise tests — these are the thesis baselines.
# Best MSM/SPY-MSM configs are added dynamically from best_configs.csv.
BASELINE_PREFIXES = ['static', 'random', 'fixed']

# Alpha threshold before correction
ALPHA = 0.05


# ----- HELPERS ----- #

def cohens_d(a, b):
    '''Compute Cohen s d effect size between two paired arrays.'''
    diff = np.array(a) - np.array(b)
    if diff.std() == 0:
        return 0.0
    return round(diff.mean() / diff.std(), 4)


def holm_bonferroni(p_values):
    '''
    Holm-Bonferroni correction for multiple comparisons.
    Returns corrected p-values (same order as input).
    '''
    n   = len(p_values)
    idx = np.argsort(p_values)
    corrected = np.empty(n)
    for rank, i in enumerate(idx):
        corrected[i] = min(1.0, p_values[i] * (n - rank))
    # monotonicity: corrected[i] >= corrected[i-1]
    for rank in range(1, n):
        corrected[idx[rank]] = max(corrected[idx[rank]], corrected[idx[rank - 1]])
    return corrected


# ----- STRATEGY SELECTION ----- #

def select_strategies(df, model_type):
    '''
    For a given model_type, return the retrainer names to include in pairwise tests:
        - best config per MSM-family exp_type (by mean F1)
        - best FixedSchedule config
        - static
        - random
    '''
    model_df = df[df['model_type'] == model_type]
    best     = compute_best_configs(model_df, top_k=1)

    selected = set()

    # best config per exp_type
    for _, row in best.iterrows():
        selected.add(row['retrainer'])

    # ensure all baseline prefixes are covered — pick any match if best_configs missed them
    for prefix in BASELINE_PREFIXES:
        matches = model_df[model_df['retrainer'].str.startswith(prefix)]['retrainer'].unique()
        if len(matches) > 0 and not any(r.startswith(prefix) for r in selected):
            # pick the one with highest mean F1 as the representative baseline
            best_match = (
                model_df[model_df['retrainer'].isin(matches)]
                .groupby('retrainer')['f1'].mean()
                .idxmax()
            )
            selected.add(best_match)

    return sorted(selected)


# ----- WILCOXON PAIRWISE ----- #

def compute_wilcoxon(df):
    '''
    For each model_type:
        1. Select representative strategies (best per exp_type + baselines)
        2. Align F1 series on date_start (inner join — same windows only)
        3. Run pairwise Wilcoxon signed-rank tests
        4. Apply Holm-Bonferroni correction
        5. Compute Cohen s d effect size

    Returns a DataFrame with one row per (model_type, strategy_a, strategy_b).
    '''
    all_records = []

    for model_type, group in df.groupby('model_type'):
        strategies = select_strategies(df, model_type)

        # pivot: rows = windows (date_start), cols = retrainer names
        pivot = group.pivot_table(
            index='date_start',
            columns='retrainer',
            values='f1',
        )[strategies].dropna()   # inner join — only windows where all strategies present

        if len(pivot) < 10:
            print(f'  [wilcoxon/{model_type}] only {len(pivot)} aligned windows — skipping.')
            continue

        pairs   = list(itertools.combinations(strategies, 2))
        p_raw   = []
        records = []

        for a, b in pairs:
            diff = pivot[a].values - pivot[b].values
            # Wilcoxon requires non-zero differences
            if np.all(diff == 0):
                p_raw.append(1.0)
                records.append({
                    'model_type':   model_type,
                    'strategy_a':   a,
                    'strategy_b':   b,
                    'exp_type_a':   get_experiment_type(a),
                    'exp_type_b':   get_experiment_type(b),
                    'n_windows':    len(pivot),
                    'mean_f1_a':    round(pivot[a].mean(), 4),
                    'mean_f1_b':    round(pivot[b].mean(), 4),
                    'mean_diff':    0.0,
                    'stat':         np.nan,
                    'p_value_raw':  1.0,
                    'p_value_corrected': np.nan,   # filled after correction
                    'significant':  False,
                    'cohens_d':     0.0,
                    'winner':       'tie',
                })
                continue

            stat, p = wilcoxon(pivot[a].values, pivot[b].values, alternative='two-sided')
            p_raw.append(p)
            records.append({
                'model_type':   model_type,
                'strategy_a':   a,
                'strategy_b':   b,
                'exp_type_a':   get_experiment_type(a),
                'exp_type_b':   get_experiment_type(b),
                'n_windows':    len(pivot),
                'mean_f1_a':    round(pivot[a].mean(), 4),
                'mean_f1_b':    round(pivot[b].mean(), 4),
                'mean_diff':    round(pivot[a].mean() - pivot[b].mean(), 4),
                'stat':         round(stat, 4),
                'p_value_raw':  round(p, 6),
                'p_value_corrected': np.nan,   # filled below
                'significant':  False,         # filled below
                'cohens_d':     cohens_d(pivot[a].values, pivot[b].values),
                'winner':       a if pivot[a].mean() > pivot[b].mean() else b,
            })

        # apply Holm-Bonferroni correction to all pairs for this model
        corrected = holm_bonferroni(p_raw)
        for i, rec in enumerate(records):
            rec['p_value_corrected'] = round(float(corrected[i]), 6)
            rec['significant']       = bool(corrected[i] < ALPHA)

        all_records.extend(records)

    result = pd.DataFrame(all_records)
    if result.empty:
        return result

    return result.sort_values(
        ['model_type', 'p_value_corrected'],
        ascending=[True, True],
    ).reset_index(drop=True)


# ----- EFFECT SIZE SUMMARY ----- #

def compute_effect_size_summary(wilcoxon_df):
    '''
    Extract the key thesis comparisons from the full pairwise table:
    MSM-family best config vs each baseline, per model_type.

    Cohen s d interpretation:
        |d| < 0.2  → negligible
        |d| < 0.5  → small
        |d| < 0.8  → medium
        |d| >= 0.8 → large
    '''
    if wilcoxon_df.empty:
        return pd.DataFrame()

    msm_types    = {'msm', 'spy_msm', 'timeout_msm', 'causal'}
    baseline_types = {'static', 'random', 'fixed'}

    mask = (
        (wilcoxon_df['exp_type_a'].isin(msm_types) & wilcoxon_df['exp_type_b'].isin(baseline_types)) |
        (wilcoxon_df['exp_type_b'].isin(msm_types) & wilcoxon_df['exp_type_a'].isin(baseline_types))
    )
    sub = wilcoxon_df[mask].copy()

    # ensure MSM is always strategy_a for consistent reading direction
    swap = sub['exp_type_a'].isin(baseline_types)
    sub.loc[swap, ['strategy_a', 'strategy_b']]   = sub.loc[swap, ['strategy_b', 'strategy_a']].values
    sub.loc[swap, ['exp_type_a', 'exp_type_b']]   = sub.loc[swap, ['exp_type_b', 'exp_type_a']].values
    sub.loc[swap, ['mean_f1_a', 'mean_f1_b']]     = sub.loc[swap, ['mean_f1_b', 'mean_f1_a']].values
    sub.loc[swap, 'mean_diff']  = -sub.loc[swap, 'mean_diff']
    sub.loc[swap, 'cohens_d']   = -sub.loc[swap, 'cohens_d']

    def effect_label(d):
        ad = abs(d)
        if ad < 0.2:  return 'negligible'
        if ad < 0.5:  return 'small'
        if ad < 0.8:  return 'medium'
        return 'large'

    sub['effect_label'] = sub['cohens_d'].apply(effect_label)

    return sub[[
        'model_type', 'strategy_a', 'exp_type_a', 'strategy_b', 'exp_type_b',
        'mean_f1_a', 'mean_f1_b', 'mean_diff', 'cohens_d', 'effect_label',
        'p_value_corrected', 'significant', 'n_windows',
    ]].sort_values(['model_type', 'exp_type_a', 'p_value_corrected']).reset_index(drop=True)


# ----- MAIN ----- #

def run():
    os.makedirs('results/analysis', exist_ok=True)

    df = load_results()
    print(
        f'Loaded {len(df):,} rows | '
        f'{df["model_type"].nunique()} models | '
        f'{df["retrainer"].nunique()} retrainers'
    )

    # pairwise Wilcoxon
    print('\nRunning pairwise Wilcoxon signed-rank tests...')
    wilcoxon_df = compute_wilcoxon(df)
    if not wilcoxon_df.empty:
        wilcoxon_df.to_csv('results/analysis/wilcoxon_results.csv', index=False)
        print(f'Wilcoxon results saved ({len(wilcoxon_df)} pairs).')
        print('\n=== Significant pairs (p_corrected < 0.05) ===')
        sig = wilcoxon_df[wilcoxon_df['significant']]
        if sig.empty:
            print('  No significant pairs found after Holm-Bonferroni correction.')
            print('  Consider reporting uncorrected p-values with a note on the correction.')
        else:
            print(sig[[
                'model_type', 'strategy_a', 'strategy_b',
                'mean_f1_a', 'mean_f1_b', 'mean_diff',
                'p_value_corrected', 'cohens_d',
            ]].to_string(index=False))
    else:
        print('No pairs computed — check that results loaded correctly.')

    # effect size summary (MSM vs baselines only)
    effect_df = compute_effect_size_summary(wilcoxon_df)
    if not effect_df.empty:
        effect_df.to_csv('results/analysis/effect_size.csv', index=False)
        print(f'\nEffect size summary saved ({len(effect_df)} rows).')
        print('\n=== MSM vs Baselines — Effect Sizes ===')
        print(effect_df[[
            'model_type', 'strategy_a', 'strategy_b',
            'mean_diff', 'cohens_d', 'effect_label', 'significant',
        ]].to_string(index=False))

    print('\nOutputs written to results/analysis/')


if __name__ == '__main__':
    run()
