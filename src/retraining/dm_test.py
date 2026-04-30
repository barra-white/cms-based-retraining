"""
dm_test.py — Diebold-Mariano forecast comparison test with HAC standard errors.

Purpose:
    Standard forecasting-literature test for comparing two forecasters' loss
    differentials. Accounts for serial correlation in loss differentials via
    Newey-West HAC standard errors — no block bootstrap needed.

    H0: E[loss_A - loss_B] = 0 (forecasters are equally accurate)
    H1: two-sided

Output:
    results/analysis/dm_test.csv
"""

import ast
import os
import sys

import numpy as np
import pandas as pd
from scipy.stats import norm

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config as cfg
from analysis import load_results

MSM_TYPES      = cfg.MSM_TYPES
BASELINE_TYPES = cfg.BASELINE_TYPES
get_experiment_type = cfg.get_experiment_type


def squared_error_loss(y_true, y_pred):
    return (np.asarray(y_true) - np.asarray(y_pred)) ** 2


def qlike_loss(y_log_true, y_log_pred):
    """Patton QLIKE in log-RV space, per-observation."""
    delta = np.asarray(y_log_true) - np.asarray(y_log_pred)
    return np.exp(delta) - delta - 1


def hac_variance(d, max_lag=None):
    """
    Newey-West HAC variance estimator for the mean of a serially correlated
    time series. Bartlett kernel, bandwidth chosen by the standard rule
    max_lag = int(n^(1/3)) + 1 unless specified.
    """
    n = len(d)
    if max_lag is None:
        max_lag = max(1, int(n ** (1 / 3)) + 1)
    d = np.asarray(d, dtype=float)
    d_centered = d - d.mean()

    gamma_0 = np.dot(d_centered, d_centered) / n
    total = gamma_0
    for k in range(1, min(max_lag + 1, n)):
        weight = 1.0 - k / (max_lag + 1)  # Bartlett kernel
        gamma_k = np.dot(d_centered[k:], d_centered[:-k]) / n
        total += 2 * weight * gamma_k

    return max(total, 1e-12) / n  # floor to avoid negative variance from noise


def dm_statistic(loss_a, loss_b, max_lag=None):
    """Diebold-Mariano test statistic and two-sided p-value."""
    d = loss_a - loss_b
    n = len(d)
    if n < 10:
        return np.nan, np.nan, np.nan

    mean_d = d.mean()
    var_d = hac_variance(d, max_lag=max_lag)
    se_d = np.sqrt(var_d)
    if se_d == 0:
        return np.nan, np.nan, np.nan

    dm_stat = mean_d / se_d
    p_value = 2 * (1 - norm.cdf(abs(dm_stat)))
    return float(dm_stat), float(p_value), float(mean_d)


def extract_loss_series(df, retrainer, loss_fn):
    """Build a per-observation loss series for one retrainer, ordered by date."""
    sub = df[df['retrainer'] == retrainer].sort_values('date_end')
    all_losses = []
    for _, row in sub.iterrows():
        y_true = ast.literal_eval(row['y_true']) if isinstance(row['y_true'], str) else row['y_true']
        y_pred = ast.literal_eval(row['y_pred']) if isinstance(row['y_pred'], str) else row['y_pred']
        all_losses.extend(loss_fn(y_true, y_pred))
    return np.asarray(all_losses, dtype=float)


def run_dm_tests(df, loss_name, loss_fn):
    """Run DM tests between each best MSM and each best baseline per model."""
    records = []

    for model, model_df in df.groupby('model_type'):
        # Best per exp_type by the corresponding aggregate metric
        agg_col = 'rmse' if loss_name == 'MSE' else 'qlike'
        if agg_col not in model_df.columns:
            continue

        best_per_type = (
            model_df.groupby(['exp_type', 'retrainer'])[agg_col]
            .mean().reset_index()
            .sort_values(agg_col, ascending=True)
            .drop_duplicates('exp_type')
        )

        msm_names = best_per_type[
            best_per_type['exp_type'].isin(MSM_TYPES)
        ]['retrainer'].tolist()
        baseline_names = best_per_type[
            best_per_type['exp_type'].isin(BASELINE_TYPES - {'static', 'drift_observer'})
        ]['retrainer'].tolist()

        for msm_name in msm_names:
            loss_msm = extract_loss_series(model_df, msm_name, loss_fn)
            for base_name in baseline_names:
                loss_base = extract_loss_series(model_df, base_name, loss_fn)

                # Align lengths (should match, but guard against partial runs)
                n = min(len(loss_msm), len(loss_base))
                if n < 20:
                    continue
                l_a = loss_msm[:n]
                l_b = loss_base[:n]

                # DM stat: MSM - baseline. Negative mean_d ⇒ MSM has lower loss.
                dm_stat, p_value, mean_d = dm_statistic(l_a, l_b)

                records.append({
                    'model_type': model,
                    'loss': loss_name,
                    'msm_retrainer': msm_name,
                    'baseline_retrainer': base_name,
                    'n_observations': n,
                    'mean_loss_diff': round(mean_d, 6) if not np.isnan(mean_d) else np.nan,
                    'dm_statistic': round(dm_stat, 4) if not np.isnan(dm_stat) else np.nan,
                    'p_value': round(p_value, 6) if not np.isnan(p_value) else np.nan,
                    'significant_at_0.05': (p_value < 0.05) if not np.isnan(p_value) else False,
                    'msm_better': (mean_d < 0) if not np.isnan(mean_d) else None,
                })

    return pd.DataFrame(records)


def main():
    os.makedirs('results/analysis', exist_ok=True)
    df = load_results()

    print('Running Diebold-Mariano tests...')
    print('  MSE loss (equivalent to RMSE comparison)...')
    mse_results = run_dm_tests(df, 'MSE', squared_error_loss)
    print(f'    {len(mse_results)} MSM-vs-baseline comparisons')

    print('  QLIKE loss...')
    qlike_results = run_dm_tests(df, 'QLIKE', qlike_loss)
    print(f'    {len(qlike_results)} MSM-vs-baseline comparisons')

    combined = pd.concat([mse_results, qlike_results], ignore_index=True)
    out = 'results/analysis/dm_test.csv'
    combined.to_csv(out, index=False)
    print(f'\nSaved: {out}')

    for model, grp in combined.groupby('model_type'):
        print(f'\n--- {model} ---')
        for loss in ('MSE', 'QLIKE'):
            sub = grp[grp['loss'] == loss]
            if sub.empty:
                continue
            sig = sub[sub['significant_at_0.05']]
            msm_wins = sig[sig['msm_better']]
            msm_loses = sig[~sig['msm_better']]
            print(
                f'  {loss}: {len(msm_wins)} MSM wins, {len(msm_loses)} MSM losses '
                f'({len(sig)}/{len(sub)} significant)'
            )


if __name__ == '__main__':
    main()