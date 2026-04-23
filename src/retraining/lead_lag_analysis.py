'''
lead_lag_analysis.py — RQ1 evidence (regression task).

Uses the DriftSignalObserver output (frozen model) to test whether MSM
predictively leads (a) RMSE degradation and (b) SPY_lr_local_std.

Outputs:
    results/analysis/lead_lag_results.csv
    results/plots/fig_lead_lag_{model}.png
    results/plots/fig_msm_target_overlay_{model}.png
'''

import os
import sys

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

from statsmodels.tsa.stattools import grangercausalitytests
from arch.bootstrap import StationaryBootstrap

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config as cfg

MODELS     = ['xgboost', 'lr', 'rf']
MAX_LAG_XC = 10
MAX_LAG_G  = 3
N_BOOTSTRAP = 1000


def cross_corr(a, b, max_lag=MAX_LAG_XC):
    valid = ~(np.isnan(a) | np.isnan(b))
    a, b = a[valid], b[valid]
    if len(a) < max_lag * 3:
        return {}
    corrs = {}
    for lag in range(-max_lag, max_lag + 1):
        if lag > 0:
            x, y = a[:-lag], b[lag:]
        elif lag < 0:
            x, y = a[-lag:], b[:lag]
        else:
            x, y = a, b
        if len(x) > 5:
            corrs[lag] = float(np.corrcoef(x, y)[0, 1])
    return corrs


def bootstrap_corr_ci(a, b, lag, n_bootstrap=N_BOOTSTRAP, seed=42, block_length=10):
    """Stationary block bootstrap for cross-correlation at a given lag."""
    valid = ~(np.isnan(a) | np.isnan(b))
    a, b = a[valid], b[valid]
    if lag > 0:
        a, b = a[:-lag], b[lag:]
    elif lag < 0:
        a, b = a[-lag:], b[:lag]
    if len(a) < max(20, 2 * block_length):
        return (np.nan, np.nan)

    # Bootstrap pairs together to preserve cross-series structure
    paired = np.column_stack([a, b])
    bs = StationaryBootstrap(block_length, paired, seed=seed)

    boot_corrs = []
    for data in bs.bootstrap(n_bootstrap):
        sample = data[0][0]
        if np.var(sample[:, 0]) > 0 and np.var(sample[:, 1]) > 0:
            boot_corrs.append(np.corrcoef(sample[:, 0], sample[:, 1])[0, 1])

    if not boot_corrs:
        return (np.nan, np.nan)
    return (round(float(np.percentile(boot_corrs, 2.5)), 4),
            round(float(np.percentile(boot_corrs, 97.5)), 4))


def granger(cause, effect, max_lag=MAX_LAG_G):
    valid = ~(np.isnan(cause) | np.isnan(effect))
    n_valid = int(valid.sum())
    if n_valid < 20:
        return {'min_p': np.nan, 'best_lag': None, 'n_obs': n_valid}
    data = np.column_stack([effect[valid], cause[valid]])
    try:
        results = grangercausalitytests(data, maxlag=max_lag, verbose=False)
        p_values = {lag: results[lag][0]['ssr_ftest'][1]
                    for lag in range(1, max_lag + 1)}
        return {
            'min_p':    round(min(p_values.values()), 4),
            'best_lag': min(p_values, key=p_values.get),
            'n_obs':    n_valid,
        }
    except Exception as e:
        return {'min_p': np.nan, 'best_lag': None, 'n_obs': n_valid, 'error': str(e)}


def align_target_to_windows(obs_df, full_df, target_col):
    out = []
    for _, row in obs_df.iterrows():
        date_end = pd.Timestamp(row['date_end'])
        mask = (full_df['Date'] > date_end)
        window_slice = full_df[mask].head(21)
        if len(window_slice) > 0 and target_col in window_slice.columns:
            out.append(window_slice[target_col].mean())
        else:
            out.append(np.nan)
    return np.array(out)


def plot_lag_correlations(model, xc_data, output_path):
    fig, axes = plt.subplots(1, 2, figsize=(12, 4))
    for ax, (title, xc) in zip(axes, xc_data.items()):
        if not xc:
            ax.text(0.5, 0.5, 'insufficient data', ha='center', va='center')
            ax.set_title(title)
            continue
        lags = sorted(xc.keys())
        corrs = [xc[lg] for lg in lags]
        colors = ['#1976D2' if lg > 0 else '#C62828' if lg < 0 else '#757575' for lg in lags]
        ax.bar(lags, corrs, color=colors, alpha=0.85)
        ax.axhline(0, color='black', lw=0.8)
        ax.axvline(0, color='grey', lw=0.5, ls='--')
        peak_lag = max(xc, key=lambda k: abs(xc[k]))
        ax.axvline(peak_lag, color='#FB8C00', lw=1.5, alpha=0.7,
                   label=f'Peak at lag {peak_lag} (r={xc[peak_lag]:.3f})')
        ax.set_xlabel('Lag (positive = MSM leads)')
        ax.set_ylabel('Correlation')
        ax.set_title(title)
        ax.legend(fontsize=8)
        ax.grid(alpha=0.2, axis='y')
    plt.suptitle(f'Lead-Lag Analysis on Observer Run — {model}')
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()


def plot_msm_target_overlay(model, obs, target_series, output_path):
    fig, ax = plt.subplots(figsize=(11, 4))

    def _norm(x):
        valid = ~np.isnan(x)
        if not valid.any():
            return x
        mn, mx = x[valid].min(), x[valid].max()
        if mx - mn < 1e-10:
            return x
        return (x - mn) / (mx - mn)

    dates = obs['date_end']
    msm_norm    = _norm(obs['graph_msm'].values)
    target_norm = _norm(target_series)

    ax.plot(dates, msm_norm, color='#1976D2', lw=1.3,
            label='Graph MSM (normalised)')
    ax.plot(dates, target_norm, color='#7B1FA2', lw=1.3, alpha=0.75,
            label=f'{cfg.TARGET_SECONDARY} (normalised)')

    labels_done = set()
    for ev_name, ev_date in cfg.STRESS_EVENTS.items():
        lbl = ev_name.replace('_', ' ').title() if ev_name not in labels_done else None
        ax.axvline(ev_date, color='#FB8C00', ls='--', lw=1, alpha=0.6, label=lbl)
        labels_done.add(ev_name)

    ax.set_xlabel('Window end date')
    ax.set_ylabel('Normalised value [0, 1]')
    ax.set_title(f'MSM and {cfg.TARGET_SECONDARY} Over Time — {model}\n'
                 f'(observer run — frozen model, no retraining)')
    ax.legend(loc='lower left', fontsize=8, ncol=2)
    ax.grid(alpha=0.2)
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()


def main():
    os.makedirs('results/analysis', exist_ok=True)
    os.makedirs('results/plots', exist_ok=True)

    full_df = pd.read_csv('data/processed/standardized_data.csv', parse_dates=['Date'])
    feat_cols = [c for c in full_df.columns
                 if c not in ('Date', cfg.TARGET_SECONDARY)]
    full_df = full_df.dropna(subset=feat_cols).reset_index(drop=True)

    records = []
    plot_data = {}
    overlay_data = {}

    for model in MODELS:
        path = f'results/experiments/{model}/drift_observer/drift_observer_results.csv'
        if not os.path.exists(path):
            print(f'  [SKIP] {path} not found')
            continue

        obs = pd.read_csv(path, parse_dates=['date_start', 'date_end'])
        obs = obs.sort_values('window').reset_index(drop=True)

        msm_graph = obs['graph_msm'].values
        msm_spy   = obs['spy_msm'].values if 'spy_msm' in obs.columns else np.full(len(obs), np.nan)
        rmse      = obs['rmse'].values
        target_secondary = align_target_to_windows(obs, full_df, cfg.TARGET_SECONDARY)

        signals = {'graph_msm': msm_graph, 'spy_msm': msm_spy}
        targets = {'RMSE': rmse, cfg.TARGET_SECONDARY: target_secondary}

        for sig_name, sig_series in signals.items():
            for tgt_name, tgt_series in targets.items():
                xc = cross_corr(sig_series, tgt_series)
                gr = granger(sig_series, tgt_series)

                best_lag = (max(xc, key=lambda k: abs(xc[k])) if xc else None)
                best_corr = xc.get(best_lag, np.nan) if best_lag is not None else np.nan

                if best_lag is not None:
                    ci_lower, ci_upper = bootstrap_corr_ci(sig_series, tgt_series, best_lag)
                else:
                    ci_lower, ci_upper = np.nan, np.nan

                records.append({
                    'model_type':          model,
                    'signal':              sig_name,
                    'vs':                  tgt_name,
                    'xc_best_lag':         best_lag,
                    'xc_corr_at_best_lag': round(best_corr, 4) if not np.isnan(best_corr) else np.nan,
                    'xc_ci_lower':         ci_lower,
                    'xc_ci_upper':         ci_upper,
                    'granger_min_p':       gr['min_p'],
                    'granger_best_lag':    gr['best_lag'],
                    'granger_n_obs':       gr.get('n_obs', np.nan),
                    'granger_significant': (gr['min_p'] < 0.05
                                            if not np.isnan(gr.get('min_p', np.nan))
                                            else False),
                })

        plot_data[model] = {
            'MSM vs RMSE':                    cross_corr(msm_graph, rmse),
            f'MSM vs {cfg.TARGET_SECONDARY}': cross_corr(msm_graph, target_secondary),
        }
        overlay_data[model] = (obs, target_secondary)

        pd.DataFrame(records).to_csv('results/analysis/lead_lag_results.csv', index=False)

    result_df = pd.DataFrame(records)
    result_df.to_csv('results/analysis/lead_lag_results.csv', index=False)
    print('\nLead-lag results:\n')
    print(result_df.to_string(index=False))

    for model, xc_data in plot_data.items():
        out = f'results/plots/fig_lead_lag_{model}.png'
        plot_lag_correlations(model, xc_data, out)
        print(f'  Saved: {out}')

    for model, (obs, target_series) in overlay_data.items():
        out = f'results/plots/fig_msm_target_overlay_{model}.png'
        plot_msm_target_overlay(model, obs, target_series, out)
        print(f'  Saved: {out}')

    print('\nDone.')


if __name__ == '__main__':
    main()