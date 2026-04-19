'''
Reads the DriftSignalObserver output (frozen model, never retrained) and
measures whether MSM predictively leads:
    (a) forecast performance (F1), and
    (b) forecast-relevant market signal (SPY_lr_local_std).

Uses both cross-correlation (bidirectional) and Granger causality (directional)
to distinguish coincident from predictive relationships.

Outputs:
    results/analysis/lead_lag_results.csv
    results/plots/fig_lead_lag_{model}.png

Run after experiment.py completes.
'''

import os
import sys

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

from statsmodels.tsa.stattools import grangercausalitytests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config as cfg


MODELS     = ['xgboost', 'lr', 'rf']
MAX_LAG_XC = 10
MAX_LAG_G  = 3


def cross_corr(a, b, max_lag=MAX_LAG_XC):
    '''
    Cross-correlation. Positive lag = a leads b.
    Contract: a and b must be aligned, same length. NaN is handled by
    dropping rows where either is NaN.
    '''
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


def granger(cause, effect, max_lag=MAX_LAG_G):
    '''
    Does `cause` Granger-cause `effect`?

    statsmodels contract: grangercausalitytests(data, maxlag) tests whether
    column 2 Granger-causes column 1. Returns min p-value and best lag.
    '''
    valid = ~(np.isnan(cause) | np.isnan(effect))
    if valid.sum() < 20:
        return {'min_p': np.nan, 'best_lag': None}
    data = np.column_stack([effect[valid], cause[valid]])
    try:
        results = grangercausalitytests(data, maxlag=max_lag, verbose=False)
        p_values = {
            lag: results[lag][0]['ssr_ftest'][1]
            for lag in range(1, max_lag + 1)
        }
        return {
            'min_p':    round(min(p_values.values()), 4),
            'best_lag': min(p_values, key=p_values.get),
            'p_by_lag': {k: round(v, 4) for k, v in p_values.items()},
        }
    except Exception as e:
        return {'min_p': np.nan, 'best_lag': None, 'error': str(e)}


def align_target_to_windows(obs_df, full_df, target_col):
    '''
    For each observer window, take the mean of target_col over that window's
    21-day test period. Returns an array of same length as obs_df.
    '''
    out = []
    for _, row in obs_df.iterrows():
        # Test window starts the day after date_end and spans 21 trading days.
        date_end = pd.Timestamp(row['date_end'])
        mask = (full_df['Date'] > date_end)
        window_slice = full_df[mask].head(21)
        if len(window_slice) > 0 and target_col in window_slice.columns:
            out.append(window_slice[target_col].mean())
        else:
            out.append(np.nan)
    return np.array(out)


def main():
    os.makedirs('results/analysis', exist_ok=True)
    os.makedirs('results/plots', exist_ok=True)

    full_df = pd.read_csv('data/processed/standardized_data.csv', parse_dates=['Date'])
    # Drop rows where features are NaN; keep SPY_lr_local_std NaN for early rows.
    feat_cols = [c for c in full_df.columns if c not in ('Date', cfg.TARGET_SECONDARY)]
    full_df = full_df.dropna(subset=feat_cols).reset_index(drop=True)

    records = []
    plot_data = {}

    for model in MODELS:
        path = f'results/experiments/{model}/static/drift_observer_results.csv'
        if not os.path.exists(path):
            print(f'  [SKIP] {path} not found')
            continue

        obs = pd.read_csv(path, parse_dates=['date_start', 'date_end'])
        obs = obs.sort_values('window').reset_index(drop=True)

        msm_graph = obs['graph_msm'].values
        msm_spy   = obs['spy_msm'].values
        f1        = obs['f1'].values
        target_secondary = align_target_to_windows(obs, full_df, cfg.TARGET_SECONDARY)

        signals = {'graph_msm': msm_graph, 'spy_msm': msm_spy}
        targets = {'F1': f1, cfg.TARGET_SECONDARY: target_secondary}

        for sig_name, sig_series in signals.items():
            for tgt_name, tgt_series in targets.items():
                xc = cross_corr(sig_series, tgt_series)
                gr = granger(sig_series, tgt_series)

                best_lag = (max(xc, key=lambda k: abs(xc[k])) if xc else None)
                records.append({
                    'model_type':          model,
                    'signal':              sig_name,
                    'vs':                  tgt_name,
                    'xc_best_lag':         best_lag,
                    'xc_corr_at_best_lag': xc.get(best_lag, np.nan) if best_lag is not None else np.nan,
                    'granger_min_p':       gr['min_p'],
                    'granger_best_lag':    gr['best_lag'],
                    'granger_significant': gr['min_p'] < 0.05 if not np.isnan(gr.get('min_p', np.nan)) else False,
                })

        plot_data[model] = {
            'msm_graph_vs_f1':     cross_corr(msm_graph, f1),
            'msm_graph_vs_target': cross_corr(msm_graph, target_secondary),
        }

    result_df = pd.DataFrame(records)
    result_df.to_csv('results/analysis/lead_lag_results.csv', index=False)
    print('\nLead-lag results:\n')
    print(result_df.to_string(index=False))

    # ---- Plots ----
    for model, xc_data in plot_data.items():
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
            peak_lag  = max(xc, key=lambda k: abs(xc[k]))
            ax.axvline(peak_lag, color='#FB8C00', lw=1.5, alpha=0.7, label=f'Peak at lag {peak_lag} (r={xc[peak_lag]:.3f})')
            ax.set_xlabel('Lag (positive = MSM leads)')
            ax.set_ylabel('Correlation')
            ax.set_title(f'{title} ({model})')
            ax.legend(fontsize=8)
            ax.grid(alpha=0.2, axis='y')
        plt.suptitle(f'Lead-Lag Analysis on Observer Run — {model}')
        plt.tight_layout()
        plt.savefig(f'results/plots/fig_lead_lag_{model}.png', dpi=300, bbox_inches='tight')
        plt.close()
        print(f'  Saved: results/plots/fig_lead_lag_{model}.png')

    print('\nDone.')


if __name__ == '__main__':
    main()