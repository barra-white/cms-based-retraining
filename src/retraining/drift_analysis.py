'''
drift_analysis.py — Visualise causal-structure drift vs forecast performance.

Produces the plots most directly tied to the thesis claim that causal
structure breakdown precedes F1 loss.

Run after analysis.py.

Outputs:
    results/plots/fig_06_msm_drift_signal_{model}.png
    results/plots/fig_07_msm_vs_adwin_firing_{model}.png
    results/plots/fig_08_causal_structure_collapse.png
    results/analysis/drift_summary.csv
    results/analysis/msm_f1_lead_lag.csv
'''

import os
import sys
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import config as cfg
from analysis import load_results

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

get_experiment_type = cfg.get_experiment_type
STRESS_EVENTS = cfg.STRESS_EVENTS
STRESS_WINDOW_DAYS = cfg.STRESS_WINDOW_DAYS
MSM_TYPES = cfg.MSM_TYPES

PLOT_DIR     = 'results/plots'
ANALYSIS_DIR = 'results/analysis'

# ── Thesis plot defaults ──
plt.rcParams.update({
    'font.family': 'serif',
    'font.size': 10,
    'axes.titlesize': 12,
    'axes.labelsize': 11,
    'xtick.labelsize': 9,
    'ytick.labelsize': 9,
    'legend.fontsize': 9,
    'figure.dpi': 300,
    'savefig.dpi': 300,
    'savefig.bbox': 'tight',
})


def _shade_stress(ax):
    labels_done = set()
    for name, ev in STRESS_EVENTS.items():
        start = ev - pd.Timedelta(days=STRESS_WINDOW_DAYS)
        end   = ev + pd.Timedelta(days=STRESS_WINDOW_DAYS)
        label = name.replace('_', ' ').title() if name not in labels_done else None
        ax.axvspan(start, end, alpha=0.10, color='#FF6F00', label=label)
        labels_done.add(name)


def _best_msm_name(model_df):
    msm = model_df[model_df['exp_type'].isin(MSM_TYPES - {'causal'})]
    if msm.empty:
        return None
    return msm.groupby('retrainer')['f1'].mean().idxmax()


# ── Plot 06: MSM drift signal vs F1 ──

def plot_msm_drift_signal(df):
    for model, model_df in df.groupby('model_type'):
        best_name = _best_msm_name(model_df)
        if best_name is None:
            continue

        best   = model_df[model_df['retrainer'] == best_name].sort_values('date_end')
        static = model_df[model_df['retrainer'] == 'static'].sort_values('date_end')

        if 'graph_msm' not in best.columns or best['graph_msm'].isna().all():
            continue

        fig, (ax_top, ax_bot) = plt.subplots(2, 1, figsize=(10, 6), sharex=True,
                                              gridspec_kw={'height_ratios': [1.2, 1]})

        # Top: MSM with thresholds and retrain markers
        ax_top.plot(best['date_end'], best['graph_msm'],
                    color='#1976D2', linewidth=1.2, label='Graph MSM')

        try:
            tau_1 = float(best_name.split('tau_1_')[1].split('_')[0])
            tau_2 = float(best_name.split('tau_2_')[1].split('_')[0])
            ax_top.axhline(tau_1, color='#FB8C00', ls='--', lw=0.8, label=f'$\\tau_1$ = {tau_1}')
            ax_top.axhline(tau_2, color='#C62828', ls='--', lw=0.8, label=f'$\\tau_2$ = {tau_2}')
        except (IndexError, ValueError):
            pass

        triggers = best[best['retrain_triggered']]
        if not triggers.empty:
            ax_top.scatter(triggers['date_end'], triggers['graph_msm'],
                           color='#C62828', s=40, marker='^', zorder=5,
                           label=f'Retrain ({len(triggers)})')

        _shade_stress(ax_top)
        ax_top.set_ylim(0, 1.05)
        ax_top.set_ylabel('Mechanism Stability Metric')
        ax_top.set_title(f'MSM Drift Signal vs Forecast Performance — {model}')
        ax_top.legend(loc='lower left', fontsize=8, ncol=2)
        ax_top.grid(alpha=0.2)

        # Bottom: rolling F1
        msm_f1    = best.set_index('date_end')['f1'].rolling(5, min_periods=1).mean()
        static_f1 = static.set_index('date_end')['f1'].rolling(5, min_periods=1).mean()
        ax_bot.plot(msm_f1.index,    msm_f1.values,    color='#1976D2', lw=1.2, label=f'MSM ({best_name[:30]})')
        ax_bot.plot(static_f1.index, static_f1.values, color='#757575', lw=1.2, label='Static')
        _shade_stress(ax_bot)
        ax_bot.set_xlabel('Window end date')
        ax_bot.set_ylabel('F1 (5-window rolling mean)')
        ax_bot.legend(loc='lower left', fontsize=8)
        ax_bot.grid(alpha=0.2)

        plt.tight_layout()
        path = os.path.join(PLOT_DIR, f'fig_06_msm_drift_signal_{model}.png')
        fig.savefig(path)
        plt.close(fig)
        print(f'  Saved: {path}')


# ── Plot 07: MSM vs ADWIN firing timeline ──

def plot_msm_vs_adwin_firing(df):
    for model, model_df in df.groupby('model_type'):
        msm_fires   = model_df[model_df['exp_type'].isin(MSM_TYPES - {'causal'}) & model_df['retrain_triggered']]
        adwin_fires = model_df[(model_df['exp_type'] == 'adwin') & model_df['retrain_triggered']]
        perf_fires  = model_df[(model_df['exp_type'] == 'perf') & model_df['retrain_triggered']]

        fig, ax = plt.subplots(figsize=(10, 3))
        ax.scatter(msm_fires['date_end'],   [2]*len(msm_fires),   marker='^', color='#1976D2', s=50, label='MSM')
        ax.scatter(adwin_fires['date_end'], [1]*len(adwin_fires), marker='s', color='#2E7D32', s=50, label='ADWIN')
        ax.scatter(perf_fires['date_end'],  [0]*len(perf_fires),  marker='o', color='#7B1FA2', s=50, label='Performance')
        _shade_stress(ax)

        ax.set_yticks([0, 1, 2])
        ax.set_yticklabels(['Performance', 'ADWIN', 'MSM'])
        ax.set_ylim(-0.5, 2.5)
        ax.set_xlabel('Window start date')
        ax.set_title(f'Retrain Firing Timeline — {model}')
        ax.legend(loc='upper right', fontsize=8)
        ax.grid(alpha=0.2, axis='x')

        plt.tight_layout()
        path = os.path.join(PLOT_DIR, f'fig_07_msm_vs_adwin_firing_{model}.png')
        fig.savefig(path)
        plt.close(fig)
        print(f'  Saved: {path}')


# ── Plot 08: Causal structure collapse ──

def plot_causal_structure_collapse(df):
    summary_path = 'results/causal_discovery/causal_discovery_summary.csv'
    if not os.path.exists(summary_path):
        print(f'  [SKIP] {summary_path} not found')
        return

    summary = pd.read_csv(summary_path, parse_dates=['date_start', 'date_end'])

    msm_sub = df[df['exp_type'].isin(MSM_TYPES - {'causal'})]
    if msm_sub.empty:
        return
    best_name = msm_sub.groupby('retrainer')['f1'].mean().idxmax()
    best_msm  = msm_sub[msm_sub['retrainer'] == best_name].sort_values('date_end')

    fig, ax1 = plt.subplots(figsize=(10, 4.5))

    x_col = 'date_end' if 'date_end' in summary.columns and summary['date_end'].notna().any() else 'date_start'
    ax1.plot(summary[x_col], summary['spy_parents'],
             color='#7B1FA2', lw=1.2, label='SPY causal parents (count)')
    ax1.set_ylabel('Edges into SPY', color='#7B1FA2')
    ax1.tick_params(axis='y', colors='#7B1FA2')

    ax2 = ax1.twinx()
    if 'graph_msm' in best_msm.columns:
        ax2.plot(best_msm['date_end'], best_msm['graph_msm'],
                 color='#1976D2', lw=1.2, alpha=0.85, label='Graph MSM')
    ax2.set_ylabel('Graph MSM', color='#1976D2')
    ax2.tick_params(axis='y', colors='#1976D2')
    ax2.set_ylim(0, 1.05)

    _shade_stress(ax1)
    ax1.set_xlabel('Window end date')
    ax1.set_title('Causal Structure Collapse vs MSM')
    ax1.grid(alpha=0.2)

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='lower left', fontsize=8)

    plt.tight_layout()
    path = os.path.join(PLOT_DIR, 'fig_08_causal_structure_collapse.png')
    fig.savefig(path)
    plt.close(fig)
    print(f'  Saved: {path}')


# ── Lead-lag cross-correlation ──

def compute_lead_lag(df, max_lag=10):
    records = []
    for model, model_df in df.groupby('model_type'):
        msm_sub = model_df[model_df['exp_type'].isin(MSM_TYPES - {'causal'})]
        if msm_sub.empty or 'graph_msm' not in msm_sub.columns:
            continue
        best_name = msm_sub.groupby('retrainer')['f1'].mean().idxmax()
        series = msm_sub[msm_sub['retrainer'] == best_name].sort_values('date_start')

        msm = series['graph_msm'].values
        f1  = series['f1'].values
        valid = ~(np.isnan(msm) | np.isnan(f1))
        msm, f1 = msm[valid], f1[valid]

        if len(msm) < max_lag + 5:
            continue

        for lag in range(-max_lag, max_lag + 1):
            if lag >= 0:
                corr = np.corrcoef(msm[:len(msm)-lag] if lag > 0 else msm,
                                    f1[lag:] if lag > 0 else f1)[0, 1]
            else:
                corr = np.corrcoef(msm[-lag:], f1[:len(f1)+lag])[0, 1]
            records.append({'model_type': model, 'retrainer': best_name,
                            'lag': lag, 'correlation': round(corr, 4)})

    result = pd.DataFrame(records)
    if not result.empty:
        result.to_csv(os.path.join(ANALYSIS_DIR, 'msm_f1_lead_lag.csv'), index=False)

        # Plot it
        for model, grp in result.groupby('model_type'):
            fig, ax = plt.subplots(figsize=(7, 4))
            ax.bar(grp['lag'], grp['correlation'], color='#1976D2', alpha=0.8)
            peak_lag = grp.loc[grp['correlation'].idxmax(), 'lag']
            ax.axvline(peak_lag, color='#C62828', ls='--', lw=1, label=f'Peak at lag={peak_lag}')
            ax.axvline(0, color='black', lw=0.5)
            ax.set_xlabel('Lag (positive = MSM leads F1)')
            ax.set_ylabel('Cross-correlation')
            ax.set_title(f'MSM–F1 Lead-Lag Correlation — {model}')
            ax.legend(fontsize=8)
            ax.grid(alpha=0.2)
            plt.tight_layout()
            path = os.path.join(PLOT_DIR, f'fig_12_lead_lag_{model}.png')
            fig.savefig(path)
            plt.close(fig)
            print(f'  Saved: {path}')

    return result


# ── Drift summary CSV ──

def drift_summary(df):
    rows = []
    for event_name, event_date in STRESS_EVENTS.items():
        for model, model_df in df.groupby('model_type'):
            for exp in ('msm', 'spy_msm', 'timeout_msm', 'adwin', 'perf'):
                sub = model_df[
                    (model_df['exp_type'] == exp)
                    & (model_df['date_start'] >= event_date)
                    & model_df['retrain_triggered']
                ].sort_values('date_start')
                if sub.empty:
                    rows.append({'event': event_name, 'model': model, 'exp_type': exp,
                                 'first_retrain': None, 'latency_days': None, 'detected': False})
                else:
                    first = sub.iloc[0]
                    rows.append({'event': event_name, 'model': model, 'exp_type': exp,
                                 'first_retrain': str(first['date_start'].date()),
                                 'latency_days': (first['date_start'] - event_date).days,
                                 'detected': True})

    out = pd.DataFrame(rows)
    path = os.path.join(ANALYSIS_DIR, 'drift_summary.csv')
    out.to_csv(path, index=False)
    print(f'  Saved: {path}')


def main():
    os.makedirs(PLOT_DIR, exist_ok=True)
    os.makedirs(ANALYSIS_DIR, exist_ok=True)

    df = load_results()
    df['exp_type'] = df['retrainer'].apply(get_experiment_type)

    # Ensure date_end is parsed for time-series x-axis
    if 'date_end' in df.columns:
        df['date_end'] = pd.to_datetime(df['date_end'])

    print('fig_06: MSM drift signal...')
    plot_msm_drift_signal(df)

    print('fig_07: MSM vs ADWIN firing...')
    plot_msm_vs_adwin_firing(df)

    print('fig_08: Causal structure collapse...')
    plot_causal_structure_collapse(df)

    print('fig_12: MSM-F1 lead-lag...')
    compute_lead_lag(df)

    print('Drift summary CSV...')
    drift_summary(df)

    print('\nDrift analysis complete.')


if __name__ == '__main__':
    main()
