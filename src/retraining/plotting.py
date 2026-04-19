'''
plotting.py — Thesis-quality figures from analysis CSVs.

Run after analysis.py, significance_test.py, and drift_analysis.py.

Outputs to results/plots/:
    fig_01  Mean F1 by strategy (grouped bar, top-5 per exp_type)
    fig_02  F1 stress advantage (horizontal bar)
    fig_03  Detection latency (dot plot, signal-driven only)
    fig_04  Retrain precision vs volume (scatter)
    fig_05  MSM sensitivity heatmap (tau_1 × tau_2)
    fig_09  Wilcoxon p-value heatmap
    fig_10  Effect size dot plot
    fig_11  Causal feature usage
    fig_13  Static vs StaticRollingBins vs MSM (F1 over time)

Plots fig_06, fig_07, fig_08, fig_12 are produced by drift_analysis.py.
'''

import os
import sys
import warnings
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
warnings.filterwarnings('ignore', category=FutureWarning)

import config as cfg
from analysis import load_results

STRESS_EVENTS = cfg.STRESS_EVENTS
STRESS_WINDOW_DAYS = cfg.STRESS_WINDOW_DAYS
MSM_TYPES = cfg.MSM_TYPES
BASELINE_TYPES = cfg.BASELINE_TYPES
get_experiment_type = cfg.get_experiment_type

ANALYSIS_DIR = 'results/analysis'
PLOT_DIR     = 'results/plots'

# ── Thesis defaults ──
plt.rcParams.update({
    'font.family': 'serif',
    'font.size': 10,
    'axes.titlesize': 12,
    'axes.titleweight': 'bold',
    'axes.labelsize': 11,
    'xtick.labelsize': 9,
    'ytick.labelsize': 9,
    'legend.fontsize': 8,
    'figure.dpi': 300,
    'savefig.dpi': 300,
    'savefig.bbox': 'tight',
})

PALETTE = {
    'msm':         '#1976D2',
    'spy_msm':     '#0D47A1',
    'timeout_msm': '#42A5F5',
    'causal':      '#00838F',
    'static':      '#757575',
    'fixed':       '#FB8C00',
    'perf':        '#7B1FA2',
    'adwin':       '#2E7D32',
    'random':      '#C62828',
    'other':       '#795548',
}

EXP_LABELS = {
    'msm': 'MSM', 'spy_msm': 'SPY-MSM', 'timeout_msm': 'Timeout-MSM',
    'causal': 'Causal', 'static': 'Static', 'fixed': 'Fixed',
    'perf': 'Performance', 'adwin': 'ADWIN', 'random': 'Random',
}


def _load(name):
    path = os.path.join(ANALYSIS_DIR, name)
    if not os.path.exists(path):
        print(f'  [SKIP] {name} not found')
        return None
    return pd.read_csv(path)

def _save(fig, name):
    os.makedirs(PLOT_DIR, exist_ok=True)
    path = os.path.join(PLOT_DIR, name)
    fig.savefig(path)
    plt.close(fig)
    print(f'  Saved: {path}')

def _col(exp):
    return PALETTE.get(str(exp), '#607D8B')

def _short(name, n=30):
    return name if len(name) <= n else name[:n-2] + '..'

def _shade(ax):
    done = set()
    for name, ev in STRESS_EVENTS.items():
        s = ev - pd.Timedelta(days=STRESS_WINDOW_DAYS)
        e = ev + pd.Timedelta(days=STRESS_WINDOW_DAYS)
        ax.axvspan(s, e, alpha=0.10, color='#FF6F00',
                   label=name.replace('_',' ').title() if name not in done else None)
        done.add(name)

def _legend_patches(exp_types):
    return [mpatches.Patch(color=_col(k), label=EXP_LABELS.get(k, k))
            for k in sorted(exp_types) if k in PALETTE]


# ── 01: Mean F1 ranking ──

def plot_01():
    df = _load('overall_summary.csv')
    if df is None: return

    for model in df['model_type'].unique():
        sub = df[df['model_type'] == model]
        # Best per exp_type (avoid groupby.apply)
        top = (sub.sort_values('mean_f1', ascending=False)
               .drop_duplicates('exp_type')
               .sort_values('mean_f1', ascending=True))

        if top.empty: continue
        fig, ax = plt.subplots(figsize=(7, max(3, len(top) * 0.5)))
        colours = [_col(e) for e in top['exp_type']]
        ax.barh(top['retrainer'].apply(lambda n: _short(n, 35)), top['mean_f1'],
                xerr=top['std_f1'], color=colours, capsize=3, alpha=0.9)
        ax.set_xlabel('Mean Macro-F1')
        ax.set_title(f'Best Strategy per Type — {model}')
        ax.axvline(top['mean_f1'].median(), color='black', ls=':', lw=0.7, label='Median')
        ax.legend(handles=_legend_patches(top['exp_type'].unique()), loc='lower right', fontsize=7)
        plt.tight_layout()
        _save(fig, f'fig_01_f1_ranking_{model}.png')


# ── 02: Stress advantage ──

def plot_02():
    df = _load('stress_period_f1.csv')
    if df is None or 'f1_stress_minus_calm' not in df.columns: return

    for model in df['model_type'].unique():
        sub = df[df['model_type'] == model].sort_values('f1_stress_minus_calm', ascending=True)
        # Best per exp_type
        top = (sub.sort_values('f1_stress_minus_calm', ascending=False)
               .drop_duplicates('exp_type')
               .sort_values('f1_stress_minus_calm', ascending=True))

        if top.empty: continue

        fig, ax = plt.subplots(figsize=(7, max(3, len(top) * 0.5)))
        colours = ['#1976D2' if v >= 0 else '#C62828' for v in top['f1_stress_minus_calm']]
        ax.barh(top['retrainer'].apply(lambda n: _short(n, 35)),
                top['f1_stress_minus_calm'], color=colours, alpha=0.9)
        ax.axvline(0, color='black', lw=0.8)
        ax.set_xlabel('F1 (stress) − F1 (calm)')
        ax.set_title(f'Stress Period F1 Advantage — {model}')
        plt.tight_layout()
        _save(fig, f'fig_02_stress_delta_{model}.png')


# ── 03: Detection latency ──

def plot_03():
    df = _load('detection_latency.csv')
    if df is None: return

    for model in df['model_type'].unique():
        sub = df[(df['model_type'] == model) & df['detected']]
        if sub.empty: continue

        fig, ax = plt.subplots(figsize=(8, max(4, len(sub['event'].unique()) * 1.5)))
        events = sub['event'].unique()
        for i, event in enumerate(events):
            ev_sub = sub[sub['event'] == event].sort_values('latency_windows')
            for _, row in ev_sub.iterrows():
                ax.scatter(row['latency_windows'], i,
                           color=_col(row['exp_type']), s=60, zorder=5)
                ax.annotate(_short(row['retrainer'], 20),
                            (row['latency_windows'], i),
                            fontsize=6, xytext=(4, 2), textcoords='offset points')

        ax.set_yticks(range(len(events)))
        ax.set_yticklabels([e.replace('_', ' ').title() for e in events])
        ax.set_xlabel('Latency (rolling windows, lower = faster)')
        ax.set_title(f'Detection Latency — {model}')
        ax.legend(handles=_legend_patches(sub['exp_type'].unique()), loc='lower right', fontsize=7)
        ax.grid(alpha=0.2, axis='x')
        plt.tight_layout()
        _save(fig, f'fig_03_latency_{model}.png')


# ── 04: Precision vs volume ──

def plot_04():
    df = _load('false_positive_rate.csv')
    if df is None: return

    for model in df['model_type'].unique():
        sub = df[df['model_type'] == model]
        fig, ax = plt.subplots(figsize=(7, 5))
        for _, row in sub.iterrows():
            ax.scatter(row['total_retrains'], row['precision'],
                       color=_col(row['exp_type']), s=50, alpha=0.8)
        ax.set_xlabel('Total retrains')
        ax.set_ylabel('Precision (TP / total)')
        ax.set_ylim(-0.05, 1.05)
        ax.axhline(0.5, color='grey', ls='--', lw=0.7)
        ax.set_title(f'Retrain Precision vs Volume — {model}')
        ax.legend(handles=_legend_patches(sub['exp_type'].unique()), loc='lower right', fontsize=7)
        ax.grid(alpha=0.2)
        plt.tight_layout()
        _save(fig, f'fig_04_precision_{model}.png')


# ── 05: Sensitivity heatmap ──

def plot_05():
    df = _load('sensitivity_summary.csv')
    if df is None: return

    for model in df['model_type'].unique():
        for exp in df[df['model_type'] == model]['exp_type'].unique():
            sub = df[(df['model_type'] == model) & (df['exp_type'] == exp)]
            pivot = sub.pivot_table(index='tau_1', columns='tau_2', values='mean_f1', aggfunc='mean')
            if pivot.empty: continue

            fig, ax = plt.subplots(figsize=(5, 4))
            im = ax.imshow(pivot.values, cmap='YlGn', aspect='auto',
                           vmin=pivot.values.min(), vmax=pivot.values.max())
            ax.set_xticks(range(len(pivot.columns)))
            ax.set_xticklabels([f'{v:.2f}' for v in pivot.columns])
            ax.set_yticks(range(len(pivot.index)))
            ax.set_yticklabels([f'{v:.2f}' for v in pivot.index])
            ax.set_xlabel('$\\tau_2$ (confirmation)')
            ax.set_ylabel('$\\tau_1$ (alert)')
            for i in range(len(pivot.index)):
                for j in range(len(pivot.columns)):
                    v = pivot.values[i, j]
                    if not np.isnan(v):
                        ax.text(j, i, f'{v:.3f}', ha='center', va='center', fontsize=8)
            plt.colorbar(im, ax=ax, label='Mean F1')
            ax.set_title(f'Sensitivity: $\\tau_1$ vs $\\tau_2$ — {model}/{exp}')
            plt.tight_layout()
            _save(fig, f'fig_05_sensitivity_{model}_{exp}.png')


# ── 09: Wilcoxon heatmap ──

def plot_09():
    df = _load('wilcoxon_results.csv')
    if df is None: return

    for model in df['model_type'].unique():
        sub = df[df['model_type'] == model]
        strategies = sorted(set(sub['strategy_a'].tolist() + sub['strategy_b'].tolist()))
        n = len(strategies)
        if n < 2: continue

        mat = np.ones((n, n))
        idx = {s: i for i, s in enumerate(strategies)}
        for _, row in sub.iterrows():
            if 'p_value_corrected' in row and not pd.isna(row['p_value_corrected']):
                i, j = idx[row['strategy_a']], idx[row['strategy_b']]
                mat[i, j] = row['p_value_corrected']
                mat[j, i] = row['p_value_corrected']

        fig, ax = plt.subplots(figsize=(max(5, n * 0.9), max(4, n * 0.9)))
        im = ax.imshow(mat, cmap='RdYlGn_r', vmin=0, vmax=0.1)
        ax.set_xticks(range(n))
        ax.set_xticklabels([_short(s, 18) for s in strategies], rotation=55, ha='right', fontsize=7)
        ax.set_yticks(range(n))
        ax.set_yticklabels([_short(s, 18) for s in strategies], fontsize=7)
        for i in range(n):
            for j in range(n):
                if i != j:
                    mk = '*' if mat[i, j] < 0.05 else ''
                    ax.text(j, i, f'{mat[i,j]:.3f}{mk}', ha='center', va='center', fontsize=6)
        plt.colorbar(im, ax=ax, label='Corrected p-value')
        ax.set_title(f'Wilcoxon Tests (Holm-Bonferroni) — {model}')
        plt.tight_layout()
        _save(fig, f'fig_09_wilcoxon_{model}.png')


# ── 10: Effect size ──

def plot_10():
    df = _load('effect_size.csv')
    if df is None or df.empty: return

    for model in df['model_type'].unique():
        sub = df[df['model_type'] == model].sort_values('cohens_d', ascending=True)
        labels = [f"{_short(r['strategy_a'],18)} vs\n{_short(r['strategy_b'],18)}" for _, r in sub.iterrows()]
        colours = ['#1976D2' if sig else '#BDBDBD' for sig in sub['significant']]

        fig, ax = plt.subplots(figsize=(7, max(3, len(sub) * 0.6)))
        ax.barh(labels, sub['cohens_d'], color=colours, alpha=0.9)
        ax.axvline(0, color='black', lw=0.8)
        for t, ls in [(0.2, ':'), (0.5, '--'), (0.8, '-.')]:
            ax.axvline(t, color='grey', ls=ls, lw=0.6)
            ax.axvline(-t, color='grey', ls=ls, lw=0.6)
        ax.set_xlabel("Cohen's d (positive = MSM better)")
        ax.set_title(f'Effect Sizes — {model}')
        sig_p = mpatches.Patch(color='#1976D2', label='p < 0.05')
        ns_p  = mpatches.Patch(color='#BDBDBD', label='Not significant')
        ax.legend(handles=[sig_p, ns_p], fontsize=7)
        plt.tight_layout()
        _save(fig, f'fig_10_effect_size_{model}.png')


# ── 11: Causal feature usage ──

def plot_11():
    df = _load('causal_feature_usage.csv')
    if df is None or df.empty: return

    for model in df['model_type'].unique():
        sub = df[df['model_type'] == model].sort_values('selection_rate', ascending=True).tail(15)
        colours = ['#1976D2' if r >= 0.5 else '#FB8C00' if r >= 0.2 else '#BDBDBD'
                   for r in sub['selection_rate']]

        fig, ax = plt.subplots(figsize=(7, max(3, len(sub) * 0.35)))
        ax.barh(sub['feature'], sub['selection_rate'], color=colours, alpha=0.9)
        ax.set_xlabel('Selection rate (fraction of windows)')
        ax.set_title(f'Causal Feature Usage — {model}')
        ax.axvline(1.0, color='#1976D2', ls='--', lw=0.7, label='Always')
        ax.axvline(0.5, color='#FB8C00', ls='--', lw=0.7, label='50%')
        ax.legend(fontsize=7)
        plt.tight_layout()
        _save(fig, f'fig_11_feature_usage_{model}.png')


# ── 13: Static vs RollingBins vs MSM (F1 over time) ──

def plot_13():
    raw_path = 'results/experiments/all_results.csv'
    if not os.path.exists(raw_path):
        print('  [SKIP] all_results.csv not found')
        return

    df = pd.read_csv(raw_path, parse_dates=['date_start', 'date_end'])
    df['exp_type'] = df['retrainer'].apply(get_experiment_type)

    for model, model_df in df.groupby('model_type'):
        static      = model_df[model_df['retrainer'] == 'static'].sort_values('date_end')
        rolling_bin = model_df[model_df['retrainer'] == 'static_rolling_bins'].sort_values('date_end')
        msm_sub     = model_df[model_df['exp_type'].isin(MSM_TYPES - {'causal'})]

        if msm_sub.empty: continue
        best_msm_name = msm_sub.groupby('retrainer')['f1'].mean().idxmax()
        msm_best = model_df[model_df['retrainer'] == best_msm_name].sort_values('date_end')

        fig, ax = plt.subplots(figsize=(10, 4.5))
        for series, label, color in [
            (static,      'Static (frozen model + bins)',         '#757575'),
            (rolling_bin, 'Static + Rolling Bins (bins only)',    '#FB8C00'),
            (msm_best,    f'MSM ({_short(best_msm_name, 25)})',  '#1976D2'),
        ]:
            if series.empty: continue
            s = series.set_index('date_end')['f1'].rolling(5, min_periods=1).mean()
            ax.plot(s.index, s.values, color=color, lw=1.2, label=label)

        _shade(ax)
        ax.set_xlabel('Window end date')
        ax.set_ylabel('F1 (5-window rolling mean)')
        ax.set_title(f'Bin Recalibration Control — {model}')
        ax.legend(loc='lower left', fontsize=7)
        ax.grid(alpha=0.2)
        plt.tight_layout()
        _save(fig, f'fig_13_bin_control_{model}.png')


def main():
    os.makedirs(PLOT_DIR, exist_ok=True)
    print(f'Writing plots to {PLOT_DIR}/')

    plot_01()
    plot_02()
    plot_03()
    plot_04()
    plot_05()
    plot_09()
    plot_10()
    plot_11()
    plot_13()

    print(f'\nAll plots saved. Run drift_analysis.py for fig_06, 07, 08, 12.')


if __name__ == '__main__':
    main()
