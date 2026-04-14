'''
plotting.py — Visualisation of all analysis CSVs.

Run AFTER analysis.py and significance_test.py.

Outputs (written to results/plots/):
    01_overall_f1_ranking.png        — bar chart: mean F1 per strategy per model
    02_stress_calm_delta.png         — bar chart: f1_stress_minus_calm per strategy
    03_detection_latency_heatmap.png — heatmap: strategy x event, value=latency_windows
    04_false_positive_rate.png       — scatter: total_retrains vs precision
    05_regime_retrain_rate.png       — grouped bar: stress vs calm retrain rate
    06_wilcoxon_heatmap.png          — heatmap: corrected p-value per pair (per model)
    07_effect_size.png               — dot plot: cohens_d for MSM vs baselines
    08_sensitivity_heatmap.png       — heatmap: tau_1 x tau_2 mean F1 per exp_type
    09_f1_over_time.png              — line chart: rolling mean F1 over windows
    10_cooldown_suppression.png      — bar chart: suppression rate per strategy
    11_causal_feature_usage.png      — horizontal bar: feature selection rate

Usage
-----
    python plotting.py
'''

import os
import warnings
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')   # non-interactive backend — safe for headless runs
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.colors import TwoSlopeNorm

warnings.filterwarnings('ignore', category=FutureWarning)

ANALYSIS_DIR = 'results/analysis'
PLOT_DIR     = 'results/plots'

MSM_TYPES      = {'msm', 'spy_msm', 'timeout_msm', 'causal'}
BASELINE_TYPES = {'static', 'random', 'fixed'}

# colour palette — consistent across all charts
EXP_COLOURS = {
    'msm':         '#2196F3',   # blue
    'spy_msm':     '#1565C0',   # dark blue
    'timeout_msm': '#42A5F5',   # light blue
    'causal':      '#00BCD4',   # cyan
    'static':      '#9E9E9E',   # grey
    'random':      '#F44336',   # red
    'fixed':       '#FF9800',   # orange
    'perf':        '#9C27B0',   # purple
    'adwin':       '#4CAF50',   # green
    'other':       '#795548',   # brown
}


# ----- UTILS ----- #

def _load(filename):
    path = os.path.join(ANALYSIS_DIR, filename)
    if not os.path.exists(path):
        print(f'  [SKIP] {filename} not found.')
        return None
    return pd.read_csv(path)

def _save(fig, filename):
    os.makedirs(PLOT_DIR, exist_ok=True)
    path = os.path.join(PLOT_DIR, filename)
    fig.savefig(path, dpi=150, bbox_inches='tight')
    plt.close(fig)
    print(f'  Saved: {path}')

def _colour(exp_type):
    return EXP_COLOURS.get(str(exp_type), '#607D8B')

def _short_name(name, max_len=28):
    '''Truncate long retrainer names for axis labels.'''
    return name if len(name) <= max_len else name[:max_len - 2] + '..'


# ----- 01. OVERALL F1 RANKING ----- #

def plot_overall_f1_ranking():
    df = _load('overall_summary.csv')
    if df is None:
        return

    models = df['model_type'].unique()
    fig, axes = plt.subplots(1, len(models), figsize=(7 * len(models), 8), squeeze=False)
    fig.suptitle('Mean F1 by Strategy', fontsize=14, fontweight='bold')

    for ax, model in zip(axes[0], models):
        sub = df[df['model_type'] == model].sort_values('mean_f1', ascending=True)
        colours = [_colour(e) for e in sub['exp_type']]
        bars = ax.barh(sub['retrainer'].apply(_short_name), sub['mean_f1'],
                       xerr=sub['std_f1'], color=colours, capsize=3, alpha=0.85)
        ax.set_title(model, fontsize=11)
        ax.set_xlabel('Mean F1')
        ax.axvline(sub['mean_f1'].median(), color='black', linestyle='--',
                   linewidth=0.8, label='Median')
        ax.legend(fontsize=8)

    # legend
    patches = [mpatches.Patch(color=c, label=k) for k, c in EXP_COLOURS.items()
               if k in df['exp_type'].values]
    fig.legend(handles=patches, loc='lower center', ncol=5, fontsize=8,
               title='Strategy type', bbox_to_anchor=(0.5, -0.02))
    plt.tight_layout()
    _save(fig, '01_overall_f1_ranking.png')


# ----- 02. STRESS / CALM DELTA ----- #

def plot_stress_calm_delta():
    df = _load('stress_period_f1.csv')
    if df is None or 'f1_stress_minus_calm' not in df.columns:
        return

    models = df['model_type'].unique()
    fig, axes = plt.subplots(1, len(models), figsize=(7 * len(models), 8), squeeze=False)
    fig.suptitle('F1 Stress Advantage (stress − calm)', fontsize=14, fontweight='bold')

    for ax, model in zip(axes[0], models):
        sub = (
            df[df['model_type'] == model]
            .sort_values('f1_stress_minus_calm', ascending=True)
        )
        colours = ['#2196F3' if v >= 0 else '#F44336'
                   for v in sub['f1_stress_minus_calm']]
        ax.barh(sub['retrainer'].apply(_short_name),
                sub['f1_stress_minus_calm'],
                color=colours, alpha=0.85)
        ax.axvline(0, color='black', linewidth=1)
        ax.set_title(model, fontsize=11)
        ax.set_xlabel('F1 stress − F1 calm (positive = better during stress)')

    pos_patch = mpatches.Patch(color='#2196F3', label='Better in stress (supports H1)')
    neg_patch = mpatches.Patch(color='#F44336', label='Worse in stress')
    fig.legend(handles=[pos_patch, neg_patch], loc='lower center',
               ncol=2, fontsize=9, bbox_to_anchor=(0.5, -0.02))
    plt.tight_layout()
    _save(fig, '02_stress_calm_delta.png')


# ----- 03. DETECTION LATENCY HEATMAP ----- #

def plot_detection_latency_heatmap():
    df = _load('detection_latency.csv')
    if df is None:
        return

    for model in df['model_type'].unique():
        sub = df[df['model_type'] == model]
        pivot = sub.pivot_table(
            index='retrainer', columns='event', values='latency_windows'
        )
        fig, ax = plt.subplots(figsize=(max(6, len(pivot.columns) * 2.5),
                                        max(4, len(pivot) * 0.35)))
        im = ax.imshow(pivot.values, aspect='auto', cmap='RdYlGn_r',
                       vmin=0, vmax=pivot.values[~np.isnan(pivot.values)].max()
                       if (~np.isnan(pivot.values)).any() else 1)
        ax.set_xticks(range(len(pivot.columns)))
        ax.set_xticklabels(pivot.columns, rotation=30, ha='right')
        ax.set_yticks(range(len(pivot.index)))
        ax.set_yticklabels([_short_name(r) for r in pivot.index], fontsize=7)
        for i in range(len(pivot.index)):
            for j in range(len(pivot.columns)):
                val = pivot.values[i, j]
                text = f'{val:.0f}' if not np.isnan(val) else 'N/D'
                ax.text(j, i, text, ha='center', va='center', fontsize=7)
        plt.colorbar(im, ax=ax, label='Latency (windows, lower=faster)')
        ax.set_title(f'Detection Latency — {model}', fontweight='bold')
        plt.tight_layout()
        _save(fig, f'03_detection_latency_heatmap_{model}.png')


# ----- 04. FALSE POSITIVE RATE SCATTER ----- #

def plot_false_positive_rate():
    df = _load('false_positive_rate.csv')
    if df is None:
        return

    models = df['model_type'].unique()
    fig, axes = plt.subplots(1, len(models), figsize=(6 * len(models), 5), squeeze=False)
    fig.suptitle('Retrain Precision vs Volume', fontsize=14, fontweight='bold')

    for ax, model in zip(axes[0], models):
        sub = df[df['model_type'] == model]
        for _, row in sub.iterrows():
            ax.scatter(row['total_retrains'], row['precision'],
                       color=_colour(row['exp_type']), s=60, alpha=0.8)
            ax.annotate(_short_name(row['retrainer'], 20),
                        (row['total_retrains'], row['precision']),
                        fontsize=6, alpha=0.7, textcoords='offset points', xytext=(4, 2))
        ax.set_xlabel('Total retrains')
        ax.set_ylabel('Precision (TP / total retrains)')
        ax.set_ylim(0, 1.05)
        ax.axhline(0.5, color='grey', linestyle='--', linewidth=0.8, label='50% precision')
        ax.set_title(model)
        ax.legend(fontsize=8)

    patches = [mpatches.Patch(color=c, label=k) for k, c in EXP_COLOURS.items()
               if k in df['exp_type'].values]
    fig.legend(handles=patches, loc='lower center', ncol=5, fontsize=8,
               bbox_to_anchor=(0.5, -0.04))
    plt.tight_layout()
    _save(fig, '04_false_positive_rate.png')


# ----- 05. REGIME RETRAIN RATE ----- #

def plot_regime_retrain_rate():
    df = _load('regime_retrain_rate.csv')
    if df is None:
        return
    if 'retrain_rate_stress' not in df.columns or 'retrain_rate_calm' not in df.columns:
        print('  [SKIP] regime columns missing from regime_retrain_rate.csv')
        return

    models = df['model_type'].unique()
    fig, axes = plt.subplots(1, len(models), figsize=(9 * len(models), 8), squeeze=False)
    fig.suptitle('Retrain Rate: Stress vs Calm', fontsize=14, fontweight='bold')

    for ax, model in zip(axes[0], models):
        sub = df[df['model_type'] == model].sort_values('stress_calm_ratio', ascending=False)
        y    = np.arange(len(sub))
        h    = 0.35
        ax.barh(y + h/2, sub['retrain_rate_stress'], h, label='Stress', color='#F44336', alpha=0.8)
        ax.barh(y - h/2, sub['retrain_rate_calm'],   h, label='Calm',   color='#2196F3', alpha=0.8)
        ax.set_yticks(y)
        ax.set_yticklabels([_short_name(r) for r in sub['retrainer']], fontsize=7)
        ax.set_xlabel('Retrains per window')
        ax.set_title(model)
        ax.legend()

    plt.tight_layout()
    _save(fig, '05_regime_retrain_rate.png')


# ----- 06. WILCOXON P-VALUE HEATMAP ----- #

def plot_wilcoxon_heatmap():
    df = _load('wilcoxon_results.csv')
    if df is None:
        return

    for model in df['model_type'].unique():
        sub = df[df['model_type'] == model]
        strategies = sorted(set(sub['strategy_a'].tolist() + sub['strategy_b'].tolist()))
        n = len(strategies)
        mat = np.ones((n, n))   # default 1.0 (not significant)
        idx = {s: i for i, s in enumerate(strategies)}

        for _, row in sub.iterrows():
            i, j = idx[row['strategy_a']], idx[row['strategy_b']]
            mat[i, j] = row['p_value_corrected']
            mat[j, i] = row['p_value_corrected']

        fig, ax = plt.subplots(figsize=(max(6, n * 0.8), max(5, n * 0.8)))
        im = ax.imshow(mat, cmap='RdYlGn_r', vmin=0, vmax=0.1)
        ax.set_xticks(range(n))
        ax.set_xticklabels([_short_name(s, 22) for s in strategies],
                            rotation=60, ha='right', fontsize=7)
        ax.set_yticks(range(n))
        ax.set_yticklabels([_short_name(s, 22) for s in strategies], fontsize=7)
        for i in range(n):
            for j in range(n):
                if i != j:
                    marker = '*' if mat[i, j] < 0.05 else ''
                    ax.text(j, i, f'{mat[i, j]:.3f}{marker}',
                            ha='center', va='center', fontsize=6)
        plt.colorbar(im, ax=ax, label='Corrected p-value (green=significant)')
        ax.set_title(f'Pairwise Wilcoxon p-values (Holm-Bonferroni) — {model}',
                     fontweight='bold')
        plt.tight_layout()
        _save(fig, f'06_wilcoxon_heatmap_{model}.png')


# ----- 07. EFFECT SIZE DOT PLOT ----- #

def plot_effect_size():
    df = _load('effect_size.csv')
    if df is None:
        return

    models = df['model_type'].unique()
    fig, axes = plt.subplots(1, len(models), figsize=(8 * len(models), 6), squeeze=False)
    fig.suptitle("Cohen's d: MSM vs Baselines (positive = MSM better)",
                 fontsize=13, fontweight='bold')

    for ax, model in zip(axes[0], models):
        sub = df[df['model_type'] == model].sort_values('cohens_d', ascending=True)
        colours = ['#2196F3' if sig else '#9E9E9E' for sig in sub['significant']]
        ax.barh(
            [f"{_short_name(r['strategy_a'], 20)} vs\n{_short_name(r['strategy_b'], 20)}"
             for _, r in sub.iterrows()],
            sub['cohens_d'],
            color=colours, alpha=0.85
        )
        ax.axvline(0, color='black', linewidth=1)
        for thresh, label in [(0.2, 'small'), (0.5, 'medium'), (0.8, 'large')]:
            ax.axvline(thresh,  color='grey', linestyle=':', linewidth=0.8)
            ax.axvline(-thresh, color='grey', linestyle=':', linewidth=0.8)
        ax.set_xlabel("Cohen's d")
        ax.set_title(model)

    sig_patch = mpatches.Patch(color='#2196F3', label='Significant (p_corr < 0.05)')
    ns_patch  = mpatches.Patch(color='#9E9E9E', label='Not significant')
    fig.legend(handles=[sig_patch, ns_patch], loc='lower center',
               ncol=2, fontsize=9, bbox_to_anchor=(0.5, -0.02))
    plt.tight_layout()
    _save(fig, '07_effect_size.png')


# ----- 08. SENSITIVITY HEATMAP ----- #

def plot_sensitivity_heatmap():
    df = _load('sensitivity_summary.csv')
    if df is None:
        return

    for model in df['model_type'].unique():
        for exp in df[df['model_type'] == model]['exp_type'].unique():
            sub = df[(df['model_type'] == model) & (df['exp_type'] == exp)]
            pivot = sub.pivot_table(
                index='tau_1', columns='tau_2', values='mean_f1', aggfunc='mean'
            )
            if pivot.empty:
                continue
            fig, ax = plt.subplots(figsize=(max(5, len(pivot.columns)),
                                            max(4, len(pivot))))
            im = ax.imshow(pivot.values, cmap='YlGn',
                           vmin=pivot.values.min(), vmax=pivot.values.max())
            ax.set_xticks(range(len(pivot.columns)))
            ax.set_xticklabels([f'{v:.2f}' for v in pivot.columns], fontsize=8)
            ax.set_yticks(range(len(pivot.index)))
            ax.set_yticklabels([f'{v:.2f}' for v in pivot.index], fontsize=8)
            ax.set_xlabel('tau_2')
            ax.set_ylabel('tau_1')
            for i in range(len(pivot.index)):
                for j in range(len(pivot.columns)):
                    val = pivot.values[i, j]
                    if not np.isnan(val):
                        ax.text(j, i, f'{val:.3f}', ha='center', va='center', fontsize=7)
            plt.colorbar(im, ax=ax, label='Mean F1')
            ax.set_title(f'Sensitivity: tau_1 vs tau_2 — {model} / {exp}', fontweight='bold')
            plt.tight_layout()
            _save(fig, f'08_sensitivity_heatmap_{model}_{exp}.png')


# ----- 09. F1 OVER TIME (from raw results) ----- #

def plot_f1_over_time():
    '''
    Reads all_results.csv directly (not a summary CSV) to show per-window F1.
    Plots the best MSM config and best baseline per model for direct comparison.
    Shades known stress windows.
    '''
    raw_path = 'results/experiments/all_results.csv'
    if not os.path.exists(raw_path):
        print('  [SKIP] all_results.csv not found for time-series plot.')
        return

    df = pd.read_csv(raw_path, parse_dates=['date_start'])
    df['exp_type'] = df['retrainer'].apply(
        lambda n: next((p for p in ['spy_msm', 'timeout_msm', 'msm', 'causal',
                                     'fixed', 'perf', 'adwin', 'random', 'static']
                        if n.startswith(p)), 'other')
    )

    # stress event shading
    STRESS = {
        'covid_crash':    ('2020-02-20', 120),
        'fed_hikes_2022': ('2022-03-16', 120),
    }

    for model, group in df.groupby('model_type'):
        # pick best MSM and best baseline
        msm_f1  = group[group['exp_type'].isin(MSM_TYPES)].groupby('retrainer')['f1'].mean()
        base_f1 = group[group['exp_type'].isin(BASELINE_TYPES)].groupby('retrainer')['f1'].mean()
        if msm_f1.empty or base_f1.empty:
            continue
        best_msm_name  = msm_f1.idxmax()
        best_base_name = base_f1.idxmax()

        fig, ax = plt.subplots(figsize=(14, 5))

        for name, colour, label in [
            (best_msm_name,  '#2196F3', f'MSM best ({best_msm_name})'),
            (best_base_name, '#F44336', f'Baseline best ({best_base_name})'),
        ]:
            series = (
                group[group['retrainer'] == name]
                .sort_values('date_start')
                .set_index('date_start')['f1']
                .rolling(5, min_periods=1).mean()
            )
            ax.plot(series.index, series.values, colour, linewidth=1.4, label=label, alpha=0.9)

        # shade stress windows
        for event, (date_str, days) in STRESS.items():
            centre  = pd.Timestamp(date_str)
            start   = centre - pd.Timedelta(days=days)
            end     = centre + pd.Timedelta(days=days)
            ax.axvspan(start, end, alpha=0.12, color='orange', label=event)

        ax.set_xlabel('Window start date')
        ax.set_ylabel('F1 (5-window rolling mean)')
        ax.set_title(f'F1 Over Time — {model}', fontweight='bold')
        ax.legend(fontsize=8, loc='lower left')
        plt.tight_layout()
        _save(fig, f'09_f1_over_time_{model}.png')


# ----- 10. COOLDOWN SUPPRESSION ----- #

def plot_cooldown_suppression():
    df = _load('cooldown_analysis.csv')
    if df is None:
        return

    models = df['model_type'].unique()
    fig, axes = plt.subplots(1, len(models), figsize=(8 * len(models), 8), squeeze=False)
    fig.suptitle('Cooldown Suppression Rate', fontsize=14, fontweight='bold')

    for ax, model in zip(axes[0], models):
        sub = (
            df[df['model_type'] == model]
            .sort_values('suppression_rate', ascending=True)
        )
        colours = [_colour(e) for e in sub['exp_type']]
        ax.barh(sub['retrainer'].apply(_short_name),
                sub['suppression_rate'], color=colours, alpha=0.85)
        ax.axvline(0.3, color='red', linestyle='--', linewidth=0.8,
                   label='30% threshold')
        ax.set_xlabel('Suppression rate (cooldown blocks / signals fired)')
        ax.set_title(model)
        ax.legend(fontsize=8)

    plt.tight_layout()
    _save(fig, '10_cooldown_suppression.png')


# ----- 11. CAUSAL FEATURE USAGE ----- #

def plot_causal_feature_usage():
    df = _load('causal_feature_usage.csv')
    if df is None or df.empty:
        return

    for model in df['model_type'].unique():
        sub = (
            df[df['model_type'] == model]
            .sort_values('selection_rate', ascending=True)
            .tail(20)  # top 20
        )
        fig, ax = plt.subplots(figsize=(8, max(4, len(sub) * 0.4)))
        colours = ['#2196F3' if r == 1.0 else
                   '#4CAF50' if r >= 0.5 else
                   '#FF9800' if r >= 0.2 else
                   '#9E9E9E'
                   for r in sub['selection_rate']]
        ax.barh(sub['feature'], sub['selection_rate'], color=colours, alpha=0.85)
        ax.set_xlabel('Selection rate (fraction of windows selected by PC algorithm)')
        ax.set_title(f'Causal Feature Usage — {model}', fontweight='bold')
        ax.axvline(1.0, color='blue',   linestyle='--', linewidth=0.8, label='Always selected')
        ax.axvline(0.5, color='green',  linestyle='--', linewidth=0.8, label='50% threshold')
        ax.axvline(0.2, color='orange', linestyle='--', linewidth=0.8, label='20% threshold')
        ax.legend(fontsize=8)
        plt.tight_layout()
        _save(fig, f'11_causal_feature_usage_{model}.png')


# ----- MAIN ----- #

def main():
    os.makedirs(PLOT_DIR, exist_ok=True)
    print(f'Writing plots to {PLOT_DIR}/')

    print('01 Overall F1 ranking...')
    plot_overall_f1_ranking()

    print('02 Stress/calm delta...')
    plot_stress_calm_delta()

    print('03 Detection latency heatmap...')
    plot_detection_latency_heatmap()

    print('04 False positive rate scatter...')
    plot_false_positive_rate()

    print('05 Regime retrain rate...')
    plot_regime_retrain_rate()

    print('06 Wilcoxon p-value heatmap...')
    plot_wilcoxon_heatmap()

    print('07 Effect size dot plot...')
    plot_effect_size()

    print('08 Sensitivity heatmap...')
    plot_sensitivity_heatmap()

    print('09 F1 over time...')
    plot_f1_over_time()

    print('10 Cooldown suppression...')
    plot_cooldown_suppression()

    print('11 Causal feature usage...')
    plot_causal_feature_usage()

    print(f'\nAll plots saved to {PLOT_DIR}/')


if __name__ == '__main__':
    main()
