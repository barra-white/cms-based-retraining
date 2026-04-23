'''
plotting.py — Thesis figures from analysis CSVs (regression task).

Outputs to results/plots/:
    fig_01  Mean RMSE by strategy (top per exp_type)
    fig_02  RMSE stress-minus-calm delta
    fig_03  Detection latency
    fig_04  Retrain precision vs volume
    fig_05  MSM sensitivity heatmap
    fig_09  Wilcoxon p-value heatmap
    fig_10  Effect size dot plot
    fig_11  Causal feature usage
    fig_14  Aggregate regression metrics (RMSE, R²)
    fig_15  Drift signal overlay (MSM + RMSE + target)
'''

import os
import re
import sys
import warnings

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.colors import TwoSlopeNorm


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
warnings.filterwarnings('ignore', category=FutureWarning)

import config as cfg
from analysis import load_results

STRESS_EVENTS        = cfg.STRESS_EVENTS
STRESS_WINDOW_DAYS   = cfg.STRESS_WINDOW_DAYS
MSM_TYPES            = cfg.MSM_TYPES
BASELINE_TYPES       = cfg.BASELINE_TYPES
get_experiment_type  = cfg.get_experiment_type

ANALYSIS_DIR = 'results/analysis'
PLOT_DIR     = 'results/plots'

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
    'drift_observer': '#455A64',
    'other':       '#795548',
}

EXP_LABELS = {
    'msm': 'MSM', 'spy_msm': 'SPY-MSM', 'timeout_msm': 'Timeout-MSM',
    'causal': 'Causal', 'static': 'Static', 'fixed': 'Fixed',
    'perf': 'Perf', 'adwin': 'ADWIN', 'random': 'Random',
    'drift_observer': 'Observer',
}


def _display_label(retrainer_name, show_params=False):
    exp = get_experiment_type(retrainer_name)
    base = EXP_LABELS.get(exp, exp)

    if not show_params:
        return base

    if exp in MSM_TYPES and exp != 'causal':
        t1 = re.search(r'tau_1_([\d.]+)', retrainer_name)
        t2 = re.search(r'tau_2_([\d.]+)', retrainer_name)
        if t1 and t2:
            return f'{base}-{t1.group(1)}/{t2.group(1)}'

    if exp == 'fixed':
        iv = re.search(r'fixed_(\d+)', retrainer_name)
        if iv:
            return f'{base}-{iv.group(1)}m'

    if exp == 'perf':
        dt = re.search(r'drop_([\d.]+)', retrainer_name)
        if dt:
            return f'{base}-{dt.group(1)}'

    if exp == 'adwin':
        dl = re.search(r'delta_([\d.]+)', retrainer_name)
        if dl:
            return f'{base}-{dl.group(1)}'

    return base


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


def _shade(ax):
    done = set()
    for name, ev in STRESS_EVENTS.items():
        s = ev - pd.Timedelta(days=STRESS_WINDOW_DAYS)
        e = ev + pd.Timedelta(days=STRESS_WINDOW_DAYS)
        ax.axvspan(s, e, alpha=0.10, color='#FF6F00',
                   label=name.replace('_', ' ').title() if name not in done else None)
        done.add(name)


def _legend_patches(exp_types):
    return [mpatches.Patch(color=_col(k), label=EXP_LABELS.get(k, k))
            for k in sorted(exp_types) if k in PALETTE]


def plot_01():
    '''Best strategy per exp_type by RMSE (lower = better), with bootstrap CIs.'''
    df = _load('overall_summary.csv')
    if df is None: return

    ci_df = _load('rmse_bootstrap_ci.csv')

    for model in df['model_type'].unique():
        sub = df[df['model_type'] == model]
        # Best per exp_type = lowest RMSE. Display ascending (best at top).
        top = (sub.sort_values('mean_rmse', ascending=True)
               .drop_duplicates('exp_type')
               .sort_values('mean_rmse', ascending=False))
        if top.empty: continue

        if ci_df is not None:
            ci_model = ci_df[ci_df['model_type'] == model]
            ci_map = {r['retrainer']: (r['ci_lower'], r['ci_upper'])
                      for _, r in ci_model.iterrows()}
            errs_lo = [top.iloc[i]['mean_rmse'] - ci_map.get(top.iloc[i]['retrainer'], (top.iloc[i]['mean_rmse'], top.iloc[i]['mean_rmse']))[0]
                       for i in range(len(top))]
            errs_hi = [ci_map.get(top.iloc[i]['retrainer'], (top.iloc[i]['mean_rmse'], top.iloc[i]['mean_rmse']))[1] - top.iloc[i]['mean_rmse']
                       for i in range(len(top))]
            errs = [errs_lo, errs_hi]
        else:
            errs = None

        fig, ax = plt.subplots(figsize=(7, max(3, len(top) * 0.45)))
        labels = [_display_label(n, show_params=True) for n in top['retrainer']]
        colours = [_col(e) for e in top['exp_type']]

        ax.barh(labels, top['mean_rmse'], xerr=errs,
                color=colours, capsize=3, alpha=0.9, error_kw={'elinewidth': 0.8})

        ax.set_xlabel('Mean RMSE (error bars = 95% bootstrap CI) — lower is better')
        ax.set_title(f'Best Strategy per Type — {model}')
        median_rmse = top['mean_rmse'].median()
        ax.axvline(median_rmse, color='black', ls=':', lw=0.7, label='Median')

        best_bar = top.iloc[-1]   # last row is the best since sorted descending
        ax.annotate(f"{best_bar['mean_rmse']:.3f}",
                    xy=(best_bar['mean_rmse'], len(top) - 1),
                    xytext=(5, 0), textcoords='offset points',
                    fontsize=8, va='center', fontweight='bold')

        ax.legend(handles=_legend_patches(top['exp_type'].unique()), loc='lower right', fontsize=7)
        plt.tight_layout()
        _save(fig, f'fig_01_rmse_ranking_{model}.png')


def plot_02():
    '''Stress-minus-calm RMSE delta. Positive delta = WORSE in stress (red).'''
    df = _load('stress_period_rmse.csv')
    if df is None or 'rmse_stress_minus_calm' not in df.columns: return

    for model in df['model_type'].unique():
        sub = df[df['model_type'] == model]
        # Best = lowest delta (least stress degradation). Sort with best at top.
        top = (sub.sort_values('rmse_stress_minus_calm', ascending=True)
               .drop_duplicates('exp_type')
               .sort_values('rmse_stress_minus_calm', ascending=False))
        if top.empty: continue

        fig, ax = plt.subplots(figsize=(7, max(3, len(top) * 0.45)))
        labels = [_display_label(n, show_params=True) for n in top['retrainer']]
        colours = []
        for _, row in top.iterrows():
            if row.get('likely_within_noise', False):
                colours.append('#BDBDBD')
            elif row['rmse_stress_minus_calm'] > 0:
                colours.append('#C62828')  # worse in stress
            else:
                colours.append('#1976D2')  # better in stress

        ax.barh(labels, top['rmse_stress_minus_calm'], color=colours, alpha=0.9)
        ax.axvline(0, color='black', lw=0.8)
        ax.set_xlabel('RMSE (stress) − RMSE (calm)  [positive = worse in stress]')
        ax.set_title(f'Stress-Period RMSE Delta — {model}')

        worse_p = mpatches.Patch(color='#C62828', label='Worse in stress')
        better_p = mpatches.Patch(color='#1976D2', label='Better in stress')
        noise_p = mpatches.Patch(color='#BDBDBD', label='Within noise')
        ax.legend(handles=[worse_p, better_p, noise_p], fontsize=7, loc='lower right')
        plt.tight_layout()
        _save(fig, f'fig_02_stress_delta_{model}.png')


def plot_03():
    '''Detection latency per event; mark fastest per event.'''
    df = _load('detection_latency.csv')
    if df is None: return

    for model in df['model_type'].unique():
        sub = df[(df['model_type'] == model) & df['detected']]
        missed = df[(df['model_type'] == model) & ~df['detected']]
        if sub.empty: continue

        events = sub['event'].unique()
        fig, ax = plt.subplots(figsize=(8, max(4, len(events) * 1.5)))

        for i, event in enumerate(events):
            ev_sub = sub[sub['event'] == event].sort_values('latency_windows')
            for _, row in ev_sub.iterrows():
                label = _display_label(row['retrainer'], show_params=True)
                fastest = row.get('is_fastest_per_event', False)
                marker_size = 90 if fastest else 55
                edge = 'black' if fastest else 'none'
                ax.scatter(row['latency_windows'], i, color=_col(row['exp_type']),
                           s=marker_size, edgecolor=edge, linewidth=1.2, zorder=5)
                if fastest:
                    ax.annotate(label, (row['latency_windows'], i),
                                fontsize=7, xytext=(5, 4), textcoords='offset points',
                                fontweight='bold')

        n_missed = len(missed)
        if n_missed > 0:
            ax.text(0.98, 0.02, f'{n_missed} (retrainer, event) pairs missed',
                    transform=ax.transAxes, ha='right', va='bottom',
                    fontsize=7, color='#C62828', style='italic')

        ax.set_yticks(range(len(events)))
        ax.set_yticklabels([e.replace('_', ' ').title() for e in events])
        ax.set_xlabel('Latency (rolling windows, lower = faster)')
        ax.set_title(f'Detection Latency — {model}\n(black edge = fastest per event)')
        ax.legend(handles=_legend_patches(sub['exp_type'].unique()), loc='lower right', fontsize=7)
        ax.grid(alpha=0.2, axis='x')
        plt.tight_layout()
        _save(fig, f'fig_03_latency_{model}.png')


def plot_04():
    df = _load('false_positive_rate.csv')
    if df is None: return
    for model in df['model_type'].unique():
        sub = df[df['model_type'] == model]
        fig, ax = plt.subplots(figsize=(7, 5))
        for _, row in sub.iterrows():
            ax.scatter(row['total_retrains'], row['precision'],
                       color=_col(row['exp_type']), s=60, alpha=0.8)
        ax.set_xlabel('Total retrains')
        ax.set_ylabel('Precision (TP / total)')
        ax.set_ylim(-0.05, 1.05)

        if 'baseline_random_fpr' in sub.columns and not sub.empty:
            baseline_precision = 1 - sub.iloc[0]['baseline_random_fpr']
            ax.axhline(baseline_precision, color='black', ls='--', lw=0.8,
                       label=f'Random baseline ({baseline_precision:.2f})')

        ax.set_title(f'Retrain Precision vs Volume — {model}')
        ax.legend(handles=_legend_patches(sub['exp_type'].unique()) +
                  [mpatches.Patch(color='black', label='Random baseline')]
                  if 'baseline_random_fpr' in sub.columns else _legend_patches(sub['exp_type'].unique()),
                  loc='lower right', fontsize=7)
        ax.grid(alpha=0.2)
        plt.tight_layout()
        _save(fig, f'fig_04_precision_{model}.png')


def plot_05():
    '''tau_1 vs tau_2 heatmap. Lower RMSE = better; colour map inverted.'''
    df = _load('sensitivity_summary.csv')
    if df is None: return
    for model in df['model_type'].unique():
        for exp in df[df['model_type'] == model]['exp_type'].unique():
            sub = df[(df['model_type'] == model) & (df['exp_type'] == exp)]
            pivot = sub.pivot_table(index='tau_1', columns='tau_2', values='mean_rmse', aggfunc='mean')
            if pivot.empty: continue

            fig, ax = plt.subplots(figsize=(5, 4))
            # Reverse colour map so LOW RMSE (good) is green.
            im = ax.imshow(pivot.values, cmap='YlGn_r', aspect='auto',
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
            plt.colorbar(im, ax=ax, label='Mean RMSE (lower = better)')
            ax.set_title(f'Sensitivity: $\\tau_1$ vs $\\tau_2$ — {model}/{EXP_LABELS.get(exp, exp)}')
            plt.tight_layout()
            _save(fig, f'fig_05_sensitivity_{model}_{exp}.png')


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

        labels = [_display_label(s, show_params=True) for s in strategies]

        fig, ax = plt.subplots(figsize=(max(5, n * 0.9), max(4, n * 0.9)))
        im = ax.imshow(mat, cmap='RdYlGn_r', vmin=0, vmax=0.1)
        ax.set_xticks(range(n))
        ax.set_xticklabels(labels, rotation=55, ha='right', fontsize=7)
        ax.set_yticks(range(n))
        ax.set_yticklabels(labels, fontsize=7)
        for i in range(n):
            for j in range(n):
                if i != j:
                    mk = '*' if mat[i, j] < 0.05 else ''
                    ax.text(j, i, f'{mat[i,j]:.3f}{mk}', ha='center', va='center', fontsize=6)
        plt.colorbar(im, ax=ax, label='Corrected p-value')
        ax.set_title(f'Wilcoxon Tests (Holm-Bonferroni) — {model}')
        plt.tight_layout()
        _save(fig, f'fig_09_wilcoxon_{model}.png')


def plot_10():
    '''Effect sizes. For regression, negative Cohen's d on RMSE = MSM better
    (lower RMSE). See significance_test.py for the subtraction order.'''
    df = _load('effect_size.csv')
    if df is None or df.empty: return
    for model in df['model_type'].unique():
        sub = df[df['model_type'] == model].sort_values('cohens_d', ascending=True)
        labels = [f"{_display_label(r['strategy_a'], show_params=True)} vs\n"
                  f"{_display_label(r['strategy_b'], show_params=True)}"
                  for _, r in sub.iterrows()]
        colours = ['#1976D2' if sig else '#BDBDBD' for sig in sub['significant']]

        fig, ax = plt.subplots(figsize=(7, max(3, len(sub) * 0.6)))
        ax.barh(labels, sub['cohens_d'], color=colours, alpha=0.9)
        ax.axvline(0, color='black', lw=0.8)
        for t, ls in [(0.2, ':'), (0.5, '--'), (0.8, '-.')]:
            ax.axvline(t, color='grey', ls=ls, lw=0.6)
            ax.axvline(-t, color='grey', ls=ls, lw=0.6)
        ax.set_xlabel("Cohen's d on RMSE  (negative = MSM better)")
        ax.set_title(f'Effect Sizes — {model}')
        sig_p = mpatches.Patch(color='#1976D2', label='p < 0.05')
        ns_p  = mpatches.Patch(color='#BDBDBD', label='Not significant')
        ax.legend(handles=[sig_p, ns_p], fontsize=7)
        plt.tight_layout()
        _save(fig, f'fig_10_effect_size_{model}.png')


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


def plot_14_aggregate_metrics():
    '''Plot RMSE and R² side-by-side for best per exp_type.'''
    df = _load('aggregate_metrics.csv')
    if df is None: return

    for model in df['model_type'].unique():
        sub = df[df['model_type'] == model]
        fig, axes = plt.subplots(1, 2, figsize=(14, 5))

        # RMSE panel: lower = better → sort ascending, best at top.
        rmse_top = (sub.sort_values('rmse', ascending=True)
                    .drop_duplicates('exp_type')
                    .sort_values('rmse', ascending=False))
        labels = [_display_label(n, show_params=True) for n in rmse_top['retrainer']]
        colors = [_col(e) for e in rmse_top['exp_type']]
        axes[0].barh(labels, rmse_top['rmse'], color=colors, alpha=0.9)
        axes[0].set_xlabel('Pooled RMSE (lower = better)')
        axes[0].set_title(f'Best per Type by RMSE — {model}')
        axes[0].grid(alpha=0.2, axis='x')
        axes[0].legend(handles=_legend_patches(rmse_top['exp_type'].unique()),
                       loc='lower right', fontsize=7)

        # R² panel: higher = better.
        r2_top = (sub.sort_values('r2', ascending=False)
                  .drop_duplicates('exp_type')
                  .sort_values('r2', ascending=True))
        labels = [_display_label(n, show_params=True) for n in r2_top['retrainer']]
        colors = [_col(e) for e in r2_top['exp_type']]
        axes[1].barh(labels, r2_top['r2'], color=colors, alpha=0.9)
        axes[1].axvline(0, color='black', lw=0.8)
        axes[1].set_xlabel('Pooled R² (higher = better)')
        axes[1].set_title(f'Best per Type by R² — {model}')
        axes[1].grid(alpha=0.2, axis='x')
        axes[1].legend(handles=_legend_patches(r2_top['exp_type'].unique()),
                       loc='lower right', fontsize=7)

        plt.tight_layout()
        _save(fig, f'fig_14_aggregate_{model}.png')


def plot_15_drift_overlay():
    '''MSM + RMSE + secondary target time series with stress shading.'''
    raw_path = 'results/experiments/all_results.csv'
    if not os.path.exists(raw_path):
        print('  [SKIP] plot_15: all_results.csv not found')
        return

    df = pd.read_csv(raw_path, parse_dates=['date_start', 'date_end'])
    df['exp_type'] = df['retrainer'].apply(get_experiment_type)

    target_available = False
    try:
        full_df = pd.read_csv('data/processed/standardized_data.csv', parse_dates=['Date'])
        feat_cols = [c for c in full_df.columns if c not in ('Date', cfg.TARGET_SECONDARY)]
        full_df = full_df.dropna(subset=feat_cols).reset_index(drop=True)
        target_available = cfg.TARGET_SECONDARY in full_df.columns
    except Exception:
        full_df = None

    for model, model_df in df.groupby('model_type'):
        obs = model_df[model_df['retrainer'] == 'drift_observer'].sort_values('date_end')
        if obs.empty or 'graph_msm' not in obs.columns or obs['graph_msm'].isna().all():
            print(f'  [SKIP] plot_15 {model}: no observer data')
            continue

        target = None
        if target_available and full_df is not None:
            target = []
            for _, row in obs.iterrows():
                date_end = pd.Timestamp(row['date_end'])
                mask = (full_df['Date'] > date_end)
                window_slice = full_df[mask].head(21)
                if len(window_slice) > 0:
                    target.append(window_slice[cfg.TARGET_SECONDARY].mean())
                else:
                    target.append(np.nan)
            target = np.array(target)

        n_panels = 3 if target is not None else 2
        heights = [1.2, 1, 1] if n_panels == 3 else [1.2, 1]
        fig, axes = plt.subplots(n_panels, 1, figsize=(12, 3 + 2 * n_panels),
                                 sharex=True, gridspec_kw={'height_ratios': heights})

        ax_msm = axes[0]
        ax_msm.plot(obs['date_end'], obs['graph_msm'], color='#1976D2', lw=1.3, label='Graph MSM')
        if 'spy_msm' in obs.columns and not obs['spy_msm'].isna().all():
            ax_msm.plot(obs['date_end'], obs['spy_msm'], color='#0D47A1', lw=1.1,
                        label='SPY-focused MSM', alpha=0.75)
        _shade(ax_msm)
        ax_msm.set_ylabel('MSM')
        ax_msm.set_ylim(0, 1.05)
        ax_msm.legend(loc='lower left', fontsize=8)
        ax_msm.set_title(f'MSM, RMSE, and {cfg.TARGET_SECONDARY} Over Time — {model}\n'
                         f'(observer run — frozen model)')
        ax_msm.grid(alpha=0.2)

        ax_rmse = axes[1]
        rmse_rolling = obs.set_index('date_end')['rmse'].rolling(3, min_periods=1).mean()
        ax_rmse.plot(rmse_rolling.index, rmse_rolling.values, color='#C62828', lw=1.3,
                     label='RMSE (3-window rolling)')
        _shade(ax_rmse)
        ax_rmse.set_ylabel('RMSE')
        ax_rmse.legend(loc='lower left', fontsize=8)
        ax_rmse.grid(alpha=0.2)

        if target is not None:
            ax_tgt = axes[2]
            ax_tgt.plot(obs['date_end'], target, color='#7B1FA2', lw=1.3,
                        label=cfg.TARGET_SECONDARY)
            _shade(ax_tgt)
            ax_tgt.set_ylabel(cfg.TARGET_SECONDARY)
            ax_tgt.set_xlabel('Window end date')
            ax_tgt.legend(loc='lower left', fontsize=8)
            ax_tgt.grid(alpha=0.2)
        else:
            ax_rmse.set_xlabel('Window end date')

        plt.tight_layout()
        _save(fig, f'fig_15_drift_overlay_{model}.png')
        
def plot_dm_heatmap():
    df = _load('dm_test.csv')
    if df is None: return
    for (model, loss), grp in df.groupby(['model_type', 'loss']):
        msm_names = grp['msm_retrainer'].unique()
        base_names = grp['baseline_retrainer'].unique()
        mat = np.full((len(msm_names), len(base_names)), np.nan)
        for _, row in grp.iterrows():
            i = list(msm_names).index(row['msm_retrainer'])
            j = list(base_names).index(row['baseline_retrainer'])
            sign = -1 if row['msm_better'] else 1
            mat[i, j] = sign * row['p_value']
        fig, ax = plt.subplots(figsize=(max(6, len(base_names) * 1.2),
                                         max(4, len(msm_names) * 0.4)))
        # green = MSM wins (neg p), red = MSM loses (pos p), white = tie
        norm = TwoSlopeNorm(vmin=-0.05, vcenter=0, vmax=0.05)
        im = ax.imshow(mat, cmap='RdYlGn_r', norm=norm, aspect='auto')
        ax.set_xticks(range(len(base_names)))
        ax.set_xticklabels([_display_label(b) for b in base_names], rotation=45, ha='right')
        ax.set_yticks(range(len(msm_names)))
        ax.set_yticklabels([_display_label(m) for m in msm_names])
        ax.set_title(f'Diebold-Mariano {loss} — {model}\n(green = MSM sig better, red = MSM sig worse)')
        for i in range(len(msm_names)):
            for j in range(len(base_names)):
                v = mat[i, j]
                if not np.isnan(v):
                    sig = '*' if abs(v) < 0.05 else ''
                    ax.text(j, i, f'{abs(v):.3f}{sig}', ha='center', va='center', fontsize=7)
        plt.colorbar(im, ax=ax, label='Signed p-value (neg = MSM better)')
        plt.tight_layout()
        _save(fig, f'fig_16_dm_heatmap_{model}_{loss}.png')


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
    plot_14_aggregate_metrics()
    plot_15_drift_overlay()

    print(f'\nCore plots saved. Run drift_analysis.py for fig_06, 07, 08, 12.')
    print(f'Run lead_lag_analysis.py for fig_lead_lag and fig_msm_target_overlay.')


if __name__ == '__main__':
    main()
