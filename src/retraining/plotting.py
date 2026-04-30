'''
plotting.py — Thesis figures from analysis CSVs (regression task).

Design rules:
  - Every output PNG is a SINGLE panel (no subplots within a figure).
  - Multi-panel views are saved as multiple separate files.
  - Colour palette is consistent across all figures and is grouped by
    semantic family: MSM variants are in the blue/indigo family,
    baselines are in distinct non-blue colours, and "neutral / not
    significant / observer" are greys.
  - Stress events are always shaded with the same orange tone.

Outputs to results/plots/ (PER MODEL where relevant — all single panel):

  fig_01_rmse_ranking_{model}.png            best per type by RMSE w/ bootstrap CIs
  fig_02_stress_delta_{model}.png            stress-minus-calm RMSE delta
  fig_03_latency_{model}.png                 detection latency per stress event
  fig_04_precision_{model}.png               retrain precision vs volume
  fig_05_sensitivity_{model}.png             tau1/tau2 heatmap, best MSM variant only
  fig_10_effect_size_{model}.png             Cohen's d_z effect sizes
  fig_11_feature_usage.png                   causal feature selection rates (one figure)
  fig_14a_pooled_rmse_{model}.png            pooled RMSE per type (was a subplot)
  fig_14b_pooled_r2_{model}.png              pooled R² per type   (was a subplot)
  fig_15a_msm_timeseries_{model}.png         MSM time series with stress shading
  fig_15b_rmse_timeseries_{model}.png        RMSE time series with stress shading
  fig_15c_target_timeseries_{model}.png      target time series with stress shading
  fig_16_dm_heatmap_{model}_{loss}.png       Diebold-Mariano significance heatmap
  fig_17_stratified_qlike_{model}.png        stress vs calm QLIKE bars
  fig_18_cofiring_matrix_{model}.png         co-firing Jaccard heatmap
  fig_19_event_matrix_{model}.png            event detection matrix
  fig_20a_headline_timeseries.png            MSM and target time series (RQ1 opener)
  fig_20b_headline_xcorr.png                 cross-correlation bars
  fig_20c_headline_granger_summary.png       Granger test summary panel

Removed (replaced by clearer single-panel figures):
  fig_06, fig_07  — replaced by fig_15{a,b,c} / fig_16 / fig_18
  fig_09          — replaced by fig_16 (DM heatmap)
  fig_12          — replaced by fig_lead_lag in lead_lag_analysis.py
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
    'font.family':       'serif',
    'font.size':         10,
    'axes.titlesize':    12,
    'axes.titleweight':  'bold',
    'axes.labelsize':    11,
    'xtick.labelsize':   9,
    'ytick.labelsize':   9,
    'legend.fontsize':   8,
    'figure.dpi':        300,
    'savefig.dpi':       300,
    'savefig.bbox':      'tight',
})


# ────────────────────────────────────────────────────────────────────
#  Single-source palette — used by every figure for consistency
# ────────────────────────────────────────────────────────────────────
#
#  Design intent:
#  - All MSM variants live in the BLUE/INDIGO family. They are visually
#    coherent as a group while still being individually distinguishable.
#  - All baseline retrainers live in distinct, NON-BLUE colours so the
#    eye reads "MSM family vs baseline family" at a glance.
#  - Stress shading, "no significance", and observer all use neutrals
#    (orange-tan and greys).
PALETTE = {
    # MSM family (blues / indigo)
    'msm':            '#1565C0',   # primary MSM — strong cobalt
    'spy_msm':        '#0D47A1',   # SPY-focused — deep indigo
    'timeout_msm':    '#42A5F5',   # timeout — pale blue
    'weighted_msm':   '#5E35B1',   # strength-weighted — purple-blue
    'fused_msm':      '#283593',   # fused — navy
    'causal':         '#00838F',   # causal feature — teal (still blue family)

    # Baselines (each its own distinct non-blue hue)
    'static':         '#9E9E9E',   # neutral grey
    'random':         '#C62828',   # red
    'fixed':          '#FB8C00',   # orange
    'perf':           '#7B1FA2',   # purple
    'adwin':          '#2E7D32',   # green
    'revised_adwin':  '#558B2F',   # olive green (close to adwin family)

    # Other / utility
    'drift_observer': '#607D8B',   # blue-grey (intentionally muted)
    'other':          '#795548',   # brown fallback
}

# Stress shading and significance colours used directly (not from PALETTE)
STRESS_COLOR = '#FF6F00'
SIG_COLOR    = '#1565C0'   # matches MSM family — "this MSM result is significant"
NS_COLOR     = '#BDBDBD'   # not significant
WORSE_COLOR  = '#C62828'   # red — bad
BETTER_COLOR = '#1565C0'   # blue — good

EXP_LABELS = {
    'msm':            'MSM',
    'spy_msm':        'SPY-MSM',
    'timeout_msm':    'Timeout-MSM',
    'weighted_msm':   'Weighted-MSM',
    'fused_msm':      'Fused-MSM',
    'causal':         'Causal',
    'static':         'Static',
    'fixed':          'Fixed',
    'perf':           'Perf',
    'adwin':          'ADWIN',
    'revised_adwin':  'ADWIN-rev',
    'random':         'Random',
    'drift_observer': 'Observer',
}


def _drop_observer(df):
    '''B.1 — exclude drift_observer from any ranking-style figure.'''
    if df is None or df.empty or 'exp_type' not in df.columns:
        return df
    return df[df['exp_type'] != 'drift_observer'].copy()


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
    '''Add stress event shading, label first occurrence of each event.'''
    done = set()
    for name, ev in STRESS_EVENTS.items():
        s = ev - pd.Timedelta(days=STRESS_WINDOW_DAYS)
        e = ev + pd.Timedelta(days=STRESS_WINDOW_DAYS)
        ax.axvspan(s, e, alpha=0.10, color=STRESS_COLOR,
                   label=name.replace('_', ' ').title() if name not in done else None)
        done.add(name)


def _legend_patches(exp_types):
    '''Build legend patches in palette order.'''
    return [mpatches.Patch(color=_col(k), label=EXP_LABELS.get(k, k))
            for k in sorted(exp_types) if k in PALETTE]


# ════════════════════════════════════════════════════════════════════
#  fig_01 — Best per exp_type by RMSE, with bootstrap CIs
# ════════════════════════════════════════════════════════════════════
def plot_01():
    df = _load('overall_summary.csv')
    if df is None: return
    df = _drop_observer(df)

    ci_df = _load('rmse_bootstrap_ci.csv')
    if ci_df is not None:
        ci_df = _drop_observer(ci_df)

    for model in df['model_type'].unique():
        sub = df[df['model_type'] == model]
        # Best per exp_type by lowest RMSE; display ascending (best at top)
        top = (sub.sort_values('mean_rmse', ascending=True)
               .drop_duplicates('exp_type')
               .sort_values('mean_rmse', ascending=False))
        if top.empty: continue

        if ci_df is not None:
            ci_model = ci_df[ci_df['model_type'] == model]
            ci_map = {r['retrainer']: (r['ci_lower'], r['ci_upper'])
                      for _, r in ci_model.iterrows()}
            errs_lo = [top.iloc[i]['mean_rmse'] - ci_map.get(top.iloc[i]['retrainer'],
                       (top.iloc[i]['mean_rmse'], top.iloc[i]['mean_rmse']))[0]
                       for i in range(len(top))]
            errs_hi = [ci_map.get(top.iloc[i]['retrainer'],
                       (top.iloc[i]['mean_rmse'], top.iloc[i]['mean_rmse']))[1]
                       - top.iloc[i]['mean_rmse'] for i in range(len(top))]
            errs = [errs_lo, errs_hi]
        else:
            errs = None

        fig, ax = plt.subplots(figsize=(8, max(3.5, len(top) * 0.45)))
        labels = [_display_label(n, show_params=True) for n in top['retrainer']]
        colours = [_col(e) for e in top['exp_type']]

        ax.barh(labels, top['mean_rmse'], xerr=errs,
                color=colours, capsize=3, alpha=0.92, error_kw={'elinewidth': 0.8})

        ax.set_xlabel('Mean RMSE (error bars: 95% bootstrap CI) — lower is better')
        ax.set_title(f'Best Strategy per Type — {model}')
        median_rmse = top['mean_rmse'].median()
        ax.axvline(median_rmse, color='black', ls=':', lw=0.7, label='Median')

        # Annotate the best bar
        best_bar = top.iloc[-1]
        ax.annotate(f"{best_bar['mean_rmse']:.3f}",
                    xy=(best_bar['mean_rmse'], len(top) - 1),
                    xytext=(5, 0), textcoords='offset points',
                    fontsize=8, va='center', fontweight='bold')

        ax.legend(handles=_legend_patches(top['exp_type'].unique()),
                  loc='lower right', fontsize=7)
        ax.grid(alpha=0.2, axis='x')
        plt.tight_layout()
        _save(fig, f'fig_01_rmse_ranking_{model}.png')


# ════════════════════════════════════════════════════════════════════
#  fig_02 — Stress-minus-calm RMSE delta
# ════════════════════════════════════════════════════════════════════
def plot_02():
    df = _load('stress_period_rmse.csv')
    if df is None or 'rmse_stress_minus_calm' not in df.columns: return
    df = _drop_observer(df)

    for model in df['model_type'].unique():
        sub = df[df['model_type'] == model]
        top = (sub.sort_values('rmse_stress_minus_calm', ascending=True)
               .drop_duplicates('exp_type')
               .sort_values('rmse_stress_minus_calm', ascending=False))
        if top.empty: continue

        fig, ax = plt.subplots(figsize=(8, max(3.5, len(top) * 0.45)))
        labels = [_display_label(n, show_params=True) for n in top['retrainer']]
        colours = []
        for _, row in top.iterrows():
            if row.get('likely_within_noise', False):
                colours.append(NS_COLOR)
            elif row['rmse_stress_minus_calm'] > 0:
                colours.append(WORSE_COLOR)
            else:
                colours.append(BETTER_COLOR)

        ax.barh(labels, top['rmse_stress_minus_calm'], color=colours, alpha=0.92)
        ax.axvline(0, color='black', lw=0.8)
        ax.set_xlabel('RMSE (stress) − RMSE (calm)  [negative = better in stress]')
        ax.set_title(f'Stress-Period RMSE Delta — {model}')

        worse_p  = mpatches.Patch(color=WORSE_COLOR,  label='Worse in stress')
        better_p = mpatches.Patch(color=BETTER_COLOR, label='Better in stress')
        noise_p  = mpatches.Patch(color=NS_COLOR,     label='Within noise')
        ax.legend(handles=[worse_p, better_p, noise_p], fontsize=7, loc='lower right')
        ax.grid(alpha=0.2, axis='x')
        plt.tight_layout()
        _save(fig, f'fig_02_stress_delta_{model}.png')


# ════════════════════════════════════════════════════════════════════
#  fig_03 — Detection latency per event (already single panel)
# ════════════════════════════════════════════════════════════════════
def plot_03():
    df = _load('detection_latency.csv')
    if df is None: return

    for model in df['model_type'].unique():
        sub = df[(df['model_type'] == model) & df['detected']]
        missed = df[(df['model_type'] == model) & ~df['detected']]
        if sub.empty: continue

        events = sub['event'].unique()
        fig, ax = plt.subplots(figsize=(9, max(4, len(events) * 1.5)))

        for i, event in enumerate(events):
            ev_sub = sub[sub['event'] == event].sort_values('latency_windows')
            for _, row in ev_sub.iterrows():
                fastest = row.get('is_fastest_per_event', False)
                marker_size = 100 if fastest else 60
                edge = 'black' if fastest else 'none'
                ax.scatter(row['latency_windows'], i, color=_col(row['exp_type']),
                           s=marker_size, edgecolor=edge, linewidth=1.2, zorder=5)
                if fastest:
                    label = _display_label(row['retrainer'], show_params=True)
                    ax.annotate(label, (row['latency_windows'], i),
                                fontsize=7, xytext=(6, 4), textcoords='offset points',
                                fontweight='bold')

        n_missed = len(missed)
        if n_missed > 0:
            ax.text(0.98, 0.02, f'{n_missed} (retrainer, event) pairs missed',
                    transform=ax.transAxes, ha='right', va='bottom',
                    fontsize=8, color=WORSE_COLOR, style='italic')

        ax.set_yticks(range(len(events)))
        ax.set_yticklabels([e.replace('_', ' ').title() for e in events])
        ax.set_xlabel('Latency (rolling windows, lower = faster)')
        ax.set_title(f'Detection Latency per Stress Event — {model}\n'
                     f'(black-edged marker = fastest per event)')
        ax.legend(handles=_legend_patches(sub['exp_type'].unique()),
                  loc='lower right', fontsize=7)
        ax.grid(alpha=0.2, axis='x')
        plt.tight_layout()
        _save(fig, f'fig_03_latency_{model}.png')


# ════════════════════════════════════════════════════════════════════
#  fig_04 — Retrain precision vs volume (single panel scatter)
# ════════════════════════════════════════════════════════════════════
def plot_04():
    df = _load('false_positive_rate.csv')
    if df is None: return
    df = _drop_observer(df)

    for model in df['model_type'].unique():
        sub = df[df['model_type'] == model]
        fig, ax = plt.subplots(figsize=(8, 5.5))
        for _, row in sub.iterrows():
            ax.scatter(row['total_retrains'], row['precision'],
                       color=_col(row['exp_type']), s=80, alpha=0.85,
                       edgecolor='white', linewidth=1.0)
        ax.set_xlabel('Total retrains')
        ax.set_ylabel('Precision (TP / total)')
        ax.set_ylim(-0.05, 1.05)

        if 'baseline_random_fpr' in sub.columns and not sub.empty:
            baseline_precision = 1 - sub.iloc[0]['baseline_random_fpr']
            ax.axhline(baseline_precision, color='black', ls='--', lw=0.8,
                       label=f'Random baseline ({baseline_precision:.2f})')

        ax.set_title(f'Retrain Precision vs Volume — {model}')
        legend_handles = _legend_patches(sub['exp_type'].unique())
        if 'baseline_random_fpr' in sub.columns:
            legend_handles += [mpatches.Patch(color='black', label='Random baseline')]
        ax.legend(handles=legend_handles, loc='lower right', fontsize=7)
        ax.grid(alpha=0.2)
        plt.tight_layout()
        _save(fig, f'fig_04_precision_{model}.png')


# ════════════════════════════════════════════════════════════════════
#  fig_05 — MSM sensitivity heatmap (best MSM variant only, single panel)
# ════════════════════════════════════════════════════════════════════
def plot_05():
    df = _load('sensitivity_summary.csv')
    summary_df = _load('overall_summary.csv')
    if df is None or summary_df is None: return
    summary_df = _drop_observer(summary_df)

    for model in df['model_type'].unique():
        msm_summary = (summary_df[(summary_df['model_type'] == model)
                                  & summary_df['exp_type'].isin(MSM_TYPES)]
                       .sort_values('mean_rmse'))
        if msm_summary.empty:
            continue
        best_msm_exp = msm_summary.iloc[0]['exp_type']

        sub = df[(df['model_type'] == model) & (df['exp_type'] == best_msm_exp)]
        pivot = sub.pivot_table(index='tau_1', columns='tau_2',
                                values='mean_rmse', aggfunc='mean')
        if pivot.empty: continue

        fig, ax = plt.subplots(figsize=(6, 4.5))
        im = ax.imshow(pivot.values, cmap='YlGn_r', aspect='auto',
                       vmin=pivot.values.min(), vmax=pivot.values.max())
        ax.set_xticks(range(len(pivot.columns)))
        ax.set_xticklabels([f'{v:.2f}' for v in pivot.columns])
        ax.set_yticks(range(len(pivot.index)))
        ax.set_yticklabels([f'{v:.2f}' for v in pivot.index])
        ax.set_xlabel(r'$\tau_2$ (confirmation)')
        ax.set_ylabel(r'$\tau_1$ (alert)')
        for i in range(len(pivot.index)):
            for j in range(len(pivot.columns)):
                v = pivot.values[i, j]
                if not np.isnan(v):
                    ax.text(j, i, f'{v:.3f}', ha='center', va='center', fontsize=8)
        plt.colorbar(im, ax=ax, label='Mean RMSE (lower = better)')
        ax.set_title(f'MSM Sensitivity ({EXP_LABELS.get(best_msm_exp, best_msm_exp)}) '
                     f'— {model}')
        plt.tight_layout()
        _save(fig, f'fig_05_sensitivity_{model}.png')


# ════════════════════════════════════════════════════════════════════
#  fig_10 — Effect sizes (Cohen's d_z), single panel
# ════════════════════════════════════════════════════════════════════
def plot_10():
    df = _load('effect_size.csv')
    if df is None or df.empty: return

    for model in df['model_type'].unique():
        sub = df[df['model_type'] == model].sort_values('cohens_d', ascending=True)
        labels = [f"{_display_label(r['strategy_a'], show_params=True)} vs\n"
                  f"{_display_label(r['strategy_b'], show_params=True)}"
                  for _, r in sub.iterrows()]
        colours = [SIG_COLOR if sig else NS_COLOR for sig in sub['significant']]

        fig, ax = plt.subplots(figsize=(8, max(3.5, len(sub) * 0.6)))
        ax.barh(labels, sub['cohens_d'], color=colours, alpha=0.92)
        ax.axvline(0, color='black', lw=0.8)
        for t, ls in [(0.2, ':'), (0.5, '--'), (0.8, '-.')]:
            ax.axvline(t,  color='grey', ls=ls, lw=0.6)
            ax.axvline(-t, color='grey', ls=ls, lw=0.6)
        ax.set_xlabel("Cohen's d_z on RMSE  (negative = MSM better)")
        ax.set_title(f'Effect Sizes — {model}')
        sig_p = mpatches.Patch(color=SIG_COLOR, label='p < 0.05')
        ns_p  = mpatches.Patch(color=NS_COLOR,  label='Not significant')
        ax.legend(handles=[sig_p, ns_p], fontsize=8)
        ax.grid(alpha=0.2, axis='x')
        plt.tight_layout()
        _save(fig, f'fig_10_effect_size_{model}.png')


# ════════════════════════════════════════════════════════════════════
#  fig_11 — Causal feature usage (single figure, model-independent)
# ════════════════════════════════════════════════════════════════════
def plot_11():
    df = _load('causal_feature_usage.csv')
    if df is None or df.empty: return

    first_model = df['model_type'].iloc[0]
    sub = (df[df['model_type'] == first_model]
           .sort_values('selection_rate', ascending=True).tail(15))

    colours = [SIG_COLOR    if r >= 0.5
               else PALETTE['fixed'] if r >= 0.2
               else NS_COLOR
               for r in sub['selection_rate']]

    fig, ax = plt.subplots(figsize=(8, max(3.5, len(sub) * 0.4)))
    ax.barh(sub['feature'], sub['selection_rate'], color=colours, alpha=0.92)
    ax.set_xlabel('Selection rate (fraction of windows)')
    ax.set_title('Causal Feature Usage (model-independent)')
    ax.axvline(1.0, color=SIG_COLOR,        ls='--', lw=0.7, label='Always selected')
    ax.axvline(0.5, color=PALETTE['fixed'], ls='--', lw=0.7, label='≥ 50%')
    ax.axvline(0.2, color=NS_COLOR,         ls='--', lw=0.7, label='≥ 20%')
    ax.legend(fontsize=8, loc='lower right')
    ax.grid(alpha=0.2, axis='x')
    plt.tight_layout()
    _save(fig, 'fig_11_feature_usage.png')


# ════════════════════════════════════════════════════════════════════
#  fig_14a / 14b — Pooled metrics, SPLIT into two single-panel figures
# ════════════════════════════════════════════════════════════════════
def plot_14a_pooled_rmse():
    df = _load('aggregate_metrics.csv')
    if df is None: return
    df = _drop_observer(df)

    for model in df['model_type'].unique():
        sub = df[df['model_type'] == model]
        rmse_top = (sub.sort_values('rmse', ascending=True)
                    .drop_duplicates('exp_type')
                    .sort_values('rmse', ascending=False))
        if rmse_top.empty: continue

        labels = [_display_label(n, show_params=True) for n in rmse_top['retrainer']]
        colors = [_col(e) for e in rmse_top['exp_type']]

        fig, ax = plt.subplots(figsize=(8, max(3.5, len(rmse_top) * 0.45)))
        ax.barh(labels, rmse_top['rmse'], color=colors, alpha=0.92)
        ax.set_xlabel('Pooled RMSE (lower = better)')
        ax.set_title(f'Best per Type by Pooled RMSE — {model}')
        ax.grid(alpha=0.2, axis='x')
        ax.legend(handles=_legend_patches(rmse_top['exp_type'].unique()),
                  loc='lower right', fontsize=7)
        plt.tight_layout()
        _save(fig, f'fig_14a_pooled_rmse_{model}.png')


def plot_14b_pooled_r2():
    df = _load('aggregate_metrics.csv')
    if df is None: return
    df = _drop_observer(df)

    for model in df['model_type'].unique():
        sub = df[df['model_type'] == model]
        r2_top = (sub.sort_values('r2', ascending=False)
                  .drop_duplicates('exp_type')
                  .sort_values('r2', ascending=True))
        if r2_top.empty: continue

        labels = [_display_label(n, show_params=True) for n in r2_top['retrainer']]
        colors = [_col(e) for e in r2_top['exp_type']]

        fig, ax = plt.subplots(figsize=(8, max(3.5, len(r2_top) * 0.45)))
        ax.barh(labels, r2_top['r2'], color=colors, alpha=0.92)
        ax.axvline(0, color='black', lw=0.8)
        ax.set_xlabel('Pooled R² (higher = better)')
        ax.set_title(f'Best per Type by Pooled R² — {model}')
        ax.grid(alpha=0.2, axis='x')
        ax.legend(handles=_legend_patches(r2_top['exp_type'].unique()),
                  loc='lower right', fontsize=7)
        plt.tight_layout()
        _save(fig, f'fig_14b_pooled_r2_{model}.png')


# ════════════════════════════════════════════════════════════════════
#  fig_15a / 15b / 15c — Drift signal overlay split into 3 single panels
# ════════════════════════════════════════════════════════════════════
def _load_observer_data(model_df, full_df):
    '''Helper: pull observer time series and aligned target for a model.'''
    obs = model_df[model_df['retrainer'] == 'drift_observer'].sort_values('date_end')
    if obs.empty or 'graph_msm' not in obs.columns or obs['graph_msm'].isna().all():
        return None, None

    target = None
    if full_df is not None and cfg.TARGET_SECONDARY in full_df.columns:
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

    return obs, target


def _load_full_df():
    try:
        full_df = pd.read_csv('data/processed/standardized_data.csv',
                              parse_dates=['Date'])
        feat_cols = [c for c in full_df.columns
                     if c not in ('Date', cfg.TARGET_SECONDARY)]
        return full_df.dropna(subset=feat_cols).reset_index(drop=True)
    except Exception:
        return None


def plot_15a_msm_timeseries():
    raw_path = 'results/experiments/all_results.csv'
    if not os.path.exists(raw_path):
        print('  [SKIP] plot_15a: all_results.csv not found')
        return

    df = pd.read_csv(raw_path, parse_dates=['date_start', 'date_end'])
    df['exp_type'] = df['retrainer'].apply(get_experiment_type)
    full_df = _load_full_df()

    for model, model_df in df.groupby('model_type'):
        obs, _ = _load_observer_data(model_df, full_df)
        if obs is None: continue

        fig, ax = plt.subplots(figsize=(11, 4.5))
        ax.plot(obs['date_end'], obs['graph_msm'],
                color=PALETTE['msm'], lw=1.4, label='Graph MSM')
        if 'spy_msm' in obs.columns and not obs['spy_msm'].isna().all():
            ax.plot(obs['date_end'], obs['spy_msm'],
                    color=PALETTE['spy_msm'], lw=1.2, alpha=0.85,
                    label='SPY-focused MSM')
        _shade(ax)
        ax.set_ylabel('MSM')
        ax.set_ylim(0, 1.05)
        ax.set_xlabel('Window end date')
        ax.set_title(f'MSM Time Series with Stress Events — {model}\n'
                     f'(observer run — frozen model)')
        ax.legend(loc='lower left', fontsize=8, ncol=3)
        ax.grid(alpha=0.2)
        plt.tight_layout()
        _save(fig, f'fig_15a_msm_timeseries_{model}.png')


def plot_15b_rmse_timeseries():
    raw_path = 'results/experiments/all_results.csv'
    if not os.path.exists(raw_path):
        print('  [SKIP] plot_15b: all_results.csv not found')
        return

    df = pd.read_csv(raw_path, parse_dates=['date_start', 'date_end'])
    df['exp_type'] = df['retrainer'].apply(get_experiment_type)
    full_df = _load_full_df()

    for model, model_df in df.groupby('model_type'):
        obs, _ = _load_observer_data(model_df, full_df)
        if obs is None: continue

        fig, ax = plt.subplots(figsize=(11, 4.5))
        rmse_rolling = obs.set_index('date_end')['rmse'].rolling(3, min_periods=1).mean()
        ax.plot(rmse_rolling.index, rmse_rolling.values,
                color=PALETTE['random'], lw=1.4,
                label='Frozen-model RMSE (3-window rolling)')
        _shade(ax)
        ax.set_ylabel('RMSE')
        ax.set_xlabel('Window end date')
        ax.set_title(f'Frozen-Model RMSE with Stress Events — {model}\n'
                     f'(observer run — model never retrained)')
        ax.legend(loc='lower left', fontsize=8, ncol=3)
        ax.grid(alpha=0.2)
        plt.tight_layout()
        _save(fig, f'fig_15b_rmse_timeseries_{model}.png')


def plot_15c_target_timeseries():
    raw_path = 'results/experiments/all_results.csv'
    if not os.path.exists(raw_path):
        print('  [SKIP] plot_15c: all_results.csv not found')
        return

    df = pd.read_csv(raw_path, parse_dates=['date_start', 'date_end'])
    df['exp_type'] = df['retrainer'].apply(get_experiment_type)
    full_df = _load_full_df()
    if full_df is None or cfg.TARGET_SECONDARY not in full_df.columns:
        print('  [SKIP] plot_15c: target unavailable')
        return

    for model, model_df in df.groupby('model_type'):
        obs, target = _load_observer_data(model_df, full_df)
        if obs is None or target is None: continue

        fig, ax = plt.subplots(figsize=(11, 4.5))
        ax.plot(obs['date_end'], target, color=PALETTE['perf'], lw=1.4,
                label=cfg.TARGET_SECONDARY)
        _shade(ax)
        ax.set_ylabel(cfg.TARGET_SECONDARY)
        ax.set_xlabel('Window end date')
        ax.set_title(f'Regime-Normalised Returns Target with Stress Events — {model}')
        ax.legend(loc='lower left', fontsize=8, ncol=3)
        ax.grid(alpha=0.2)
        plt.tight_layout()
        _save(fig, f'fig_15c_target_timeseries_{model}.png')


# ════════════════════════════════════════════════════════════════════
#  fig_16 — Diebold-Mariano significance heatmap (single panel)
# ════════════════════════════════════════════════════════════════════
def plot_16_dm_heatmap():
    df = _load('dm_test.csv')
    if df is None or df.empty: return
    for (model, loss), grp in df.groupby(['model_type', 'loss']):
        msm_names = sorted(grp['msm_retrainer'].unique())
        base_names = sorted(grp['baseline_retrainer'].unique())
        mat = np.full((len(msm_names), len(base_names)), np.nan)
        for _, row in grp.iterrows():
            i = msm_names.index(row['msm_retrainer'])
            j = base_names.index(row['baseline_retrainer'])
            sign = -1 if row['msm_better'] else 1
            mat[i, j] = sign * row['p_value']

        fig, ax = plt.subplots(figsize=(max(7, len(base_names) * 1.3),
                                         max(4.5, len(msm_names) * 0.5)))
        norm = TwoSlopeNorm(vmin=-0.05, vcenter=0, vmax=0.05)
        im = ax.imshow(mat, cmap='RdYlGn_r', norm=norm, aspect='auto')
        ax.set_xticks(range(len(base_names)))
        ax.set_xticklabels([_display_label(b, show_params=True) for b in base_names],
                           rotation=45, ha='right')
        ax.set_yticks(range(len(msm_names)))
        ax.set_yticklabels([_display_label(m, show_params=True) for m in msm_names])
        ax.set_title(f'Diebold-Mariano {loss} — {model}\n'
                     f'(green = MSM sig better, red = MSM sig worse, * = p < 0.05)')
        for i in range(len(msm_names)):
            for j in range(len(base_names)):
                v = mat[i, j]
                if not np.isnan(v):
                    sig = '*' if abs(v) < 0.05 else ''
                    ax.text(j, i, f'{abs(v):.3f}{sig}',
                            ha='center', va='center', fontsize=7)
        plt.colorbar(im, ax=ax, label='Signed p-value (negative = MSM better)')
        plt.tight_layout()
        _save(fig, f'fig_16_dm_heatmap_{model}_{loss}.png')


# ════════════════════════════════════════════════════════════════════
#  fig_17 — Stratified QLIKE bars (single panel per model)
# ════════════════════════════════════════════════════════════════════
def plot_17_stratified_qlike():
    df = _load('stratified_qlike.csv')
    if df is None or df.empty: return
    df = _drop_observer(df)
    if 'mean_qlike_stress' not in df.columns or 'mean_qlike_calm' not in df.columns:
        return

    for model in df['model_type'].unique():
        sub = df[df['model_type'] == model]
        best = (sub.sort_values('mean_qlike_stress', ascending=True)
                .drop_duplicates('exp_type'))
        best = best.sort_values('mean_qlike_stress', ascending=True)
        if best.empty: continue

        labels = [f"{EXP_LABELS.get(e, e)}" for e in best['exp_type']]
        x = np.arange(len(best))
        width = 0.4

        fig, ax = plt.subplots(figsize=(max(8, len(best) * 1.0), 5.5))
        ax.bar(x - width / 2, best['mean_qlike_stress'], width,
               label='Stress windows',
               color=WORSE_COLOR, alpha=0.88, edgecolor='white', linewidth=0.5)
        ax.bar(x + width / 2, best['mean_qlike_calm'], width,
               label='Calm windows',
               color=BETTER_COLOR, alpha=0.88, edgecolor='white', linewidth=0.5)

        # Sample-size annotation
        if 'n_windows_stress' in best.columns and 'n_windows_calm' in best.columns:
            try:
                n_s = int(best['n_windows_stress'].dropna().iloc[0])
                n_c = int(best['n_windows_calm'].dropna().iloc[0])
                ax.text(0.99, 0.99,
                        f'n_stress = {n_s}, n_calm = {n_c}',
                        transform=ax.transAxes, ha='right', va='top',
                        fontsize=9, style='italic',
                        bbox=dict(boxstyle='round', facecolor='white',
                                  alpha=0.85, edgecolor='grey'))
            except Exception:
                pass

        ax.set_xticks(x)
        ax.set_xticklabels(labels, rotation=25, ha='right', fontsize=9)
        ax.set_ylabel('Mean QLIKE (lower = better)')
        ax.set_title(f'Stratified QLIKE: Best per Type, Stress vs Calm — {model}')
        ax.legend(fontsize=9, loc='upper left')
        ax.grid(alpha=0.2, axis='y')
        plt.tight_layout()
        _save(fig, f'fig_17_stratified_qlike_{model}.png')


# ════════════════════════════════════════════════════════════════════
#  fig_18 — Co-firing matrix (single panel)
# ════════════════════════════════════════════════════════════════════
def plot_18_cofiring_matrix():
    df = _load('cofiring_analysis.csv')
    if df is None or df.empty: return

    for model in df['model_type'].unique():
        sub = df[df['model_type'] == model]
        if sub.empty: continue

        msm_names  = sorted(sub['msm_retrainer'].unique())
        base_names = sorted(sub['baseline_retrainer'].unique())
        mat = np.full((len(msm_names), len(base_names)), np.nan)
        for _, row in sub.iterrows():
            i = msm_names.index(row['msm_retrainer'])
            j = base_names.index(row['baseline_retrainer'])
            mat[i, j] = row['jaccard_overlap']

        fig, ax = plt.subplots(figsize=(max(7, len(base_names) * 1.1),
                                         max(4.5, len(msm_names) * 0.55)))
        im = ax.imshow(mat, cmap='Reds', vmin=0, vmax=0.6, aspect='auto')
        ax.set_xticks(range(len(base_names)))
        ax.set_xticklabels([_display_label(b, show_params=True) for b in base_names],
                           rotation=45, ha='right')
        ax.set_yticks(range(len(msm_names)))
        ax.set_yticklabels([_display_label(m, show_params=True) for m in msm_names])

        for i in range(len(msm_names)):
            for j in range(len(base_names)):
                v = mat[i, j]
                if not np.isnan(v):
                    txt_col = 'white' if v > 0.3 else 'black'
                    ax.text(j, i, f'{v:.2f}', ha='center', va='center',
                            fontsize=7, color=txt_col)

        plt.colorbar(im, ax=ax, label='Jaccard overlap')
        ax.set_title(f'Co-firing Matrix: MSM vs Baseline Retrainers — {model}\n'
                     f'(low values = orthogonal triggering pattern)')
        plt.tight_layout()
        _save(fig, f'fig_18_cofiring_matrix_{model}.png')


# ════════════════════════════════════════════════════════════════════
#  fig_19 — Event detection matrix (single panel)
# ════════════════════════════════════════════════════════════════════
def plot_19_event_detection():
    df = _load('detection_latency.csv')
    if df is None or df.empty: return

    for model in df['model_type'].unique():
        sub = df[df['model_type'] == model]

        det_only = sub[sub['detected']]
        if det_only.empty: continue
        means = (det_only.groupby(['exp_type', 'retrainer'])['latency_windows']
                 .mean().reset_index()
                 .sort_values('latency_windows'))
        best = means.drop_duplicates('exp_type')
        retrainer_set = best['retrainer'].tolist()

        events = sorted(sub['event'].unique())
        n_r, n_e = len(retrainer_set), len(events)

        mat = np.full((n_r, n_e), np.nan)
        text = np.empty((n_r, n_e), dtype=object)
        for i, retr in enumerate(retrainer_set):
            for j, ev in enumerate(events):
                row = sub[(sub['retrainer'] == retr) & (sub['event'] == ev)]
                if row.empty:
                    text[i, j] = '—'
                    continue
                r0 = row.iloc[0]
                if r0['detected']:
                    mat[i, j] = r0['latency_windows']
                    text[i, j] = f"{r0['latency_windows']:.1f}"
                else:
                    text[i, j] = 'miss'

        fig, ax = plt.subplots(figsize=(max(7, n_e * 1.6 + 2), max(3.5, n_r * 0.55)))
        im = ax.imshow(mat, cmap='RdYlGn_r', vmin=0, vmax=4, aspect='auto')

        ax.set_xticks(range(n_e))
        ax.set_xticklabels([e.replace('_', ' ').title() for e in events],
                           rotation=15, ha='right', fontsize=10)
        ax.set_yticks(range(n_r))
        ax.set_yticklabels([_display_label(r, show_params=True) for r in retrainer_set])

        for i in range(n_r):
            for j in range(n_e):
                v = mat[i, j]
                lab = text[i, j]
                if lab == 'miss':
                    ax.add_patch(plt.Rectangle((j - 0.5, i - 0.5), 1, 1,
                                               facecolor='#E0E0E0', edgecolor='none'))
                    ax.text(j, i, 'miss', ha='center', va='center',
                            fontsize=8, color='#424242', style='italic')
                elif lab == '—':
                    ax.text(j, i, '—', ha='center', va='center',
                            fontsize=8, color='#757575')
                else:
                    txt_col = 'white' if not np.isnan(v) and v > 2 else 'black'
                    ax.text(j, i, lab, ha='center', va='center',
                            fontsize=8, color=txt_col)

        plt.colorbar(im, ax=ax, label='Latency (windows)')
        ax.set_title(f'Event Detection Matrix: Best per Type — {model}\n'
                     f'(grey = event missed entirely)')
        plt.tight_layout()
        _save(fig, f'fig_19_event_matrix_{model}.png')


# ════════════════════════════════════════════════════════════════════
#  fig_20a / 20b / 20c — Headline lead-lag, SPLIT into 3 single panels
# ════════════════════════════════════════════════════════════════════
def _resolve_observer_path():
    for m in ('xgboost', 'lr', 'rf'):
        path = f'results/experiments/{m}/drift_observer/drift_observer_results.csv'
        if os.path.exists(path):
            return path
    return None


def _headline_data():
    '''Shared loader for fig_20a/20b/20c. Returns (row, obs, target, signal_name)
    or (None, None, None, None) if data unavailable.'''
    lag_df = _load('lead_lag_results.csv')
    if lag_df is None or lag_df.empty:
        return None, None, None, None

    primary = lag_df[(lag_df['model_type'] == 'shared_across_models')
                     & (lag_df['signal'] == 'spy_msm')
                     & (lag_df['vs'] == cfg.TARGET_SECONDARY)]
    if primary.empty:
        primary = lag_df[(lag_df['model_type'] == 'shared_across_models')
                         & (lag_df['signal'] == 'graph_msm')
                         & (lag_df['vs'] == cfg.TARGET_SECONDARY)]
    if primary.empty:
        return None, None, None, None
    row = primary.iloc[0]
    signal_name = row['signal']

    obs_path = _resolve_observer_path()
    if obs_path is None:
        return row, None, None, signal_name

    obs = pd.read_csv(obs_path, parse_dates=['date_start', 'date_end'])
    obs = obs.sort_values('window').reset_index(drop=True)

    full_df = _load_full_df()
    target = []
    if full_df is not None and cfg.TARGET_SECONDARY in full_df.columns:
        for _, r in obs.iterrows():
            mask = full_df['Date'] > pd.Timestamp(r['date_end'])
            slc = full_df[mask].head(21)
            target.append(slc[cfg.TARGET_SECONDARY].mean() if len(slc) > 0 else np.nan)
        target = np.array(target)
    else:
        target = np.full(len(obs), np.nan)

    return row, obs, target, signal_name


def plot_20a_headline_timeseries():
    row, obs, target, signal_name = _headline_data()
    if row is None or obs is None:
        print('  [SKIP] fig_20a: headline data unavailable')
        return

    msm_col = signal_name if signal_name in obs.columns else 'graph_msm'
    msm_series = obs[msm_col].values

    def _norm(x):
        v = ~np.isnan(x)
        if not v.any(): return x
        mn, mx = x[v].min(), x[v].max()
        if mx - mn < 1e-10: return x
        return (x - mn) / (mx - mn)

    msm_n = _norm(msm_series)
    tgt_n = _norm(target)

    fig, ax = plt.subplots(figsize=(11, 5))
    ax.plot(obs['date_end'], msm_n,
            color=PALETTE['msm'], lw=1.5,
            label=f'{signal_name} (normalised)')
    ax.plot(obs['date_end'], tgt_n,
            color=PALETTE['perf'], lw=1.5, alpha=0.78,
            label=f'{cfg.TARGET_SECONDARY} (normalised)')
    _shade(ax)
    ax.set_ylabel('Normalised value [0, 1]')
    ax.set_xlabel('Window end date')
    ax.set_title(f'Headline Lead-Lag Evidence — '
                 f'{signal_name} predictively leads {cfg.TARGET_SECONDARY}')
    ax.legend(loc='lower left', fontsize=9, ncol=3)
    ax.grid(alpha=0.2)
    plt.tight_layout()
    _save(fig, 'fig_20a_headline_timeseries.png')


def plot_20b_headline_xcorr():
    row, obs, target, signal_name = _headline_data()
    if row is None or obs is None:
        print('  [SKIP] fig_20b: headline data unavailable')
        return

    msm_col = signal_name if signal_name in obs.columns else 'graph_msm'
    msm_series = obs[msm_col].values

    valid = ~(np.isnan(msm_series) | np.isnan(target))
    a, b = msm_series[valid], target[valid]
    if len(a) < 15:
        print('  [SKIP] fig_20b: too few aligned observations')
        return

    max_lag = 10
    lags = list(range(-max_lag, max_lag + 1))
    corrs = []
    for lg in lags:
        if lg > 0:
            x, y = a[:-lg], b[lg:]
        elif lg < 0:
            x, y = a[-lg:], b[:lg]
        else:
            x, y = a, b
        if len(x) > 5 and np.var(x) > 0 and np.var(y) > 0:
            corrs.append(float(np.corrcoef(x, y)[0, 1]))
        else:
            corrs.append(np.nan)

    fig, ax = plt.subplots(figsize=(9, 5))
    colors = [PALETTE['msm']    if lg > 0
              else WORSE_COLOR  if lg < 0
              else PALETTE['static']
              for lg in lags]
    ax.bar(lags, corrs, color=colors, alpha=0.88, edgecolor='white', linewidth=0.5)
    ax.axhline(0, color='black', lw=0.8)
    ax.axvline(0, color='grey', lw=0.6, ls='--')

    valid_corrs = [(lg, c) for lg, c in zip(lags, corrs) if not np.isnan(c)]
    if valid_corrs:
        peak_lag, peak_corr = max(valid_corrs, key=lambda t: abs(t[1]))
        ax.axvline(peak_lag, color=PALETTE['fixed'], lw=2, alpha=0.75,
                   label=f'Peak at lag {peak_lag} (r = {peak_corr:.3f})')

    if not pd.isna(row.get('xc_ci_lower', np.nan)):
        ax.text(0.02, 0.98,
                f"Block-bootstrap 95% CI at best lag:\n"
                f"[{row['xc_ci_lower']:.3f}, {row['xc_ci_upper']:.3f}]",
                transform=ax.transAxes, va='top', ha='left',
                fontsize=9, style='italic',
                bbox=dict(boxstyle='round', facecolor='white',
                          alpha=0.92, edgecolor='grey'))

    ax.set_xlabel('Lag (positive = MSM leads target)')
    ax.set_ylabel('Cross-correlation')
    ax.set_title(f'Cross-Correlation: {signal_name} vs {cfg.TARGET_SECONDARY}')
    ax.legend(loc='lower right', fontsize=9)
    ax.grid(alpha=0.2, axis='y')
    plt.tight_layout()
    _save(fig, 'fig_20b_headline_xcorr.png')


def plot_20c_headline_granger_summary():
    row, _, _, signal_name = _headline_data()
    if row is None:
        print('  [SKIP] fig_20c: lead_lag_results.csv missing primary row')
        return

    p_val    = row['granger_min_p']
    best_lag = row['granger_best_lag']
    sig_str  = 'SIGNIFICANT' if row.get('granger_significant', False) else 'not significant'
    sig_col  = '#2E7D32' if row.get('granger_significant', False) else WORSE_COLOR
    n_obs    = (int(row['granger_n_obs'])
                if not pd.isna(row.get('granger_n_obs', np.nan)) else None)

    p_text = f"p = {p_val:.4f}"
    if p_val < 0.001:
        p_text = "p < 0.001"

    summary_text = (
        f'Granger Causality Test\n'
        f'──────────────────────\n\n'
        f'  H₀: {signal_name} does not Granger-cause\n'
        f'      {cfg.TARGET_SECONDARY}\n\n'
        f'  Best lag         : {best_lag} window(s)\n'
        f'  Test p-value     : {p_text}\n'
        f'  Outcome          : {sig_str}\n'
    )
    if n_obs is not None:
        summary_text += f'  Observations     : n = {n_obs}\n'

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.axis('off')
    ax.text(0.05, 0.92, summary_text,
            transform=ax.transAxes, va='top', ha='left',
            family='monospace', fontsize=11)
    ax.text(0.05, 0.30, sig_str,
            transform=ax.transAxes, va='top', ha='left',
            fontsize=18, fontweight='bold', color=sig_col,
            bbox=dict(boxstyle='round', facecolor='white',
                      edgecolor=sig_col, linewidth=2))

    fig.suptitle('Granger-Causality Summary — Headline RQ1 Result',
                 fontsize=12, fontweight='bold', y=0.99)
    plt.tight_layout()
    _save(fig, 'fig_20c_headline_granger_summary.png')


def main():
    os.makedirs(PLOT_DIR, exist_ok=True)
    print(f'Writing plots to {PLOT_DIR}/\n')

    print('--- Headline RQ1 figures (Chapter 4 opener) ---')
    plot_20a_headline_timeseries()
    plot_20b_headline_xcorr()
    plot_20c_headline_granger_summary()

    print('\n--- Core ranking and evidence figures ---')
    plot_01()
    plot_02()
    plot_03()
    plot_04()
    plot_05()
    plot_10()
    plot_11()
    plot_14a_pooled_rmse()
    plot_14b_pooled_r2()

    print('\n--- Time series figures (15a/b/c) ---')
    plot_15a_msm_timeseries()
    plot_15b_rmse_timeseries()
    plot_15c_target_timeseries()

    print('\n--- Diebold-Mariano + new thesis-supporting figures ---')
    plot_16_dm_heatmap()
    plot_17_stratified_qlike()
    plot_18_cofiring_matrix()
    plot_19_event_detection()

    print('Run lead_lag_analysis.py for the diagnostic fig_lead_lag and fig_msm_target_overlay figures.')


if __name__ == '__main__':
    main()