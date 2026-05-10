'''
plotting.py — Thesis figures (rebuild).

DESIGN PRINCIPLES (apply to every figure):
  1. One panel per file. No in-figure explanatory text — captions belong in
     the thesis prose. Strategy labels stay (functional). Statistical results
     go in the prose, never in the figure.
  2. Drop trailing NaN windows so time-series plots end at the last real data
     point rather than dragging an empty x-axis to today.
  3. No twin y-axes. When two series with different scales must coexist, use
     two stacked panels sharing an x-axis (one figure file, two panels).
  4. Padding: every value-annotated bar gets ymax * 1.18 so labels never clip.
  5. Sort consistency: every retrainer-comparison figure sorts by mean stress
     QLIKE, same y-position across figures when the retrainer recurs.
  6. Single colour per exp_type family. No tertile colour-coding inside bars.
  7. Variable resolution: edge tuples (i, j, lag) are resolved to readable
     strings 'GLD_lr → SPY_lr @ lag 1' via the graph variable list.
  8. Default model for the main-text version of each figure is xgboost. Per-
     model variants saved with model suffix for the appendix.

OUTPUT FILES (15 main-text figures + appendix variants):

  RQ1
    fig_rq1_overlay.png                MSM (top) + target (bottom), shared x-axis
    fig_rq1_xcorr.png                  cross-correlation bars (no in-figure annotations)
    fig_rq1_per_event_lead.png         per-event MSM lead-time bar chart
    fig_rq1_msm_timeline.png           MSM time series alone (full date range)

  RQ2
    fig_rq2_jaccard_distribution.png   single-colour density of all 72 Jaccards
    fig_rq2_jaccard_heatmap.png        xgboost heatmap, marginals annotated

  RQ3
    fig_rq3_dm_winrate.png             clean stacked-bar wins/ties/losses
    fig_rq3_stratified_qlike.png       xgboost stress vs calm
    fig_rq3_msm_vs_best_baseline.png   single-panel head-to-head

  RQ4
    fig_rq4_retrains.png               retrain count comparison only
    fig_rq4_stress_qlike.png           stress QLIKE comparison only
    fig_rq4_selectivity_xgboost.png    selectivity scatter (single model)

  RQ5
    fig_rq5_feature_persistence.png    causal feature usage
    fig_rq5_unstable_edges.png         readable edge labels

  Appendix
    fig_app_rmse_ranking_xgboost.png
    fig_app_sensitivity_xgboost.png
    fig_app_friedman_ranks_xgboost.png
'''

import ast
import os
import re
import sys
import warnings
import math

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

STRESS_EVENTS       = cfg.STRESS_EVENTS
STRESS_WINDOW_DAYS  = cfg.STRESS_WINDOW_DAYS
MSM_TYPES           = cfg.MSM_TYPES
BASELINE_TYPES      = cfg.BASELINE_TYPES
get_experiment_type = cfg.get_experiment_type

ANALYSIS_DIR = 'results/analysis'
PLOT_DIR     = 'results/plots'

# Pin the main-text model. Per-model variants saved separately for appendix.
MAIN_MODEL = 'xgboost'

# Causal-graph variable order — must match generate_causal_graphs.py
GRAPH_VAR_NAMES = [
    'SPY_lr', 'GLD_lr', 'UUP_lr', 'USO_lr', 'VIX_ld',
    'OVX', 'MOVE_d', 'T10Y2Y_d', 'BAA10Y_d', 'DGS10_d', 'USEPUINDXD_ld'
]

plt.rcParams.update({
    'font.family':       'serif',
    'font.size':         10,
    'axes.titlesize':    12,
    'axes.titleweight':  'bold',
    'axes.labelsize':    11,
    'xtick.labelsize':   9,
    'ytick.labelsize':   9,
    'legend.fontsize':   9,
    'figure.dpi':        300,
    'savefig.dpi':       300,
    'savefig.bbox':      'tight',
})

# Colour palette: one colour per exp_type family
PALETTE = {
    'msm':            '#1565C0',   # dark blue
    'spy_msm':        '#D84315',   # burnt orange
    'timeout_msm':    '#2E7D32',   # dark green
    'weighted_msm':   '#6A1B9A',   # purple
    'fused_msm':      '#00838F',   # teal
    'causal':         '#F9A825',   # amber
    'static':         '#9E9E9E',
    'random':         '#C62828',
    'fixed':          '#FB8C00',
    'perf':           '#7B1FA2',
    'adwin':          '#37474F',
    'revised_adwin':  '#558B2F',
    'drift_observer': '#607D8B',
}

STRESS_COLOR = '#FF6F00'
WIN_COLOR    = '#2E7D32'
LOSS_COLOR   = '#C62828'
TIE_COLOR    = '#BDBDBD'
NEUTRAL      = '#9E9E9E'

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

# ────────────────────────────────────────────────────────────────────
#  Helpers
# ────────────────────────────────────────────────────────────────────

def _drop_observer(df):
    if df is None or df.empty or 'exp_type' not in df.columns:
        return df
    return df[df['exp_type'] != 'drift_observer'].copy()


def _label(retrainer_name):
    '''Compact retrainer label. Hyperparameter values omitted by default.'''
    return EXP_LABELS.get(get_experiment_type(retrainer_name),
                          get_experiment_type(retrainer_name))


def _label_full(retrainer_name):
    '''Full label including hyperparameters.'''
    exp = get_experiment_type(retrainer_name)
    base = EXP_LABELS.get(exp, exp)
    if exp in MSM_TYPES and exp != 'causal':
        t1 = re.search(r'tau_1_([\d.]+)', retrainer_name)
        t2 = re.search(r'tau_2_([\d.]+)', retrainer_name)
        if t1 and t2:
            return f'{base} ({t1.group(1)}/{t2.group(1)})'
    if exp == 'fixed':
        iv = re.search(r'fixed_(\d+)', retrainer_name)
        if iv: return f'{base}-{iv.group(1)}'
    if exp == 'perf':
        dt = re.search(r'drop_([\d.]+)', retrainer_name)
        if dt: return f'{base}-{dt.group(1)}'
    if exp == 'adwin':
        dl = re.search(r'delta_([\d.]+)', retrainer_name)
        if dl: return f'{base}-{dl.group(1)}'
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
    fig.savefig(path, bbox_inches='tight', pad_inches=0.15)
    plt.close(fig)
    print(f'  Saved: {path}')


def _col(exp):
    return PALETTE.get(str(exp), '#607D8B')


def _shade_stress(ax, label_first=True):
    '''Shade stress event windows. Single legend entry across all events.'''
    label_used = False
    for name, ev in STRESS_EVENTS.items():
        s = ev - pd.Timedelta(days=STRESS_WINDOW_DAYS)
        e = ev + pd.Timedelta(days=STRESS_WINDOW_DAYS)
        lbl = 'Stress event window' if (label_first and not label_used) else None
        ax.axvspan(s, e, alpha=0.13, color=STRESS_COLOR, label=lbl, zorder=0)
        label_used = True


def _resolve_edge(edge_str):
    '''
    Parse edge tuple/string into 'SOURCE -> TARGET @ lag N' format.

    Accepts strings like '(2, 0, 1)' or already-parsed tuples.
    Falls back to the raw string if parsing fails.
    '''
    try:
        if isinstance(edge_str, str):
            parsed = ast.literal_eval(edge_str)
        else:
            parsed = edge_str
        if isinstance(parsed, tuple) and len(parsed) == 3:
            src, tgt, lag = parsed
            src_name = GRAPH_VAR_NAMES[src] if 0 <= src < len(GRAPH_VAR_NAMES) else f'V{src}'
            tgt_name = GRAPH_VAR_NAMES[tgt] if 0 <= tgt < len(GRAPH_VAR_NAMES) else f'V{tgt}'
            return f'{src_name} -> {tgt_name} (lag {lag})'
    except (SyntaxError, ValueError, IndexError):
        pass
    return str(edge_str)


def _load_full_df():
    try:
        full_df = pd.read_csv('data/processed/standardized_data.csv',
                              parse_dates=['Date'])
        feat_cols = [c for c in full_df.columns
                     if c not in ('Date', cfg.TARGET_SECONDARY)]
        return full_df.dropna(subset=feat_cols).reset_index(drop=True)
    except Exception:
        return None


def _resolve_observer_path():
    for m in (MAIN_MODEL, 'xgboost', 'lr', 'rf'):
        path = f'results/experiments/{m}/drift_observer/drift_observer_results.csv'
        if os.path.exists(path):
            return path
    return None


def _headline_data():
    '''
    Load the headline lead-lag row + observer windows + per-window RMSE target.

    Signal priority:
      1. Per-model (MAIN_MODEL) graph_msm → RMSE  — the significant result
      2. shared_across_models graph_msm            — fallback if above absent

    Target: the 'rmse' column from the observer results CSV (one value per
    evaluation window, already aligned by construction). This avoids the
    aggregate_metrics.csv which is a summary file with no per-window dates.
    '''
    lag_df = _load('lead_lag_results.csv')
    if lag_df is None or lag_df.empty:
        return None, None, None, None

    # Normalise 'vs' for case-insensitive matching
    if 'vs' in lag_df.columns:
        vs_upper = lag_df['vs'].str.upper()
    else:
        vs_upper = pd.Series([''] * len(lag_df), index=lag_df.index)

    # Primary: per-model significant result (graph_msm → RMSE, xgboost)
    primary = lag_df[
        (lag_df['model_type'] == MAIN_MODEL) &
        (lag_df['signal'] == 'graph_msm') &
        (vs_upper == 'RMSE')
    ]
    if primary.empty:
        # Fallback: shared graph_msm row (non-RMSE target)
        primary = lag_df[
            (lag_df['model_type'] == 'shared_across_models') &
            (lag_df['signal'] == 'graph_msm')
        ]
    if primary.empty:
        return None, None, None, None

    row = primary.iloc[0]
    signal_name = row['signal']

    obs_path = _resolve_observer_path()
    if obs_path is None:
        return row, None, None, signal_name

    obs = pd.read_csv(obs_path, parse_dates=['date_start', 'date_end'])
    obs = obs.sort_values('window').reset_index(drop=True)

    # Use per-window RMSE directly from the observer results CSV.
    # This is guaranteed to be aligned with obs rows (same file, same windows).
    # aggregate_metrics.csv is a summary file only — do NOT use it here.
    if 'rmse' in obs.columns:
        target = obs['rmse'].values.astype(float)
    else:
        target = np.full(len(obs), np.nan)

    return row, obs, target, signal_name


def _trim_trailing_nan(obs, target):
    '''Drop trailing rows where target is NaN. Returns (obs_trimmed, target_trimmed).'''
    if target is None or len(target) == 0:
        return obs, target
    valid = ~np.isnan(target)
    if not valid.any():
        return obs, target
    last_valid = np.where(valid)[0].max() + 1
    return obs.iloc[:last_valid].reset_index(drop=True), target[:last_valid]


# ════════════════════════════════════════════════════════════════════
#  RQ1 — Predictive lead
# ════════════════════════════════════════════════════════════════════

def plot_rq1_overlay():
    '''
    Two stacked panels sharing x-axis.
    Top:    graph_msm (full observer record).
    Bottom: per-window RMSE from the drift observer (trimmed to last valid).

    The bottom panel shows rolling RMSE rather than a financial return series
    because the Granger test is graph_msm → RMSE (significant for both
    XGBoost p=0.021 and RF p=0.028). RMSE is always positive so no zero-line.
    '''
    row, obs, target, signal_name = _headline_data()
    if row is None or obs is None:
        print('  [SKIP] rq1_overlay')
        return

    obs_full = obs.copy()
    target_valid_mask = ~np.isnan(target) if target is not None else None
    if target is not None and target_valid_mask.any():
        last_target_idx = np.where(target_valid_mask)[0].max() + 1
        obs_target = obs_full.iloc[:last_target_idx].copy()
        target_trim = target[:last_target_idx]
    else:
        obs_target = obs_full
        target_trim = target

    msm_col = signal_name if signal_name in obs_full.columns else 'graph_msm'
    msm_series = obs_full[msm_col].values

    fig, (ax_top, ax_bot) = plt.subplots(
        2, 1, figsize=(11, 6), sharex=True,
        gridspec_kw={'height_ratios': [1, 1], 'hspace': 0.12}
    )

    # Top panel: full graph_msm signal
    ax_top.plot(obs_full['date_end'], msm_series,
                color=PALETTE['msm'], lw=1.6, zorder=3)
    _shade_stress(ax_top)
    ax_top.set_ylabel('graph_msm\n(causal edge persistence)')
    ax_top.set_ylim(0, 1.05)
    ax_top.grid(alpha=0.25, axis='y')

    # Bottom panel: per-window RMSE (drift observer, MAIN_MODEL)
    # RMSE is always positive — no zero-line needed.
    ax_bot.plot(obs_target['date_end'], target_trim,
                color='#D81B60', lw=1.4, zorder=3)
    _shade_stress(ax_bot, label_first=False)
    ax_bot.set_ylabel('Rolling RMSE\n(drift observer)')
    ax_bot.set_xlabel('Window end date')
    ax_bot.grid(alpha=0.25, axis='y')

    # Both panels share the full MSM x-range so stress shading is consistent
    full_xlim = (obs_full['date_end'].min(), obs_full['date_end'].max())
    ax_top.set_xlim(full_xlim)
    ax_bot.set_xlim(full_xlim)

    handles, labels = ax_top.get_legend_handles_labels()
    if handles:
        ax_top.legend(handles, labels, loc='lower left', framealpha=0.9)

    _save(fig, 'fig_rq1_overlay.png')



def plot_rq1_xcorr():
    row, obs, target, signal_name = _headline_data()
    if row is None or obs is None:
        print('  [SKIP] rq1_xcorr')
        return

    obs, target = _trim_trailing_nan(obs, target)
    msm_col = signal_name if signal_name in obs.columns else 'graph_msm'
    msm_series = obs[msm_col].values
    valid = ~(np.isnan(msm_series) | np.isnan(target))
    a, b = msm_series[valid], target[valid]
    if len(a) < 15:
        return

    max_lag = 8
    lags = list(range(-max_lag, max_lag + 1))
    corrs = []
    for lg in lags:
        if lg > 0:   x, y = a[:-lg], b[lg:]
        elif lg < 0: x, y = a[-lg:], b[:lg]
        else:        x, y = a, b
        corrs.append(float(np.corrcoef(x, y)[0, 1])
                     if len(x) > 5 and np.var(x) > 0 and np.var(y) > 0
                     else np.nan)

    granger_lag = (int(row['granger_best_lag'])
                   if not pd.isna(row.get('granger_best_lag', np.nan)) else None)
    granger_p   = row.get('granger_min_p', np.nan)

    fig, ax = plt.subplots(figsize=(10, 5))

    NEUTRAL_BAR = '#BDBDBD'
    GRANGER_BAR = PALETTE['msm']   # blue — the meaningful result
    colors = [NEUTRAL_BAR] * len(lags)
    if granger_lag is not None and granger_lag in lags:
        colors[lags.index(granger_lag)] = GRANGER_BAR

    ax.bar(lags, corrs, color=colors, alpha=0.92,
           edgecolor='black', linewidth=0.5)
    ax.axhline(0, color='black', lw=0.8)
    ax.axvline(0, color='grey', lw=0.5, ls=':')
    ax.set_xlabel('Lag (windows). Positive lag = MSM leads RMSE.')
    ax.set_ylabel('Pearson cross-correlation')
    ax.set_xticks(lags)
    ax.grid(alpha=0.25, axis='y')

    # Build label safely — no quotes inside f-string
    if granger_lag is not None and not pd.isna(granger_p):
        granger_label = f'Granger-best lag = {granger_lag} (p = {granger_p:.3f})'
    elif granger_lag is not None:
        granger_label = f'Granger-best lag = {granger_lag}'
    else:
        granger_label = 'Granger-best lag'

    legend_elements = [
        mpatches.Patch(color=GRANGER_BAR, label=granger_label),
        mpatches.Patch(color=NEUTRAL_BAR, label='Other lags'),
    ]
    ax.legend(handles=legend_elements, loc='lower right')

    _save(fig, 'fig_rq1_xcorr.png')




# ════════════════════════════════════════════════════════════════════
#  RQ2 — Structural distinctness
# ════════════════════════════════════════════════════════════════════

def plot_rq2_jaccard_distribution():
    '''
    Single histogram in one colour, with mean and median lines clearly
    separated.  No three-model overlap; per-model breakdown handled by
    the heatmap.  Bin range capped to actual data extent.
    '''
    df = _load('cofiring_analysis.csv')
    if df is None or df.empty:
        return

    vals = df['jaccard_overlap'].values
    upper = max(vals.max() + 0.02, 0.30)
    bin_edges = np.linspace(0, upper, 16)

    fig, ax = plt.subplots(figsize=(9, 5))
    ax.hist(vals, bins=bin_edges, color=PALETTE['msm'], alpha=0.85,
            edgecolor='white', linewidth=0.8)

    mean_v   = vals.mean()
    median_v = np.median(vals)
    ax.axvline(mean_v, color=LOSS_COLOR, ls='--', lw=2.0,
               label=f'Mean = {mean_v:.3f}')
    ax.axvline(median_v, color='black', ls=':', lw=1.6,
               label=f'Median = {median_v:.3f}')

    ax.set_xlabel('Jaccard overlap (MSM trigger windows ∩ baseline trigger windows)')
    ax.set_ylabel('Count of MSM-baseline pairs')
    ax.set_xlim(0, upper)

    n_below_02 = int((vals < 0.20).sum())
    n_zero = int((vals == 0).sum())
    legend_extras = [
        mpatches.Patch(color='none', label=f'  n = {len(vals)} pairs'),
        mpatches.Patch(color='none', label=f'  {n_below_02} below 0.20'),
        mpatches.Patch(color='none', label=f'  {n_zero} at exactly zero'),
    ]
    handles, labels = ax.get_legend_handles_labels()
    handles += legend_extras
    ax.legend(handles=handles, loc='upper right', framealpha=0.9)
    ax.grid(alpha=0.25, axis='y')

    _save(fig, 'fig_rq2_jaccard_distribution.png')


def plot_rq2_jaccard_heatmap(model=None):
    '''
    Heatmap with row and column means as marginal annotations on the axes.
    Default to MAIN_MODEL (xgboost). Other models saved to appendix.
    '''
    df = _load('cofiring_analysis.csv')
    if df is None or df.empty: return

    models = [model] if model else df['model_type'].unique()
    for m in models:
        sub = df[df['model_type'] == m]
        if sub.empty: continue
        msm_names  = sorted(sub['msm_retrainer'].unique())
        base_names = sorted(sub['baseline_retrainer'].unique())
        mat = np.full((len(msm_names), len(base_names)), np.nan)
        for _, r in sub.iterrows():
            i = msm_names.index(r['msm_retrainer'])
            j = base_names.index(r['baseline_retrainer'])
            mat[i, j] = r['jaccard_overlap']

        msm_labels  = [_label(n) for n in msm_names]
        base_labels = [_label_full(n) for n in base_names]

        fig, ax = plt.subplots(figsize=(max(8, len(base_names) * 1.3),
                                         max(5, len(msm_names) * 0.6)))
        im = ax.imshow(mat, cmap='Reds', vmin=0, vmax=0.30, aspect='auto')

        ax.set_xticks(range(len(base_names)))
        ax.set_xticklabels(base_labels, rotation=40, ha='right')
        ax.set_yticks(range(len(msm_names)))
        ax.set_yticklabels(msm_labels)
        ax.set_xlabel('Baseline retrainer')
        ax.set_ylabel('MSM retrainer')

        for i in range(len(msm_names)):
            for j in range(len(base_names)):
                v = mat[i, j]
                if not np.isnan(v):
                    txt_col = 'white' if v > 0.18 else 'black'
                    ax.text(j, i, f'{v:.2f}', ha='center', va='center',
                            fontsize=8, color=txt_col)

        cbar = plt.colorbar(im, ax=ax, label='Jaccard overlap', pad=0.02)
        cbar.ax.tick_params(labelsize=8)

        # Main-text version (model=MAIN_MODEL passed explicitly): no suffix
        # Per-model variants for appendix: always model suffix
        if model == MAIN_MODEL:
            _save(fig, 'fig_rq2_jaccard_heatmap.png')
        else:
            _save(fig, f'fig_rq2_jaccard_heatmap_{m}.png')


# ════════════════════════════════════════════════════════════════════
#  RQ3 — DM and asymmetric loss
# ════════════════════════════════════════════════════════════════════

def plot_rq3_dm_winrate():
    '''
    Stacked bars of DM significant wins / ties / losses per (model, loss).
    No in-figure totals annotation — the figures speak for themselves.
    '''
    df = _load('dm_test.csv')
    if df is None or df.empty:
        return

    records = []
    for (m, loss), grp in df.groupby(['model_type', 'loss']):
        sig = grp[grp['significant_at_0.05']]
        n_total = len(grp)
        wins = int((sig['msm_better'] == True).sum())
        losses = int((sig['msm_better'] == False).sum())
        ties = n_total - wins - losses
        records.append({
            'label': f'{m} ({loss})',
            'model': m, 'loss': loss,
            'wins': wins, 'losses': losses, 'ties': ties, 'total': n_total,
        })
    summary = pd.DataFrame(records).sort_values(['model', 'loss']).reset_index(drop=True)
    if summary.empty: return

    fig, ax = plt.subplots(figsize=(10, max(4, len(summary) * 0.5 + 1)))
    y = np.arange(len(summary))

    ax.barh(y, summary['wins'],
            color=WIN_COLOR, alpha=0.92, label='MSM wins (p<0.05)',
            edgecolor='white', linewidth=0.8)
    ax.barh(y, summary['ties'], left=summary['wins'],
            color=TIE_COLOR, alpha=0.85, label='Tie (p≥0.05)',
            edgecolor='white', linewidth=0.8)
    ax.barh(y, summary['losses'],
            left=summary['wins'] + summary['ties'],
            color=LOSS_COLOR, alpha=0.92, label='MSM losses (p<0.05)',
            edgecolor='white', linewidth=0.8)

    for i, row in summary.iterrows():
        if row['wins'] > 0:
            ax.text(row['wins'] / 2, i, str(row['wins']),
                    ha='center', va='center', fontweight='bold',
                    color='white', fontsize=10)
        if row['ties'] > 0:
            ax.text(row['wins'] + row['ties'] / 2, i, str(row['ties']),
                    ha='center', va='center', color='#424242', fontsize=9)
        if row['losses'] > 0:
            ax.text(row['wins'] + row['ties'] + row['losses'] / 2, i,
                    str(row['losses']),
                    ha='center', va='center', fontweight='bold',
                    color='white', fontsize=10)

    ax.set_yticks(y)
    ax.set_yticklabels(summary['label'])
    ax.set_xlabel('Number of MSM-vs-baseline pairwise comparisons')
    ax.set_xlim(0, summary['total'].max() * 1.05)
    ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.16),
              ncol=3, framealpha=0.9, frameon=True)
    ax.grid(alpha=0.25, axis='x')
    plt.subplots_adjust(bottom=0.25)

    _save(fig, 'fig_rq3_dm_winrate.png')


def plot_rq3_msm_vs_best_baseline():
    '''
    Direct head-to-head: best MSM vs best non-static baseline on stress QLIKE,
    one bar pair per model. Single-panel, value labels only (no parenthesised
    retrainer name on bar — saved for the legend below).
    '''
    df = _load('stratified_qlike.csv')
    if df is None or df.empty: return
    df = _drop_observer(df)
    if 'mean_qlike_stress' not in df.columns: return

    BASELINE_NO_STATIC = BASELINE_TYPES - {'static'}
    records = []
    for m in df['model_type'].unique():
        grp = df[df['model_type'] == m]
        msm = grp[grp['exp_type'].isin(MSM_TYPES)]
        base = grp[grp['exp_type'].isin(BASELINE_NO_STATIC)]
        if msm.empty or base.empty: continue
        bm = msm.sort_values('mean_qlike_stress').iloc[0]
        bb = base.sort_values('mean_qlike_stress').iloc[0]
        records.append({
            'model': m,
            'msm_q': bm['mean_qlike_stress'],
            'base_q': bb['mean_qlike_stress'],
            'msm_label': _label(bm['retrainer']),
            'base_label': _label(bb['retrainer']),
        })
    sub = pd.DataFrame(records)
    if sub.empty: return

    fig, ax = plt.subplots(figsize=(9, 5))
    x = np.arange(len(sub))
    width = 0.36

    ax.bar(x - width / 2, sub['msm_q'], width,
           color=PALETTE['msm'], alpha=0.92,
           label='Best MSM',
           edgecolor='white', linewidth=0.6)
    ax.bar(x + width / 2, sub['base_q'], width,
           color=PALETTE['perf'], alpha=0.92,
           label='Best non-static baseline',
           edgecolor='white', linewidth=0.6)

    ymax = max(sub['msm_q'].max(), sub['base_q'].max())
    ax.set_ylim(0, ymax * 1.30)

    for i, row in sub.iterrows():
        ax.text(i - width / 2, row['msm_q'] + ymax * 0.01,
                f'{row["msm_q"]:.3f}\n{row["msm_label"]}',
                ha='center', va='bottom', fontsize=8)
        ax.text(i + width / 2, row['base_q'] + ymax * 0.01,
                f'{row["base_q"]:.3f}\n{row["base_label"]}',
                ha='center', va='bottom', fontsize=8)
        gap = row['msm_q'] - row['base_q']
        gap_color = WIN_COLOR if gap < 0 else LOSS_COLOR
        bar_top = max(row['msm_q'], row['base_q'])
        ax.text(i, bar_top + ymax * 0.18,
                f'Δ = {gap:+.3f}',
                ha='center', va='bottom', fontsize=11, fontweight='bold',
                color=gap_color)

    ax.set_xticks(x)
    ax.set_xticklabels(sub['model'])
    ax.set_ylabel('Mean QLIKE in stress windows')
    ax.legend(loc='upper right', framealpha=0.9)
    ax.grid(alpha=0.25, axis='y')

    _save(fig, 'fig_rq3_msm_vs_best_baseline.png')

def plot_rq3_r2_ladder():
    '''
    Pooled R^2 by retraining strategy, one panel per model. Read directly from
    aggregate_metrics.csv (which uses pooled SS_res / SS_tot across all 1218
    evaluation observations). Per-window R^2 is unstable for this evaluation
    due to small within-window target variance; pooled R^2 is the appropriate
    measure.

    Story: static models pool to negative R^2 (worse than always predicting
    the mean), every retraining strategy recovers positive R^2.
    '''
    df = _load('aggregate_metrics.csv')
    if df is None or df.empty:
        print('  [SKIP] r2_ladder — aggregate_metrics.csv missing')
        return

    df = _drop_observer(df)
    if 'r2' not in df.columns:
        print('  [SKIP] r2_ladder — r2 column missing')
        return

    models = sorted(df['model_type'].unique())
    fig, axes = plt.subplots(1, len(models), figsize=(5.0 * len(models), 5.5),
                             sharey=False, constrained_layout=True)
    if len(models) == 1:
        axes = [axes]

    for ax, model in zip(axes, models):
        sub = df[df['model_type'] == model].copy()
        # Pick best config per exp_type by pooled R^2 (highest)
        best = (sub.sort_values('r2', ascending=False)
                .drop_duplicates('exp_type'))
        best = best.sort_values('r2', ascending=True)
        labels = [_label(r) for r in best['retrainer']]
        colors = [_col(e) for e in best['exp_type']]

        bars = ax.barh(labels, best['r2'], color=colors, alpha=0.92,
                       edgecolor='white', linewidth=0.6)
        ax.axvline(0, color='black', lw=0.8)

        for bar, val in zip(bars, best['r2']):
            offset = 0.01 if val >= 0 else -0.01
            ha = 'left' if val >= 0 else 'right'
            ax.text(val + offset, bar.get_y() + bar.get_height() / 2,
                    f'{val:+.3f}', va='center', ha=ha,
                    fontsize=8, fontweight='bold')

        ax.set_xlabel('Pooled $R^2$ (across 1{,}218 observations)')
        ax.set_title(model)
        ax.grid(alpha=0.25, axis='x')

    fig.suptitle('Pooled $R^2$ by retraining strategy', y=1.02, fontsize=11)
    _save(fig, 'fig_rq3_r2_ladder.png')
# ════════════════════════════════════════════════════════════════════
#  RQ4 — Operational superiority
# ════════════════════════════════════════════════════════════════════

def _gather_rq4_data():
    sq = _load('stratified_qlike.csv')
    fpr = _load('false_positive_rate.csv')
    if sq is None or fpr is None: return None

    records = []
    for m in sq['model_type'].unique():
        sq_m = sq[sq['model_type'] == m]
        fpr_m = fpr[fpr['model_type'] == m]
        msm_grp = sq_m[sq_m['exp_type'].isin(MSM_TYPES)]
        if msm_grp.empty: continue
        best_msm = msm_grp.sort_values('mean_qlike_stress').iloc[0]
        fixed_3 = sq_m[sq_m['retrainer'] == 'fixed_3']
        if fixed_3.empty: continue
        fixed_3 = fixed_3.iloc[0]

        msm_fpr = fpr_m[fpr_m['retrainer'] == best_msm['retrainer']]
        fixed_fpr = fpr_m[fpr_m['retrainer'] == 'fixed_3']
        if msm_fpr.empty or fixed_fpr.empty: continue

        records.append({
            'model': m,
            'msm_label': _label(best_msm['retrainer']),
            'msm_retrains': msm_fpr.iloc[0]['total_retrains'],
            'fixed_retrains': fixed_fpr.iloc[0]['total_retrains'],
            'msm_stress_q': best_msm['mean_qlike_stress'],
            'fixed_stress_q': fixed_3['mean_qlike_stress'],
        })
    return pd.DataFrame(records)


def plot_rq4_retrains():
    '''Retrain count comparison only — single panel.'''
    df = _gather_rq4_data()
    if df is None or df.empty: return

    fig, ax = plt.subplots(figsize=(8, 5))
    x = np.arange(len(df))
    width = 0.36

    ax.bar(x - width / 2, df['msm_retrains'], width,
           color=PALETTE['msm'], alpha=0.92, label='Best MSM',
           edgecolor='white', linewidth=0.6)
    ax.bar(x + width / 2, df['fixed_retrains'], width,
           color=PALETTE['fixed'], alpha=0.92, label='Fixed-3',
           edgecolor='white', linewidth=0.6)

    ymax = df['fixed_retrains'].max()
    ax.set_ylim(0, ymax * 1.18)

    for i, row in df.iterrows():
        ax.text(i - width / 2, row['msm_retrains'] + ymax * 0.015,
                f"{int(row['msm_retrains'])}",
                ha='center', va='bottom', fontsize=10, fontweight='bold')
        ax.text(i + width / 2, row['fixed_retrains'] + ymax * 0.015,
                f"{int(row['fixed_retrains'])}",
                ha='center', va='bottom', fontsize=10, fontweight='bold')

    ax.set_xticks(x)
    ax.set_xticklabels(df['model'])
    ax.set_ylabel('Number of retraining events over 9-year sample')
    ax.legend(loc='upper left', framealpha=0.9)
    ax.grid(alpha=0.25, axis='y')

    _save(fig, 'fig_rq4_retrains.png')








# ════════════════════════════════════════════════════════════════════
#  RQ5 — Interpretability
# ════════════════════════════════════════════════════════════════════

def plot_rq5_feature_persistence():
    '''Causal feature usage. Single colour, value labels.'''
    df = _load('causal_feature_usage.csv')
    if df is None or df.empty: return

    first_model = df['model_type'].iloc[0]
    sub = (df[df['model_type'] == first_model]
           .sort_values('selection_rate', ascending=True).tail(15))

    fig, ax = plt.subplots(figsize=(9, max(4, len(sub) * 0.4)))
    ax.barh(sub['feature'], sub['selection_rate'],
            color=PALETTE['msm'], alpha=0.92,
            edgecolor='white', linewidth=0.6)
    for bar, rate in zip(ax.patches, sub['selection_rate']):
        ax.text(rate + 0.01, bar.get_y() + bar.get_height() / 2,
                f'{int(rate*100)}%',
                va='center', fontsize=9, fontweight='bold')
    ax.set_xlabel('Fraction of windows where feature is a causal parent of SPY')
    ax.set_xlim(0, 1.15)
    ax.grid(alpha=0.25, axis='x')

    _save(fig, 'fig_rq5_feature_persistence.png')


def plot_rq5_unstable_edges():
    raw_path = 'results/experiments/all_results.csv'
    if not os.path.exists(raw_path):
        print('  [SKIP] rq5_unstable_edges')
        return

    df = pd.read_csv(raw_path, parse_dates=['date_start', 'date_end'])
    df['exp_type'] = df['retrainer'].apply(get_experiment_type)
    msm_retrains = df[
        df['exp_type'].isin(MSM_TYPES) &
        (df['retrain_triggered'] == True)
    ]
    if msm_retrains.empty or 'unstable_edges' not in msm_retrains.columns:
        return

    # One record per unique window per model — ignores which retrainer fired
    msm_retrains_deduped = msm_retrains.drop_duplicates(
        subset=['model_type', 'window']
    )

    edge_counts = {}
    for _, row in msm_retrains_deduped.iterrows():
        ue = row['unstable_edges']
        if pd.isna(ue) or ue in ('', '{}'):
            continue
        try:
            parsed = ast.literal_eval(ue) if isinstance(ue, str) else ue
            if isinstance(parsed, dict):
                for edge_str in parsed.keys():
                    edge_counts[edge_str] = edge_counts.get(edge_str, 0) + 1
        except (SyntaxError, ValueError):
            continue

    if not edge_counts: return
    sorted_edges = sorted(edge_counts.items(), key=lambda x: -x[1])[:15]
    edges = [_resolve_edge(e) for e, _ in sorted_edges]
    counts = [c for _, c in sorted_edges]

    fig, ax = plt.subplots(figsize=(10, max(4, len(edges) * 0.4)))
    ax.barh(edges, counts, color=PALETTE['msm'], alpha=0.92,
            edgecolor='white', linewidth=0.6)
    cmax = max(counts)
    ax.set_xlim(0, cmax * 1.12)
    for i, c in enumerate(counts):
        ax.text(c + cmax * 0.01, i, str(c),
                va='center', fontsize=9, fontweight='bold')
    ax.set_xlabel('Number of evaluation windows where edge was flagged unstable')
    ax.invert_yaxis()
    ax.grid(alpha=0.25, axis='x')

    _save(fig, 'fig_rq5_unstable_edges.png')


# ════════════════════════════════════════════════════════════════════
#  Appendix
# ════════════════════════════════════════════════════════════════════







# ════════════════════════════════════════════════════════════════════
#  RQ4 — Operational efficiency (REBUILT)
# ════════════════════════════════════════════════════════════════════

def plot_rq4_efficiency_frontier(model=None):
    '''
    Pareto efficiency frontier: retrain count vs aggregate RMSE across all
    configurations. Coloured by exp_type family, Pareto-optimal configs
    highlighted with a dashed frontier line.

    Reads: overall_summary.csv (has both mean_rmse and retrains per config)
    Replaces: plot_rq4_selectivity_scatter + plot_rq4_stress_qlike
    '''
    m = model or MAIN_MODEL
    df = _load('overall_summary.csv')
    if df is None:
        return

    df = df[df['model_type'] == m].copy()
    df = _drop_observer(df)
    if df.empty:
        return

    df['exp_type'] = df['retrainer'].apply(get_experiment_type)

    # ── Pareto frontier: minimise both retrain count and RMSE ──
    pts = df[['retrains', 'mean_rmse']].values
    is_pareto = []
    for i, (x, y) in enumerate(pts):
        dominated = any(
            pts[j, 0] <= x and pts[j, 1] <= y and
            (pts[j, 0] < x or pts[j, 1] < y)
            for j in range(len(pts)) if j != i
        )
        is_pareto.append(not dominated)

    pareto_df = df[is_pareto].sort_values('retrains').reset_index(drop=True)
    print(f'  [DEBUG] Pareto points: {sum(is_pareto)} of {len(df)}')

    fig, ax = plt.subplots(figsize=(11, 6))

    # ── All configs (background scatter) ──
    for exp, grp in df.groupby('exp_type'):
        marker = 'o' if exp in MSM_TYPES else 's'
        ax.scatter(
            grp['retrains'], grp['mean_rmse'],
            color=_col(exp), marker=marker,
            s=45 if exp in MSM_TYPES else 30,
            alpha=0.85 if exp in MSM_TYPES else 0.50,
            label=EXP_LABELS.get(exp, exp),
            zorder=3,
        )

    # ── Pareto frontier dashed line ──
    if not pareto_df.empty and len(pareto_df) > 1:
        ax.plot(
            pareto_df['retrains'], pareto_df['mean_rmse'],
            color='black', lw=1.8, ls='--', alpha=0.85,
            label='Pareto frontier', zorder=5,
        )
        # Mark each Pareto-optimal point with a black ring
        ax.scatter(
            pareto_df['retrains'], pareto_df['mean_rmse'],
            s=90, facecolors='none', edgecolors='black',
            linewidths=1.4, zorder=6,
        )

    # ── Annotate MSM configs on the frontier ──
    msm_pareto = pareto_df[pareto_df['exp_type'].isin(MSM_TYPES)]
    for _, row in msm_pareto.iterrows():
        ax.annotate(
            EXP_LABELS.get(row['exp_type'], row['exp_type']),
            xy=(row['retrains'], row['mean_rmse']),
            xytext=(6, -10), textcoords='offset points',
            fontsize=7.5, color=_col(row['exp_type']),
        )

    ax.set_xlabel('Number of retraining events (58-window evaluation)')
    ax.set_ylabel('Mean RMSE (aggregate across all windows)')

    # ── Deduplicated legend ──
    handles, labels_leg = ax.get_legend_handles_labels()
    by_label = dict(zip(labels_leg, handles))
    ax.legend(
        by_label.values(), by_label.keys(),
        loc='upper right', fontsize=8, ncol=2, framealpha=0.9,
    )
    ax.grid(alpha=0.25)

    suffix = f'_{m}' if model and model != MAIN_MODEL else ''
    _save(fig, f'fig_rq4_efficiency_frontier{suffix}.png')


def plot_rq4_regime_retrain_rate(model=None):
    '''
    Horizontal bar chart: stress-to-calm retrain rate ratio per strategy
    family (best config per family).
    Uses regime_retrain_rate.csv.
    Reference line at ratio = 1 (no regime preference).
    '''
    m = model or MAIN_MODEL
    df = _load('regime_retrain_rate.csv')
    agg = _load('aggregate_metrics.csv')
    if df is None or agg is None:
        return

    df = df[df['model_type'] == m].copy()
    df = _drop_observer(df)
    if 'exp_type' not in df.columns:
        df['exp_type'] = df['retrainer'].apply(get_experiment_type)

    agg_m = agg[agg['model_type'] == m].copy()
    agg_m = _drop_observer(agg_m)
    agg_m['exp_type'] = agg_m['retrainer'].apply(get_experiment_type)

    # Best config per family = lowest pooled RMSE
    best_retrainers = (
        agg_m.sort_values('rmse')
             .groupby('exp_type')
             .first()
             .reset_index()[['exp_type', 'retrainer']]
    )

    merged = best_retrainers.merge(df, on=['exp_type', 'retrainer'], how='left')
    if 'stress_calm_ratio' not in merged.columns:
        print('  [SKIP] regime_retrain_rate — stress_calm_ratio column missing')
        return

    # Drop rows with missing ratio (e.g. fixed/static which have no signal)
    merged = merged.dropna(subset=['stress_calm_ratio'])
    merged = merged.sort_values('stress_calm_ratio', ascending=True)

    if merged.empty:
        return

    fig, ax = plt.subplots(figsize=(9, max(4, len(merged) * 0.55)))

    bars = ax.barh(
        range(len(merged)),
        merged['stress_calm_ratio'],
        color=[_col(e) for e in merged['exp_type']],
        alpha=0.90,
        edgecolor='white',
        linewidth=0.8,
    )

    ax.axvline(1.0, color='black', lw=0.9, ls='--', alpha=0.6)

    for bar, (_, row) in zip(bars, merged.iterrows()):
        ax.text(
            row['stress_calm_ratio'] + 0.04,
            bar.get_y() + bar.get_height() / 2,
            f"{row['stress_calm_ratio']:.2f}×",
            va='center', fontsize=9,
        )

    ax.set_yticks(range(len(merged)))
    ax.set_yticklabels(
        [EXP_LABELS.get(e, e) for e in merged['exp_type']],
        fontsize=9,
    )
    ax.set_xlabel('Stress-to-calm retrain rate ratio')
    ax.set_xlim(0, merged['stress_calm_ratio'].max() * 1.20)
    ax.grid(alpha=0.25, axis='x')

    suffix = f'_{m}' if model and model != MAIN_MODEL else ''
    _save(fig, f'fig_rq4_regime_retrain_rate{suffix}.png')
def plot_rq4_retrain_corridor(model=None):
    '''
    Mean RMSE in a ±2 window corridor centred on each retrain event,
    one line per strategy family (best config by mean_rmse).

    Strategy: for every retrain event fired by a strategy, extract
    windows at relative positions [-2, -1, 0, +1, +2] and average
    across all events. A strategy that fires EARLY shows a rising RMSE
    at position 0 (still climbing at retrain moment) and a falling line
    after. A strategy that fires LATE shows RMSE already peaked at 0.

    Reads:
        results/experiments/all_results.csv  (raw per-window rows)
        overall_summary.csv                   (to pick best config per family)

    Replaces: plot_rq4_stress_qlike (and the abandoned retrain_impact idea)
    '''
    raw_path = 'results/experiments/all_results.csv'
    if not os.path.exists(raw_path):
        print('  [SKIP] rq4_retrain_corridor — all_results.csv not found')
        return

    m = model or MAIN_MODEL
    summary = _load('overall_summary.csv')
    if summary is None:
        return

    raw = pd.read_csv(raw_path, parse_dates=['date_start', 'date_end'])
    raw['exp_type'] = raw['retrainer'].apply(get_experiment_type)
    raw_m = raw[raw['model_type'] == m].copy()
    raw_m = _drop_observer(raw_m)
    if raw_m.empty:
        return

    # Pick best config per family = lowest mean_rmse in overall_summary
    sum_m = summary[summary['model_type'] == m].copy()
    sum_m['exp_type'] = sum_m['retrainer'].apply(get_experiment_type)
    sum_m = _drop_observer(sum_m)

    best_configs = (
        sum_m.sort_values('mean_rmse')
             .drop_duplicates('exp_type')[['exp_type', 'retrainer']]
    )

    HALF_WIDTH = 2   # windows either side of retrain event
    POSITIONS  = list(range(-HALF_WIDTH, HALF_WIDTH + 1))

    corridor_data = {}   # exp_type -> array shape (n_positions,)

    for _, cfg_row in best_configs.iterrows():
        exp   = cfg_row['exp_type']
        retr  = cfg_row['retrainer']
        grp   = (raw_m[raw_m['retrainer'] == retr]
                 .sort_values('window')
                 .reset_index(drop=True))

        if 'retrain_triggered' not in grp.columns or 'rmse' not in grp.columns:
            continue

        retrain_idxs = grp.index[grp['retrain_triggered'].astype(str)
                                  .str.lower()
                                  .isin(['true', '1', '1.0'])].tolist()
        if not retrain_idxs:
            continue

        # Collect RMSE at each relative position across all events
        bucket = {p: [] for p in POSITIONS}
        for idx in retrain_idxs:
            for p in POSITIONS:
                target_idx = idx + p
                if 0 <= target_idx < len(grp):
                    val = grp.at[target_idx, 'rmse']
                    if not np.isnan(val):
                        bucket[p].append(val)

        # Only include families that have data at every position
        means = [np.mean(bucket[p]) if bucket[p] else np.nan
                 for p in POSITIONS]
        if any(np.isnan(m_) for m_ in means):
            continue

        corridor_data[exp] = means

    if not corridor_data:
        print('  [SKIP] rq4_retrain_corridor — no corridor data computed')
        return

    # ── Plotting ──
    fig, ax = plt.subplots(figsize=(10, 5.5))

    # Sort so MSM families are drawn last (on top)
    families_sorted = sorted(
        corridor_data.keys(),
        key=lambda e: (0 if e in MSM_TYPES else 1),
        reverse=True,
    )

    for exp in families_sorted:
        means = corridor_data[exp]
        is_msm = exp in MSM_TYPES
        ax.plot(
            POSITIONS, means,
            color=_col(exp),
            lw=2.4 if is_msm else 1.2,
            ls='-'  if is_msm else '--',
            alpha=1.0 if is_msm else 0.65,
            marker='o' if is_msm else None,
            markersize=5,
            label=EXP_LABELS.get(exp, exp),
            zorder=4 if is_msm else 2,
        )

    # Reference line at position 0 (the retrain moment)
    ax.axvline(0, color='black', lw=0.9, ls=':', alpha=0.7,
               label='Retrain fires (window 0)')

    ax.set_xlabel(
        'Window relative to retrain event\n'
        '(negative = before retrain, positive = after)'
    )
    ax.set_ylabel('Mean RMSE')
    ax.set_xticks(POSITIONS)
    ax.set_xticklabels([f'{p:+d}' for p in POSITIONS])
    ax.legend(loc='upper right', fontsize=8.5, ncol=2, framealpha=0.9)
    ax.grid(alpha=0.25)

    suffix = f'_{m}' if model and model != MAIN_MODEL else ''
    _save(fig, f'fig_rq4_retrain_corridor{suffix}.png')
    
def plot_rq3_stratified_rmse(model=None):
    '''
    Stress vs calm RMSE per exp_type, best-of-type config.
    Replaces plot_rq3_stratified_qlike.
    Reads: all_results.csv (per-window rows) for regime split.
    '''
    raw_path = 'results/experiments/all_results.csv'
    if not os.path.exists(raw_path):
        print('  [SKIP] rq3_stratified_rmse — all_results.csv missing')
        return

    df = pd.read_csv(raw_path, parse_dates=['date_start', 'date_end'])
    df['exp_type'] = df['retrainer'].apply(get_experiment_type)
    df = _drop_observer(df)

    m = model or MAIN_MODEL
    df = df[df['model_type'] == m].copy()
    if df.empty:
        return

    # Compute per-window RMSE from squared_error if rmse col absent
    if 'rmse' not in df.columns and 'squared_error' in df.columns:
        df['rmse'] = np.sqrt(df['squared_error'])

    if 'rmse' not in df.columns:
        print('  [SKIP] rq3_stratified_rmse — no rmse or squared_error column')
        return

    # Identify stress windows using STRESS_EVENTS config
    df['is_stress'] = False
    for ev_name, ev_date in STRESS_EVENTS.items():
        ev_ts = pd.Timestamp(ev_date)
        window_half = pd.Timedelta(days=STRESS_WINDOW_DAYS)
        mask = (df['date_end'] >= ev_ts - window_half) & \
               (df['date_end'] <= ev_ts + window_half)
        df.loc[mask, 'is_stress'] = True

    # Best config per family = lowest mean RMSE across all windows
    agg = (df.groupby(['exp_type', 'retrainer'])['rmse']
             .mean().reset_index()
             .rename(columns={'rmse': 'mean_rmse'}))
    best_configs = (agg.sort_values('mean_rmse')
                       .groupby('exp_type').first()
                       .reset_index()[['exp_type', 'retrainer']])

    records = []
    for _, row in best_configs.iterrows():
        sub = df[df['retrainer'] == row['retrainer']]
        stress_rmse = sub[sub['is_stress']]['rmse'].mean()
        calm_rmse   = sub[~sub['is_stress']]['rmse'].mean()
        records.append({
            'exp_type':    row['exp_type'],
            'retrainer':   row['retrainer'],
            'stress_rmse': stress_rmse,
            'calm_rmse':   calm_rmse,
        })

    result = pd.DataFrame(records).sort_values('stress_rmse')

    x = np.arange(len(result))
    w = 0.4
    fig, ax = plt.subplots(figsize=(max(8, len(result) * 1.1), 5.5))

    bars_s = ax.bar(x - w / 2, result['stress_rmse'], w,
                    label='Stress windows', color='#FF8A65',
                    alpha=0.92, edgecolor='white', linewidth=0.6)
    bars_c = ax.bar(x + w / 2, result['calm_rmse'], w,
                    label='Calm windows', color='#4FC3F7',
                    alpha=0.92, edgecolor='white', linewidth=0.6)

    ymax = max(result['stress_rmse'].max(), result['calm_rmse'].max())
    ax.set_ylim(0, ymax * 1.20)

    for i, row in result.reset_index(drop=True).iterrows():
        ax.text(i - w / 2, row['stress_rmse'] + ymax * 0.015,
                f'{row["stress_rmse"]:.4f}',
                ha='center', va='bottom', fontsize=8,
                fontweight='bold' if row['exp_type'] in MSM_TYPES else 'normal',
                color=_col(row['exp_type']))
        ax.text(i + w / 2, row['calm_rmse'] + ymax * 0.015,
                f'{row["calm_rmse"]:.4f}',
                ha='center', va='bottom', fontsize=8)

    ax.set_xticks(x)
    ax.set_xticklabels([EXP_LABELS.get(e, e) for e in result['exp_type']],
                       rotation=35, ha='right')
    ax.set_ylabel('Mean RMSE')
    ax.legend(loc='upper left', framealpha=0.9)
    ax.grid(alpha=0.25, axis='y')

    suffix = f'_{m}' if model and model != MAIN_MODEL else ''
    _save(fig, f'fig_rq3_stratified_rmse{suffix}.png')
    
    
def plot_rq3_msm_vs_best_baseline_rmse(model=None):
    '''
    Direct head-to-head: best MSM vs best non-static baseline on
    stress and calm RMSE, one bar pair per model architecture.
    Replaces plot_rq3_msm_vs_best_baseline (which used stress QLIKE).
    Reads: all_results.csv
    '''
    raw_path = 'results/experiments/all_results.csv'
    if not os.path.exists(raw_path):
        print('  [SKIP] rq3_msm_vs_best_baseline_rmse — all_results.csv missing')
        return

    df = pd.read_csv(raw_path, parse_dates=['date_start', 'date_end'])
    df['exp_type'] = df['retrainer'].apply(get_experiment_type)
    df = _drop_observer(df)

    if 'rmse' not in df.columns and 'squared_error' in df.columns:
        df['rmse'] = np.sqrt(df['squared_error'])
    if 'rmse' not in df.columns:
        print('  [SKIP] rq3_msm_vs_best_baseline_rmse — no rmse column')
        return

    # Tag stress windows
    df['is_stress'] = False
    for ev_name, ev_date in STRESS_EVENTS.items():
        ev_ts = pd.Timestamp(ev_date)
        window_half = pd.Timedelta(days=STRESS_WINDOW_DAYS)
        mask = (df['date_end'] >= ev_ts - window_half) & \
               (df['date_end'] <= ev_ts + window_half)
        df.loc[mask, 'is_stress'] = True

    BASELINE_NO_STATIC = BASELINE_TYPES - {'static'}
    records = []

    for m in sorted(df['model_type'].unique()):
        sub = df[df['model_type'] == m]

        # Best MSM = lowest mean RMSE across all windows
        msm_sub  = sub[sub['exp_type'].isin(MSM_TYPES)]
        base_sub = sub[sub['exp_type'].isin(BASELINE_NO_STATIC)]
        if msm_sub.empty or base_sub.empty:
            continue

        msm_best  = msm_sub.groupby('retrainer')['rmse'].mean().idxmin()
        base_best = base_sub.groupby('retrainer')['rmse'].mean().idxmin()

        for regime, flag in [('Stress', True), ('Calm', False)]:
            msm_rmse  = sub[(sub['retrainer'] == msm_best) &
                            (sub['is_stress'] == flag)]['rmse'].mean()
            base_rmse = sub[(sub['retrainer'] == base_best) &
                            (sub['is_stress'] == flag)]['rmse'].mean()
            records.append({
                'model':      m,
                'regime':     regime,
                'msm_rmse':   msm_rmse,
                'base_rmse':  base_rmse,
                'delta':      msm_rmse - base_rmse,
                'msm_label':  EXP_LABELS.get(get_experiment_type(msm_best), msm_best),
                'base_label': EXP_LABELS.get(get_experiment_type(base_best), base_best),
            })

    if not records:
        return

    result = pd.DataFrame(records)
    model_order  = sorted(result['model'].unique())
    MODEL_LABELS = {'linear_ridge': 'Linear Ridge',
                    'random_forest': 'Random Forest',
                    'xgboost': 'XGBoost',
                    'lr': 'Linear Ridge', 'rf': 'Random Forest'}

    fig, axes = plt.subplots(1, 2, figsize=(13, 5), sharey=False)

    for ax, regime in zip(axes, ['Stress', 'Calm']):
        sub_r = result[result['regime'] == regime]
        x = np.arange(len(model_order))
        w = 0.36
        msm_vals  = [sub_r[sub_r['model'] == mt]['msm_rmse'].values[0]
                     for mt in model_order]
        base_vals = [sub_r[sub_r['model'] == mt]['base_rmse'].values[0]
                     for mt in model_order]
        deltas    = [sub_r[sub_r['model'] == mt]['delta'].values[0]
                     for mt in model_order]

        ax.bar(x - w / 2, msm_vals, width=w,
               color=PALETTE['msm'], alpha=0.92,
               label='Best MSM', edgecolor='white')
        ax.bar(x + w / 2, base_vals, width=w,
               color=PALETTE['perf'], alpha=0.72,
               label='Best baseline', edgecolor='white')

        ymax = max(max(msm_vals), max(base_vals))
        ax.set_ylim(0, ymax * 1.25)

        for i, (mv, bv, delta) in enumerate(zip(msm_vals, base_vals, deltas)):
            top = max(mv, bv)
            sign = '▼' if delta < 0 else '▲'
            color = WIN_COLOR if delta < 0 else LOSS_COLOR
            ax.text(i, top + ymax * 0.06,
                    f'{sign}{abs(delta):.4f}',
                    ha='center', va='bottom', fontsize=9,
                    color=color, fontweight='bold')

        ax.set_xticks(x)
        ax.set_xticklabels([MODEL_LABELS.get(mt, mt) for mt in model_order])
        ax.set_ylabel('Mean RMSE')
        ax.set_title(f'{regime} windows')
        ax.legend(loc='upper right', fontsize=8)
        ax.grid(alpha=0.25, axis='y')

    _save(fig, 'fig_rq3_msm_vs_best_baseline_rmse.png')
    
    
def plot_rq3_window_winrate(model=None):
    '''
    Per-window win/tie/loss heatmap: MSM best config vs each baseline,
    across all evaluation windows.

    Colourblind-safe palette: Blue=MSM wins, Light grey=Tie, Orange=MSM loses.
    Stress windows marked with a dashed vertical line spanning the full column
    height + a downward triangle (v) at the bottom edge of the heatmap — both
    drawn in data coordinates inside the axes. No clip_on, no transform tricks.
    Rows sorted by MSM win-rate descending.
    '''
    raw_path = 'results/experiments/all_results.csv'
    if not os.path.exists(raw_path):
        print('  [SKIP] rq3_window_winrate — all_results.csv missing')
        return

    df = pd.read_csv(raw_path, parse_dates=['date_start', 'date_end'])
    df['exp_type'] = df['retrainer'].apply(get_experiment_type)
    df = _drop_observer(df)

    m = model or MAIN_MODEL
    df = df[df['model_type'] == m].copy()

    if 'rmse' not in df.columns and 'squared_error' in df.columns:
        df['rmse'] = np.sqrt(df['squared_error'])
    if 'rmse' not in df.columns:
        print('  [SKIP] rq3_window_winrate — no rmse or squared_error column')
        return

    # Best MSM config = lowest mean RMSE
    msm_sub = df[df['exp_type'].isin(MSM_TYPES)]
    if msm_sub.empty:
        print('  [SKIP] rq3_window_winrate — no MSM rows')
        return
    msm_best = msm_sub.groupby('retrainer')['rmse'].mean().idxmin()
    msm_by_window = (msm_sub[msm_sub['retrainer'] == msm_best]
                     .set_index('window')['rmse'])

    # One best config per baseline family (lowest mean RMSE)
    BASELINE_NO_STATIC = BASELINE_TYPES - {'static'}
    base_sub = df[df['exp_type'].isin(BASELINE_NO_STATIC)]
    if base_sub.empty:
        print('  [SKIP] rq3_window_winrate — no baseline rows')
        return
    best_baselines = (base_sub.groupby(['exp_type', 'retrainer'])['rmse']
                               .mean().reset_index()
                               .sort_values('rmse')
                               .groupby('exp_type').first()
                               .reset_index())

    windows = sorted(df['window'].unique())

    # Tag stress windows by matching date_end against STRESS_EVENTS config
    df['is_stress'] = False
    for ev_date in STRESS_EVENTS.values():
        ev_ts  = pd.Timestamp(ev_date)
        half   = pd.Timedelta(days=STRESS_WINDOW_DAYS)
        mask   = (df['date_end'] >= ev_ts - half) & \
                 (df['date_end'] <= ev_ts + half)
        df.loc[mask, 'is_stress'] = True
    stress_windows    = set(df[df['is_stress']]['window'].unique())
    stress_col_idxs   = [j for j, w in enumerate(windows) if w in stress_windows]

    # Build result matrix: 1=MSM wins, 0=tie/missing, -1=MSM loses
    rows, row_labels, win_rates = [], [], []
    for _, brow in best_baselines.iterrows():
        base_by_window = (base_sub[base_sub['retrainer'] == brow['retrainer']]
                          .set_index('window')['rmse'])
        result_row = []
        for w in windows:
            mv = msm_by_window.get(w, np.nan)
            bv = base_by_window.get(w, np.nan)
            if np.isnan(mv) or np.isnan(bv):
                result_row.append(0)
            elif mv < bv - 1e-6:
                result_row.append(1)    # lower RMSE = MSM wins
            elif mv > bv + 1e-6:
                result_row.append(-1)
            else:
                result_row.append(0)
        contested = [v for v in result_row if v != 0]
        rate = sum(1 for v in contested if v == 1) / len(contested) \
               if contested else 0.0
        rows.append(result_row)
        row_labels.append(EXP_LABELS.get(brow['exp_type'], brow['exp_type']))
        win_rates.append(rate)

    # Sort rows by win-rate descending (best matchup at top)
    order      = sorted(range(len(rows)), key=lambda i: win_rates[i], reverse=True)
    rows       = [rows[i]       for i in order]
    row_labels = [row_labels[i] for i in order]
    win_rates  = [win_rates[i]  for i in order]

    mat = np.array(rows, dtype=float)

    # Colourblind-safe: blue / light-grey / orange
    WIN_CB  = '#0077BB'
    TIE_CB  = '#DDDDDD'
    LOSS_CB = '#EE7733'
    STRESS_MARKER_COLOR = '#AA3377'

    cmap = matplotlib.colors.ListedColormap([LOSS_CB, TIE_CB, WIN_CB])
    norm = matplotlib.colors.BoundaryNorm([-1.5, -0.5, 0.5, 1.5], cmap.N)

    n_rows = len(rows)
    n_cols = len(windows)
    fig_h  = max(3.5, n_rows * 0.65 + 2.2)
    fig_w  = max(14,  n_cols * 0.30 + 3.0)

    fig, ax = plt.subplots(figsize=(fig_w, fig_h))
    ax.imshow(mat, cmap=cmap, norm=norm, aspect='auto',
              interpolation='nearest')

    # ── Stress window markers ────────────────────────────────────────
    # axvline with no ymin/ymax spans the full column in data coords.
    # ax.plot with default transform places the triangle at row n_rows-0.5,
    # which is the bottom edge of the last heatmap row — fully inside axes.
    for j in stress_col_idxs:
        ax.axvline(j, color=STRESS_MARKER_COLOR,
                   lw=1.1, ls='--', alpha=0.70, zorder=5)
        ax.plot(j, n_rows - 0.5,
                marker='v', color=STRESS_MARKER_COLOR,
                markersize=6, zorder=6,
                linestyle='None')

    # ── Win-rate label, right of each row ────────────────────────────
    for i, rate in enumerate(win_rates):
        label_color = WIN_CB if rate > 0.5 else \
                      (LOSS_CB if rate < 0.5 else '#888888')
        ax.text(n_cols + 0.4, i,
                f'{rate:.0%}',
                va='center', ha='left',
                fontsize=9, fontweight='bold',
                color=label_color)

    # ── Thin white cell borders ──────────────────────────────────────
    for j in range(n_cols):
        ax.axvline(j - 0.5, color='white', lw=0.3, zorder=4)
    for i in range(n_rows):
        ax.axhline(i - 0.5, color='white', lw=0.5, zorder=4)

    # ── Axis labels ──────────────────────────────────────────────────
    ax.set_xticks(range(0, n_cols, 5))
    ax.set_xticklabels([windows[j] for j in range(0, n_cols, 5)], fontsize=8)
    ax.set_xlabel('Evaluation window  (▼ = stress window)', fontsize=9)
    ax.set_yticks(range(n_rows))
    ax.set_yticklabels(row_labels, fontsize=10)
    ax.set_xlim(-0.5, n_cols - 0.5)
    ax.set_ylim(n_rows - 0.5, -0.5)   # keep top-to-bottom row order

    # ── Title ────────────────────────────────────────────────────────
    ax.set_title(f'MSM per-window win/tie/loss vs each baseline  ({m})',
                 fontsize=10, pad=10)

    # ── Legend ───────────────────────────────────────────────────────
    legend_elements = [
        mpatches.Patch(color=WIN_CB,  label='MSM wins (lower RMSE)'),
        mpatches.Patch(color=TIE_CB,  label='Tie',
                       edgecolor='#AAAAAA', linewidth=0.8),
        mpatches.Patch(color=LOSS_CB, label='MSM loses'),
        plt.Line2D([0], [0], color=STRESS_MARKER_COLOR,
                   lw=1.1, ls='--', marker='v', markersize=6,
                   label='Stress window'),
    ]
    ax.legend(handles=legend_elements,
              loc='upper center',
              bbox_to_anchor=(0.5, -0.14),
              ncol=4, framealpha=0.95, fontsize=9)

    suffix = f'_{m}' if model and model != MAIN_MODEL else ''
    _save(fig, f'fig_rq3_window_winrate{suffix}.png')
# ════════════════════════════════════════════════════════════════════
#  Main
# ════════════════════════════════════════════════════════════════════

def main():
    os.makedirs(PLOT_DIR, exist_ok=True)
    print(f'Writing plots to {PLOT_DIR}/  (main model = {MAIN_MODEL})\n')

    print('═══ RQ1: Predictive lead ═══')
    plot_rq1_overlay()
    plot_rq1_xcorr()


    print('\n═══ RQ2: Structural distinctness ═══')
    plot_rq2_jaccard_distribution()
    plot_rq2_jaccard_heatmap(model=MAIN_MODEL)

    print('\n═══ RQ3: Forecast performance under DM and asymmetric loss ═══')
    plot_rq3_dm_winrate()
    plot_rq3_msm_vs_best_baseline()
    plot_rq3_r2_ladder()

    print('\n═══ RQ4: Operational superiority ═══')
    plot_rq4_retrains()


    print('\n═══ RQ5: Interpretability ═══')
    plot_rq5_feature_persistence()
    plot_rq5_unstable_edges()

    print('\n═══ Appendix ═══')

    
    print('\n═══ RQ4: Operational superiority ═══')
    plot_rq4_retrains()                          # keep — retrain count bar
    plot_rq4_efficiency_frontier(model=MAIN_MODEL)
    plot_rq4_retrain_corridor(model=MAIN_MODEL)
    
    plot_rq4_efficiency_frontier(model=MAIN_MODEL)
    plot_rq4_regime_retrain_rate(model=MAIN_MODEL)
    
    
    print('\n═══ RQ3: Forecast performance under DM and asymmetric loss ═══')
    plot_rq3_stratified_rmse(model=MAIN_MODEL)
    plot_rq3_msm_vs_best_baseline_rmse(model=MAIN_MODEL)
    plot_rq3_window_winrate(model=MAIN_MODEL)
    
    

    # Per-model variants for appendix
    for m in ('lr', 'rf'):
        print(f'\n--- per-model variants ({m}) ---')
        plot_rq2_jaccard_heatmap(model=m)



if __name__ == '__main__':
    main()
