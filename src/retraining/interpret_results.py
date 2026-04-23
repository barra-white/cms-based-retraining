'''
interpret_results.py — Structured narrative report from analysis CSVs.

Regression task. Primary metric: RMSE (lower = better).

Run AFTER analysis.py, significance_test.py, and lead_lag_analysis.py.

Reads results/analysis/*.csv and produces results/analysis/narrative_report.txt.

Sections:
    1. Overall strategy ranking (by RMSE)
    2. Pooled regression metrics (RMSE, MAE, R², Pearson r)
    3. Stress vs calm regime performance (RMSE delta)
    4. Detection latency
    5. Retrain selectivity
    6. Statistical significance (on RMSE; negative d = MSM better)
    7. Hyperparameter sensitivity (RMSE range)
    8. Causal feature usage
    9. Lead-lag analysis (RQ1 evidence)
    10. Bootstrap RMSE confidence intervals
'''

import os
import sys
import textwrap

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config as cfg

MSM_TYPES           = cfg.MSM_TYPES
BASELINE_TYPES      = cfg.BASELINE_TYPES
get_experiment_type = cfg.get_experiment_type

ANALYSIS_DIR = 'results/analysis'
W = 80


def _h(title):
    return f'\n{"=" * W}\n  {title}\n{"=" * W}'

def _sub(title):
    return f'\n--- {title} ---'

def _f(label, text):
    prefix = f'  {label}: '
    return textwrap.fill(text, width=W, initial_indent=prefix,
                         subsequent_indent=' ' * len(prefix))

def _load(name):
    path = os.path.join(ANALYSIS_DIR, name)
    if not os.path.exists(path):
        return None
    df = pd.read_csv(path)
    if df.empty:
        return df
    if 'exp_type' not in df.columns and 'retrainer' in df.columns:
        df['exp_type'] = df['retrainer'].apply(get_experiment_type)
    return df

def _tag(exp):
    if exp in MSM_TYPES:      return ' [MSM]'
    if exp in BASELINE_TYPES: return ' [baseline]'
    return ''


# ── 1. Overall ranking (by RMSE) ──

def section_ranking(L):
    L.append(_h('1. OVERALL STRATEGY RANKING (lower RMSE = better)'))
    df = _load('overall_summary.csv')
    pooled = _load('aggregate_metrics.csv')
    if df is None or pooled is None:
        L.append('  [MISSING] overall_summary.csv or aggregate_metrics.csv')
        return

    # Drift observer is a measurement instrument, not a retraining strategy
    df = df[df['exp_type'] != 'drift_observer'].copy()

    for model, grp in df.groupby('model_type'):
        L.append(_sub(f'Model: {model}'))
        grp = grp.sort_values('mean_rmse', ascending=True).reset_index(drop=True)
        top = grp.iloc[0]

        # Look up pooled R² for the top retrainer
        pooled_row = pooled[(pooled['model_type'] == model)
                            & (pooled['retrainer'] == top['retrainer'])]
        pooled_r2 = pooled_row['r2'].iloc[0] if not pooled_row.empty else np.nan

        L.append(_f('FINDING',
            f"Best (lowest RMSE): '{top['retrainer']}' "
            f"(RMSE={top['mean_rmse']:.4f} ±{top['std_rmse']:.4f}, "
            f"MAE={top['mean_mae']:.4f}, pooled R²={pooled_r2:.4f})"))
        L.append(_f('NOTE',
            'Pooled R² reflects cross-regime predictive value. '
            'Per-window R² is negative for most strategies because within-window '
            'target variance is small; this is expected and reflects the absence of '
            'useful within-window structure rather than poor global fit.'))

        msm  = grp[grp['exp_type'].isin(MSM_TYPES)]
        base = grp[grp['exp_type'].isin(BASELINE_TYPES)]
        if not msm.empty and not base.empty:
            bm = msm.iloc[0]
            bb = base.iloc[0]
            # MSM better if its RMSE is lower.
            delta = bm['mean_rmse'] - bb['mean_rmse']
            L.append(_f('DETAIL',
                f"Best MSM: '{bm['retrainer']}' RMSE={bm['mean_rmse']:.4f} | "
                f"Best baseline: '{bb['retrainer']}' RMSE={bb['mean_rmse']:.4f} | "
                f"Diff (MSM-baseline): {delta:+.4f}  (negative ⇒ MSM better)"))
            if delta >= 0:
                L.append(_f('CONCERN',
                    'MSM does not outperform best baseline on raw RMSE. '
                    'Rely on stress-period, detection-latency, and lead-lag evidence.'))

        L.append('  RMSE by type (ascending):')
        for exp, rmse in grp.groupby('exp_type')['mean_rmse'].mean().sort_values(ascending=True).items():
            L.append(f'    {exp:<20} {rmse:.4f}{_tag(exp)}')


# ── 2. Pooled regression metrics ──

def section_aggregate_metrics(L):
    L.append(_h('2. POOLED REGRESSION METRICS'))
    df = _load('aggregate_metrics.csv')
    if df is None:
        L.append('  [MISSING] aggregate_metrics.csv')
        return

    L.append('')
    L.append('  Metrics computed on flattened y_true / y_pred across all windows.')
    L.append('  RMSE / MAE lower = better. R² and Pearson r higher = better.')

    for model, grp in df.groupby('model_type'):
        L.append(_sub(f'Model: {model}'))
        grp = grp.sort_values('rmse', ascending=True)
        top = grp.iloc[0]
        L.append(_f('FINDING',
            f"Best by pooled RMSE: '{top['retrainer']}' "
            f"(RMSE={top['rmse']:.4f}, MAE={top['mae']:.4f}, "
            f"R²={top['r2']:.4f}, Pearson r={top['pearson_r']:.4f})"))

        msm  = grp[grp['exp_type'].isin(MSM_TYPES)]
        base = grp[grp['exp_type'].isin(BASELINE_TYPES)]
        if not msm.empty and not base.empty:
            bm = msm.iloc[0]
            bb = base.iloc[0]
            L.append(_f('DETAIL',
                f"Best MSM RMSE={bm['rmse']:.4f} R²={bm['r2']:.4f} | "
                f"Best baseline RMSE={bb['rmse']:.4f} R²={bb['r2']:.4f} | "
                f"RMSE diff: {bm['rmse'] - bb['rmse']:+.4f}  (negative ⇒ MSM better)"))


# ── 3. Stress vs calm ──
def section_stress(L):
    L.append(_h('3. STRESS vs CALM  (negative delta = RMSE lower in stress)'))
    L.append('')
    L.append('  For log-realized-variance on SPY, RMSE is typically LOWER in stress')
    L.append('  than calm because volatility clusters and becomes more predictable')
    L.append('  during crises. All deltas are expected to be negative — the comparison')
    L.append('  is between MAGNITUDES of stress advantage.')

    df = _load('stress_period_rmse.csv')
    if df is None or 'rmse_stress_minus_calm' not in df.columns:
        L.append('  [MISSING]')
        return

    df = df[df['exp_type'] != 'drift_observer'].copy()

    for model, grp in df.groupby('model_type'):
        L.append(_sub(f'Model: {model}'))
        grp = grp.sort_values('rmse_stress_minus_calm', ascending=True)
        msm  = grp[grp['exp_type'].isin(MSM_TYPES)]
        base = grp[grp['exp_type'].isin(BASELINE_TYPES)]
        if not msm.empty and not base.empty:
            bm = msm.iloc[0]
            bb = base.iloc[0]
            L.append(_f('DETAIL',
                f"Best MSM delta: {bm['rmse_stress_minus_calm']:+.4f} ('{bm['retrainer']}') | "
                f"Best baseline delta: {bb['rmse_stress_minus_calm']:+.4f} ('{bb['retrainer']}')"))
            gap = bm['rmse_stress_minus_calm'] - bb['rmse_stress_minus_calm']
            if gap < -0.01:
                L.append(_f('FINDING',
                    f'MSM has LARGER stress-vs-calm advantage (more negative delta) '
                    f'than best baseline by {abs(gap):.4f}.'))
            elif abs(gap) <= 0.01:
                L.append(_f('FINDING',
                    'MSM and best baseline have indistinguishable stress advantage '
                    '(within 0.01 RMSE).'))
            else:
                L.append(_f('DETAIL',
                    f'MSM has SMALLER stress advantage than best baseline by {gap:.4f}.'))


# ── 4. Detection latency ──

def section_latency(L):
    L.append(_h('4. DETECTION LATENCY (signal-driven only)'))
    df = _load('detection_latency.csv')
    if df is None:
        L.append('  [MISSING]')
        return

    for model, grp in df.groupby('model_type'):
        L.append(_sub(f'Model: {model}'))
        for event, ev in grp.groupby('event'):
            det = ev[ev['detected'] == True]
            nd  = ev[ev['detected'] == False]
            L.append(f'\n  Event: {event}')
            if not nd.empty:
                L.append(_f('CONCERN', f"{len(nd)} strategies missed this event"))
            if not det.empty:
                fastest = det.sort_values('latency_windows').iloc[0]
                L.append(_f('FINDING',
                    f"Fastest: '{fastest['retrainer']}' at {fastest['latency_windows']} windows"))


# ── 5. Selectivity ──

def section_selectivity(L):
    L.append(_h('5. RETRAIN SELECTIVITY'))
    df = _load('false_positive_rate.csv')
    if df is None or df.empty:
        L.append('  [MISSING or EMPTY] — no retrains detected across any strategy.')
        return

    if 'precision' not in df.columns:
        L.append('  [SKIP] precision column missing')
        return

    for model, grp in df.groupby('model_type'):
        L.append(_sub(f'Model: {model}'))
        msm  = grp[grp['exp_type'].isin(MSM_TYPES)]
        base = grp[grp['exp_type'].isin(BASELINE_TYPES)]
        if not msm.empty and not base.empty:
            L.append(_f('DETAIL',
                f"Mean MSM precision: {msm['precision'].mean():.4f} | "
                f"Mean baseline precision: {base['precision'].mean():.4f}"))


# ── 6. Significance ──

def section_significance(L):
    L.append(_h("6. STATISTICAL SIGNIFICANCE  (Cohen's d on RMSE; negative ⇒ a better)"))

    friedman = _load('friedman_test.csv')
    if friedman is not None:
        L.append(_sub('Friedman Test'))
        for _, row in friedman.iterrows():
            sig = 'SIGNIFICANT' if row['significant'] else 'NOT significant'
            L.append(f"  {row['model_type']}: chi2={row['chi2_stat']:.4f} p={row['p_value']:.6f} → {sig}")
            if not row['significant']:
                L.append(_f('CONCERN', 'Friedman not significant. Rely on effect sizes.'))

    wilcoxon = _load('wilcoxon_results.csv')
    if wilcoxon is not None:
        L.append(_sub('Wilcoxon (Holm-Bonferroni)'))
        for model, grp in wilcoxon.groupby('model_type'):
            sig = grp[grp.get('significant', False) == True]
            L.append(f'  {model}: {len(sig)}/{len(grp)} significant pairs')

    effect = _load('effect_size.csv')
    if effect is not None:
        L.append(_sub("Effect Sizes (Cohen's d; negative = MSM better)"))
        for model, grp in effect.groupby('model_type'):
            L.append(f'  {model}:')
            # Rank by most-negative d (= MSM most ahead) first.
            for _, row in grp.sort_values('cohens_d', ascending=True).iterrows():
                mk = '[SIG]' if row['significant'] else '[ns] '
                L.append(f"    {mk} {row['strategy_a'][:25]} vs {row['strategy_b'][:25]}: "
                         f"d={row['cohens_d']:+.3f} ({row['effect_label']})")


# ── 7. Sensitivity ──

def section_sensitivity(L):
    L.append(_h('7. HYPERPARAMETER SENSITIVITY'))
    df = _load('sensitivity_summary.csv')
    if df is None:
        L.append('  [MISSING]')
        return

    for model, grp in df.groupby('model_type'):
        L.append(_sub(f'Model: {model}'))
        for exp, sub in grp.groupby('exp_type'):
            rng = sub['mean_rmse'].max() - sub['mean_rmse'].min()
            L.append(f'  {exp}: RMSE range={rng:.4f} across {len(sub)} configs')
            # 0.02 was the old F1 threshold; for log-RV RMSE (~0.5–1.5 range)
            # we loosen it slightly.
            if rng <= 0.05:
                L.append(_f('FINDING', 'Robust plateau — results not sensitive to hyperparameters.'))
            else:
                L.append(_f('CONCERN', 'Range > 0.05 — hyperparameters matter.'))


# ── 8. Causal features ──
def section_features(L):
    L.append(_h('8. CAUSAL FEATURE USAGE (model-independent)'))
    df = _load('causal_feature_usage.csv')
    if df is None or df.empty:
        L.append('  [MISSING or EMPTY]')
        return

    L.append('')
    L.append('  Note: causal features are derived from shared graphs across all models,')
    L.append('  so selection rates are identical for lr/rf/xgboost. Reporting once.')

    # Use the first model as reference; they're all identical
    first_model = df['model_type'].iloc[0]
    grp = df[df['model_type'] == first_model].sort_values('selection_rate', ascending=False).head(5)
    always = df[(df['model_type'] == first_model) & (df['selection_rate'] == 1.0)]
    if not always.empty:
        L.append(_f('FINDING', f"Always selected: {', '.join(always['feature'].tolist())}"))
    for _, row in grp.iterrows():
        L.append(f"    {row['feature']:<25} rate={row['selection_rate']:.3f}")


# ── 9. Lead-lag (RQ1 primary evidence) ──
def section_lead_lag(L):
    L.append(_h('9. LEAD-LAG ANALYSIS — RQ1 PRIMARY EVIDENCE'))
    df = _load('lead_lag_results.csv')
    if df is None:
        L.append('  [MISSING] lead_lag_results.csv')
        return

    L.append('')
    L.append('  MSM is model-independent — it is computed from shared causal graphs.')
    L.append('  The primary MSM→target test is therefore ONE statistical test, not')
    L.append('  three. Per-model variation appears only in the MSM-vs-RMSE tests')
    L.append('  where RMSE is specific to each model type.')

    # Primary test: single shared test
    primary = df[(df['model_type'] == 'shared_across_models')
                 & (df['signal'] == 'graph_msm')
                 & (df['vs'] == cfg.TARGET_SECONDARY)]

    L.append(_sub(f'Primary test: graph_msm → {cfg.TARGET_SECONDARY} (single test, shared)'))
    for _, row in primary.iterrows():
        sig = 'SIGNIFICANT' if row.get('granger_significant', False) else 'not significant'
        xc_str = (f"xc={row['xc_corr_at_best_lag']:.3f}@lag{row['xc_best_lag']}"
                  if not pd.isna(row.get('xc_corr_at_best_lag', np.nan)) else 'xc=N/A')
        if 'xc_ci_lower' in row and not pd.isna(row['xc_ci_lower']):
            xc_str += f" [block-boot CI: {row['xc_ci_lower']:.3f}, {row['xc_ci_upper']:.3f}]"
        L.append(f"  Granger p={row['granger_min_p']:.4f} ({sig})  "
                 f"best lag={row['granger_best_lag']}  {xc_str}")
        if row.get('granger_significant', False):
            L.append(_f('FINDING',
                f'MSM Granger-causes {cfg.TARGET_SECONDARY}. RQ1 supported: MSM provides '
                f'predictive lead time over regime-normalised returns.'))
        else:
            L.append(_f('CONCERN', f'MSM does not Granger-cause {cfg.TARGET_SECONDARY}.'))

    # SPY-focused MSM primary
    spy_primary = df[(df['model_type'] == 'shared_across_models')
                     & (df['signal'] == 'spy_msm')
                     & (df['vs'] == cfg.TARGET_SECONDARY)]
    if not spy_primary.empty:
        L.append(_sub(f'SPY-focused MSM → {cfg.TARGET_SECONDARY} (single test, shared)'))
        for _, row in spy_primary.iterrows():
            sig = 'SIGNIFICANT' if row.get('granger_significant', False) else 'not significant'
            L.append(f"  Granger p={row['granger_min_p']:.4f} ({sig})  "
                     f"best lag={row['granger_best_lag']}")

    # MSM-vs-RMSE: per-model, legitimate variation
    rmse_test = df[(df['model_type'] != 'shared_across_models')
                   & (df['signal'] == 'graph_msm')
                   & (df['vs'] == 'RMSE')]
    if not rmse_test.empty:
        L.append(_sub('MSM vs frozen-model RMSE (per-model, legitimate variation)'))
        for _, row in rmse_test.iterrows():
            sig = 'SIG' if row.get('granger_significant', False) else 'ns '
            L.append(f"  {row['model_type']:<10}  Granger p={row['granger_min_p']:.4f} [{sig}]  "
                     f"xc best lag={row['xc_best_lag']}")


# ── 10. Bootstrap RMSE CIs ──

def section_bootstrap_ci(L):
    L.append(_h('10. BOOTSTRAP RMSE CONFIDENCE INTERVALS'))
    df = _load('rmse_bootstrap_ci.csv')
    if df is None or df.empty:
        L.append('  [MISSING]')
        return

    L.append('')
    L.append('  95% CI on mean RMSE via 1000-sample bootstrap resampling.')
    L.append('  If CIs for MSM and best baseline overlap, the RMSE difference')
    L.append('  is within sampling noise and should not be emphasised.')

    for model, grp in df.groupby('model_type'):
        L.append(_sub(f'Model: {model}'))
        grp = grp.sort_values('mean_rmse', ascending=True)

        msm  = grp[grp['exp_type'].isin(MSM_TYPES)]
        base = grp[grp['exp_type'].isin(BASELINE_TYPES)]
        if not msm.empty and not base.empty:
            bm = msm.iloc[0]
            bb = base.iloc[0]
            L.append(f"  Best MSM:      {bm['retrainer'][:30]:<30}  "
                     f"{bm['mean_rmse']:.4f} [{bm['ci_lower']:.4f}, {bm['ci_upper']:.4f}]")
            L.append(f"  Best baseline: {bb['retrainer'][:30]:<30}  "
                     f"{bb['mean_rmse']:.4f} [{bb['ci_lower']:.4f}, {bb['ci_upper']:.4f}]")
            overlap = (bm['ci_lower'] <= bb['ci_upper']) and (bb['ci_lower'] <= bm['ci_upper'])
            if overlap:
                L.append(_f('CONCERN',
                    'CIs overlap — RMSE difference is within sampling noise. '
                    'Rely on stress-period and lead-lag evidence.'))
            else:
                L.append(_f('FINDING',
                    'CIs do not overlap — RMSE difference is genuine.'))

#    11. QLIKE loss (Patton 2011 secondary metric)
def section_qlike(L):
    L.append(_h('11. QLIKE LOSS (Patton 2011 — vol literature standard)'))
    df = _load('qlike_summary.csv')
    if df is None or df.empty:
        L.append('  [MISSING] qlike_summary.csv — re-run experiment.py and analysis.py')
        return

    L.append('')
    L.append('  QLIKE = E[RV_true/RV_pred - log(RV_true/RV_pred) - 1]')
    L.append('  Lower = better. Penalises underprediction of variance asymmetrically.')
    L.append('  This is the accepted secondary metric in the HAR/Corsi vol literature.')

    for model, grp in df.groupby('model_type'):
        L.append(_sub(f'Model: {model}'))
        grp = grp.sort_values('mean_qlike', ascending=True).reset_index(drop=True)
        top = grp.iloc[0]
        L.append(_f('FINDING',
            f"Best QLIKE: '{top['retrainer']}' "
            f"(QLIKE={top['mean_qlike']:.6f} ±{top['std_qlike']:.6f})"))

        msm  = grp[grp['exp_type'].isin(MSM_TYPES)]
        base = grp[grp['exp_type'].isin(BASELINE_TYPES)]
        if not msm.empty and not base.empty:
            bm = msm.iloc[0]
            bb = base.iloc[0]
            delta = bm['mean_qlike'] - bb['mean_qlike']
            L.append(_f('DETAIL',
                f"Best MSM: '{bm['retrainer']}' QLIKE={bm['mean_qlike']:.6f} | "
                f"Best baseline: '{bb['retrainer']}' QLIKE={bb['mean_qlike']:.6f} | "
                f"Diff (MSM-baseline): {delta:+.6f}  (negative => MSM better)"))
            if delta < 0:
                L.append(_f('FINDING',
                    'MSM produces lower QLIKE than best baseline. '
                    'Confirms RMSE finding under asymmetric vol loss.'))
            else:
                L.append(_f('CONCERN',
                    'MSM does not win on QLIKE despite winning on RMSE. '
                    'MSM may be systematically underpredicting variance. '
                    'Check rv_pred distribution vs rv_true.'))

        L.append('  QLIKE by type (ascending):')
        for exp, q in grp.groupby('exp_type')['mean_qlike'].mean().sort_values().items():
            L.append(f'    {exp:<20} {q:.6f}{_tag(exp)}')
# ── 12. Stratified QLIKE ──

def section_stratified_qlike(L):
    L.append(_h('12. STRATIFIED QLIKE — regime-specific performance'))
    df = _load('stratified_qlike.csv')
    if df is None or df.empty:
        L.append('  [MISSING] stratified_qlike.csv')
        return

    L.append('')
    L.append('  QLIKE split by stress vs calm regime.')
    L.append('  The hypothesis is that MSM advantage concentrates in stress windows.')

    for model, grp in df.groupby('model_type'):
        L.append(_sub(f'Model: {model}'))
        if 'mean_qlike_stress' not in grp.columns:
            continue

        grp = grp.sort_values('mean_qlike_stress', ascending=True)
        msm  = grp[grp['exp_type'].isin(MSM_TYPES)]
        base = grp[grp['exp_type'].isin(BASELINE_TYPES)]

        if not msm.empty and not base.empty:
            bm = msm.iloc[0]
            bb = base.iloc[0]
            L.append(_f('STRESS',
                f"Best MSM stress-QLIKE: {bm['mean_qlike_stress']:.4f} ({bm['retrainer']}) | "
                f"Best baseline stress-QLIKE: {bb['mean_qlike_stress']:.4f} ({bb['retrainer']}) | "
                f"Diff: {bm['mean_qlike_stress'] - bb['mean_qlike_stress']:+.4f}"))

            if 'mean_qlike_calm' in grp.columns:
                grp_calm = grp.sort_values('mean_qlike_calm', ascending=True)
                bmc = grp_calm[grp_calm['exp_type'].isin(MSM_TYPES)].iloc[0] if not msm.empty else None
                bbc = grp_calm[grp_calm['exp_type'].isin(BASELINE_TYPES)].iloc[0] if not base.empty else None
                if bmc is not None and bbc is not None:
                    L.append(_f('CALM',
                        f"Best MSM calm-QLIKE: {bmc['mean_qlike_calm']:.4f} ({bmc['retrainer']}) | "
                        f"Best baseline calm-QLIKE: {bbc['mean_qlike_calm']:.4f} ({bbc['retrainer']}) | "
                        f"Diff: {bmc['mean_qlike_calm'] - bbc['mean_qlike_calm']:+.4f}"))

            stress_gap = bm['mean_qlike_stress'] - bb['mean_qlike_stress']
            if stress_gap < 0:
                L.append(_f('FINDING',
                    'MSM outperforms best baseline in stress windows on QLIKE.'))
            else:
                L.append(_f('DETAIL',
                    f'Stress QLIKE gap (MSM−baseline) = {stress_gap:+.4f}. '
                    f'MSM is not strictly better in stress on QLIKE.'))


# ── 13. Co-firing Analysis ──
def section_cofiring(L):
    L.append(_h('13. CO-FIRING ANALYSIS — do MSM and baselines fire together?'))
    df = _load('cofiring_analysis.csv')
    if df is None or df.empty:
        L.append('  [MISSING] cofiring_analysis.csv')
        return

    L.append('')
    L.append('  Jaccard overlap = fraction of retrain events where MSM and baseline')
    L.append('  fire in the same window. Interpretation depends on the magnitude:')
    L.append('    High (>0.6) = same signal, MSM adds interpretability only')
    L.append('    Moderate (0.3-0.6) = partial agreement, both useful')
    L.append('    Low (<0.3) = DIFFERENT signals — MSM captures distinct events')

    for model, grp in df.groupby('model_type'):
        L.append(_sub(f'Model: {model}'))
        summary = (
            grp.groupby('msm_retrainer')
            .agg(mean_jaccard=('jaccard_overlap', 'mean'),
                 max_jaccard=('jaccard_overlap', 'max'),
                 mean_msm_cofire=('pct_msm_cofire', 'mean'))
            .round(3).reset_index()
        )
        for _, row in summary.iterrows():
            L.append(f"  {row['msm_retrainer'][:35]:<35}  "
                     f"mean Jaccard with baselines = {row['mean_jaccard']:.3f}  "
                     f"(max = {row['max_jaccard']:.3f})")

        mean_jacc = float(summary['mean_jaccard'].mean())
        if mean_jacc < 0.3:
            L.append(_f('FINDING',
                f'Mean Jaccard = {mean_jacc:.3f} (<0.3). MSM fires on substantially '
                f'DIFFERENT windows than baselines. This supports the thesis claim '
                f'that MSM captures an orthogonal retraining signal — not a faster '
                f'reaction to the same events baselines already detect.'))
        elif mean_jacc < 0.6:
            L.append(_f('DETAIL',
                f'Mean Jaccard = {mean_jacc:.3f}: moderate overlap. MSM and baselines '
                f'respond to partly overlapping but partly distinct signals.'))
        else:
            L.append(_f('DETAIL',
                f'Mean Jaccard = {mean_jacc:.3f}: high overlap. MSM adds '
                f'interpretability to similar-timing retrains.'))

        max_pairs = grp.sort_values('jaccard_overlap', ascending=False).head(3)
        L.append('\n  Top co-firing pairs:')
        for _, row in max_pairs.iterrows():
            L.append(f"    {row['msm_retrainer'][:30]} ↔ {row['baseline_retrainer'][:30]}: "
                     f"Jaccard={row['jaccard_overlap']:.3f} "
                     f"({row['n_cofires']}/{row['n_msm_fires']} MSM fires match baseline)")



# ── 14. Diebold-Mariano ──

def section_dm_test(L):
    L.append(_h('14. DIEBOLD-MARIANO TEST — forecast comparison with HAC SE'))
    df = _load('dm_test.csv')
    if df is None or df.empty:
        L.append('  [MISSING] dm_test.csv')
        return

    L.append('')
    L.append('  Diebold-Mariano with Newey-West HAC variance accounts for serial')
    L.append('  correlation in loss differentials. This is the standard forecast')
    L.append('  comparison test in the econometrics literature.')
    L.append('')
    L.append('  Negative mean_loss_diff = MSM has lower loss = MSM better.')

    for (model, loss), grp in df.groupby(['model_type', 'loss']):
        L.append(_sub(f'{model}, {loss} loss'))
        sig = grp[grp['significant_at_0.05']]
        if sig.empty:
            L.append(_f('DETAIL',
                f'No pairs reach p<0.05. On {loss} with HAC-corrected variance, '
                f'MSM and best baselines are statistically indistinguishable.'))
            continue
        wins = sig[sig['msm_better']]
        losses = sig[~sig['msm_better']]
        L.append(f'  MSM significantly better: {len(wins)}/{len(grp)} pairs')
        L.append(f'  MSM significantly worse:  {len(losses)}/{len(grp)} pairs')
        for _, row in wins.iterrows():
            L.append(f"    [WIN]  {row['msm_retrainer'][:25]} vs {row['baseline_retrainer'][:25]}: "
                     f"Δ={row['mean_loss_diff']:+.5f}, p={row['p_value']:.4f}")
        for _, row in losses.iterrows():
            L.append(f"    [LOSS] {row['msm_retrainer'][:25]} vs {row['baseline_retrainer'][:25]}: "
                     f"Δ={row['mean_loss_diff']:+.5f}, p={row['p_value']:.4f}")

def main():
    lines = [
        '=' * W,
        '  CMS-BASED RETRAINING — RESULTS REPORT (regression task)',
        '  Generated by interpret_results.py',
        '=' * W, '',
    ]

    section_ranking(lines)
    section_aggregate_metrics(lines)
    section_stress(lines)
    section_latency(lines)
    section_selectivity(lines)
    section_significance(lines)
    section_sensitivity(lines)
    section_features(lines)
    section_lead_lag(lines)
    section_bootstrap_ci(lines)
    section_qlike(lines)
    section_stratified_qlike(lines)
    section_cofiring(lines)
    section_dm_test(lines)

    report = '\n'.join(lines)
    print(report)

    out = os.path.join(ANALYSIS_DIR, 'narrative_report.txt')
    os.makedirs(ANALYSIS_DIR, exist_ok=True)
    with open(out, 'w') as f:
        f.write(report)
    print(f'\nSaved: {out}')


if __name__ == '__main__':
    main()
