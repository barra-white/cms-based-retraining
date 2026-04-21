'''
interpret_results.py — Structured narrative report from analysis CSVs.

Run AFTER analysis.py, significance_test.py, and lead_lag_analysis.py.

Reads results/analysis/*.csv and produces results/analysis/narrative_report.txt.

Sections:
    1. Overall strategy ranking
    2. Aggregate metrics (macro F1, weighted F1, MCC, Cohen's kappa)
    3. Stress vs calm regime performance
    4. Detection latency
    5. Retrain selectivity
    6. Statistical significance
    7. Hyperparameter sensitivity
    8. Causal feature usage
    9. Lead-lag analysis (RQ1 evidence)
    10. Bootstrap F1 confidence intervals
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


# ── 1. Overall ranking ──

def section_ranking(L):
    L.append(_h('1. OVERALL STRATEGY RANKING'))
    df = _load('overall_summary.csv')
    if df is None:
        L.append('  [MISSING] overall_summary.csv')
        return

    for model, grp in df.groupby('model_type'):
        L.append(_sub(f'Model: {model}'))
        grp = grp.sort_values('mean_f1', ascending=False).reset_index(drop=True)
        top = grp.iloc[0]
        L.append(_f('FINDING', f"Best: '{top['retrainer']}' (F1={top['mean_f1']:.4f} ±{top['std_f1']:.4f})"))

        msm  = grp[grp['exp_type'].isin(MSM_TYPES)]
        base = grp[grp['exp_type'].isin(BASELINE_TYPES)]
        if not msm.empty and not base.empty:
            bm = msm.iloc[0]
            bb = base.iloc[0]
            delta = bm['mean_f1'] - bb['mean_f1']
            L.append(_f('DETAIL',
                f"Best MSM: '{bm['retrainer']}' F1={bm['mean_f1']:.4f} | "
                f"Best baseline: '{bb['retrainer']}' F1={bb['mean_f1']:.4f} | "
                f"Diff: {delta:+.4f}"))
            if delta <= 0:
                L.append(_f('CONCERN', 'MSM does not outperform best baseline on raw F1. '
                            'Rely on stress-period, detection-latency, and lead-lag evidence.'))

        L.append('  F1 by type:')
        for exp, f1 in grp.groupby('exp_type')['mean_f1'].mean().sort_values(ascending=False).items():
            L.append(f'    {exp:<20} {f1:.4f}{_tag(exp)}')


# ── 2. Aggregate metrics ──

def section_aggregate_metrics(L):
    L.append(_h('2. AGGREGATE METRICS (robust to class imbalance)'))
    df = _load('aggregate_metrics.csv')
    if df is None:
        L.append('  [MISSING] aggregate_metrics.csv')
        return

    L.append('')
    L.append('  MCC and Cohen\'s kappa correct for chance agreement and are not')
    L.append('  biased by class imbalance. Use these when macro F1 differences')
    L.append('  are small.')

    for model, grp in df.groupby('model_type'):
        L.append(_sub(f'Model: {model}'))
        grp = grp.sort_values('mcc', ascending=False)
        top = grp.iloc[0]
        L.append(_f('FINDING',
            f"Best by MCC: '{top['retrainer']}' (MCC={top['mcc']:.4f}, "
            f"kappa={top['cohens_kappa']:.4f}, weighted_f1={top['weighted_f1']:.4f})"))

        msm  = grp[grp['exp_type'].isin(MSM_TYPES)]
        base = grp[grp['exp_type'].isin(BASELINE_TYPES)]
        if not msm.empty and not base.empty:
            bm = msm.iloc[0]
            bb = base.iloc[0]
            L.append(_f('DETAIL',
                f"Best MSM MCC={bm['mcc']:.4f} kappa={bm['cohens_kappa']:.4f} | "
                f"Best baseline MCC={bb['mcc']:.4f} kappa={bb['cohens_kappa']:.4f} | "
                f"MCC diff: {bm['mcc'] - bb['mcc']:+.4f}"))


# ── 3. Stress vs calm ──

def section_stress(L):
    L.append(_h('3. STRESS vs CALM'))
    df = _load('stress_period_f1.csv')
    if df is None or 'f1_stress_minus_calm' not in df.columns:
        L.append('  [MISSING]')
        return

    for model, grp in df.groupby('model_type'):
        L.append(_sub(f'Model: {model}'))
        grp = grp.sort_values('f1_stress_minus_calm', ascending=False)
        msm  = grp[grp['exp_type'].isin(MSM_TYPES)]
        base = grp[grp['exp_type'].isin(BASELINE_TYPES)]
        if not msm.empty and not base.empty:
            bm = msm.iloc[0]
            bb = base.iloc[0]
            L.append(_f('FINDING',
                f"Best MSM delta: {bm['f1_stress_minus_calm']:+.4f} ('{bm['retrainer']}') | "
                f"Best baseline delta: {bb['f1_stress_minus_calm']:+.4f} ('{bb['retrainer']}')"))
            if bm['f1_stress_minus_calm'] > bb['f1_stress_minus_calm']:
                L.append(_f('FINDING', 'MSM degrades less during stress than best baseline.'))
            else:
                L.append(_f('CONCERN', 'MSM shows no clear stress advantage on F1.'))


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
    L.append(_h('6. STATISTICAL SIGNIFICANCE'))

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
        L.append(_sub("Effect Sizes (Cohen's d)"))
        for model, grp in effect.groupby('model_type'):
            L.append(f'  {model}:')
            for _, row in grp.sort_values('cohens_d', ascending=False).iterrows():
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
            rng = sub['mean_f1'].max() - sub['mean_f1'].min()
            L.append(f'  {exp}: F1 range={rng:.4f} across {len(sub)} configs')
            if rng <= 0.02:
                L.append(_f('FINDING', 'Robust plateau — results not sensitive to hyperparameters.'))
            else:
                L.append(_f('CONCERN', 'Range > 0.02 — hyperparameters matter.'))


# ── 8. Causal features ──

def section_features(L):
    L.append(_h('8. CAUSAL FEATURE USAGE'))
    df = _load('causal_feature_usage.csv')
    if df is None or df.empty:
        L.append('  [MISSING or EMPTY]')
        return

    for model, grp in df.groupby('model_type'):
        L.append(_sub(f'Model: {model}'))
        always = grp[grp['selection_rate'] == 1.0]
        if not always.empty:
            L.append(_f('FINDING', f"Always selected: {', '.join(always['feature'].tolist())}"))
        top5 = grp.sort_values('selection_rate', ascending=False).head(5)
        for _, row in top5.iterrows():
            L.append(f"    {row['feature']:<25} rate={row['selection_rate']:.3f}")


# ── 9. Lead-lag (RQ1 primary evidence) ──

def section_lead_lag(L):
    L.append(_h('9. LEAD-LAG ANALYSIS — RQ1 PRIMARY EVIDENCE'))
    df = _load('lead_lag_results.csv')
    if df is None:
        L.append('  [MISSING] lead_lag_results.csv')
        return

    L.append('')
    L.append('  Does MSM predictively lead regime-normalised returns?')
    L.append('  (Granger causality on drift observer run, which uses a frozen')
    L.append('   model so F1 degradation is unconfounded by retraining events.)')

    # Primary test: graph MSM vs secondary target
    primary = df[(df['signal'] == 'graph_msm') & (df['vs'] == cfg.TARGET_SECONDARY)]

    L.append(_sub(f'Primary test: graph_msm → {cfg.TARGET_SECONDARY}'))
    for _, row in primary.iterrows():
        sig = 'SIGNIFICANT' if row.get('granger_significant', False) else 'not significant'
        xc_str = (f"xc={row['xc_corr_at_best_lag']:.3f}@lag{row['xc_best_lag']}"
                  if not pd.isna(row.get('xc_corr_at_best_lag', np.nan))
                  else 'xc=N/A')
        if 'xc_ci_lower' in row and not pd.isna(row['xc_ci_lower']):
            xc_str += f" [CI: {row['xc_ci_lower']:.3f}, {row['xc_ci_upper']:.3f}]"
        L.append(f"  {row['model_type']:<10}  Granger p={row['granger_min_p']:.4f} ({sig})  "
                 f"best lag={row['granger_best_lag']}  {xc_str}")

    sig_count = primary['granger_significant'].sum() if 'granger_significant' in primary.columns else 0
    total = len(primary)
    if total > 0:
        if sig_count == total:
            L.append(_f('FINDING',
                f'MSM Granger-causes {cfg.TARGET_SECONDARY} on all {total} models. '
                f'Strong support for RQ1: MSM provides predictive lead time over '
                f'regime-normalised returns.'))
        elif sig_count >= max(1, total / 2):
            L.append(_f('FINDING',
                f'MSM Granger-causes {cfg.TARGET_SECONDARY} on {sig_count}/{total} models. '
                f'RQ1 supported with model-dependent caveats.'))
        else:
            L.append(_f('CONCERN',
                f'MSM Granger-causes {cfg.TARGET_SECONDARY} on only {sig_count}/{total} models. '
                f'RQ1 weakly supported.'))

    # Secondary: does MSM lead F1?
    f1_test = df[(df['signal'] == 'graph_msm') & (df['vs'] == 'F1')]
    if not f1_test.empty:
        L.append(_sub('Secondary: does MSM lead F1 on frozen model?'))
        for _, row in f1_test.iterrows():
            sig = 'SIG' if row.get('granger_significant', False) else 'ns '
            L.append(f"  {row['model_type']:<10}  Granger p={row['granger_min_p']:.4f} [{sig}]  "
                     f"xc best lag={row['xc_best_lag']}")

    # Additional: SPY-focused MSM
    spy_test = df[(df['signal'] == 'spy_msm') & (df['vs'] == cfg.TARGET_SECONDARY)]
    if not spy_test.empty:
        L.append(_sub(f'SPY-focused MSM → {cfg.TARGET_SECONDARY}'))
        for _, row in spy_test.iterrows():
            sig = 'SIG' if row.get('granger_significant', False) else 'ns '
            L.append(f"  {row['model_type']:<10}  Granger p={row['granger_min_p']:.4f} [{sig}]")


# ── 10. Bootstrap CIs ──

def section_bootstrap_ci(L):
    L.append(_h('10. BOOTSTRAP F1 CONFIDENCE INTERVALS'))
    df = _load('f1_bootstrap_ci.csv')
    if df is None or df.empty:
        L.append('  [MISSING]')
        return

    L.append('')
    L.append('  95% CI on mean F1 via 1000-sample bootstrap resampling.')
    L.append('  If CIs for MSM and best baseline overlap, the F1 difference is')
    L.append('  within sampling noise and should not be emphasised.')

    for model, grp in df.groupby('model_type'):
        L.append(_sub(f'Model: {model}'))
        grp = grp.sort_values('mean_f1', ascending=False)

        msm  = grp[grp['exp_type'].isin(MSM_TYPES)]
        base = grp[grp['exp_type'].isin(BASELINE_TYPES)]
        if not msm.empty and not base.empty:
            bm = msm.iloc[0]
            bb = base.iloc[0]
            L.append(f"  Best MSM:      {bm['retrainer'][:30]:<30}  "
                     f"{bm['mean_f1']:.4f} [{bm['ci_lower']:.4f}, {bm['ci_upper']:.4f}]")
            L.append(f"  Best baseline: {bb['retrainer'][:30]:<30}  "
                     f"{bb['mean_f1']:.4f} [{bb['ci_lower']:.4f}, {bb['ci_upper']:.4f}]")
            overlap = (bm['ci_lower'] <= bb['ci_upper']) and (bb['ci_lower'] <= bm['ci_upper'])
            if overlap:
                L.append(_f('CONCERN',
                    'CIs overlap — F1 difference is within sampling noise. '
                    'Rely on stress-period and lead-lag evidence.'))
            else:
                L.append(_f('FINDING',
                    'CIs do not overlap — F1 difference is genuine.'))


def main():
    lines = [
        '=' * W,
        '  CMS-BASED RETRAINING — RESULTS REPORT',
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

    report = '\n'.join(lines)
    print(report)

    out = os.path.join(ANALYSIS_DIR, 'narrative_report.txt')
    os.makedirs(ANALYSIS_DIR, exist_ok=True)
    with open(out, 'w') as f:
        f.write(report)
    print(f'\nSaved: {out}')


if __name__ == '__main__':
    main()