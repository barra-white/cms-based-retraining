'''
interpret_results.py — Structured narrative report from analysis CSVs.

Run AFTER analysis.py and significance_test.py.

Reads results/analysis/*.csv and prints a structured report.
Also saves to results/analysis/narrative_report.txt.

Sections:
    1. Overall strategy ranking
    2. Stress vs calm regime performance
    3. Detection latency
    4. Retrain selectivity
    5. Statistical significance
    6. Hyperparameter sensitivity
    7. Causal feature usage
    8. Bin recalibration control
'''

import os
import textwrap
import numpy as np
import pandas as pd

ANALYSIS_DIR = 'results/analysis'
W = 80

MSM_TYPES      = {'msm', 'spy_msm', 'timeout_msm', 'causal'}
BASELINE_TYPES = {'static', 'random', 'fixed'}


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
    # Backfill exp_type if missing (stale CSV from older analysis.py)
    if 'exp_type' not in df.columns and 'retrainer' in df.columns:
        df['exp_type'] = df['retrainer'].apply(
            lambda n: next((p for p in ['spy_msm','timeout_msm','msm','causal',
                            'fixed','perf','adwin','random','static'] if n.startswith(p)), 'other'))
    return df

def _tag(exp):
    if exp in MSM_TYPES:      return ' [MSM]'
    if exp in BASELINE_TYPES:  return ' [baseline]'
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
                            'Check stress-period analysis.'))

        # Mean F1 by exp_type
        L.append('  F1 by type:')
        for exp, f1 in grp.groupby('exp_type')['mean_f1'].mean().sort_values(ascending=False).items():
            L.append(f'    {exp:<20} {f1:.4f}{_tag(exp)}')


# ── 2. Stress vs calm ──

def section_stress(L):
    L.append(_h('2. STRESS vs CALM'))
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
                L.append(_f('CONCERN', 'MSM shows no clear stress advantage.'))


# ── 3. Detection latency ──

def section_latency(L):
    L.append(_h('3. DETECTION LATENCY (signal-driven only)'))
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


# ── 4. Selectivity ──

def section_selectivity(L):
    L.append(_h('4. RETRAIN SELECTIVITY'))
    df = _load('false_positive_rate.csv')
    if df is None or df.empty:
        L.append('  [MISSING or EMPTY] — no retrains detected across any strategy.')
        return

    if 'precision' not in df.columns:
        L.append('  [SKIP] precision column missing — delete results/analysis/ and re-run analysis.py')
        return

    for model, grp in df.groupby('model_type'):
        L.append(_sub(f'Model: {model}'))
        msm = grp[grp['exp_type'].isin(MSM_TYPES)]
        base = grp[grp['exp_type'].isin(BASELINE_TYPES)]
        if not msm.empty and not base.empty:
            L.append(_f('DETAIL',
                f"Mean MSM precision: {msm['precision'].mean():.4f} | "
                f"Mean baseline precision: {base['precision'].mean():.4f}"))


# ── 5. Significance ──

def section_significance(L):
    L.append(_h('5. STATISTICAL SIGNIFICANCE'))

    friedman = _load('friedman_test.csv')
    if friedman is not None:
        L.append(_sub('Friedman Test'))
        for _, row in friedman.iterrows():
            sig = 'SIGNIFICANT' if row['significant'] else 'NOT significant'
            L.append(f"  {row['model_type']}: chi2={row['chi2_stat']:.4f} p={row['p_value']:.6f} → {sig}")
            if not row['significant']:
                L.append(_f('CONCERN', 'Friedman not significant. Report effect sizes instead.'))

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


# ── 6. Sensitivity ──

def section_sensitivity(L):
    L.append(_h('6. HYPERPARAMETER SENSITIVITY'))
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
                L.append(_f('CONCERN', f'Range > 0.02 — hyperparameters matter.'))


# ── 7. Causal features ──

def section_features(L):
    L.append(_h('7. CAUSAL FEATURE USAGE'))
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


# ── 8. Bin recalibration ──

def section_bin_control(L):
    L.append(_h('8. BIN RECALIBRATION CONTROL'))
    df = _load('overall_summary.csv')
    if df is None:
        L.append('  [MISSING]')
        return

    for model, grp in df.groupby('model_type'):
        L.append(_sub(f'Model: {model}'))
        static = grp[grp['retrainer'] == 'static']
        rolling = grp[grp['retrainer'] == 'static_rolling_bins']
        msm = grp[grp['exp_type'].isin(MSM_TYPES)]

        if static.empty or msm.empty:
            continue

        s_f1 = static.iloc[0]['mean_f1']
        r_f1 = rolling.iloc[0]['mean_f1'] if not rolling.empty else np.nan
        m_f1 = msm.iloc[0]['mean_f1']

        L.append(f'  Static:             F1 = {s_f1:.4f}')
        if not np.isnan(r_f1):
            L.append(f'  StaticRollingBins:  F1 = {r_f1:.4f}  (bin recalibration gain: {r_f1 - s_f1:+.4f})')
        L.append(f'  Best MSM:           F1 = {m_f1:.4f}  (total gain: {m_f1 - s_f1:+.4f})')

        if not np.isnan(r_f1) and abs(m_f1 - r_f1) < 0.005:
            L.append(_f('CONCERN',
                'MSM and StaticRollingBins are nearly identical. '
                'Most of the apparent MSM gain may be from bin recalibration, not model retraining.'))
        elif not np.isnan(r_f1):
            model_gain = m_f1 - r_f1
            L.append(_f('FINDING',
                f'Model retraining contributes {model_gain:+.4f} beyond bin recalibration. '
                f'This supports the thesis claim.'))


def main():
    lines = [
        '=' * W,
        '  CMS-BASED RETRAINING — RESULTS REPORT',
        '  Generated by interpret_results.py',
        '=' * W, '',
    ]

    section_ranking(lines)
    section_stress(lines)
    section_latency(lines)
    section_selectivity(lines)
    section_significance(lines)
    section_sensitivity(lines)
    section_features(lines)
    section_bin_control(lines)

    report = '\n'.join(lines)
    print(report)

    out = os.path.join(ANALYSIS_DIR, 'narrative_report.txt')
    os.makedirs(ANALYSIS_DIR, exist_ok=True)
    with open(out, 'w') as f:
        f.write(report)
    print(f'\nSaved: {out}')


if __name__ == '__main__':
    main()