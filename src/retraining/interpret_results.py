"""
interpret_results.py — Structured analysis of all retraining experiment results.

Run this AFTER analysis.py and significance_test.py.

    python src/retraining/interpret_results.py

Reads every CSV from results/analysis/ and prints a structured report covering:

    Section 1  — Overall strategy ranking by mean F1
    Section 2  — Stress vs calm regime performance
    Section 3  — Stress event detection latency
    Section 4  — Retrain selectivity (false positive rate)
    Section 5  — Regime retrain rate (stress vs calm concentration)
    Section 6  — Statistical significance (Friedman + Wilcoxon + effect sizes)
    Section 7  — MSM hyperparameter sensitivity
    Section 8  — Cooldown mechanism analysis
    Section 9  — Causal feature usage

Each section prints three labelled lines per finding:

    FINDING       — what the numbers show
    DETAIL        — the exact values that support it
    CONCERN       — automatic red-flag detection (e.g. MSM not beating baseline)

The full report is also saved to results/analysis/narrative_report.txt.
"""

import os
import textwrap
import numpy as np
import pandas as pd

ANALYSIS_DIR   = 'results/analysis'
WIDTH          = 80

MSM_TYPES      = {'msm', 'spy_msm', 'timeout_msm', 'causal'}
BASELINE_TYPES = {'static', 'random', 'fixed'}


# ---------------------------------------------------------------------------
# UTILITIES
# ---------------------------------------------------------------------------

def _h(title):
    sep = '=' * WIDTH
    return f'\n{sep}\n  {title}\n{sep}'

def _sub(title):
    return f'\n--- {title} ---'

def _wrap(label, text):
    prefix  = f'  {label}: '
    indent  = ' ' * len(prefix)
    return textwrap.fill(text, width=WIDTH,
                         initial_indent=prefix, subsequent_indent=indent)

def _load(filename):
    path = os.path.join(ANALYSIS_DIR, filename)
    if not os.path.exists(path):
        return None
    return pd.read_csv(path)

def _tag(exp_type):
    if exp_type in MSM_TYPES:
        return ' <-- MSM'
    if exp_type in BASELINE_TYPES:
        return ' <-- baseline'
    return ''


# ---------------------------------------------------------------------------
# SECTION 1 — OVERALL RANKING
# ---------------------------------------------------------------------------

def section_overall_ranking(lines):
    lines.append(_h('1. OVERALL STRATEGY RANKING'))
    lines.append(
        '  Strategies are ranked by mean macro-F1 across all rolling windows.\n'
        '  MSM-family types: msm, spy_msm, timeout_msm, causal\n'
        '  Baseline types:   static, random, fixed'
    )

    df = _load('overall_summary.csv')
    if df is None:
        lines.append('  [MISSING] overall_summary.csv — run analysis.py first.')
        return

    for model, group in df.groupby('model_type'):
        lines.append(_sub(f'Model: {model}'))
        group = group.sort_values('mean_f1', ascending=False).reset_index(drop=True)

        top = group.iloc[0]
        lines.append(_wrap('FINDING',
            f"Highest mean F1 across all {len(group)} strategies: "
            f"{top['mean_f1']:.4f} (±{top['std_f1']:.4f}) — "
            f"'{top['retrainer']}' (exp_type: {top['exp_type']})."
        ))

        msm_rows  = group[group['exp_type'].isin(MSM_TYPES)]
        base_rows = group[group['exp_type'].isin(BASELINE_TYPES)]

        if not msm_rows.empty and not base_rows.empty:
            best_msm  = msm_rows.loc[msm_rows['mean_f1'].idxmax()]
            best_base = base_rows.loc[base_rows['mean_f1'].idxmax()]
            delta     = best_msm['mean_f1'] - best_base['mean_f1']

            lines.append(_wrap('DETAIL',
                f"Best MSM    : '{best_msm['retrainer']}'  F1={best_msm['mean_f1']:.4f}  "
                f"retrains={int(best_msm.get('retrains', 0))}\n"
                f"  Best baseline: '{best_base['retrainer']}'  F1={best_base['mean_f1']:.4f}  "
                f"retrains={int(best_base.get('retrains', 0))}\n"
                f"  Difference: {delta:+.4f} ({delta / best_base['mean_f1']:+.2%} relative)"
            ))

            if delta <= 0:
                lines.append(_wrap('CONCERN',
                    'MSM does NOT outperform the best baseline on raw mean F1. '
                    'Check Section 2 (stress vs calm) — MSM may still concentrate '
                    'improvement in the high-volatility windows that matter most.'
                ))

        # exp_type means — lets reader see which family wins, not just one config
        lines.append('  Mean F1 by exp_type:')
        type_means = (
            group.groupby('exp_type')['mean_f1']
            .mean().sort_values(ascending=False)
        )
        for exp_type, mean_f1 in type_means.items():
            lines.append(f'    {exp_type:<20} {mean_f1:.4f}{_tag(exp_type)}')

        # retrain frequency context
        lines.append('  Retrain frequency (top 5 by retrain count):')
        top5 = group.nlargest(5, 'retrains')[['retrainer', 'retrains', 'windows']].copy()
        top5['rate'] = (top5['retrains'] / top5['windows']).round(3)
        for _, row in top5.iterrows():
            lines.append(
                f"    {row['retrainer']:<45} "
                f"{int(row['retrains'])} retrains / {int(row['windows'])} windows "
                f"(rate={row['rate']})"
            )

        # full ranked table
        lines.append('  Full ranking:')
        lines.append(f"    {'#':<4} {'retrainer':<45} {'mean_f1':>8} {'std_f1':>8} {'retrains':>9}")
        for i, row in group.iterrows():
            lines.append(
                f"    {i+1:<4} {row['retrainer']:<45} "
                f"{row['mean_f1']:>8.4f} {row['std_f1']:>8.4f} "
                f"{int(row.get('retrains', 0)):>9}{_tag(row['exp_type'])}"
            )


# ---------------------------------------------------------------------------
# SECTION 2 — STRESS vs CALM
# ---------------------------------------------------------------------------

def section_stress_vs_calm(lines):
    lines.append(_h('2. STRESS vs. CALM REGIME PERFORMANCE'))
    lines.append(
        '  f1_stress_minus_calm = mean F1 during stress windows minus mean F1 during\n'
        '  calm windows. Positive = better during stress. Negative = degrades during stress.\n'
        '  MSM is designed to degrade less than baselines during stress.'
    )

    df = _load('stress_period_f1.csv')
    if df is None:
        lines.append('  [MISSING] stress_period_f1.csv')
        return

    for model, group in df.groupby('model_type'):
        lines.append(_sub(f'Model: {model}'))
        group = group.sort_values('f1_stress_minus_calm', ascending=False).reset_index(drop=True)

        msm_rows  = group[group['exp_type'].isin(MSM_TYPES)]
        base_rows = group[group['exp_type'].isin(BASELINE_TYPES)]

        if not msm_rows.empty and not base_rows.empty:
            best_msm_row  = msm_rows.iloc[0]
            worst_base_row = base_rows.iloc[-1]
            best_base_row  = base_rows.iloc[0]

            lines.append(_wrap('FINDING',
                f"Best MSM delta:      '{best_msm_row['retrainer']}' "
                f"({best_msm_row['f1_stress_minus_calm']:+.4f})\n"
                f"  Best baseline delta: '{best_base_row['retrainer']}' "
                f"({best_base_row['f1_stress_minus_calm']:+.4f})\n"
                f"  Worst baseline:      '{worst_base_row['retrainer']}' "
                f"({worst_base_row['f1_stress_minus_calm']:+.4f})"
            ))

            if best_msm_row['f1_stress_minus_calm'] > best_base_row['f1_stress_minus_calm']:
                lines.append(_wrap('FINDING',
                    'MSM degrades LESS during stress than the best baseline. '
                    'This is the expected behaviour from regime-conditional retraining.'
                ))
            else:
                lines.append(_wrap('CONCERN',
                    'MSM does not show a clear stress-period advantage over the best baseline. '
                    'Check detection_latency.csv — MSM may be triggering too late to help '
                    'within the stress window itself.'
                ))

        # full table
        lines.append(
            f"\n    {'retrainer':<45} {'stress_f1':>9} {'calm_f1':>8} {'delta':>7}"
        )
        cols = ['retrainer', 'exp_type', 'mean_f1_stress', 'mean_f1_calm', 'f1_stress_minus_calm']
        cols = [c for c in cols if c in group.columns]
        for _, row in group[cols].iterrows():
            lines.append(
                f"    {row['retrainer']:<45} "
                f"{row.get('mean_f1_stress', float('nan')):>9.4f} "
                f"{row.get('mean_f1_calm', float('nan')):>8.4f} "
                f"{row['f1_stress_minus_calm']:>+7.4f}"
                f"{_tag(row['exp_type'])}"
            )


# ---------------------------------------------------------------------------
# SECTION 3 — DETECTION LATENCY
# ---------------------------------------------------------------------------

def section_detection_latency(lines):
    lines.append(_h('3. STRESS EVENT DETECTION LATENCY'))
    lines.append(
        '  Latency = number of rolling windows between stress event onset and the\n'
        '  first retrain triggered by that strategy. Lower is better.\n'
        '  A strategy that never triggers is marked detected=False.\n'
        '\n'
        '  NOTE: this section is restricted to signal-driven retrainers\n'
        '  (MSM family, ADWIN, performance-based). Fixed-schedule and random\n'
        '  retrainers are excluded — their "detection" is timing, not capability,\n'
        '  and including them gives misleading rankings.'
    )

    df = _load('detection_latency.csv')
    if df is None:
        lines.append('  [MISSING] detection_latency.csv')
        return

    for model, group in df.groupby('model_type'):
        lines.append(_sub(f'Model: {model}'))

        for event, ev_group in group.groupby('event'):
            lines.append(f'\n  Event: {event}')

            detected     = ev_group[ev_group['detected'] == True]
            not_detected = ev_group[ev_group['detected'] == False]

            if not not_detected.empty:
                names = ', '.join(not_detected['retrainer'].tolist()[:5])
                lines.append(_wrap('CONCERN',
                    f"{len(not_detected)} strategies never triggered after this event: "
                    f"{names}" + (' ...' if len(not_detected) > 5 else '')
                ))

            if detected.empty:
                lines.append('    No strategies detected this event.')
                continue

            fastest = detected.sort_values('latency_windows').iloc[0]
            slowest = detected.sort_values('latency_windows').iloc[-1]

            lines.append(_wrap('FINDING',
                f"Fastest: '{fastest['retrainer']}' — "
                f"{fastest['latency_windows']} windows ({fastest.get('latency_days', '?')} days).\n"
                f"  Slowest: '{slowest['retrainer']}' — "
                f"{slowest['latency_windows']} windows."
            ))

            msm_det  = detected[detected['exp_type'].isin(MSM_TYPES)]
            base_det = detected[detected['exp_type'].isin(BASELINE_TYPES)]

            if not msm_det.empty and not base_det.empty:
                msm_lat  = msm_det['latency_windows'].mean()
                base_lat = base_det['latency_windows'].mean()
                lines.append(_wrap('DETAIL',
                    f"Mean MSM latency:      {msm_lat:.1f} windows\n"
                    f"  Mean baseline latency: {base_lat:.1f} windows\n"
                    f"  MSM is {'faster' if msm_lat < base_lat else 'slower'} on average "
                    f"({abs(msm_lat - base_lat):.1f} window difference)."
                ))

            lines.append(
                f"    {'retrainer':<45} {'latency':>7}  {'detected':<9}"
            )
            for _, row in detected.sort_values('latency_windows').iterrows():
                lines.append(
                    f"    {row['retrainer']:<45} {row['latency_windows']:>7}  "
                    f"{str(row['detected']):<9}{_tag(row['exp_type'])}"
                )


# ---------------------------------------------------------------------------
# SECTION 4 — SELECTIVITY / FALSE POSITIVE RATE
# ---------------------------------------------------------------------------

def section_selectivity(lines):
    lines.append(_h('4. RETRAIN SELECTIVITY (FALSE POSITIVE RATE)'))
    lines.append(
        '  precision = fraction of retrains that fell inside a known stress window.\n'
        '  false_positive_rate = retrains outside stress windows / total retrains.\n'
        '  High precision + low fpr = strategy is well-calibrated to real volatility.'
    )

    df = _load('false_positive_rate.csv')
    if df is None:
        lines.append('  [MISSING] false_positive_rate.csv')
        return

    for model, group in df.groupby('model_type'):
        lines.append(_sub(f'Model: {model}'))
        group = group.sort_values('precision', ascending=False).reset_index(drop=True)

        msm_rows  = group[group['exp_type'].isin(MSM_TYPES)]
        base_rows = group[group['exp_type'].isin(BASELINE_TYPES)]

        if not msm_rows.empty:
            best = msm_rows.iloc[0]
            lines.append(_wrap('FINDING',
                f"Best MSM precision: '{best['retrainer']}' = {best['precision']:.4f} "
                f"({int(best['true_positives'])} TP, "
                f"{int(best['false_positives'])} FP, "
                f"{int(best['total_retrains'])} total retrains)"
            ))

        if not msm_rows.empty and not base_rows.empty:
            msm_p  = msm_rows['precision'].mean()
            base_p = base_rows['precision'].mean()
            lines.append(_wrap('DETAIL',
                f"Mean MSM precision:      {msm_p:.4f}\n"
                f"  Mean baseline precision: {base_p:.4f}\n"
                f"  MSM is {'more' if msm_p > base_p else 'less'} selective "
                f"({abs(msm_p - base_p):.4f} difference)."
            ))

        lines.append(
            f"\n    {'retrainer':<45} {'precision':>9} {'fpr':>7} {'total':>6}"
        )
        for _, row in group.iterrows():
            lines.append(
                f"    {row['retrainer']:<45} "
                f"{row['precision']:>9.4f} "
                f"{row['false_positive_rate']:>7.4f} "
                f"{int(row['total_retrains']):>6}"
                f"{_tag(row['exp_type'])}"
            )


# ---------------------------------------------------------------------------
# SECTION 5 — REGIME RETRAIN RATE
# ---------------------------------------------------------------------------

def section_regime_retrain_rate(lines):
    lines.append(_h('5. REGIME RETRAIN RATE (STRESS vs. CALM)'))
    lines.append(
        '  stress_calm_ratio = retrain_rate_stress / retrain_rate_calm.\n'
        '  > 1.0  = strategy retrains more during stress (correct behaviour).\n'
        '  < 1.0  = strategy retrains more during calm (likely misconfigured).\n'
        '  = 1.0  = retrain rate is identical in both regimes (likely a fixed schedule).'
    )

    df = _load('regime_retrain_rate.csv')
    if df is None:
        lines.append('  [MISSING] regime_retrain_rate.csv')
        return

    for model, group in df.groupby('model_type'):
        lines.append(_sub(f'Model: {model}'))

        if 'stress_calm_ratio' not in group.columns:
            lines.append('  stress_calm_ratio column missing — re-run analysis.py.')
            continue

        inverted = group[group['stress_calm_ratio'] < 1.0]
        if not inverted.empty:
            lines.append(_wrap('CONCERN',
                f"{len(inverted)} strategies retrain MORE during calm than stress "
                f"(ratio < 1.0): "
                + ', '.join(inverted['retrainer'].tolist()[:5])
                + (' ...' if len(inverted) > 5 else '')
            ))

        group_sorted = group.sort_values('stress_calm_ratio', ascending=False)
        lines.append(
            f"\n    {'retrainer':<45} {'stress_rt':>9} {'calm_rt':>7} {'ratio':>6}"
        )
        for _, row in group_sorted.iterrows():
            stress = row.get('retrain_rate_stress', float('nan'))
            calm   = row.get('retrain_rate_calm',   float('nan'))
            ratio  = row.get('stress_calm_ratio',   float('nan'))
            lines.append(
                f"    {row['retrainer']:<45} "
                f"{stress:>9.3f} {calm:>7.3f} {ratio:>6.2f}"
                f"{_tag(row['exp_type'])}"
            )


# ---------------------------------------------------------------------------
# SECTION 6 — STATISTICAL SIGNIFICANCE
# ---------------------------------------------------------------------------

def section_significance(lines):
    lines.append(_h('6. STATISTICAL SIGNIFICANCE'))

    # Friedman global test
    friedman = _load('friedman_test.csv')
    if friedman is not None:
        lines.append(_sub('Friedman Test — are ANY strategies significantly different?'))
        lines.append(
            '  Non-parametric alternative to one-way repeated-measures ANOVA.\n'
            '  Significant result justifies pairwise comparisons below.'
        )
        for _, row in friedman.iterrows():
            sig_str = 'SIGNIFICANT' if row['significant'] else 'NOT significant'
            lines.append(
                f"  {row['model_type']:<15}  chi2={row['chi2_stat']:.4f}  "
                f"p={row['p_value']:.6f}  --> {sig_str} at alpha=0.05"
            )
            if not row['significant']:
                lines.append(_wrap('CONCERN',
                    f"Friedman test is not significant for {row['model_type']}. "
                    'We cannot reject the null that all strategies perform identically. '
                    'Still check effect sizes below — small-to-medium effects with '
                    'moderate window counts (~100-200) often fail to reach significance '
                    'but remain practically meaningful.'
                ))

    # Wilcoxon pairwise
    wilcoxon = _load('wilcoxon_results.csv')
    if wilcoxon is None:
        lines.append('  [MISSING] wilcoxon_results.csv — run significance_test.py first.')
    else:
        lines.append(_sub('Pairwise Wilcoxon Tests (Holm-Bonferroni corrected)'))
        lines.append(
            '  Each row is one strategy pair. diff = mean_f1_a minus mean_f1_b.\n'
            '  p_corr = corrected p-value. d = Cohen\'s d effect size.\n'
            '  [SIG] = significant after correction. [ns] = not significant.'
        )
        for model, group in wilcoxon.groupby('model_type'):
            lines.append(f'\n  Model: {model} — {len(group)} pairs tested')

            sig   = group[group['significant']]
            ns    = group[~group['significant']]

            if sig.empty:
                lines.append(_wrap('CONCERN',
                    'No pairs are significant after Holm-Bonferroni correction. '
                    'This is common with moderate window counts. '
                    'Report effect sizes from effect_size.csv rather than p-values.'
                ))
            else:
                lines.append(f'  Significant pairs ({len(sig)}):')
                lines.append(
                    f"    {'strategy_a':<35} {'strategy_b':<35} {'diff':>7} {'p_corr':>8} {'d':>6}"
                )
                for _, row in sig.sort_values('p_value_corrected').iterrows():
                    winner = (
                        ' ** MSM wins **'
                        if row.get('exp_type_a') in MSM_TYPES and row['mean_f1_a'] > row['mean_f1_b']
                        else (
                            ' ** baseline wins **'
                            if row.get('exp_type_b') in MSM_TYPES and row['mean_f1_b'] > row['mean_f1_a']
                            else ''
                        )
                    )
                    lines.append(
                        f"    {row['strategy_a']:<35} {row['strategy_b']:<35} "
                        f"{row['mean_diff']:>+7.4f} {row['p_value_corrected']:>8.4f} "
                        f"{row['cohens_d']:>6.3f}{winner}"
                    )

            # notable non-significant pairs with medium+ effect size
            notable = ns[ns['cohens_d'].abs() >= 0.2]
            if not notable.empty:
                lines.append(
                    f'\n  Non-significant pairs with |d| >= 0.2 '
                    f'(practically meaningful even if not corrected-significant):'
                )
                for _, row in notable.sort_values('cohens_d', key=abs, ascending=False).head(10).iterrows():
                    lines.append(
                        f"    [ns]  {row['strategy_a']:<35} {row['strategy_b']:<35} "
                        f"diff={row['mean_diff']:>+7.4f}  d={row['cohens_d']:.3f}"
                    )

    # Effect sizes
    effect = _load('effect_size.csv')
    if effect is not None:
        lines.append(_sub("Effect Sizes — Cohen's d (MSM vs baselines)"))
        lines.append(
            "  Interpretation guide:\n"
            "    |d| < 0.2  negligible\n"
            "    0.2 – 0.5  small\n"
            "    0.5 – 0.8  medium  <-- practically meaningful\n"
            "    > 0.8      large"
        )
        for model, group in effect.groupby('model_type'):
            lines.append(f'\n  Model: {model}')
            lines.append(
                f"    {'':5} {'strategy_a':<35} {'strategy_b':<30} {'diff':>7} {'d':>7} {'magnitude'}"
            )
            for _, row in group.sort_values('cohens_d', ascending=False).iterrows():
                sig_marker = '[SIG]' if row['significant'] else '[ns] '
                lines.append(
                    f"    {sig_marker} {row['strategy_a']:<35} {row['strategy_b']:<30} "
                    f"{row['mean_diff']:>+7.4f} {row['cohens_d']:>+7.3f} "
                    f"({row['effect_label']})"
                )


# ---------------------------------------------------------------------------
# SECTION 7 — HYPERPARAMETER SENSITIVITY
# ---------------------------------------------------------------------------

def section_sensitivity(lines):
    lines.append(_h('7. MSM HYPERPARAMETER SENSITIVITY'))
    lines.append(
        '  Covers tau_1, tau_2, and lookback sweep results.\n'
        '  Small F1 range across configs = robust plateau (good).\n'
        '  Large F1 range              = fragile peak (be careful making claims).\n'
        '  Rule of thumb: range <= 0.02 is considered a stable plateau.'
    )

    df = _load('sensitivity_summary.csv')
    if df is None:
        lines.append('  [MISSING] sensitivity_summary.csv')
        return

    for model, group in df.groupby('model_type'):
        lines.append(_sub(f'Model: {model}'))
        for exp, sub in group.groupby('exp_type'):
            f1_range = sub['mean_f1'].max() - sub['mean_f1'].min()
            top5     = sub.sort_values('mean_f1', ascending=False).head(5)

            lines.append(f'\n  exp_type: {exp}')
            lines.append(f'  F1 range across {len(sub)} configs: {f1_range:.4f}')

            if f1_range <= 0.02:
                lines.append(_wrap('FINDING',
                    'Range <= 0.02: performance is robust to hyperparameter choice. '
                    'The result does not depend on a narrow band of tau/lookback values.'
                ))
            else:
                lines.append(_wrap('CONCERN',
                    f'Range > 0.02 ({f1_range:.4f}): hyperparameter choice has a '
                    'meaningful effect on performance. Report the sensitivity heatmap '
                    '(plot 08) and describe the stable region.'
                ))

            lines.append('  Top 5 configs:')
            lines.append(
                f"    {'tau_1':>6} {'tau_2':>6} {'lookback':>8} {'mean_f1':>8} {'std_f1':>7} {'retrains':>8}"
            )
            for _, row in top5.iterrows():
                lines.append(
                    f"    {row['tau_1']:>6.2f} {row['tau_2']:>6.2f} "
                    f"{int(row['lookback']):>8} {row['mean_f1']:>8.4f} "
                    f"±{row['std_f1']:>6.4f} {int(row['retrains']):>8}"
                )


# ---------------------------------------------------------------------------
# SECTION 8 — COOLDOWN ANALYSIS
# ---------------------------------------------------------------------------

def section_cooldown(lines):
    lines.append(_h('8. COOLDOWN MECHANISM ANALYSIS'))
    lines.append(
        '  suppression_rate = cooldown_suppressed / signals_fired.\n'
        '  High rate (> 0.30) = cooldown is blocking many genuine signals.\n'
        '                       Consider reducing the cooldown period.\n'
        '  Near zero          = cooldown rarely activates (may be too permissive).'
    )

    df = _load('cooldown_analysis.csv')
    if df is None:
        lines.append('  [MISSING] cooldown_analysis.csv')
        return

    for model, group in df.groupby('model_type'):
        lines.append(_sub(f'Model: {model}'))

        high = group[group['suppression_rate'] > 0.3]
        if not high.empty:
            lines.append(_wrap('CONCERN',
                f"{len(high)} strategies have suppression_rate > 0.30: "
                + ', '.join(high['retrainer'].tolist()[:5])
                + (' ...' if len(high) > 5 else '')
                + '. These retrainers are being held back by cooldown — '
                  'consider reducing the cooldown period in the config.'
            ))

        lines.append(
            f"\n    {'retrainer':<45} {'signals':>7} {'suppressed':>10} {'rate':>6}"
        )
        for _, row in group.sort_values('suppression_rate', ascending=False).iterrows():
            lines.append(
                f"    {row['retrainer']:<45} "
                f"{int(row['signals_fired']):>7} "
                f"{int(row['cooldown_suppressed']):>10} "
                f"{row['suppression_rate']:>6.3f}"
                f"{_tag(row['exp_type'])}"
            )


# ---------------------------------------------------------------------------
# SECTION 9 — CAUSAL FEATURE USAGE
# ---------------------------------------------------------------------------

def section_causal_features(lines):
    lines.append(_h('9. CAUSAL FEATURE USAGE (CausalFeatureRetrainer)'))
    lines.append(
        '  selection_rate = fraction of rolling windows in which each feature\n'
        '  was selected as a causal predictor of SPY direction.\n'
        '  High rate (> 0.8) = structurally stable predictor across all regimes.\n'
        '  Low rate  (< 0.1) = highly regime-specific or noise feature.'
    )

    df = _load('causal_feature_usage.csv')
    if df is None or df.empty:
        lines.append('  [MISSING or EMPTY] causal_feature_usage.csv')
        return

    for model, group in df.groupby('model_type'):
        lines.append(_sub(f'Model: {model}'))

        top10 = group.sort_values('selection_rate', ascending=False).head(10)
        always  = group[group['selection_rate'] == 1.0]
        never   = group[group['selection_rate'] < 0.05]

        if not always.empty:
            lines.append(_wrap('FINDING',
                f"Always-selected (rate=1.0): {', '.join(always['feature'].tolist())}. "
                'These features are unconditional causal predictors regardless of market regime.'
            ))

        if not never.empty:
            lines.append(_wrap('FINDING',
                f"Rarely-selected (rate<0.05): "
                f"{', '.join(never['feature'].tolist()[:10])}. "
                'These may be noise or highly regime-specific features.'
            ))

        lines.append('\n  Top 10 by selection_rate:')
        lines.append(
            f"    {'feature':<30} {'rate':>6}  {'selected':>8} / {'windows':<8}"
        )
        for _, row in top10.iterrows():
            bar = '#' * int(row['selection_rate'] * 25)
            lines.append(
                f"    {row['feature']:<30} {row['selection_rate']:>6.3f}  "
                f"{int(row['selection_count']):>8} / {int(row['total_windows']):<8}  "
                f"|{bar}|"
            )


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    lines = [
        '=' * WIDTH,
        '  CMS-BASED RETRAINING — RESULTS INTERPRETATION REPORT',
        '  Generated by: src/retraining/interpret_results.py',
        '  Source CSVs:  results/analysis/',
        '=' * WIDTH,
        '',
        '  Run order:',
        '    1. python src/retraining/analysis.py          # produces results/analysis/*.csv',
        '    2. python src/retraining/significance_test.py # produces wilcoxon + effect_size CSVs',
        '    3. python src/retraining/interpret_results.py # this file',
        '    4. python src/retraining/plotting.py          # produces results/plots/*.png',
        '',
        '  Labels used throughout:',
        '    FINDING       = what the data shows',
        '    DETAIL        = exact values supporting the finding',
        '    CONCERN       = automatic red flag — something worth investigating',
        '',
    ]

    section_overall_ranking(lines)
    section_stress_vs_calm(lines)
    section_detection_latency(lines)
    section_selectivity(lines)
    section_regime_retrain_rate(lines)
    section_significance(lines)
    section_sensitivity(lines)
    section_cooldown(lines)
    section_causal_features(lines)

    report = '\n'.join(lines)
    print(report)

    out_path = os.path.join(ANALYSIS_DIR, 'narrative_report.txt')
    os.makedirs(ANALYSIS_DIR, exist_ok=True)
    with open(out_path, 'w') as f:
        f.write(report)
    print(f'\nReport saved to {out_path}')


if __name__ == '__main__':
    main()
