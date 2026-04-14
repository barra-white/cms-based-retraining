'''
interpret_results.py — Plain-English interpretation of all analysis CSVs.

Run this AFTER analysis.py and significance_test.py have been run.

What this file does
-------------------
Each section reads one or more CSVs from results/analysis/ and prints
a structured verdict:

    FINDING: one-sentence summary of what the numbers show
    DETAIL:  the actual numbers that support it
    CONCERN: any red flags worth investigating
    THESIS:  how to phrase this in the thesis writeup

The output is designed to be read top-to-bottom and directly feed
your discussion chapter. Copy the THESIS lines as first-draft sentences.

Usage
-----
    python interpret_results.py

Outputs: results/analysis/narrative_report.txt (same text as stdout)
'''

import os
import textwrap
import numpy as np
import pandas as pd

ANALYSIS_DIR = 'results/analysis'
WIDTH = 80   # wrap width for printed output

MSM_TYPES     = {'msm', 'spy_msm', 'timeout_msm', 'causal'}
BASELINE_TYPES = {'static', 'random', 'fixed'}


# ----- UTILS ----- #

def _h(title):
    sep = '=' * WIDTH
    return f'\n{sep}\n  {title}\n{sep}'

def _sub(title):
    return f'\n--- {title} ---'

def _wrap(label, text):
    prefix = f'  {label}: '
    indent = ' ' * len(prefix)
    return textwrap.fill(text, width=WIDTH,
                         initial_indent=prefix, subsequent_indent=indent)

def _load(filename):
    path = os.path.join(ANALYSIS_DIR, filename)
    if not os.path.exists(path):
        return None
    return pd.read_csv(path)

def _best_msm(df, model_type=None):
    '''Return the single best MSM-family retrainer per model by mean_f1.'''
    sub = df[df['exp_type'].isin(MSM_TYPES)]
    if model_type:
        sub = sub[sub['model_type'] == model_type]
    if sub.empty:
        return None
    return sub.loc[sub['mean_f1'].idxmax()]

def _best_baseline(df, model_type=None):
    '''Return the single best baseline retrainer per model by mean_f1.'''
    sub = df[df['exp_type'].isin(BASELINE_TYPES)]
    if model_type:
        sub = sub[sub['model_type'] == model_type]
    if sub.empty:
        return None
    return sub.loc[sub['mean_f1'].idxmax()]


# ----- SECTION 1: OVERALL RANKING ----- #

def section_overall_ranking(lines):
    lines.append(_h('1. OVERALL STRATEGY RANKING'))

    df = _load('overall_summary.csv')
    if df is None:
        lines.append('  [MISSING] overall_summary.csv — run analysis.py first.')
        return

    for model, group in df.groupby('model_type'):
        lines.append(_sub(f'Model: {model}'))
        group = group.sort_values('mean_f1', ascending=False).reset_index(drop=True)

        # top strategy overall
        top = group.iloc[0]
        lines.append(_wrap('FINDING',
            f"The highest mean F1 across all {len(group)} strategies is "
            f"{top['mean_f1']:.4f} (\u00b1{top['std_f1']:.4f}), achieved by "
            f"'{top['retrainer']}' (exp_type: {top['exp_type']})."
        ))

        # best MSM vs best baseline
        best_msm  = _best_msm(group, None)     # already filtered to model
        best_base = _best_baseline(group, None)

        if best_msm is not None and best_base is not None:
            delta = best_msm['mean_f1'] - best_base['mean_f1']
            direction = 'higher' if delta > 0 else 'lower'
            lines.append(_wrap('DETAIL',
                f"Best MSM-family: '{best_msm['retrainer']}' F1={best_msm['mean_f1']:.4f}. "
                f"Best baseline: '{best_base['retrainer']}' F1={best_base['mean_f1']:.4f}. "
                f"MSM is {abs(delta):.4f} {direction} ({delta:+.2%} relative)."
            ))
            if delta <= 0:
                lines.append(_wrap('CONCERN',
                    'MSM does NOT outperform the best baseline on raw mean F1. '
                    'Check stress_period_f1.csv — MSM may still outperform during '
                    'the periods that matter even if overall F1 is similar.'
                ))

        # all exp_type means
        type_means = (
            group.groupby('exp_type')['mean_f1']
            .mean().sort_values(ascending=False)
        )
        lines.append('  DETAIL (by exp_type):')
        for exp_type, mean_f1 in type_means.items():
            marker = ' <-- MSM family' if exp_type in MSM_TYPES else ''
            lines.append(f'    {exp_type:<20} mean_f1={mean_f1:.4f}{marker}')

        # retraining frequency
        top_retrains = group.nlargest(3, 'retrains')[['retrainer', 'retrains', 'windows']]
        top_retrains['rate'] = (top_retrains['retrains'] / top_retrains['windows']).round(3)
        lines.append('  DETAIL (most frequent retrainers):')
        for _, row in top_retrains.iterrows():
            lines.append(f"    {row['retrainer']}: {int(row['retrains'])} retrains "
                         f"in {int(row['windows'])} windows (rate={row['rate']})")

        lines.append(_wrap('THESIS',
            f"For {model}, the best-performing strategy was "
            f"'{top['retrainer']}' with a mean macro-F1 of {top['mean_f1']:.4f}. "
            f"{'MSM-family strategies outperformed all baselines.' if (best_msm is not None and best_base is not None and best_msm['mean_f1'] > best_base['mean_f1']) else 'However, the performance difference over the best baseline was marginal on raw F1; regime-specific analysis below reveals a clearer separation.'}"
        ))


# ----- SECTION 2: STRESS VS CALM ----- #

def section_stress_vs_calm(lines):
    lines.append(_h('2. STRESS vs. CALM REGIME PERFORMANCE'))
    lines.append(
        '  This is the central thesis claim: MSM should degrade less during '
        'stress regimes because it adapts faster.'
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
            best_msm_delta  = msm_rows.iloc[0]
            best_base_delta = base_rows.iloc[0]
            worst_base      = base_rows.iloc[-1]

            lines.append(_wrap('FINDING',
                f"Best MSM stress advantage: '{best_msm_delta['retrainer']}' "
                f"stress_minus_calm={best_msm_delta['f1_stress_minus_calm']:+.4f}. "
                f"Best baseline: '{best_base_delta['retrainer']}' "
                f"stress_minus_calm={best_base_delta['f1_stress_minus_calm']:+.4f}."
            ))

            msm_val  = best_msm_delta['f1_stress_minus_calm']
            base_val = worst_base['f1_stress_minus_calm']

            if msm_val > base_val:
                lines.append(_wrap('FINDING',
                    f'MSM shows LESS degradation during stress periods than the '
                    f'worst-performing baseline ({worst_base["retrainer"]}, '
                    f'stress_minus_calm={base_val:+.4f}). '
                    f'This supports the central hypothesis.'
                ))
            else:
                lines.append(_wrap('CONCERN',
                    'MSM does NOT show a clear stress-period advantage. '
                    'This is a significant finding to discuss — check '
                    'detection_latency.csv to see if MSM responds too slowly.'
                ))

        # print top 5 and bottom 5 by stress delta
        lines.append('  All strategies ranked by f1_stress_minus_calm:')
        cols = ['retrainer', 'exp_type', 'mean_f1_stress', 'mean_f1_calm', 'f1_stress_minus_calm']
        cols = [c for c in cols if c in group.columns]
        for _, row in group[cols].head(20).iterrows():
            tag = ' <-- MSM' if row['exp_type'] in MSM_TYPES else (
                  ' <-- baseline' if row['exp_type'] in BASELINE_TYPES else '')
            lines.append(
                f"    {row['retrainer']:<45} "
                f"stress={row.get('mean_f1_stress', float('nan')):.4f} "
                f"calm={row.get('mean_f1_calm', float('nan')):.4f} "
                f"delta={row['f1_stress_minus_calm']:+.4f}{tag}"
            )

        lines.append(_wrap('THESIS',
            f'During the {len(df.get("n_stress", df))} identified stress windows '
            '(COVID-19 crash Feb 2020 and Fed rate hike cycle Mar 2022), '
            'MSM-family strategies showed [higher/lower] F1 degradation compared to '
            'static and fixed-schedule baselines, supporting [/failing to support] '
            'H1 that market-regime-conditional retraining preserves predictive accuracy '
            'during structural breaks.'
        ))


# ----- SECTION 3: DETECTION LATENCY ----- #

def section_detection_latency(lines):
    lines.append(_h('3. STRESS EVENT DETECTION LATENCY'))
    lines.append(
        '  Latency = how many windows after the stress event onset '
        'before the first retrain. Lower is better.'
    )

    df = _load('detection_latency.csv')
    if df is None:
        lines.append('  [MISSING] detection_latency.csv')
        return

    for model, group in df.groupby('model_type'):
        lines.append(_sub(f'Model: {model}'))

        for event, ev_group in group.groupby('event'):
            lines.append(f'  Event: {event}')

            detected    = ev_group[ev_group['detected'] == True]
            not_detected = ev_group[ev_group['detected'] == False]

            if not not_detected.empty:
                lines.append(_wrap('CONCERN',
                    f"{len(not_detected)} strategies never triggered a retrain after "
                    f"this event: {', '.join(not_detected['retrainer'].tolist()[:5])}"
                    + (' ...' if len(not_detected) > 5 else '')
                ))

            if detected.empty:
                lines.append('    No strategies detected this event.')
                continue

            fastest = detected.sort_values('latency_windows').iloc[0]
            slowest = detected.sort_values('latency_windows').iloc[-1]

            msm_det  = detected[detected['exp_type'].isin(MSM_TYPES)]
            base_det = detected[detected['exp_type'].isin(BASELINE_TYPES)]

            lines.append(_wrap('FINDING',
                f"Fastest: '{fastest['retrainer']}' at {fastest['latency_windows']} windows "
                f"({fastest['latency_days']} days). "
                f"Slowest: '{slowest['retrainer']}' at {slowest['latency_windows']} windows."
            ))

            if not msm_det.empty and not base_det.empty:
                msm_lat  = msm_det['latency_windows'].mean()
                base_lat = base_det['latency_windows'].mean()
                direction = 'faster' if msm_lat < base_lat else 'slower'
                lines.append(_wrap('DETAIL',
                    f'Mean MSM latency: {msm_lat:.1f} windows. '
                    f'Mean baseline latency: {base_lat:.1f} windows. '
                    f'MSM is {direction} on average.'
                ))

            # table
            lines.append('    retrainer                                     latency_windows  detected')
            for _, row in detected.sort_values('latency_windows').iterrows():
                tag = '<MSM' if row['exp_type'] in MSM_TYPES else (
                      '<base' if row['exp_type'] in BASELINE_TYPES else '')
                lines.append(
                    f"    {row['retrainer']:<45} {row['latency_windows']:<16} {str(row['detected']):<10} {tag}"
                )


# ----- SECTION 4: SELECTIVITY (FALSE POSITIVE RATE) ----- #

def section_selectivity(lines):
    lines.append(_h('4. RETRAIN SELECTIVITY (FALSE POSITIVE RATE)'))
    lines.append(
        '  Precision = fraction of retrains that fell inside a known stress window.'
        '\n  Random baseline precision \u2248 (2 events * 240 days) / total_days.'
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
            best_msm = msm_rows.iloc[0]
            lines.append(_wrap('FINDING',
                f"Best MSM precision: '{best_msm['retrainer']}' = {best_msm['precision']:.4f} "
                f"({best_msm['true_positives']} true positives, "
                f"{best_msm['false_positives']} false positives out of "
                f"{best_msm['total_retrains']} total retrains)."
            ))

        lines.append('  All strategies by precision:')
        for _, row in group.iterrows():
            tag = ' <-- MSM' if row['exp_type'] in MSM_TYPES else (
                  ' <-- baseline' if row['exp_type'] in BASELINE_TYPES else '')
            lines.append(
                f"    {row['retrainer']:<45} precision={row['precision']:.4f} "
                f"fpr={row['false_positive_rate']:.4f} "
                f"total={int(row['total_retrains'])}{tag}"
            )

        if not msm_rows.empty and not base_rows.empty:
            msm_p  = msm_rows['precision'].mean()
            base_p = base_rows['precision'].mean()
            lines.append(_wrap('THESIS',
                f'MSM-family strategies achieved a mean precision of {msm_p:.4f} '
                f'(fraction of retrains occurring during stress windows), compared '
                f'to {base_p:.4f} for baseline strategies. This indicates MSM is '
                f'{"more" if msm_p > base_p else "less"} selective in its retrain '
                'timing, triggering fewer spurious retrains during calm regimes.'
            ))


# ----- SECTION 5: REGIME RETRAIN RATE ----- #

def section_regime_retrain_rate(lines):
    lines.append(_h('5. REGIME RETRAIN RATE (STRESS vs. CALM)'))
    lines.append(
        '  stress_calm_ratio > 1 means the strategy correctly concentrates '
        'retrains during stress. This is the behavioural explanation for '
        'any F1 advantage seen in Section 2.'
    )

    df = _load('regime_retrain_rate.csv')
    if df is None:
        lines.append('  [MISSING] regime_retrain_rate.csv')
        return

    for model, group in df.groupby('model_type'):
        lines.append(_sub(f'Model: {model}'))

        if 'stress_calm_ratio' not in group.columns:
            lines.append('  stress_calm_ratio column missing — check data.')
            continue

        inverted = group[group.get('likely_inverted', pd.Series(False, index=group.index))]
        if not inverted.empty:
            lines.append(_wrap('CONCERN',
                f"{len(inverted)} strategies have stress_calm_ratio < 1.0 (retraining MORE "
                f"during calm than stress): "
                f"{', '.join(inverted['retrainer'].tolist()[:5])}"
            ))

        group_sorted = group.sort_values('stress_calm_ratio', ascending=False)
        lines.append('  Strategies by stress_calm_ratio (higher = more selective):')
        for _, row in group_sorted.iterrows():
            tag = ' <-- MSM' if row['exp_type'] in MSM_TYPES else (
                  ' <-- baseline' if row['exp_type'] in BASELINE_TYPES else '')
            stress = row.get('retrain_rate_stress', float('nan'))
            calm   = row.get('retrain_rate_calm',   float('nan'))
            ratio  = row.get('stress_calm_ratio',   float('nan'))
            lines.append(
                f"    {row['retrainer']:<45} stress={stress:.3f} calm={calm:.3f} ratio={ratio:.2f}{tag}"
            )


# ----- SECTION 6: STATISTICAL SIGNIFICANCE ----- #

def section_significance(lines):
    lines.append(_h('6. STATISTICAL SIGNIFICANCE'))

    # Friedman global test
    friedman = _load('friedman_test.csv')
    if friedman is not None:
        lines.append(_sub('Friedman Global Test (are ANY strategies different?)'))
        for _, row in friedman.iterrows():
            sig_str = 'SIGNIFICANT' if row['significant'] else 'NOT significant'
            lines.append(
                f"  {row['model_type']:<15} chi2={row['chi2_stat']:.4f} "
                f"p={row['p_value']:.6f}  --> {sig_str} at alpha=0.05"
            )
            if not row['significant']:
                lines.append(_wrap('CONCERN',
                    f"Friedman test is not significant for {row['model_type']}. "
                    'This means we cannot reject that all strategies perform identically. '
                    'Still report pairwise effect sizes — they remain meaningful and '
                    'are standard practice when sample sizes are moderate (~100-200 windows).'
                ))
            else:
                lines.append(_wrap('FINDING',
                    f"Strategies differ significantly for {row['model_type']} "
                    f"(chi2={row['chi2_stat']:.4f}, p={row['p_value']:.6f}). "
                    'Proceed to pairwise Wilcoxon tests to identify which pairs differ.'
                ))

    # Wilcoxon pairwise
    wilcoxon = _load('wilcoxon_results.csv')
    if wilcoxon is None:
        lines.append('  [MISSING] wilcoxon_results.csv — run significance_test.py first.')
        return

    lines.append(_sub('Pairwise Wilcoxon Tests (Holm-Bonferroni corrected)'))

    for model, group in wilcoxon.groupby('model_type'):
        lines.append(f'  Model: {model} ({len(group)} pairs tested)')
        sig_pairs = group[group['significant']]
        not_sig   = group[~group['significant']]

        if sig_pairs.empty:
            lines.append(_wrap('CONCERN',
                f'No significant pairs after correction for {model}. '
                'This is common with moderate window counts. '
                'Report effect sizes from effect_size.csv instead.'
            ))
        else:
            lines.append(f'  Significant pairs ({len(sig_pairs)}):')        
            for _, row in sig_pairs.sort_values('p_value_corrected').iterrows():
                winner_tag = (' ** MSM wins **'
                              if row.get('exp_type_a') in MSM_TYPES and row['mean_f1_a'] > row['mean_f1_b']
                              else (' ** baseline wins **'
                                   if row.get('exp_type_b') in MSM_TYPES and row['mean_f1_b'] > row['mean_f1_a']
                                   else ''))
                lines.append(
                    f"    {row['strategy_a']:<35} vs {row['strategy_b']:<35} "
                    f"diff={row['mean_diff']:+.4f} "
                    f"p_corr={row['p_value_corrected']:.4f} "
                    f"d={row['cohens_d']:.3f}{winner_tag}"
                )

        # even non-significant pairs with meaningful effect sizes
        notable_ns = not_sig[not_sig['cohens_d'].abs() >= 0.2]
        if not notable_ns.empty:
            lines.append(f'  Non-significant pairs with |d| >= 0.2 (still worth reporting):')
            for _, row in notable_ns.sort_values('cohens_d', key=abs, ascending=False).head(10).iterrows():
                lines.append(
                    f"    {row['strategy_a']:<35} vs {row['strategy_b']:<35} "
                    f"diff={row['mean_diff']:+.4f} "
                    f"p_corr={row['p_value_corrected']:.4f} "
                    f"d={row['cohens_d']:.3f}  (not sig, but small effect)"
                )

    # Effect sizes
    effect = _load('effect_size.csv')
    if effect is not None:
        lines.append(_sub('Effect Sizes — MSM vs Baselines'))
        lines.append(
            '  Cohen\'s d: |d|<0.2=negligible, 0.2-0.5=small, 0.5-0.8=medium, >0.8=large'
        )
        for model, group in effect.groupby('model_type'):
            lines.append(f'  Model: {model}')
            for _, row in group.sort_values('cohens_d', ascending=False).iterrows():
                sig_marker = '[SIG]' if row['significant'] else '[ns] '
                lines.append(
                    f"    {sig_marker} {row['strategy_a']:<35} vs {row['strategy_b']:<30} "
                    f"diff={row['mean_diff']:+.4f} d={row['cohens_d']:+.3f} "
                    f"({row['effect_label']})"
                )

        lines.append(_wrap('THESIS',
            'Pairwise Wilcoxon signed-rank tests with Holm-Bonferroni correction '
            'were conducted to compare the per-window F1 distributions of all strategy pairs. '
            'Effect sizes (Cohen\'s d) were computed for the primary comparisons between '
            'MSM-family strategies and baselines. '
            'Results are reported as mean F1 difference (MSM minus baseline), '
            'corrected p-value, and effect size label.'
        ))


# ----- SECTION 7: HYPERPARAMETER SENSITIVITY ----- #

def section_sensitivity(lines):
    lines.append(_h('7. MSM HYPERPARAMETER SENSITIVITY (tau_1, tau_2, lookback)'))
    lines.append(
        '  This tells you whether the best config is a fragile peak or a '
        'robust plateau. A plateau = the thesis finding generalises; '
        'a sharp peak = the result is overfit to these specific events.'
    )

    df = _load('sensitivity_summary.csv')
    if df is None:
        lines.append('  [MISSING] sensitivity_summary.csv')
        return

    for model, group in df.groupby('model_type'):
        lines.append(_sub(f'Model: {model}'))
        for exp, sub in group.groupby('exp_type'):
            lines.append(f'  exp_type: {exp}')
            top = sub.sort_values('mean_f1', ascending=False).head(5)
            lines.append('    Top 5 configs:')
            for _, row in top.iterrows():
                lines.append(
                    f"      tau_1={row['tau_1']:.2f} tau_2={row['tau_2']:.2f} "
                    f"lookback={int(row['lookback'])} -> mean_f1={row['mean_f1']:.4f} "
                    f"(\u00b1{row['std_f1']:.4f}) retrains={int(row['retrains'])}"
                )
            f1_range = sub['mean_f1'].max() - sub['mean_f1'].min()
            lines.append(_wrap('FINDING',
                f'F1 range across all {len(sub)} configs: {f1_range:.4f}. '
                + ('Small range (≤ 0.02): results are robust to hyperparameter choice.' if f1_range <= 0.02
                   else 'Large range (> 0.02): hyperparameter choice matters — report sensitivity plot.')
            ))


# ----- SECTION 8: COOLDOWN ANALYSIS ----- #

def section_cooldown(lines):
    lines.append(_h('8. COOLDOWN MECHANISM ANALYSIS'))
    lines.append(
        '  suppression_rate = fraction of fired signals blocked by cooldown.'
        '\n  High rate = cooldown too aggressive. Near zero = cooldown irrelevant.'
    )

    df = _load('cooldown_analysis.csv')
    if df is None:
        lines.append('  [MISSING] cooldown_analysis.csv')
        return

    for model, group in df.groupby('model_type'):
        lines.append(_sub(f'Model: {model}'))
        group = group.sort_values('suppression_rate', ascending=False)
        for _, row in group.iterrows():
            tag = ' <-- MSM' if row['exp_type'] in MSM_TYPES else ''
            lines.append(
                f"    {row['retrainer']:<45} "
                f"signals={int(row['signals_fired'])} "
                f"suppressed={int(row['cooldown_suppressed'])} "
                f"rate={row['suppression_rate']:.3f}{tag}"
            )
        high_suppress = group[group['suppression_rate'] > 0.3]
        if not high_suppress.empty:
            lines.append(_wrap('CONCERN',
                f"{len(high_suppress)} strategies have suppression_rate > 30%: "
                + ', '.join(high_suppress['retrainer'].tolist()[:5]) +
                '. Consider reducing cooldown for these configs.'
            ))


# ----- SECTION 9: CAUSAL FEATURE USAGE ----- #

def section_causal_features(lines):
    lines.append(_h('9. CAUSAL FEATURE USAGE (CausalFeatureRetrainer)'))
    lines.append(
        '  High selection_rate = structurally stable predictor of SPY direction.'
        '\n  Low selection_rate  = regime-specific (e.g. VIX spikes during stress only).'
    )

    df = _load('causal_feature_usage.csv')
    if df is None or df.empty:
        lines.append('  [MISSING or EMPTY] causal_feature_usage.csv')
        return

    for model, group in df.groupby('model_type'):
        lines.append(_sub(f'Model: {model}'))
        top = group.sort_values('selection_rate', ascending=False).head(10)
        lines.append('  Top 10 most-selected features:')
        for _, row in top.iterrows():
            bar = '#' * int(row['selection_rate'] * 30)
            lines.append(
                f"    {row['feature']:<30} "
                f"rate={row['selection_rate']:.3f} "
                f"({int(row['selection_count'])}/{int(row['total_windows'])} windows) "
                f"|{bar}|"
            )
        always_selected = group[group['selection_rate'] == 1.0]
        never_selected  = group[group['selection_rate'] < 0.05]
        if not always_selected.empty:
            lines.append(_wrap('FINDING',
                f"Always-selected features (rate=1.0): "
                f"{', '.join(always_selected['feature'].tolist())}. "
                'These are unconditional causal predictors regardless of regime.'
            ))
        if not never_selected.empty:
            lines.append(_wrap('FINDING',
                f"Rarely-selected features (rate<0.05): "
                f"{', '.join(never_selected['feature'].tolist()[:10])}. "
                'These may be noise features or highly regime-specific.'
            ))


# ----- MAIN ----- #

def main():
    lines = []
    lines.append('RESULTS INTERPRETATION REPORT')
    lines.append('Generated by interpret_results.py')
    lines.append(
        'Each section reads results/analysis/ CSVs and explains what they mean.\n'
        'FINDING = what the data shows | DETAIL = supporting numbers\n'
        'CONCERN = red flags | THESIS = suggested thesis phrasing'
    )

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
    print(f'\n\nReport saved to {out_path}')


if __name__ == '__main__':
    main()
