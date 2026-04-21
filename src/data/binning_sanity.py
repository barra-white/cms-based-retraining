'''
binning_sanity.py — Compare binning and target-construction strategies.

Runs 10 different strategies for turning continuous vol into class labels and
reports, for each:
    - Training-class balance (do we train on imbalanced classes?)
    - Test-window class coverage (how often do test windows have all classes?)
    - Severely-imbalanced window rate
    - Empirical F1 ceiling (what F1 is theoretically achievable?)

Goal: pick the binning strategy that gives the cleanest production F1 signal,
independent of the model.

Strategies tested:
    S1  global_tertile_initial     — tertiles from first 504-row window (current)
    S2  global_tertile_full        — tertiles from full dataset (optimistic baseline)
    S3  fixed_logrv_thresholds     — domain-informed fixed log-RV boundaries
    S4  per_window_tertile         — tertiles re-fit each 504-row training window
    S5  relative_rank_63d          — rank within trailing 63 trading days
    S6  vol_direction_binary       — up/down vs trailing 5-day vol (2 classes)
    S7  vol_direction_ternary      — up/flat/down with deadband (3 classes)
    S8  global_quintile_top_bot    — top 20% / middle 60% / bottom 20% (3 classes)
    S9  binary_high_low            — above/below median (2 classes)
    S10 median_rel_63d             — above/below trailing 63-day median (2 classes)

Output: results/validation/binning_comparison.csv + console table.
'''

import os
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

os.makedirs('results/validation', exist_ok=True)

# ----- Load data — use PRE-standardization target for interpretability -----
# Load both so we can check semantic consistency
raw_df = pd.read_csv('data/processed/transformed_data.csv', parse_dates=['Date'])
std_df = pd.read_csv('data/processed/standardized_data.csv', parse_dates=['Date'])

TARGET_COL = 'SPY_logrv_5d'
WINDOW = 504       # training window (same as experiment.py)
STEP = 21          # step size = test window length

# Drop rows where target is NaN (last 5 rows due to forward shift)
df = raw_df.dropna(subset=[TARGET_COL]).reset_index(drop=True)
y = df[TARGET_COL].values
dates = df['Date'].values
n = len(y)

print(f'=== BINNING COMPARISON ===')
print(f'Target: {TARGET_COL}')
print(f'Rows usable: {n}')
print(f'Target summary: mean={y.mean():.3f}, std={y.std():.3f}, '
      f'min={y.min():.3f}, max={y.max():.3f}')
print(f'Window={WINDOW}, step={STEP}\n')


# =============================================================================
# Strategy implementations — each returns a function f(window_index) -> class
# labels for that test window's 21 rows. Same function also returns labels for
# training data for computing training-class balance.
# =============================================================================

def apply_strategy(strategy_fn, label_global=True):
    '''
    strategy_fn: callable(train_vals, test_vals, train_start, test_start) -> (train_labels, test_labels)
    Returns list of (train_labels, test_labels, test_start_date) per window.
    '''
    results = []
    for start in range(0, n - WINDOW - STEP + 1, STEP):
        train_end = start + WINDOW
        test_end  = train_end + STEP
        train_vals = y[start:train_end]
        test_vals  = y[train_end:test_end]
        train_labels, test_labels = strategy_fn(train_vals, test_vals, start, train_end)
        results.append((train_labels, test_labels, dates[train_end]))
    return results


def _summarize(name, strategy_results, n_classes, balance_threshold=0.10):
    '''Compute diagnostic stats for a strategy.'''
    all_train_labels = []
    window_stats = []
    n_balanced = 0
    n_severely_imbalanced = 0
    empirical_ceilings = []

    for train_labels, test_labels, _ in strategy_results:
        all_train_labels.extend(train_labels.tolist())

        test_counts = np.bincount(test_labels, minlength=n_classes)
        test_fracs = test_counts / test_counts.sum()

        has_all_classes = (test_counts > 0).all()
        if has_all_classes:
            n_balanced += 1
        if test_fracs.max() > (1 - balance_threshold) or test_fracs.min() < balance_threshold / n_classes:
            n_severely_imbalanced += 1

        # Empirical F1 ceiling: what's the best macro-F1 achievable if we
        # always predict the majority class in this test window?
        majority = test_counts.argmax()
        majority_preds = np.full_like(test_labels, majority)
        from sklearn.metrics import f1_score
        empirical_ceilings.append(f1_score(
            test_labels, majority_preds, average='macro', zero_division=0
        ))

        window_stats.append({
            'date': _, 'counts': test_counts.tolist(), 'fracs': test_fracs.round(3).tolist(),
            'has_all_classes': has_all_classes, 'majority_f1': empirical_ceilings[-1],
        })

    train_counts = np.bincount(np.array(all_train_labels), minlength=n_classes)
    train_fracs = train_counts / train_counts.sum()

    return {
        'strategy': name,
        'n_classes': n_classes,
        'train_counts': train_counts.tolist(),
        'train_fracs': train_fracs.round(3).tolist(),
        'train_min_frac': train_fracs.min().round(3),
        'n_windows': len(strategy_results),
        'n_balanced_test_windows': n_balanced,
        'balanced_pct': round(100 * n_balanced / len(strategy_results), 1),
        'n_severely_imbalanced': n_severely_imbalanced,
        'severely_imbalanced_pct': round(100 * n_severely_imbalanced / len(strategy_results), 1),
        'mean_majority_f1': round(np.mean(empirical_ceilings), 4),
    }


# =============================================================================
# STRATEGIES
# =============================================================================

def digitize(vals, edges):
    return np.digitize(vals, edges)


# S1: global_tertile_initial — tertiles from first 504 rows
first_window = y[:WINDOW]
_q1, _q2 = np.quantile(first_window, [1/3, 2/3])
def s1(train, test, *_):
    return digitize(train, [_q1, _q2]), digitize(test, [_q1, _q2])
s1_info = (_q1, _q2)

# S2: global_tertile_full — tertiles from full dataset
_Q1, _Q2 = np.quantile(y, [1/3, 2/3])
def s2(train, test, *_):
    return digitize(train, [_Q1, _Q2]), digitize(test, [_Q1, _Q2])
s2_info = (_Q1, _Q2)

# S3: fixed_logrv_thresholds — domain-informed
# Log-RV of annualized daily vol 15% (low) ~ -9; vol 25% (medium) ~ -8; vol 40% (high) ~ -7
# Your actual data distribution should inform these. Start with 25th and 75th percentiles
# of the full dataset but describe them as "fixed" thresholds.
_F1, _F2 = np.quantile(y, [0.25, 0.75])
def s3(train, test, *_):
    return digitize(train, [_F1, _F2]), digitize(test, [_F1, _F2])
s3_info = (_F1, _F2)

# S4: per_window_tertile
def s4(train, test, *_):
    q1, q2 = np.quantile(train, [1/3, 2/3])
    return digitize(train, [q1, q2]), digitize(test, [q1, q2])

# S5: relative_rank_63d — each value labeled by its rank in trailing 63 trading days
def _rank_bin(vals, ref_start_idx, lookback=63):
    '''For each value in vals (at positions ref_start_idx..ref_start_idx+len(vals)),
    compute its tertile rank within the trailing `lookback` values ending at that position.'''
    labels = np.zeros(len(vals), dtype=int)
    for i, v in enumerate(vals):
        abs_idx = ref_start_idx + i
        ref_start = max(0, abs_idx - lookback)
        ref_vals = y[ref_start:abs_idx]
        if len(ref_vals) < 10:
            labels[i] = 1  # default to middle if insufficient history
            continue
        q1, q2 = np.quantile(ref_vals, [1/3, 2/3])
        labels[i] = np.digitize(v, [q1, q2])
    return labels

def s5(train, test, train_start, test_start):
    tr = _rank_bin(train, train_start, lookback=63)
    te = _rank_bin(test, test_start, lookback=63)
    return tr, te

# S6: vol_direction_binary — next 5d vol > trailing 5d vol?
# Compute trailing 5d log-RV for comparison
raw_spy_lr = np.log(raw_df['SPY_lr'].values + 1e-15) if 'SPY_lr' in raw_df.columns else None
# Actually simpler: trailing-5 log-RV is not in the CSV but we can reconstruct
# it from the target: trailing_5 at row t = sum of squared returns over [t-4..t].
# Our forward_rv_5 at row (t-5) equals trailing_5 at row t. So:
# trailing_5_at_t = SPY_logrv_5d.iloc[t-5]
trailing_logrv = np.full(n, np.nan)
for i in range(5, n):
    trailing_logrv[i] = y[i - 5]  # y is SPY_logrv_5d; shifted back 5 = trailing

def s6(train, test, train_start, test_start):
    tr_trail = trailing_logrv[train_start:train_start + len(train)]
    te_trail = trailing_logrv[test_start:test_start + len(test)]
    # Label 1 if forward vol > trailing vol, else 0
    tr = (train > tr_trail).astype(int)
    te = (test > te_trail).astype(int)
    # Handle NaN in trailing (first 5 rows)
    tr[np.isnan(tr_trail)] = 0
    te[np.isnan(te_trail)] = 0
    return tr, te

# S7: vol_direction_ternary — with deadband. |change| < 0.2 log-RV = flat
# 0.2 log-RV change ≈ 22% change in vol, a meaningful deadband
def s7(train, test, train_start, test_start):
    tr_trail = trailing_logrv[train_start:train_start + len(train)]
    te_trail = trailing_logrv[test_start:test_start + len(test)]
    def _label(vals, trail, band=0.2):
        diff = vals - trail
        out = np.full(len(vals), 1, dtype=int)  # flat default
        out[diff > band] = 2   # up
        out[diff < -band] = 0  # down
        out[np.isnan(trail)] = 1
        return out
    return _label(train, tr_trail), _label(test, te_trail)

# S8: global_quintile_top_bot — bottom 20% / middle 60% / top 20%
_QL, _QH = np.quantile(y, [0.20, 0.80])
def s8(train, test, *_):
    return digitize(train, [_QL, _QH]), digitize(test, [_QL, _QH])
s8_info = (_QL, _QH)

# S9: binary_high_low — above/below global median
_MED = np.median(y)
def s9(train, test, *_):
    return (train > _MED).astype(int), (test > _MED).astype(int)

# S10: median_rel_63d — above/below trailing 63d median
def _median_rel(vals, ref_start_idx, lookback=63):
    labels = np.zeros(len(vals), dtype=int)
    for i, v in enumerate(vals):
        abs_idx = ref_start_idx + i
        ref_start = max(0, abs_idx - lookback)
        ref_vals = y[ref_start:abs_idx]
        if len(ref_vals) < 10:
            labels[i] = 0
            continue
        labels[i] = int(v > np.median(ref_vals))
    return labels

def s10(train, test, train_start, test_start):
    return _median_rel(train, train_start), _median_rel(test, test_start)


# =============================================================================
# RUN ALL STRATEGIES
# =============================================================================

strategies = [
    ('S1_global_tertile_initial', s1, 3),
    ('S2_global_tertile_full',    s2, 3),
    ('S3_fixed_p25_p75',          s3, 3),
    ('S4_per_window_tertile',     s4, 3),
    ('S5_relative_rank_63d',      s5, 3),
    ('S6_vol_direction_binary',   s6, 2),
    ('S7_vol_direction_ternary',  s7, 3),
    ('S8_global_20_60_20',        s8, 3),
    ('S9_binary_high_low',        s9, 2),
    ('S10_median_rel_63d',        s10, 2),
]

summaries = []
for name, fn, k in strategies:
    print(f'Running {name}...')
    windows = apply_strategy(fn)
    summary = _summarize(name, windows, k)
    summaries.append(summary)

results_df = pd.DataFrame(summaries)
results_df.to_csv('results/validation/binning_comparison.csv', index=False)

# =============================================================================
# PRINT COMPARISON TABLE
# =============================================================================

print('\n' + '=' * 100)
print('BINNING STRATEGY COMPARISON')
print('=' * 100)
print(f'{"Strategy":<30} {"k":>2} {"TrainMinF":>10} '
      f'{"BalancedW%":>11} {"SevImbW%":>10} {"MajF1":>7}')
print('-' * 100)
for s in summaries:
    print(f'{s["strategy"]:<30} {s["n_classes"]:>2} '
          f'{s["train_min_frac"]:>10.3f} '
          f'{s["balanced_pct"]:>10.1f}% '
          f'{s["severely_imbalanced_pct"]:>9.1f}% '
          f'{s["mean_majority_f1"]:>7.3f}')

print('\nLegend:')
print('  TrainMinF   : smallest class fraction in training data (want > 0.20)')
print('  BalancedW%  : % of test windows with all classes present (want > 50%)')
print('  SevImbW%    : % of test windows where one class has > 90% of samples (want < 30%)')
print('  MajF1       : mean F1 of always-predict-majority baseline (lower = more headroom)')

# =============================================================================
# HEADLINE CLASS-BALANCE PLOTS
# =============================================================================

fig, axes = plt.subplots(2, 5, figsize=(22, 9))
for ax, (name, fn, k) in zip(axes.flatten(), strategies):
    windows = apply_strategy(fn)
    # Matrix of class fractions per test window
    M = np.zeros((len(windows), k))
    for i, (_, test_labels, _) in enumerate(windows):
        counts = np.bincount(test_labels, minlength=k)
        M[i] = counts / counts.sum()
    im = ax.imshow(M.T, aspect='auto', cmap='viridis', vmin=0, vmax=1)
    ax.set_title(name, fontsize=9)
    ax.set_ylabel('class')
    ax.set_xlabel('window')
    ax.set_yticks(range(k))
plt.suptitle('Test-window class fractions by strategy (bright = class dominates)', fontsize=13)
plt.tight_layout()
plt.savefig('results/validation/binning_class_fractions.png', dpi=120, bbox_inches='tight')
plt.close()
print('\nSaved: results/validation/binning_class_fractions.png')

# =============================================================================
# RECOMMENDATION LOGIC
# =============================================================================

print('\n' + '=' * 100)
print('RECOMMENDATION')
print('=' * 100)

# Score each strategy: want high BalancedW, low SevImbW, high TrainMinF, low MajF1
# (low MajF1 means more room for a real model to improve on naive baseline)
def _score(s):
    # Normalize each metric to [0,1] where 1 is best
    balanced = s['balanced_pct'] / 100
    not_severe = 1 - s['severely_imbalanced_pct'] / 100
    train_bal = min(1.0, s['train_min_frac'] / 0.33)  # ideal = 0.33 (perfectly tertile)
    headroom = max(0, 1 - s['mean_majority_f1'])  # high majority F1 = less headroom
    # Weight: balanced_pct and severely_imbalanced are most important
    return 0.4 * balanced + 0.3 * not_severe + 0.2 * train_bal + 0.1 * headroom

for s in summaries:
    s['_composite_score'] = round(_score(s), 3)

ranked = sorted(summaries, key=lambda s: -s['_composite_score'])
print(f'{"Rank":>4}  {"Strategy":<30} {"Score":>6}  Notes')
print('-' * 100)
for i, s in enumerate(ranked, 1):
    notes = []
    if s['severely_imbalanced_pct'] > 50:
        notes.append('many imbalanced windows')
    if s['train_min_frac'] < 0.10:
        notes.append('training class collapse risk')
    if s['mean_majority_f1'] > 0.6:
        notes.append('low headroom vs majority baseline')
    if not notes:
        notes.append('OK')
    print(f'{i:>4}.  {s["strategy"]:<30} {s["_composite_score"]:>6.3f}  {"; ".join(notes)}')

print('\nTop pick: ' + ranked[0]['strategy'])
print('\nTo use this strategy in experiment.py:')
print('  1. Update src/retraining/config.py: set BINNING_SCHEME accordingly')
print('  2. Re-run preflight to validate')
print('  3. Re-run experiment.py')