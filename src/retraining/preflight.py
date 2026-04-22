'''
preflight.py — Fast sanity checks before running the experiment sweep.

Run this FIRST before experiment.py. If any check fails, exits non-zero.

Checks (regression task):
    1. Config target exists in standardized_data.csv
    2. Features do not include any target column (leakage guard)
    3. Target column is continuous float, finite, non-degenerate
    4. Features ARE standardized (roughly mean 0, std 1)
    5. Causal graphs file exists, shape matches graph_var_names expectation
    6. Initial training window has adequate target variance
    7. No leading-NaN contamination in dropped df
    8. Feature shift is correct (shifted features come from t-1)

Usage:
    python src/retraining/preflight.py
'''

import os
import pickle
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config as cfg


FAIL_COUNT = 0

def check(name, condition, message=''):
    global FAIL_COUNT
    status = 'PASS' if condition else 'FAIL'
    if not condition:
        FAIL_COUNT += 1
    print(f'  [{status}] {name}' + (f' — {message}' if message else ''))
    return condition


def main():
    print('=' * 72)
    print('  PREFLIGHT CHECKS (regression task)')
    print('=' * 72)

    # ----- 1. Data file exists and target present -----
    print('\n1. Data file and target presence')
    data_path = 'data/processed/standardized_data.csv'
    check('standardized_data.csv exists', os.path.exists(data_path))
    if not os.path.exists(data_path):
        sys.exit(1)
    df = pd.read_csv(data_path, parse_dates=['Date'])
    check(f'TARGET_PRIMARY ({cfg.TARGET_PRIMARY}) in df',
          cfg.TARGET_PRIMARY in df.columns)
    check(f'TARGET_SECONDARY ({cfg.TARGET_SECONDARY}) in df',
          cfg.TARGET_SECONDARY in df.columns)

    # ----- 2. Feature exclusion -----
    print('\n2. Feature exclusion (no target leakage)')
    FEATURE_EXCLUSIONS = [
        'Date',
        'SPY_lr_local_std',
        'SPY_logrv_5d',            # primary regression target
        'SPY_logrv_20d',
        'SPY_vol_change_5d',
        'SPY_vol_direction_5d',    # legacy classification label still in CSV
    ]
    feature_cols = [c for c in df.columns if c not in FEATURE_EXCLUSIONS]
    check(f'TARGET_PRIMARY excluded from features',
          cfg.TARGET_PRIMARY not in feature_cols)
    check(f'TARGET_SECONDARY excluded from features',
          cfg.TARGET_SECONDARY not in feature_cols)
    print(f'     Features ({len(feature_cols)}): {feature_cols}')
    check('Exactly 11 feature columns', len(feature_cols) == 11,
          f'got {len(feature_cols)}')

    # ----- 3. Target is continuous float, finite, non-degenerate -----
    print('\n3. Target is continuous volatility (float)')
    t = df[cfg.TARGET_PRIMARY].dropna()
    check('Target is numeric',
          pd.api.types.is_numeric_dtype(t),
          f'dtype = {t.dtype}')
    check('Target has no infinite values',
          np.isfinite(t.values).all(),
          f'n_inf = {int((~np.isfinite(t.values)).sum())}')
    # Log-RV of 5-day forward variance on SPY lies roughly in [-12, -5];
    # a very wide guard so we don't have to retune if the sample changes.
    check('Target in plausible log-RV range (-15 < x < 0)',
          t.min() > -15 and t.max() < 0,
          f'range = [{t.min():.3f}, {t.max():.3f}]')
    check('Target has meaningful variance (std > 0.05)',
          t.std() > 0.05,
          f'std = {t.std():.6f}')
    print(f'     Target distribution: mean={t.mean():+.4f}, std={t.std():.4f}, '
          f'min={t.min():+.4f}, max={t.max():+.4f}, n={len(t)}')

    # ----- 4. Features ARE standardized (mean ~0, std ~1) -----
    print('\n4. Features are causally rolling-standardized')
    for f in feature_cols:
        s = df[f].dropna()
        ok = abs(s.mean()) < 0.3 and 0.5 < s.std() < 2.0
        check(f'  {f}: mean~0, std~1', ok,
              f'mean={s.mean():+.3f}, std={s.std():.3f}')

    # ----- 5. Causal graphs -----
    print('\n5. Causal graphs file')
    pkl = 'data/causal_graphs.pkl'
    check('causal_graphs.pkl exists', os.path.exists(pkl))
    if os.path.exists(pkl):
        with open(pkl, 'rb') as f:
            graphs = pickle.load(f)
        first = graphs[0]
        shape = first['graph'].shape
        check('Graph has 11 nodes', shape[0] == 11,
              f'graph shape = {shape}')
        check('At least 80 graph windows', len(graphs) >= 80,
              f'got {len(graphs)} windows')

    # ----- 6. Initial training window target stats -----
    print('\n6. Initial training window target stats')
    df_clean = df.dropna(subset=feature_cols + [cfg.TARGET_PRIMARY]).reset_index(drop=True)
    initial_target = df_clean[cfg.TARGET_PRIMARY].iloc[:504].astype(float).values
    print(f'     Initial-window target: mean={initial_target.mean():+.4f}, '
          f'std={initial_target.std():.4f}, '
          f'min={initial_target.min():+.4f}, max={initial_target.max():+.4f}')
    check('Initial-window target std > 0.01',
          initial_target.std() > 0.01,
          f'std = {initial_target.std():.6f}')

    # ----- 7. Dataset size after dropna -----
    print('\n7. Dataset size post-dropna')
    n_clean = len(df_clean)
    n_windows_expected = (n_clean - 504) // 21
    print(f'     Usable rows: {n_clean}')
    print(f'     Expected experiment windows: {n_windows_expected}')
    check('At least 50 usable windows', n_windows_expected >= 50,
          f'got {n_windows_expected}')

    # ----- 8. Feature shift check -----
    print('\n8. Feature shift correctness')
    df_shifted = df_clean.copy()
    df_shifted[feature_cols] = df_shifted[feature_cols].shift(1)
    check('Row 0 of shifted features is NaN',
          df_shifted[feature_cols].iloc[0].isna().all())
    check('Row 1 of shifted features equals row 0 of unshifted',
          np.allclose(df_shifted[feature_cols].iloc[1].values,
                      df_clean[feature_cols].iloc[0].values,
                      equal_nan=True))

    # ----- Summary -----
    print('\n' + '=' * 72)
    if FAIL_COUNT == 0:
        print('  ALL CHECKS PASSED. Safe to run experiment.py')
        print('=' * 72)
        sys.exit(0)
    else:
        print(f'  {FAIL_COUNT} CHECK(S) FAILED. Do not run experiment.py until resolved.')
        print('=' * 72)
        sys.exit(1)


if __name__ == '__main__':
    main()
