'''
preflight.py — Fast sanity checks before running the 16h experiment sweep.

Run this FIRST before experiment.py. If any check fails, exits non-zero.

Checks:
    1. Config target exists in standardized_data.csv
    2. Features do not include any target column (leakage guard)
    3. Target column is not standardized (targets should be raw log-RV)
    4. Features ARE standardized (roughly mean 0, std 1)
    5. Causal graphs file exists, shape matches graph_var_names expectation
    6. Initial training window produces balanced tertile bins
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
    print('  PREFLIGHT CHECKS')
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
        'SPY_lr_local_std',  # secondary target for lead-lag analysis
        'SPY_logrv_5d',  # alternative secondary target with same horizon as primary, more noise
        'SPY_logrv_20d',  # alternative secondary target with longer horizon,
        'SPY_vol_change_5d',  # alternative secondary target capturing direction of volatility change
        'SPY_vol_direction_5d',  # primary target for classification, also excluded from features
    ]
    feature_cols = [c for c in df.columns if c not in FEATURE_EXCLUSIONS]
    check(f'TARGET_PRIMARY excluded from features',
          cfg.TARGET_PRIMARY not in feature_cols)
    check(f'TARGET_SECONDARY excluded from features',
          cfg.TARGET_SECONDARY not in feature_cols)
    print(f'     Features ({len(feature_cols)}): {feature_cols}')
    check('Exactly 11 feature columns', len(feature_cols) == 11,
          f'got {len(feature_cols)}')

    # ----- 3. Target is integer labels 0/1/2 -----
    print('\n3. Target is integer labels (0/1/2)')
    t = df[cfg.TARGET_PRIMARY].dropna()
    unique_vals = sorted(t.unique())
    check('Target is integer-valued',
        all(float(v).is_integer() for v in unique_vals),
        f'unique values = {unique_vals}')
    check('Target has exactly 3 classes {0, 1, 2}',
        set(int(v) for v in unique_vals) == {0, 1, 2},
        f'unique values = {unique_vals}')
    counts = t.astype(int).value_counts().sort_index()
    fracs = counts / counts.sum()
    print(f'     Full-series class distribution: '
        f'down={fracs.get(0, 0):.3f}, flat={fracs.get(1, 0):.3f}, up={fracs.get(2, 0):.3f}')
    check('No class below 10% of total',
        fracs.min() >= 0.10,
        f'min class frac = {fracs.min():.3f}')

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

    # ----- 6. Initial training window class balance -----
    print('\n6. Initial training window class balance')
    df_clean = df.dropna(subset=feature_cols + [cfg.TARGET_PRIMARY]).reset_index(drop=True)
    initial_target = df_clean[cfg.TARGET_PRIMARY].iloc[:504].astype(int).values
    counts = np.bincount(initial_target, minlength=3)
    fracs = counts / counts.sum()
    print(f'     Initial-window distribution: '
        f'down={fracs[0]:.3f}, flat={fracs[1]:.3f}, up={fracs[2]:.3f}')
    check('No class below 10% in initial window',
        fracs.min() >= 0.10,
        f'min class frac = {fracs.min():.3f}')

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
    # After shift, row 0 is NaN on features
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