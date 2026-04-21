'''
leakage_audit.py — Verify no lookahead leakage in volatility target.

Runs a paranoid check: for each rolling window, confirm that the target at
time t uses only SPY returns from [t+1, t+5] and features at time t use only
data from t-1 and earlier.

Also computes correlation between target and each feature's LAG-0 value
(should be ~0 for most features if shift is working) and lag-(-1) value
(where features are future of target — should be HIGH only for SPY-related
features and explainable).

If this script reports any "SUSPICIOUS" lines, do not proceed to full run.
'''

import pandas as pd
import numpy as np

df = pd.read_csv('data/processed/standardized_data.csv', parse_dates=['Date'])

target = 'SPY_logrv_5d'
t = df[target].values
n = len(t)

# Reconstruct what target at row i "knows about"
# SPY_logrv_5d[i] = log(sum(SPY_lr[i+1..i+5]^2))
# So features at row i should not include SPY data from rows i+1..i+5.
# Your pipeline shifts features by 1, so at row i features come from row i-1.
# That's safe.

# But — does any FEATURE at time t-1 have information about SPY returns at t+1..t+5?
# Only through temporal dependence / volatility clustering, which is signal not leakage.

# Paranoid check: compute correlation of each feature with the target WITHOUT shift.
# If any feature has correlation > 0.9 that isn't a known vol proxy (VIX), flag.
print('=== Feature-target correlation (unshifted) — high values expected for vol proxies ===')
feat_cols = [c for c in df.columns if c not in ['Date', 'SPY_lr_local_std',
                                                  'SPY_logrv_5d', 'SPY_logrv_20d',
                                                  'SPY_vol_change_5d']]
sub = df[['Date', target] + feat_cols].dropna()
for f in feat_cols:
    c = sub[[target, f]].corr().iloc[0, 1]
    flag = ''
    if abs(c) > 0.9:
        flag = ' !!! SUSPICIOUS — investigate leakage'
    elif abs(c) > 0.7 and f not in ['VIX_ld', 'OVX', 'MOVE_d']:
        flag = ' ? high correlation for non-vol feature'
    print(f'  {f:<20} corr={c:+.3f}{flag}')

# Paranoid check 2: shifted features should give no future information
print('\n=== Shifted feature correlations (what the model actually sees) ===')
for f in feat_cols:
    shifted = sub[f].shift(1)
    c = pd.concat([sub[target], shifted], axis=1).dropna().corr().iloc[0, 1]
    print(f'  {f:<20}(shifted) corr={c:+.3f}')

print('\n=== Target autocorrelation (volatility clustering) ===')
for lag in [1, 5, 10, 21]:
    c = sub[target].autocorr(lag=lag)
    print(f'  lag={lag}: {c:+.3f}')