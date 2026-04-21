'''
target_exploratory.py — Verify volatility targets have predictable signal.

Produces sanity-check plots and simple baseline F1 scores using a logistic
regression on the existing feature set. If these show clear signal (F1 > 0.45
on the validation split) then the full experimental pivot is worth running.

Gate 2: at least one of SPY_logrv_5d or SPY_logrv_20d must show F1 > 0.45
on a held-out split before proceeding. If neither does, revert to binning-
restructure branch — the pivot will not rescue F1.
'''

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import f1_score, classification_report
import os

os.makedirs('results/validation', exist_ok=True)

df = pd.read_csv('data/processed/standardized_data.csv', parse_dates=['Date'])

targets_to_test = ['SPY_logrv_5d', 'SPY_logrv_20d', 'SPY_vol_change_5d']
feature_cols = [c for c in df.columns if c not in ['Date'] + targets_to_test + ['SPY_lr', 'SPY_lr_local_std']]

print(f'Features used: {feature_cols}')
print(f'Targets tested: {targets_to_test}')

# ----- 1. Target distribution checks -----
fig, axes = plt.subplots(1, 3, figsize=(15, 4))
for ax, tgt in zip(axes, targets_to_test):
    s = df[tgt].dropna()
    ax.hist(s, bins=50)
    ax.set_title(f'{tgt}\nmean={s.mean():.3f} std={s.std():.3f}\nskew={s.skew():.2f} kurt={s.kurtosis():.2f}')
plt.tight_layout()
plt.savefig('results/validation/target_distributions.png', dpi=120)
print('Saved: results/validation/target_distributions.png')

# ----- 2. Autocorrelation (does past vol predict future vol?) -----
from statsmodels.tsa.stattools import acf
fig, axes = plt.subplots(1, 3, figsize=(15, 4))
for ax, tgt in zip(axes, targets_to_test):
    s = df[tgt].dropna()
    ac = acf(s, nlags=40)
    ax.bar(range(len(ac)), ac)
    ax.axhline(0.1, color='red', ls='--', alpha=0.5)
    ax.axhline(-0.1, color='red', ls='--', alpha=0.5)
    ax.set_title(f'{tgt} ACF (nlags=40)')
    ax.set_xlabel('lag')
plt.tight_layout()
plt.savefig('results/validation/target_acf.png', dpi=120)

# ----- 3. Simple predictability test -----
# Use last 20% as test, simple logistic regression with tertile binning
print('\n=== Simple predictability test (logistic regression, tertile binning) ===')
for tgt in targets_to_test:
    sub = df[['Date'] + feature_cols + [tgt]].dropna().reset_index(drop=True)
    if len(sub) < 100:
        print(f'  {tgt}: too few non-NaN rows ({len(sub)}) — skipping')
        continue
    n = len(sub)
    split = int(n * 0.8)
    y = sub[tgt].values
    X = sub[feature_cols].values

    # Shift X by 1 to avoid lookahead (same as production)
    X_shifted = np.vstack([np.zeros(X.shape[1]), X[:-1]])
    X_shifted = X_shifted[1:]  # drop leading zero row
    y = y[1:]

    # Tertile bins on training data only
    q = np.quantile(y[:split], [1/3, 2/3])
    y_binned = np.digitize(y, q)

    clf = LogisticRegression(max_iter=1000, class_weight='balanced')
    clf.fit(X_shifted[:split], y_binned[:split])
    pred = clf.predict(X_shifted[split:])
    f1 = f1_score(y_binned[split:], pred, average='macro')
    print(f'  {tgt}: macro F1 = {f1:.4f} (N_train={split}, N_test={n-split})')

    # Also RF for comparison
    rf = RandomForestClassifier(n_estimators=200, random_state=42, class_weight='balanced', n_jobs=-1)
    rf.fit(X_shifted[:split], y_binned[:split])
    pred_rf = rf.predict(X_shifted[split:])
    f1_rf = f1_score(y_binned[split:], pred_rf, average='macro')
    print(f'  {tgt}: RF macro F1 = {f1_rf:.4f}')

print('\n=== Gate 2 ===')
print('At least one target must achieve F1 > 0.45 (LR or RF) to proceed with pivot.')
print('If all three score < 0.40, revert to binning-restructure branch.')