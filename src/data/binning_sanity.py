# NEW FILE: src/data/binning_sanity.py
'''
binning_sanity.py — Check that per-window tertile binning produces
reasonable and stable class distributions across rolling windows.
'''

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os

os.makedirs('results/validation', exist_ok=True)

df = pd.read_csv('data/processed/standardized_data.csv', parse_dates=['Date'])
df = df.dropna(subset=['SPY_logrv_5d']).reset_index(drop=True)

WINDOW = 504
STEP = 21
target_col = 'SPY_logrv_5d'

# Per-window tertile thresholds
low_thresholds, high_thresholds = [], []
class_fractions = []  # [(low_frac, mid_frac, high_frac), ...]
window_dates = []

# Replace the per-window tertile loop with:
# Set bins ONCE from the first training window
first_window_vals = df[target_col].iloc[:WINDOW].values
q1_global, q2_global = np.quantile(first_window_vals, [1/3, 2/3])
print(f'Global tertile thresholds (from initial window):')
print(f'  q1={q1_global:.3f}, q2={q2_global:.3f}')

class_fractions = []
window_dates = []
for start in range(0, len(df) - WINDOW, STEP):
    end = start + WINDOW
    test_vals = df[target_col].iloc[end:end+STEP].values
    binned = np.digitize(test_vals, [q1_global, q2_global])
    counts = np.bincount(binned, minlength=3)
    class_fractions.append(counts / counts.sum() if counts.sum() > 0 else [np.nan]*3)
    window_dates.append(df['Date'].iloc[end])

cf = np.array(class_fractions)
mean_class_frac = cf.mean(axis=0)
print(f'\nMean test-window class fractions with GLOBAL bins: '
      f'low={mean_class_frac[0]:.2%}, mid={mean_class_frac[1]:.2%}, high={mean_class_frac[2]:.2%}')
imbalanced = (cf[:, 0] < 0.1) | (cf[:, 2] < 0.1)
print(f'Severely imbalanced windows (any class <10%): {imbalanced.sum()}/{len(cf)}')