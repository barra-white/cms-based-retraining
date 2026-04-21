import pandas as pd
import numpy as np

df = pd.read_csv("data/processed/combined_data.csv", parse_dates=["Date"])

print(f'Raw Data Shape: {df.shape}')
print(f'Date Range: {df.index.min()} to {df.index.max()}')
print(f'Total Days: {len(df)}')

cols_to_keep = [
    "Date",
    "SPY", "GLD", "UUP", "VIX", "OVX", "MOVE", "USO",
    "T10Y2Y", "BAA10Y", "USEPUINDXD", "DGS10",
]

df = df[cols_to_keep].copy()

out = pd.DataFrame()
out['Date'] = df["Date"]

# Log returns for price-based features
for col in ["SPY", "GLD", "UUP", "USO"]:
    out[f'{col}_lr'] = np.log(df[col] / df[col].shift(1))

# Log for VIX (retains regime information)
out['VIX_ld'] = np.log(df['VIX'])

# OVX stationary without transformation
out['OVX'] = df['OVX']

# First-diff for interest-rate series
for col in ["MOVE", "T10Y2Y", "BAA10Y", "DGS10"]:
    out[f'{col}_d'] = df[col].diff()

# Log-diff for policy uncertainty index
out['USEPUINDXD_ld'] = np.log(df["USEPUINDXD"] / df["USEPUINDXD"].shift(1))

# ----- Secondary target for RQ1 lead-lag analysis -----
# Locally-standardised log return: (r_t - rolling_mean_21) / rolling_std_21.
# The .shift(1) on rolling statistics makes normalisation CAUSAL —
# today's value is normalised using only yesterday and earlier.
raw_spy_lr   = np.log(df['SPY'] / df['SPY'].shift(1))

rolling_std  = raw_spy_lr.rolling(21, min_periods=21).std().shift(1)
rolling_mean = raw_spy_lr.rolling(21, min_periods=21).mean().shift(1)
out['SPY_lr_local_std'] = (raw_spy_lr - rolling_mean) / rolling_std

# ----- Primary target: 5-day forward log realised variance -----
# At row t: sum of squared returns from t+1 to t+5 (5 trading days ahead).
# .rolling(5).sum() at row t covers rows t-4..t, then .shift(-5) moves that
# window forward by 5, so the value at row t is sum(r_{t+1}^2 .. r_{t+5}^2).
# The last 5 rows will be NaN — this is correct and expected.
# NOTE: features in experiment.py are shifted by 1 before training, so
# at prediction time the model sees features from t-1 to predict vol at t+1..t+5.
# No temporal overlap — no leakage.
forward_rv_5  = (raw_spy_lr ** 2).rolling(5).sum().shift(-5)
out['SPY_logrv_5d']  = np.log(forward_rv_5.clip(lower=1e-10))

forward_rv_20 = (raw_spy_lr ** 2).rolling(20).sum().shift(-20)
out['SPY_logrv_20d'] = np.log(forward_rv_20.clip(lower=1e-10))

# SPY_vol_change_5d is intentionally NOT included — it is a forward-looking
# column derived from forward_rv_5 and has no use as a feature or stable target.

out.to_csv("data/processed/transformed_data.csv", index=False)
print("Saved transformed data to data/processed/transformed_data.csv")

# ----- Causal rolling standardisation (replaces global z-score) -----
# Each feature column is standardised using a rolling window of 504 trading days
# (2 years), with the rolling statistics shifted by 1 day so that the value at
# row t uses only information from rows t-504..t-1.
# This makes standardisation FULLY CAUSAL — no future data contaminates features
# or targets. It is consistent with the approach used for SPY_lr_local_std.
#
# The target columns (SPY_logrv_5d, SPY_logrv_20d) are standardised the same way.
# Although their raw values are forward-looking by construction, standardising
# them causally means the scale information used for normalisation is always
# from the past only.
ROLLING_WINDOW = 504  # ~2 trading years, matches experiment.py training window
NON_DATE_COLS  = [col for col in out.columns if col != "Date"]

standardized = out.copy()
for col in NON_DATE_COLS:
    s            = out[col]
    roll_mean    = s.rolling(ROLLING_WINDOW, min_periods=ROLLING_WINDOW).mean().shift(1)
    roll_std     = s.rolling(ROLLING_WINDOW, min_periods=ROLLING_WINDOW).std().shift(1)
    standardized[col] = (s - roll_mean) / roll_std

standardized.to_csv("data/processed/standardized_data.csv", index=False)
print("Saved standardized data to data/processed/standardized_data.csv")
print(f"Note: first {ROLLING_WINDOW + 1} rows will be NaN due to rolling window — "
      "these are dropped by experiment.py's dropna call.")

# Sanity check: print mean/std of each col — should be near 0/1 on non-NaN rows
for col in NON_DATE_COLS:
    s = standardized[col].dropna()
    print(f"  {col:30s}  mean={s.mean():.4f}  std={s.std():.4f}  non-NaN={len(s)}")