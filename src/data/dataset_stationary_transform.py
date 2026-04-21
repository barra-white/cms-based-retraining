import pandas as pd
import numpy as np

df = pd.read_csv("data/processed/combined_data.csv", parse_dates=["Date"])

print(f'Raw Data Shape: {df.shape}')

cols_to_keep = [
    "Date",
    "SPY", "GLD", "UUP", "VIX", "OVX", "MOVE", "USO",
    "T10Y2Y", "BAA10Y", "USEPUINDXD", "DGS10",
]
df = df[cols_to_keep].copy()

out = pd.DataFrame()
out['Date'] = df["Date"]

# ----- Feature transformations -----
# Log returns for price-based features
for col in ["SPY", "GLD", "UUP", "USO"]:
    out[f'{col}_lr'] = np.log(df[col] / df[col].shift(1))

# Log level for VIX (retains regime information)
out['VIX_ld'] = np.log(df['VIX'])

# OVX stationary without transformation
out['OVX'] = df['OVX']

# First-diff for interest-rate series
for col in ["MOVE", "T10Y2Y", "BAA10Y", "DGS10"]:
    out[f'{col}_d'] = df[col].diff()

# Log-diff for policy uncertainty index
out['USEPUINDXD_ld'] = np.log(df["USEPUINDXD"] / df["USEPUINDXD"].shift(1))

# ----- Targets (all forward-looking, NOT standardized) -----
# Standardizing forward-looking targets produces subtle leakage because the
# rolling statistics of a forward-shifted column include values that depend
# on the future. We keep targets in their natural units and let global_tertile
# binning handle class boundaries. Features below ARE standardized (causally).
raw_spy_lr = np.log(df['SPY'] / df['SPY'].shift(1))

# Secondary target (returns direction, regime-normalized, for RQ1 lead-lag)
# The rolling statistics are shifted by 1 so normalization uses only past data.
rolling_std  = raw_spy_lr.rolling(21, min_periods=21).std().shift(1)
rolling_mean = raw_spy_lr.rolling(21, min_periods=21).mean().shift(1)
out['SPY_lr_local_std'] = (raw_spy_lr - rolling_mean) / rolling_std

# Primary target: 5-day forward log realised variance
# At row t: log(sum of squared returns from t+1 to t+5)
# Last 5 rows are NaN — dropped downstream.
forward_rv_5  = (raw_spy_lr ** 2).rolling(5).sum().shift(-5)
out['SPY_logrv_5d']  = np.log(forward_rv_5.clip(lower=1e-10))

# Alternative target: 20-day forward (robustness)
forward_rv_20 = (raw_spy_lr ** 2).rolling(20).sum().shift(-20)
out['SPY_logrv_20d'] = np.log(forward_rv_20.clip(lower=1e-10))

# ----- Direction of vol change (primary target for ternary classification) -----
# Trailing 5-day log realised variance, used as reference for direction.
# Fully causal: at row t, sums squared returns from [t-4..t] (no future info).
trailing_rv_5 = (raw_spy_lr ** 2).rolling(5).sum()
trailing_logrv_5 = np.log(trailing_rv_5.clip(lower=1e-10))

# Direction: compare forward-5 log-RV to trailing-5 log-RV.
# Deadband of 0.2 log-RV ≈ 22% multiplicative change in vol — a meaningful
# threshold below which vol is considered "flat".
# Labels: 0 = vol decrease, 1 = flat, 2 = vol increase.
DEADBAND = 0.2
vol_change = out['SPY_logrv_5d'] - trailing_logrv_5
direction = pd.Series(1, index=out.index, dtype='Int64')
direction[vol_change > DEADBAND] = 2
direction[vol_change < -DEADBAND] = 0
direction[out['SPY_logrv_5d'].isna() | trailing_logrv_5.isna()] = pd.NA
out['SPY_vol_direction_5d'] = direction

# Save pre-standardization version
out.to_csv("data/processed/transformed_data.csv", index=False)
print("Saved transformed data to data/processed/transformed_data.csv")

# ----- Rolling causal standardization — FEATURES ONLY -----
# Each feature at row t is standardized using rolling(504).shift(1), meaning
# statistics come from rows t-504..t-1 only. No future information used.
# TARGETS ARE NOT STANDARDIZED — see note above.
ROLLING_WINDOW = 504
TARGET_COLS = ['SPY_lr_local_std', 'SPY_logrv_5d', 'SPY_logrv_20d', 'SPY_vol_direction_5d']
FEATURE_COLS_TO_STANDARDIZE = [
    col for col in out.columns
    if col not in (['Date'] + TARGET_COLS)
]

standardized = out.copy()
for col in FEATURE_COLS_TO_STANDARDIZE:
    s         = out[col]
    roll_mean = s.rolling(ROLLING_WINDOW, min_periods=ROLLING_WINDOW).mean().shift(1)
    roll_std  = s.rolling(ROLLING_WINDOW, min_periods=ROLLING_WINDOW).std().shift(1)
    standardized[col] = (s - roll_mean) / roll_std

standardized.to_csv("data/processed/standardized_data.csv", index=False)
print("Saved standardized data to data/processed/standardized_data.csv")
print(f"Note: first {ROLLING_WINDOW + 1} rows will be NaN on features — dropped downstream.")

# Diagnostic print
print("\n=== Post-standardization summary (features should have mean~0, std~1) ===")
for col in out.columns:
    if col == 'Date':
        continue
    s = standardized[col].dropna()
    tag = "TARGET" if col in TARGET_COLS else "feature"
    print(f"  [{tag:7s}] {col:25s}  mean={s.mean():+.4f}  std={s.std():.4f}  non-NaN={len(s)}")