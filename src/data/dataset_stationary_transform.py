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
# Stationary (passes ADF and KPSS), compatible with PCMCI+ assumptions.
raw_spy_lr   = np.log(df['SPY'] / df['SPY'].shift(1))

rolling_std  = raw_spy_lr.rolling(21, min_periods=21).std().shift(1)
rolling_mean = raw_spy_lr.rolling(21, min_periods=21).mean().shift(1)
out['SPY_lr_local_std'] = (raw_spy_lr - rolling_mean) / rolling_std

forward_rv_5 = (raw_spy_lr ** 2).rolling(5).sum().shift(-5)  # 5-day realized volatility
out['SPY_logrv_5d'] = np.log(forward_rv_5.clip(lower=1e-10))  # log to stabilise variance, clip to avoid -inf

forward_rv_20 = (raw_spy_lr ** 2).rolling(20).sum().shift(-20)  # 20-day realized volatility
out['SPY_logrv_20d'] = np.log(forward_rv_20.clip(lower=1e-10))  # log to stabilise variance, clip to avoid -

trailing_rv_5 = (raw_spy_lr ** 2).rolling(5).sum() # 5-day trailing realized volatility
vol_change = forward_rv_5 - trailing_rv_5 # change in volatility over the next 5 days compared to the past 5 days
out['SPY_vol_change_5d'] = vol_change

out.to_csv("data/processed/transformed_data.csv", index=False)
print("Saved transformed data to data/processed/transformed_data.csv")

# Standardisation: z-score across full dataset
feature_cols = [col for col in out.columns if col != "Date"]
out[feature_cols] = (out[feature_cols] - out[feature_cols].mean()) / out[feature_cols].std()

out.to_csv("data/processed/standardized_data.csv", index=False)
print("Saved standardized data to data/processed/standardized_data.csv")