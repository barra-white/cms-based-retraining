import pandas as pd
import numpy as np

df = pd.read_csv("data/processed/combined_data.csv",parse_dates=["Date"])

print(f'Raw Data Shape: {df.shape}')
print(f'Date Range: {df.index.min()} to {df.index.max()}')
print(f'Total Days: {len(df)}')

cols_to_keep = [
  "Date",
  "SPY", "GLD", "UUP", "VIX", "OVX", "MOVE", "USO",
  "T10Y2Y", "BAA10Y", "USEPUINDXD", "DGS10"
]

df = df[cols_to_keep].copy()


# transformations
out = pd.DataFrame()
out['Date'] = df["Date"]


# log returns for price-based features
# returns: to allow for stationarity
# log: to allow for multiplicative effects and to stabilize variance
# log returns = log(price_t / price_{t-1})
for col in ["SPY", "GLD", "UUP", "USO"]:
  out[f'{col}_lr'] = np.log(df[col] / df[col].shift(1))
  

# log for VIX
# log: large range of data values, keeps regime info
# no first diff: destroys regime information

out['VIX_ld'] = np.log(df['VIX'])


# nothing for OVX
# stats show OVX is stationary, this may harm performance if we take log or diff
out['OVX'] = df['OVX']

# first diff for MOVE
# same for T10Y2Y, BAA10Y, DGS10
# diff: to allow for stationarity, as MOVE is non-stationary
# maybe destroys regime info but results in more consistent causal graphs
for col in ["MOVE", "T10Y2Y", "BAA10Y", "DGS10"]:
  out[f'{col}_d'] = df[col].diff()
  
# log diff for USEPUINDXD
# log diff: to allow for stationarity, as USEPUINDXD is non-stationary
# log: to allow for multiplicative effects and to stabilize variance
out['USEPUINDXD_ld'] = np.log(df["USEPUINDXD"] / df["USEPUINDXD"].shift(1))

out.to_csv("data/processed/transformed_data.csv", index=False)
print("Saved transformed data to data/processed/transformed_data.csv")

# standardisation: z-score normalization
feature_cols = [col for col in out.columns if col != "Date"]
out[feature_cols] = (out[feature_cols] - out[feature_cols].mean()) / out[feature_cols].std()

out.to_csv("data/processed/standardized_data.csv", index=False)
print("Saved standardized data to data/processed/standardized_data.csv")