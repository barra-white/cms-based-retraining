import numpy as np
import pandas as pd

np.random.seed(42)

df = pd.read_csv("data/processed/standardized_data.csv", parse_dates=["Date"])
features = [col for col in df.columns if col != "Date"]
df = df.dropna(subset=features).reset_index(drop=True)

TARGET = "SPY_lr"
TRAINING_WINDOW = 504 # 504 observations == 2 trading years

print(df["SPY_lr"].min())
print(df["SPY_lr"].max())