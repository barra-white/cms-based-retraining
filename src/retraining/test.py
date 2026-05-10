import pandas as pd, numpy as np

lag_df = pd.read_csv('results/analysis/lead_lag_results.csv')
row = lag_df[(lag_df['model_type'] == 'xgboost') & (lag_df['signal'] == 'graph_msm')].iloc[0]
print(row[['xc_best_lag', 'granger_best_lag', 'granger_p', 'vs']])