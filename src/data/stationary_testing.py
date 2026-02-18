import statsmodels.tsa.stattools as tsa
import pandas as pd

# test for data stationarity using Augmented Dickey-Fuller and Kwiatkowski-Phillips-Schmidt-Shin tests

df = pd.read_csv("data/processed/transformed_data.csv",parse_dates=["Date"])

cols = df.columns

print("Stationarity Tests:")
print(f"{'Feature':<15} {'ADF p-value':<15} {'KPSS p-value':<15}")
for col in cols[1:]:
    s = df[col].dropna()
    adf_p = tsa.adfuller(s, autolag='AIC')[1]
    try:
        kpss_p = tsa.kpss(s, regression='c', nlags='auto')[1]
    except:
        kpss_p = float('nan') #kpss may fail
        
    print(f"{col:<15} {adf_p:<15.4f} {kpss_p:<15.4f}")
    
    

