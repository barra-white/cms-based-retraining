import pandas as pd

YFINANCE_FILE = "data/raw/yfinance_combined.csv"
FRED_FILE = "data/raw/fred_data_raw.csv"

OUTPUT_FILE = "data/processed/combined_data.csv"


def load_yfinance_data():
    try:
        df = pd.read_csv(YFINANCE_FILE, index_col='Date', parse_dates=True)
        print(f"Loaded yfinance data: {df.shape[0]} rows, {df.shape[1]} columns")
        
        # check for missing values
        if df.isna().sum().sum() > 0:
            df = df.ffill().bfill() # simple imputation for missing values
            print("Filled missing values in yfinance data")
        
        return df
    except Exception as e:
        print(f"Error loading yfinance data: {e}")
        return pd.DataFrame() # return empty DataFrame on error
    
def load_fred_data():
    try:
        df = pd.read_csv(FRED_FILE, index_col='date', parse_dates=True)
        print(f"Loaded FRED data: {df.shape[0]} rows, {df.shape[1]} columns")
        

        
        return df
    except Exception as e:
        print(f"Error loading FRED data: {e}")
        return pd.DataFrame() # return empty DataFrame on error
    
    
def align_data(yf_df, fred_df):
    print("Aligning data on date index")
    
    
    fred_aligned = fred_df.reindex(yf_df.index, method='ffill').bfill()
    
    fred_aligned = fred_aligned.ffill().bfill() # ensure no missing values after alignment
    
    combined_df = pd.concat([yf_df, fred_aligned], axis=1)
    
    combined_df.to_csv(OUTPUT_FILE, index=True)
    
    
def main():
    print("="*50)
    print("Combining yfinance and FRED data")
    print("="*50)
    
    yf_df = load_yfinance_data()
    fred_df = load_fred_data()
    
    if yf_df.empty or fred_df.empty:
        print("ERROR: One or both datasets failed to load. Cannot combine.")
        return
    
    align_data(yf_df, fred_df)
    print(f"Combined data saved to {OUTPUT_FILE}")
    
    
    
if __name__ == "__main__":
    main()