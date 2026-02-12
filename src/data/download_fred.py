from fredapi import Fred
from dotenv import load_dotenv
from datetime import datetime

import pandas as pd

import os

load_dotenv()
FRED_API_KEY = os.getenv("FRED_API_KEY")

fred = Fred(api_key=FRED_API_KEY)

# start and end dates for data retrieval
START_DATE = "2017-01-01"
END_DATE = "2025-12-31"

# series: shows id: (name, frequency)
FRED_SERIES = {
    "T10Y2Y": ("10Y-2Y Treasury Yield Spread", "daily"), #
    "BAA10Y": ("BAA-10Y Credit Spread", "daily"),
    "USEPUINDXD": ("US Economic Policy Uncertainty Index", "daily"),
    "GDP": ("Gross Domestic Product", "quarterly"),
    "UNRATE": ("Unemployment Rate", "monthly"),
    "CPIAUCSL": ("Consumer Price Index", "monthly"),
    "FEDFUNDS": ("Federal Funds Rate", "monthly"),
    "M2SL": ("M2 Money Supply", "monthly")
}


def download_fred_series(series_id: str, name: str, freq: str) -> pd.Series:
    """
    Downloads a single FRED series
    
    Args:
        series_id: FRED series id
        name: readable name from series
        freq: frequency of the series
        
    Returns:
        Dataframe series with data
    """
    
    try:
        print(f"Downloading {name} ({series_id})...")
        data = fred.get_series(series_id, observation_start=START_DATE, observation_end=END_DATE)
        
        # debugging: returns nothing
        if data is None or len(data) == 0:
            print(f"No data returned for {name} ({series_id})")
            return pd.DataFrame()
        
        print(f"Downloaded {len(data)} points for {name} ({series_id})")
        return data
        
    except Exception as e:
        print(f"Error downloading {name} ({series_id}): {e}")
        return pd.DataFrame() # return empty DataFrame on error
    
def main():
    
    print("="*50)
    print("FRED Economic Data Download")
    print("="*50)
    print(f"Date range: {START_DATE} to {END_DATE}")
    print(f"Series to download: {len(FRED_SERIES)}")
    print("="*50)
    print()
    
    # Download all series
    all_data = {}
    
    for series_id, (name, freq) in FRED_SERIES.items():
        data = download_fred_series(series_id, name, freq)
        
        if data is not None:
            all_data[series_id] = data
        
        print()
    
    if not all_data:
        print("ERROR: No data downloaded successfully")
        return
    
    # Combine into DataFrame (keeps original frequencies)
    print("="*50)
    print("Combining Data")
    print("="*50)
    
    df = pd.DataFrame(all_data)
    df.index.name = 'date'
    
    print(f"Combined shape: {df.shape}")
    print(f"Date range: {df.index[0].date()} to {df.index[-1].date()}")
    print()
    
    print("Missing values per series:")
    nan_counts = df.isna().sum()
    print(nan_counts)
    print(f"Total NaNs: {nan_counts.sum()}")
    print()
    

    output_file = 'data/raw/fred_data_raw.csv'
    df.to_csv(output_file, index=True)
    print(f"✓ Saved raw FRED data to {output_file}")
    print()
    
if __name__ == "__main__":
    main()