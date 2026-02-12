import yfinance as yf
import pandas as pd
from pathlib import Path


# Static variables for data retrieval and storage

# start and end dates for data retrieval
START_DATE = "2017-01-01"
END_DATE = "2025-12-31"

# list of different sectors to retrieve ETF data from
SECTORS = [
    "technology",
    "financial-services",
    "energy",
    "healthcare",
    "consumer-defensive",
    "industrials",
    "utilities",
    "basic-materials",
    "real-estate",
    "communication-services"
]

# other tickers used for indices and indicators
#indices == target variables for prediction
TICKERS = {
    "indices": {
        "SPY": "S&P 500 ETF", # large-cap index
        "QQQ": "NASDAQ 100 ETF", # large-cap tech-focused index
        "IWM": "Russell 2000 ETF" # small-cap index
    },
    "indicators": {
        "^VIX": "CBOE Volatility Index", # market volatility indicator
        "UUP": "US Dollar Index", # dollar etf
        "TLT": "iShares 20+ Year Treasury Bond ETF", # long-term bond index
        "GLD": "SPDR Gold Shares ETF", # gold price indicator
        "HYG": "iShares iBoxx $ High Yield Corporate Bond ETF", # high-yield bond index
        "^MOVE": "MOVE Index", # bond market volatility indicator
        "USO": "United States Oil Fund", # oil price indicator
        "^OVX": "CBOE Crude Oil Volatility Index" # oil volatility indicator
    }
}

# functions
def setup_dirs():
    """
    Creates directory structure
    """
    dirs = ["data/raw/yfinance", "data/processed"]
    for d in dirs:
        Path(d).mkdir(parents=True, exist_ok=True)
        


def download_ticker(ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Downloads historical data for a given ticker and date range using yfinance
    
    Args:
        ticker (str): Ticker symbol to download
        start_date (str): Start date in YYYY-MM-DD format
        end_date (str): End date in YYYY-MM-DD format
    Returns:
        pd.DataFrame: DataFrame containing historical data for the ticker
    """
    try:
        print(f"Downloading data for {ticker} from {start_date} to {end_date}...")
        data = yf.download(
            ticker,
            start=start_date,
            end=end_date,
            progress=False,
            keepna=False,
            auto_adjust=True
        )
        if len(data) == 0:
            print(f"No data found for {ticker}.")
            return pd.DataFrame() # return empty DataFrame if no data
        print(f"Successfully downloaded data for {ticker}.")
        return data
    except Exception as e:
        print(f"Error downloading data for {ticker}: {e}")
        return pd.DataFrame() # return empty DataFrame on error
    
    
    
def extract_features(ticker: str, data) -> dict:
    """Extracts Close price from data."""
    features = {}
    clean_name = ticker.replace("^", "")
    
    try:
        # Get Close column - handle both Series and DataFrame
        close_data = data['Close']
        
        # Convert to Series if it's a DataFrame
        if isinstance(close_data, pd.DataFrame):
            close_data = close_data.squeeze()  # Convert single-column DF to Series
        
        # Verify it's a Series now
        if isinstance(close_data, pd.Series):
            features[clean_name] = close_data
            print(f"Extracted {len(close_data)} points")
        else:
            print(f"Failed: type is {type(close_data)}")
            
    except Exception as e:
        print(f"Error: {e}")
    
    return features


def download_all(ticker_dict: dict[str, dict[str, str]], start_date: str, end_date: str) -> dict[str, pd.DataFrame]:
    """
    Downloads historical data for all tickers in the dictionary and extracts features
    
    Args:
        ticker_dict (dict): Dictionary of tickers to download
        start_date (str): Start date for data retrieval
        end_date (str): End date for data retrieval
        
    Returns:
        dict: {ticker: features} mapping each ticker to its extracted features DataFrame
    """
    all_features = {}
    failed_tickers = []
    
    
    print("\nStarting download and feature extraction for all tickers...")
    
    for category, tickers in ticker_dict.items():
        print("\n\n\n" + "=" * 60)
        print(f"Processing category: {category}")


        for ticker, info in tickers.items():
            name = info if isinstance(info, str) else info.get("name", ticker) # get name from info dict or use ticker as fallback
            print("=" * 60)
            print(f"Processing ticker: {ticker} - {name}")
            
            # download
            data = download_ticker(ticker, start_date, end_date)
            
            if data is None or data.empty:
                print(f"Skipping feature extraction for {ticker} due to no data.")
                continue
            
            # print data shape
            print(f'Downloaded: {data.shape[0]} rows and {data.shape[1]} columns for {ticker}')
            
            features = extract_features(ticker, data)
            
            # debugging: check if features is empty
            if not features:
                print(f"Warning: No features extracted for {ticker}.")
                failed_tickers.append(ticker)
                continue
            all_features.update(features)
            
            print(f'\nCompleted processing for {ticker} - {name}')
            print(f'{len(data):>4} dats [{len(features)} features]')
    print("\nDownload and feature extraction complete for all tickers.")
    
    
    # summary of failed tickers
    if failed_tickers:
        print("\nThe following tickers failed to download or extract features:")
        for ticker in failed_tickers:
            print(f"\t{ticker}")
    else:
        print("\nAll tickers processed successfully with extracted features.")
        
        
    # debugging: check whats in all_features
    if not all_features:
        print("\nChecking: all_features contents")
        for k, v in list(all_features.items())[:5]: # show first 5 items
            print(f"\t{k}: type={type(v)}, length={len(v) if hasattr(v, '__len__') else 'N/A'}")
    combined = pd.DataFrame(all_features)
    
    
    try:
        print("\nCreating combined DataFrame...")
        combined = pd.DataFrame(all_features)
    except Exception as e:
        print(f"Error creating combined DataFrame: {e}")
        return pd.DataFrame() # return empty DataFrame on error   
    
     
    missing = combined.isnull().sum()
    
    if missing.any():
        print("\nWarning: Missing data detected in combined DataFrame:")
        for col, count in missing[missing > 0].items():
            pct = (count / len(combined)) * 100
            print(f"\t{col}: {count} missing values ({pct:.2f})")

    combined.to_csv("data/processed/yfinance_combined.csv", index=True)
    
    return combined

def main():
    setup_dirs()
    ticker_dict = TICKERS
    combined_data = download_all(ticker_dict, START_DATE, END_DATE)
    
    
if __name__ == "__main__":
    main()