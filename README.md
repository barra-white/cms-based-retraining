# CMS-Based Retraining

**Causal Mechanism Stability as a Retraining Trigger for Financial Forecasting Models**

MSc Artificial Intelligence Thesis — Munster Technological University Cork, 2026

This project investigates whether Causal Mechanism Stability (CMS) can provide earlier, more interpretable retraining signals for financial forecasting models than fixed-schedule, performance-based, or statistical drift detection approaches.

---

## Overview

A rolling-window PCMCI+ causal discovery pipeline is applied to financial time series data. A Mechanism Stability Metric (MSM) is computed across windows to detect structural breakdown in causal relationships. When MSM degrades below a threshold, model retraining is triggered — before accuracy has visibly dropped.

Five retraining strategies are compared:

- **Static** — train once, never retrain
- **Fixed Schedule** — retrain every N windows
- **Performance-based** — retrain when F1 drops below a threshold
- **ADWIN** — retrain when ADWIN detects drift in F1
- **MSM** — retrain when causal structure degrades (this project's contribution)

---

## Requirements

- Python 3.10+
- A [FRED API key](https://fred.stlouisfed.org/docs/api/api_key.html) (free)

Install dependencies:

```bash
pip install -r requirements.txt
```

---

## Environment Setup

Create a `.env` file in the root of the project:

```
FRED_API_KEY=your_fred_api_key_here
```

This key is used by `download_fred.py` to pull macroeconomic data from the St. Louis Fed. The `.env` file is gitignored and should never be committed.

---

## Project Structure

```
cms-based-retraining/
├── data/
│   ├── raw/                        # downloaded data, gitignored
│   └── processed/                  # transformed data, gitignored
├── results/
│   ├── validation/                 # PCMCI pre-validation outputs
│   ├── causal_discovery/           # causal graph summaries
│   ├── experiments/                # experiment CSVs, gitignored
│   └── plots/                      # thesis figures
├── src/
│   ├── data/                       # data pipeline scripts
│   ├── causal_discovery/           # PCMCI + validation scripts
│   └── retraining/                 # retrainers, experiment, analysis, plotting
└── tests/
    └── test_retrainers.py
```

---

## Running the Pipeline

Run scripts in this exact order. Each step depends on the output of the previous one.

### Step 1 — Download market data

```bash
python src/data/download_yfinance.py
```

Downloads SPY, GLD, VIX, USO and other ETF/index data from Yahoo Finance.
Writes to: `data/raw/yfinance_tickers.csv`

### Step 2 — Download macroeconomic data

```bash
python src/data/download_fred.py
```

Downloads Treasury yield spread, credit spread, Fed funds rate and other macro series from FRED. Requires `FRED_API_KEY` in `.env`.
Writes to: `data/raw/fred_data_raw.csv`

### Step 3 — Combine datasets

```bash
python src/data/combine_data.py
```

Aligns yfinance and FRED data on trading day index using forward-fill.
Writes to: `data/processed/combined_data.csv`

### Step 4 — Stationarity transformation

```bash
python src/data/dataset_stationary_transform.py
```

Applies log returns, log differences and first differences per variable. Z-score standardises all features for PCMCI compatibility.
Writes to: `data/processed/transformed_data.csv` and `data/processed/standardized_data.csv`

### Step 5 — Pre-PCMCI validation *(optional, already run)*

```bash
python src/causal_discovery/pre_msm_validation.py
```

Runs ARCH effect testing, multicollinearity checks and toy model validation to confirm PCMCI+ is appropriate for this dataset.
Writes to: `results/validation/`

### Step 6 — Causal graph generation *(slow)*

```bash
python src/causal_discovery/generate_causal_graphs.py
```

Runs PCMCI+ in a rolling window across the full dataset. This is the most computationally expensive step. If it crashes midway, re-run — it will automatically resume from `results/causal_graphs_progress.pkl` if present.
Writes to: `results/causal_graphs.pkl` and `results/causal_discovery/`

### Step 7 — Run experiments

```bash
python src/retraining/experiment.py
```

Runs all five retraining strategies across the rolling window. Includes sensitivity analysis over MSM hyperparameters.
Writes to: `results/experiments/`

### Step 8 — Analysis

```bash
python src/retraining/analysis.py
```

Computes Wilcoxon signed-rank tests, lead time analysis and summary tables.
Writes to: `results/experiments/`

### Step 9 — Plotting

```bash
python src/retraining/plotting.py
```

Generates all thesis figures.
Writes to: `results/plots/`

---

## Re-running from Scratch

To do a full clean re-run, delete the following before starting:

```
data/raw/yfinance_tickers.csv
data/raw/fred_data_raw.csv
data/processed/
results/causal_graphs.pkl
results/causal_graphs_progress.pkl
results/causal_discovery/
results/validation/
results/experiments/
results/plots/
```

---

## Notes

- All random seeds are fixed at `42` for reproducibility
- Data range: 2017-01-01 to 2025-12-31
- Rolling window: 504 trading days (~2 years), step 21 days (~1 month)
- PCMCI+ configured with `tau_max=5`, `RobustParCorr`, FDR correction (`fdr_bh`)
- Target variable: `SPY_lr` (S&P 500 log return, discretised into 3 bins)
