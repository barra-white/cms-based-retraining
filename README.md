# CMS-Based Retraining

**Causal Mechanism Stability as a Retraining Trigger for Financial Volatility Forecasting**

MSc Artificial Intelligence Thesis — Munster Technological University Cork, 2026

---

## Overview

A rolling-window PCMCI+ causal discovery pipeline applied to US equity and macroeconomic data. A **Mechanism Stability Metric (MSM)** tracks how consistently the same causal edges appear across successive windows. When stability drops below a threshold, a two-stage confirmation check fires a retraining trigger — *before* forecast accuracy has materially degraded.

**Forecast target:** `SPY_logrv_5d` — five-day forward log realised variance of SPY.  
**Evaluation period:** January 2017 – April 2025 (58 rolling windows).  
**Models evaluated:** Linear Ridge, Random Forest, XGBoost.

### Retraining Strategies

| Type | Strategies |
|------|-----------|
| **MSM (this project)** | Core MSM, SPY-focused MSM, Fused MSM, Timeout MSM, Strength-weighted MSM, Causal Feature MSM |
| **Baselines** | Static, Fixed Schedule (3/6/9/12 windows), Performance-based, ADWIN, Revised ADWIN, Random |
| **Control** | Drift Signal Observer (frozen model, logs MSM signal only) |

---

## Requirements

- Python 3.10+
- [FRED API key](https://fred.stlouisfed.org/docs/api/api_key.html) (free)

```bash
pip install -r requirements.txt
```

Create `.env` in the project root:
```
FRED_API_KEY=your_key_here
```

---

## Project Structure

```
cms-based-retraining/
├── data/
│   ├── raw/                        # downloaded data (gitignored)
│   └── processed/                  # transformed features + targets (gitignored)
├── results/
│   ├── validation/                 # PCMCI+ pre-validation outputs
│   ├── causal_discovery/           # causal graph summaries
│   ├── experiments/                # per-retrainer CSVs (gitignored)
│   ├── analysis/                   # aggregated analysis CSVs
│   └── plots/                      # thesis figures
├── src/
│   ├── data/                       # download, combine, transform
│   ├── causal_discovery/           # PCMCI+ validation + graph generation
│   └── retraining/                 # retrainers, experiment runner, analysis, plotting
└── requirements.txt
```

---

## Pipeline

Run steps in order. Steps 3–4 are slow one-time operations; outputs are cached.

```bash
# 1. Download raw data
python src/data/download_yfinance.py
python src/data/download_fred.py

# 2. Build feature dataset
python src/data/combine_data.py
python src/data/dataset_stationary_transform.py

# 3. Pre-PCMCI+ validation (checks for ARCH effects, validates with synthetic data)
python src/causal_discovery/pre_msm_validation.py

# 4. Generate causal graphs (~6 hrs)
python src/causal_discovery/generate_causal_graphs.py
python src/causal_discovery/extract_causal_summary.py

# 5. Validate MSM thresholds
python src/retraining/msm_diagnostics.py

# 6. Run all retraining experiments (~18 hrs)
python src/retraining/experiment.py

# 7. Post-experiment analysis
python src/retraining/analysis.py
python src/retraining/significance_test.py
python src/retraining/lead_lag_analysis.py
python src/retraining/drift_analysis.py
python src/retraining/interpret_results.py

# 8. Generate thesis figures
python src/retraining/plotting.py
```

---

## Experimental Setup

| Parameter | Value |
|-----------|-------|
| Training window | 504 trading days |
| Test step | 21 trading days |
| Forecast horizon shift | 5 days (leakage prevention) |
| Total rolling windows | 58 |
| Stress windows | 12 |
| Calm windows | 46 |
| Random seed | 42 |
| PCMCI+ test | `RobustParCorr` (ARCH-robust) |
| PCMCI+ correction | FDR (`fdr_bh`), `tau_max=5` |

### Stress Events

| Event | Date | Window |
|-------|------|--------|
| Federal Reserve rate hike cycle | March 2022 | ±60 days |
| Yen carry trade unwind | August 2024 | ±60 days |
| Liberation Day tariff shock | April 2025 | ±60 days |

---

## Key Results

- MSM **Granger-causes** XGBoost and Random Forest forecast error at a ~63-day lead (3 evaluation windows, p < 0.05)
- Mean pairwise Jaccard overlap between MSM and baselines: **0.097** (MSM fires on a structurally distinct set of windows)
- **40 wins / 8 losses** across 144 Diebold-Mariano paired tests vs. all baselines
- MSM matches stress-period accuracy of the 3-window fixed schedule using ~**one-third fewer retrains**

---

## Notes

- Results CSVs are gitignored due to size. Re-run `experiment.py` to reproduce.
- Causal graphs are cached in `data/causal_graphs.pkl` after step 4.
- XGBoost is the primary representative model in all figures; Linear Ridge and Random Forest results are in `results/experiments/`.