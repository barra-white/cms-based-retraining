# CMS-Based Retraining

**Causal Mechanism Stability as a Retraining Trigger for Financial Forecasting Models**

MSc Artificial Intelligence Thesis — Munster Technological University Cork, 2026

---

## Overview

A rolling-window PCMCI+ causal discovery pipeline applied to financial time series. A Mechanism Stability Metric (MSM) detects structural breakdown in causal relationships and triggers model retraining before accuracy drops.

Five retraining strategies compared: **Static**, **Fixed Schedule**, **Performance-based**, **ADWIN**, **MSM** (this project's contribution). Plus control baselines: **StaticRollingBins** (isolates bin recalibration), **Random**.

## Requirements

- Python 3.10+
- [FRED API key](https://fred.stlouisfed.org/docs/api/api_key.html) (free)

```bash
pip install -r requirements.txt
```

Create `.env` in project root:
```
FRED_API_KEY=your_key_here
```

## Project Structure

```
cms-based-retraining/
├── data/raw/                          # downloaded data (gitignored)
├── data/processed/                    # transformed data (gitignored)
├── results/
│   ├── validation/                    # PCMCI pre-validation outputs
│   ├── causal_discovery/              # causal graph summaries
│   ├── experiments/                   # per-experiment CSVs (gitignored)
│   ├── analysis/                      # analysis CSVs
│   └── plots/                         # thesis figures
├── src/
│   ├── data/                          # data pipeline
│   ├── causal_discovery/              # PCMCI + validation
│   └── retraining/                    # retrainers, experiment, analysis, plotting
└── requirements.txt
```

## Pipeline (run in order)

```bash
# 1. Download data
python src/data/download_yfinance.py
python src/data/download_fred.py

# 2. Combine and transform
python src/data/combine_data.py
python src/data/dataset_stationary_transform.py

# 3. Pre-PCMCI validation (optional, already run)
python src/causal_discovery/pre_msm_validation.py

# 4. Causal graph generation (slow, ~6hrs)
python src/causal_discovery/generate_causal_graphs.py

# 5. Extract causal summary
python src/causal_discovery/extract_causal_summary.py

# 6. MSM diagnostics (threshold validation)
python src/retraining/msm_diagnostics.py

# 7. Run experiments (slow, ~3-6hrs)
python src/retraining/experiment.py

# 8. Analysis pipeline
python src/retraining/analysis.py
python src/retraining/significance_test.py
python src/retraining/drift_analysis.py
python src/retraining/interpret_results.py

# 9. Generate thesis figures
python src/retraining/plotting.py
```

## Stress Events

| Event | Onset Date | Rationale |
|-------|-----------|-----------|
| COVID-19 crash | 2020-02-20 | Global market sell-off |
| Fed rate hikes | 2022-03-16 | Start of aggressive tightening cycle |
| Yen carry trade unwind | 2024-08-05 | VIX spiked to 65, S&P 500 dropped 6% in 3 days |

## Notes

- All random seeds fixed at `42`
- Data: 2017-01-01 to 2025-12-31
- Rolling window: 504 days (~2 years), step 21 days (~1 month)
- PCMCI+: `tau_max=5`, `RobustParCorr`, FDR correction (`fdr_bh`)
- Target: `SPY_lr` (S&P 500 log return, discretised into 3 bins)
