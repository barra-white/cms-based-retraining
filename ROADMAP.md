# ROADMAP — CMS-Based Retraining Thesis
> Last updated: April 2026  
> Status legend: 🔴 Not started · 🟡 In progress · ✅ Done

---

## FILES IN THE REPO (for reference)

```
src/retraining/
├── experiment.py            ← rolling experiment runner
├── retrainers.py            ← all retrainer classes
├── analysis.py              ← 12 CSV outputs
├── significance_test.py     ← Wilcoxon + Cohen's d
├── interpret_results.py     ← narrative_report.txt
├── plotting.py              ← 11 plot types
├── msm_diagnostics.py       ← MSM signal validation
└── __init__.py
```

**Files you still need to create:**
- `src/retraining/extract_causal_summary.py` — ~15 lines, extracts `spy_parents` from `causal_graphs.pkl`
- `src/retraining/drift_analysis.py` — 3 new plots + drift_summary.csv

---

## PRE-WORK — Fix Before Running Anything (~1–2 hrs coding)

These are code defects. Every one must be resolved before any experiment produces
trustworthy results. Do not skip to Phase 1 until all are ticked.

---

### 🔴 Fix 1 — Hard-code SPY column name; remove the silent fallback (CRITICAL)

**File:** `experiment.py`  
**Problem:** The line `spy_idx = feature_cols.index('SPYlr') if 'SPYlr' in feature_cols else 10`
will silently use column 10 if the column name does not match. If column 10 is not SPY,
every CausalFeatureRetrainer run anchors the causal graph on the wrong variable and
produces invalid results with no error or warning.

**Action — replace that line with:**
```python
SPY_COL = 'SPY_lr'   # confirm this matches the exact column name in standardized_data.csv
if SPY_COL not in feature_cols:
    raise ValueError(
        f"CausalFeatureRetrainer: '{SPY_COL}' not in feature_cols. "
        f"First 15 cols: {feature_cols[:15]}"
    )
spy_idx = feature_cols.index(SPY_COL)
```
Before running: open `data/processed/standardized_data.csv`, check the exact SPY column name,
and substitute it above.

---

### 🔴 Fix 2 — Add `StaticRollingBinsRetrainer` to `retrainers.py` (CRITICAL — thesis validity)

**File:** `retrainers.py` + `experiment.py`  
**Problem:** Without this control, you cannot distinguish whether MSM beats Static because:
  (a) the model is retrained at the right time  ← your thesis claim  
  (b) the bin edges are simply fresher          ← a confound not yet isolated

**Action — add this class to `retrainers.py`** (after `StaticRetrainer`):
```python
class StaticRollingBinsRetrainer(BaseRetrainer):
    """
    Control: never retrains the model, but recalculates bin edges every window.
    Isolates the bin-recalibration effect from genuine model adaptation.
    If this baseline approaches MSM performance, the MSM gain is mostly
    from fresher bins, not from model updating.
    """
    def should_retrain(self, w, **kwargs):
        return False

    def run(self, all_graphs):
        self.results             = []
        self.last_retrain_window = -999
        self.best_params         = None
        self.bin_edges           = None
        model = None

        for w, g in enumerate(all_graphs):
            train_start = g['train_start_idx']
            train_end   = g['train_end_idx']
            test_start  = train_end
            test_end    = test_start + self.step
            if test_end > len(self.df):
                continue

            y_r_train = self._impute_target(
                self.df[self.target].iloc[train_start:train_end].values)
            y_r_test  = self._impute_target(
                self.df[self.target].iloc[test_start:test_end].values)

            # Always refresh bin edges from current window — never retrain model
            self.freeze_bin_edges(y_r_train)

            if w == 0 or model is None:
                X_train = self._impute(
                    self.df[self.feature_cols].iloc[train_start:train_end].values)
                y_train = self.apply_bins(y_r_train)
                model   = self.run_grid_search(X_train, y_train, warm=False)
                self.last_retrain_window = w

            X_test = self._impute(
                self.df[self.feature_cols].iloc[test_start:test_end].values)
            y_test = self.apply_bins(y_r_test)
            f1, directional_acc, pred, y_true = self.evaluate(model, X_test, y_test)

            self.results.append({
                'window':                w + 1,
                'date_start':            g['date_start'],
                'date_end':              g['date_end'],
                'total_edges':           len(g['edges']),
                'retrain_triggered':     False,
                'signal_fired':          False,
                'cooldown_active':       False,
                'windows_since_retrain': w - self.last_retrain_window,
                'f1':                    round(f1, 4),
                'directional_acc':       round(directional_acc, 4),
                'y_true':                _to_json(y_true.tolist()),
                'y_pred':                _to_json(pred.tolist()),
                'best_params':           _to_json(self.best_params),
                'graph_msm':             None,
                'unstable_edges':        None,
                'active_features':       None,
            })
        return pd.DataFrame(self.results)
```

**In `experiment.py`, add to `build_static_configs()`:**
```python
from retrainers import StaticRollingBinsRetrainer
("static_rolling_bins", StaticRollingBinsRetrainer, {})
```

**This is the cheapest experiment to run** — one grid search at window 0, no further retraining.
Same wall-clock time as a single static run per model type.
**Do not rerun any other experiments.** Only `static_rolling_bins` needs a new run.

---

### 🔴 Fix 3 — `active_features` must be logged per window in `experiment.py` (CRITICAL)

**File:** `experiment.py` + `retrainers.py`  
**Problem:** `analysis.py::compute_causal_feature_usage()` reads an `active_features` column.
If absent, Section 10 silently returns an empty DataFrame and the narrative report prints
`[MISSING]` for causal feature analysis — a core part of the thesis claim.

**Action — in the result dict in `experiment.py`, confirm this key is present:**
```python
'active_features': _to_json(getattr(retrainer, 'active_features_', None)),
```

**In `retrainers.py`, confirm `CausalFeatureRetrainer` sets `self.active_features_` after each PC run:**
```python
self.active_features_ = [feature_cols[i] for i in selected_indices]
```

---

### 🔴 Fix 4 — Retrainer names must match the regex patterns in `analysis.py` (CRITICAL)

**File:** `retrainers.py` / `experiment.py`  
**Problem:** `compute_sensitivity_summary()` parses `tau_1`, `tau_2`, `lookback` from the
retrainer name string using these exact regexes:
`tau_1_([\d.]+)`, `tau_2_([\d.]+)`, `lb_(\d+)`

If names are formatted differently (e.g. `msm_0.83_0.75_4` instead of
`msm_tau_1_0.83_tau_2_0.75_lb_4`), every sensitivity row will have NaN hyperparameters
and `sensitivity_summary.csv` will be useless.

**Action — verify NOW before running anything:**
```python
import pandas as pd
df = pd.read_csv('results/experiments/all_results_partial.csv')
print(df['retrainer'].unique())
```
Names must look like: `msm_tau_1_0.83_tau_2_0.75_lb_4`
If not, fix the naming in `experiment.py` where the retrainer name string is constructed.
It is easier to fix the name than the regex.

---

### 🟡 Fix 5 — Filter detection latency to signal-driven retrainers only (IMPORTANT)

**File:** `analysis.py` + `interpret_results.py`  
**Problem:** `FixedScheduleRetrainer` and `RandomRetrainer` fire on a schedule, not a signal.
Including them in latency analysis produces misleading numbers — a random retrainer will
appear to "detect" stress events simply by firing frequently. A reviewer will catch this.

**Action — in `analysis.py`, `compute_detection_latency()`:**
```python
SIGNAL_DRIVEN = {'msm', 'spy_msm', 'timeout_msm', 'causal', 'adwin', 'performance'}
df_latency = df[df['exp_type'].isin(SIGNAL_DRIVEN)].copy()
# use df_latency for all latency calculations instead of df
```

**In `interpret_results.py`, Section 3, add:**
```python
lines.append(
    '  NOTE: Fixed-schedule and random retrainers are excluded from latency analysis.\n'
    '  Detection latency is only meaningful for signal-driven strategies.\n'
)
```

---

### 🟡 Fix 6 — Add `compute_retrain_efficiency()` to `analysis.py` (IMPORTANT)

**File:** `analysis.py`  
**Problem:** Without this you cannot answer "is each MSM retrain worth it?" — a question
any examiner will ask. You need to show MSM retrains produce meaningful F1 gains, not just
that MSM retrains more during stress.

**Action — add this function and call from `main()`:**
```python
def compute_retrain_efficiency(df, n_windows=3):
    records = []
    for (model, retrainer), group in df.groupby(['model_type', 'retrainer']):
        group = group.sort_values('window').reset_index(drop=True)
        exp_type = group['exp_type'].iloc[0] if 'exp_type' in group.columns else 'unknown'
        retrain_idxs = group.index[group['retrain_triggered'].astype(bool)].tolist()
        gains = []
        for idx in retrain_idxs:
            pre  = group.iloc[max(0, idx - n_windows):idx]['f1'].mean()
            post = group.iloc[idx + 1: idx + 1 + n_windows]['f1'].mean()
            if not (np.isnan(pre) or np.isnan(post)):
                gains.append(post - pre)
        records.append({
            'model_type':               model,
            'retrainer':                retrainer,
            'exp_type':                 exp_type,
            'n_retrains':               len(retrain_idxs),
            'mean_f1_gain_per_retrain': round(np.mean(gains), 4) if gains else np.nan,
            'positive_retrains':        sum(1 for g in gains if g > 0),
            'negative_retrains':        sum(1 for g in gains if g < 0),
            'pct_positive':             round(
                sum(1 for g in gains if g > 0) / len(gains), 3) if gains else np.nan,
        })
    return (pd.DataFrame(records)
            .sort_values(['model_type', 'mean_f1_gain_per_retrain'], ascending=[True, False])
            .reset_index(drop=True))
```
Save as `results/analysis/retrain_efficiency.csv`.

---

### 🟡 Fix 7 — Add stress-window robustness table to `analysis.py` (IMPORTANT)

**File:** `analysis.py`  
**Problem:** Your stress/calm classification uses `STRESS_WINDOW_DAYS = 120`. If results
change dramatically under 45 or 90 days, an examiner will question whether the finding
is real or a product of the window choice.

**Action — add to `main()` in `analysis.py`:**
```python
def compute_stress_robustness(df):
    records = []
    for days in [45, 63, 90, 120]:
        labelled = label_stress_windows(df, window_days=days)
        summary  = compute_stress_period_f1(labelled)
        summary['window_days'] = days
        records.append(summary)
    return pd.concat(records).reset_index(drop=True)
```
Save as `results/analysis/stress_robustness.csv`.  
If MSM's stress advantage is stable across all four sizes, your results are robust.
If it only appears at 120 days, note that limitation explicitly in the Discussion.

---

### 🟢 Fix 8 — Confirm `STRESS_WINDOW_DAYS` is imported, not hardcoded (MINOR)

**Files:** `plotting.py`, `interpret_results.py`  
Search both files for the literal `120` and confirm neither hardcodes the stress window.
If they do, replace with `from analysis import STRESS_WINDOW_DAYS`.

---

## PHASE 0 — Create `extract_causal_summary.py` (~15 min)

Prerequisite for Phase 6. Create it now before you forget the pkl structure.

**File to create:** `src/retraining/extract_causal_summary.py`

```python
"""
extract_causal_summary.py
Extracts per-window SPY parent counts from causal_graphs.pkl.
Run once before drift_analysis.py.
Output: results/causal_discovery/causal_discovery_summary.csv
"""
import os, pickle
import pandas as pd

PKL_PATH = 'data/causal_graphs.pkl'
OUT_PATH = 'results/causal_discovery/causal_discovery_summary.csv'
SPY_COL  = 'SPY_lr'   # must match exact feature name used in graph construction

graphs = pickle.load(open(PKL_PATH, 'rb'))

first_key = list(graphs.keys())[0]
print(f"Keys in first graph entry: {list(graphs[first_key].keys())}")
print(f"Total windows in pkl: {len(graphs)}")

records = []
for window_id, g in graphs.items():
    feature_names = g['feature_names']       # adjust key name if different
    adj           = g['adjacency_matrix']    # shape: (n_features, n_features)
    if SPY_COL not in feature_names:
        print(f"WARNING: '{SPY_COL}' not found in window {window_id}. Skipping.")
        continue
    spy_idx      = feature_names.index(SPY_COL)
    spy_parents  = int(adj[:, spy_idx].sum())   # edges pointing INTO SPY
    spy_children = int(adj[spy_idx, :].sum())   # edges pointing OUT of SPY
    records.append({
        'window':        window_id,
        'date_start':    g.get('date_start', None),
        'n_edges':       int(adj.sum()),
        'spy_parents':   spy_parents,
        'spy_children':  spy_children,
    })

os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
pd.DataFrame(records).sort_values('window').to_csv(OUT_PATH, index=False)
print(f"Saved {len(records)} rows to {OUT_PATH}")
```

Run this and verify the output has rows before moving to Phase 6.

---

## PHASE 1 — Validate Data (~15 min)

Do not skip. These checks take 10 minutes and can save 8 hours of wasted compute.

- [ ] **1.1** Open `data/processed/standardized_data.csv` — confirm the exact SPY column name
       and update `SPY_COL` in `experiment.py` and `extract_causal_summary.py`
- [ ] **1.2** Load `data/causal_graphs.pkl` in Python and print the structure of the first entry;
       confirm `feature_names`, `adjacency_matrix`, and `date_start` keys exist (or note actual names)
- [ ] **1.3** `pip install -r requirements.txt` — confirm `causallearn`, `river`, `xgboost`,
       `scikit-learn`, `scipy`, `statsmodels`, `pandas`, `matplotlib`, `seaborn` all import cleanly
- [ ] **1.4** Run `python src/retraining/extract_causal_summary.py` — confirm CSV has rows and
       `spy_parents` values are non-zero for most windows

---

## PHASE 2 — MSM Signal Diagnostics (~5 min)

Run before experiments. Confirms tau values produce a sensible signal before committing hours
of compute to them.

```bash
python src/retraining/msm_diagnostics.py
```

- [ ] **2.1** `results/validation/msm_over_time.png` — MSM should visibly weaken around
       Feb 2020 (COVID) and Mar 2022 (Fed hikes). If not, debug graph construction first.
- [ ] **2.2** `results/validation/msm_summary.csv` — confirm suggested tau values are close
       to your configured `0.83` / `0.75`. If they differ by more than 0.05, reconsider the tau grid.
- [ ] **2.3** `results/validation/msm_threshold_heatmap.png` — confirm your chosen tau combo
       triggers between 5 and 15 retrains per 100 windows. If it triggers every window, MSM
       has degenerated into a fixed schedule.

**If MSM does not dip at stress events:** the problem is in `causal_graphs.pkl`, not
`retrainers.py`. Stop here and debug graph construction.

---

## PHASE 3 — Run Experiments (2–8 hrs compute)

Complete all Pre-Work fixes before starting this phase.

- [ ] **3.1** Set `RUN_SENSITIVITY = True` in `experiment.py`
- [ ] **3.2** Confirm `static_rolling_bins` config is in `build_static_configs()` (Fix 2)
- [ ] **3.3** Run:
  ```bash
  python src/retraining/experiment.py
  ```
- [ ] **3.4** Monitor console — every completed config writes immediately to
       `results/experiments/<model>/<type>/`. If it crashes, re-run; completed configs are
       skipped automatically via the file-exists check.
- [ ] **3.5** When complete, confirm `results/experiments/all_results.csv` exists:
  ```bash
  wc -l results/experiments/all_results.csv
  ```
  Expected rows: (n_windows) × (n_models) × (n_retrainer_configs) + 1 header

**What you do NOT need to rerun:**
- All previously completed MSM, ADWIN, Performance, Fixed, Random, Static configs — skipped automatically
- You are only adding: `static_rolling_bins` (1 new config × 3 model types)
- Do NOT rerun anything because of `TimeSeriesSplit(n_splits=3)` — 504 obs / 3 = 168
  per fold, which is adequate. Defend in one sentence if asked.

---

## PHASE 4 — Core Analysis (~10 min)

Run in this exact order. Each script depends on the previous.

### Step 4.1 — `analysis.py`
```bash
python src/retraining/analysis.py
```

Produces 14 CSVs in `results/analysis/` (12 original + `retrain_efficiency.csv` + `stress_robustness.csv`):

| CSV | What it tells you |
|---|---|
| `overall_summary.csv` | Mean F1 per retrainer per model — the headline number |
| `best_configs.csv` | Top 3 tau combos per exp_type per model |
| `sensitivity_summary.csv` | F1 surface across tau_1 × tau_2 × lookback grid |
| `stress_period_f1.csv` | Stress vs calm F1 per strategy |
| `stress_robustness.csv` | Stress advantage across 4 window sizes (NEW) |
| `regime_retrain_rate.csv` | Stress/calm retrain ratio — confirms MSM is regime-aware |
| `detection_latency.csv` | Windows to detect each stress event (signal-driven only) |
| `false_positive_rate.csv` | Retrain precision — how often retrains happen outside stress |
| `causal_feature_usage.csv` | Which features CausalRetrainer selects during stress |
| `per_class_f1.csv` | F1 breakdown by class 0/1/2 |
| `cooldown_analysis.csv` | How often cooldown suppresses a valid signal |
| `retrain_efficiency.csv` | Mean F1 gain per retrain by strategy (NEW) |
| `friedman_ranks.csv` | Non-parametric global ranking |
| `retrain_efficiency.csv` | Cost-per-retrain analysis |

**Check immediately after running:**
- `sensitivity_summary.csv` — has rows? Empty = Fix 4 (regex naming) failed
- `causal_feature_usage.csv` — has rows? Empty = Fix 3 (`active_features`) failed
- `regime_retrain_rate.csv` — any `likely_inverted = True`? Those SPY-MSM results are invalid
- `stress_robustness.csv` — is MSM's advantage consistent across all 4 window sizes?

### Step 4.2 — `significance_test.py`
```bash
python src/retraining/significance_test.py
```

Produces:
- `results/analysis/wilcoxon_results.csv` — pairwise Holm-Bonferroni corrected p-values
- `results/analysis/effect_size.csv` — Cohen's d for MSM vs every baseline

**Check immediately:**
- Console: `=== Significant pairs ===`
  - Nothing significant after correction → not a code problem. Report honestly, note that
    Holm-Bonferroni is conservative given moderate sample sizes. Show practical effect size.
  - Borderline (p ~ 0.06–0.10) → report uncorrected p-values in appendix with a note.
- `effect_size.csv`: Cohen's d ≥ 0.2 is the minimum bar for claiming a practically
  meaningful advantage.

### Step 4.3 — `interpret_results.py`
```bash
python src/retraining/interpret_results.py
```

Produces: `results/analysis/narrative_report.txt`

Rules when reading:
- Every `[CONCERN]` → must be addressed in Discussion chapter
- Every `[MISSING]` → broken dependency; trace back and fix before writing
- Every `[FINDING]` → candidate thesis result; verify against the underlying CSV

---

## PHASE 5 — Generate Plots (~10 min)

```bash
python src/retraining/plotting.py
```

| Plot | What it shows | Thesis location |
|---|---|---|
| `01_f1_over_time_*.png` | Rolling F1 per strategy | Results §4.1 |
| `02_retrain_heatmap_*.png` | Retrain timing across all windows | Results §4.2 |
| `03_sensitivity_heatmap_*.png` | tau_1 × tau_2 F1 surface | Results §4.3 |
| `04_regime_comparison_*.png` | Stress vs calm F1 bars (include static_rolling_bins) | Results §4.4 |
| `05_detection_latency_*.png` | Latency bars — signal-driven only | Results §4.5 |
| `06_effect_size_*.png` | Cohen's d forest plot | Results §4.6 |
| `07_per_class_f1_*.png` | Class 0/1/2 F1 breakdown | Appendix |
| `08_cooldown_suppression_*.png` | Suppression rate bars | Results §4.7 |
| `09_retrain_efficiency_*.png` | F1 delta histogram per strategy | Results §4.7 |
| `10_friedman_ranks_*.png` | Critical difference diagram | Results §4.8 |
| `11_false_positive_rate_*.png` | Retrain precision per strategy | Results §4.5 |

Add `static_rolling_bins` to all plots that compare MSM against `static`.
If any plot fails, check whether its source CSV exists first — failures are almost always
missing input CSVs.

---

## PHASE 6 — Create `drift_analysis.py` (2–3 hrs coding)

The only fully new script needed. Connects MSM signal to causal structure changes and
retrain decisions — the visual centrepiece of the thesis argument.
Requires Phase 0 (`extract_causal_summary.py`) to have run first.

**File to create:** `src/retraining/drift_analysis.py`

### Structure:

```
drift_analysis.py
│
├── load_data()
│   ├── all_results.csv                        (from Phase 3)
│   └── causal_discovery_summary.csv           (from Phase 0)
│
├── plot_msm_drift_signal(df, model_type)
│   → results/plots/12_msm_drift_signal_{model}.png
│   ├── TOP PANEL:    graph_msm over time
│   │                 + horizontal dashed lines at tau_1 and tau_2
│   │                 + vertical markers where retrain_triggered == True
│   │                 + orange shading for STRESS_EVENTS ± STRESS_WINDOW_DAYS
│   └── BOTTOM PANEL: rolling F1 for best MSM config vs static vs static_rolling_bins
│                     (3 lines — this directly shows whether bins or model drives the gap)
│
├── plot_msm_vs_adwin_firing(df, model_type)
│   → results/plots/13_msm_vs_adwin_firing_{model}.png
│   └── Shared date x-axis
│       MSM retrain events:   upward triangles (▲)
│       ADWIN retrain events: squares (■)
│       Orange shading for stress periods
│       Caption note: "Agreement between MSM and ADWIN during stress periods
│       provides mutual validation of regime detection"
│
├── plot_causal_structure_collapse(df_results, df_causal)
│   → results/plots/14_causal_structure_collapse.png
│   ├── LEFT y-axis:  spy_parents over time (from causal_discovery_summary.csv)
│   │                 — number of causal parents of SPY per window
│   └── RIGHT y-axis: mean graph_msm from best MSM config
│       Orange shading for stress periods
│       This is the visual thesis argument: when causal graph around SPY
│       collapses (stress), MSM detects it and triggers retraining
│
└── save_drift_summary(df_results, df_causal)
    → results/analysis/drift_summary.csv
    Columns: stress_event | msm_detection_window | adwin_detection_window |
             which_fired_first | f1_delta_post_retrain | spy_parents_at_trigger |
             graph_msm_at_trigger
```

**Critical implementation rules:**
- Import `STRESS_EVENTS` and `STRESS_WINDOW_DAYS` from `analysis.py` — never hardcode dates
- Use the best MSM config from `best_configs.csv` for all MSM lines, not all configs
- All 3 plots must use identical x-axis date ranges for direct visual comparison
- `static_rolling_bins` MUST appear in Plot 12 bottom panel — it is the control that
  separates the bin effect from the model effect visually

---

## PHASE 7 — Final Validation Checklist (~30 min, before writing)

Every `NO` is not a crisis — it is something to address honestly in Discussion.

**Core thesis validity:**
- [ ] `wilcoxon_results.csv` — does MSM beat Static significantly (p_corrected < 0.05)
       for at least one model type?
- [ ] `effect_size.csv` — is Cohen's d ≥ 0.2 for MSM vs Static?
- [ ] `stress_period_f1.csv` — is `f1_stress_minus_calm` higher for MSM than for Static?
- [ ] `regime_retrain_rate.csv` — is `stress_calm_ratio` > 1.0 for MSM configs?

**Causal structure claim:**
- [ ] Plot 14 — do `spy_parents` and `graph_msm` visually co-move during stress?
       If yes, this is the strongest piece of visual evidence for the thesis.
- [ ] `causal_feature_usage.csv` — do features CausalRetrainer selects during stress differ
       from calm-period feature sets? If yes, quantify this difference.

**Bin isolation — StaticRollingBins control:**
- [ ] Plot 12 bottom panel — is MSM clearly above `static_rolling_bins`?
  - If MSM >> static_rolling_bins: clean thesis claim. State it directly.
  - If MSM ≈ static_rolling_bins: honest conclusion = "adaptive relabelling drives most
    of the improvement; model adaptation provides a smaller but consistent additional benefit."

**Detection quality:**
- [ ] `detection_latency.csv` (signal-driven only) — does MSM detect COVID and Fed hikes
       faster than ADWIN and Performance-based?
- [ ] `false_positive_rate.csv` — does MSM have lower FPR than Random/Fixed?

**Robustness:**
- [ ] `stress_robustness.csv` — is MSM's stress advantage stable across 45/63/90/120 days?
- [ ] `sensitivity_summary.csv` — does F1 degrade gracefully around the best tau combo,
       or is the surface highly peaked (fragile to hyperparameter choice)?
- [ ] `regime_retrain_rate.csv` — all `likely_inverted = False` for MSM configs?
       Any `True` means those SPY-MSM results are invalid and must be excluded.

**Statistical quality:**
- [ ] `wilcoxon_results.csv` — at least 10 aligned windows per model type? Fewer = underpowered.
- [ ] `retrain_efficiency.csv` — does MSM have higher `mean_f1_gain_per_retrain` than
       FixedSchedule? This is the "targeted vs blind retraining" argument.

---

## PHASE 8 — Write the Thesis (only after Phase 7 checklist is complete)

| Chapter | Primary sources | Key claims |
|---|---|---|
| **Methodology — MSM** | `msm_diagnostics.py` output, Plot 3 | Justify tau choice with heatmap |
| **Methodology — Experiment design** | `experiment.py` structure | Rolling window, 504-obs, TimeSeriesSplit(3) — justify |
| **Results §4.1 — Overall** | `overall_summary.csv`, `best_configs.csv`, Plot 1 | MSM rank vs all baselines |
| **Results §4.2 — Sensitivity** | `sensitivity_summary.csv`, Plot 3 | Robustness of best tau combo |
| **Results §4.3 — Regime** | `stress_period_f1.csv`, `stress_robustness.csv`, Plot 4 | MSM stress advantage |
| **Results §4.4 — Detection** | `detection_latency.csv`, `false_positive_rate.csv`, Plots 5, 11 | Faster detection, fewer wasted retrains |
| **Results §4.5 — Efficiency** | `retrain_efficiency.csv`, Plot 9 | MSM retrains are targeted |
| **Results §4.6 — Statistics** | `wilcoxon_results.csv`, `effect_size.csv`, Plots 6, 10 | Statistical + practical significance |
| **Results §4.7 — Causal structure** | `drift_summary.csv`, `causal_feature_usage.csv`, Plots 12, 14 | Causal graph collapse predicts stress |
| **Results §4.8 — MSM vs ADWIN** | `drift_summary.csv`, Plot 13 | Mutual validation |
| **Discussion — Bin isolation** | `retrain_efficiency.csv`, Plot 12 bottom | Honest separation of bin vs model effect |
| **Discussion — Limitations** | `narrative_report.txt` [CONCERN] lines | TimeSeriesSplit(3), window sensitivity, class imbalance |
| **Appendix** | `per_class_f1.csv`, `cooldown_analysis.csv`, full sensitivity tables, Plots 7–8 | Supporting detail |

**Discussion chapter must explicitly address:**
1. Whether `static_rolling_bins` closes most of the gap — be honest about what the number shows
2. Why `TimeSeriesSplit(n_splits=3)` is appropriate for a 504-observation window
3. Stress-window sensitivity — if `stress_robustness.csv` shows instability at 45 days, say so
4. Any non-significant Wilcoxon results — note Holm-Bonferroni is conservative at this sample size
5. Whether MSM and ADWIN agreement during stress constitutes mutual validation

---

## APPENDIX A — What Each Script Produces

| Script | Inputs | Outputs |
|---|---|---|
| `msm_diagnostics.py` | `causal_graphs.pkl` | `results/validation/*.png`, `msm_summary.csv` |
| `extract_causal_summary.py` (NEW) | `causal_graphs.pkl` | `results/causal_discovery/causal_discovery_summary.csv` |
| `experiment.py` | `standardized_data.csv`, `causal_graphs.pkl` | `results/experiments/**/*.csv`, `all_results.csv` |
| `analysis.py` | `all_results.csv` | 14 CSVs in `results/analysis/` |
| `significance_test.py` | `all_results.csv` | `wilcoxon_results.csv`, `effect_size.csv` |
| `interpret_results.py` | `results/analysis/*.csv` | `narrative_report.txt` |
| `plotting.py` | `results/analysis/*.csv` | 11 plot types in `results/plots/` |
| `drift_analysis.py` (NEW) | `all_results.csv`, `causal_discovery_summary.csv` | Plots 12–14, `drift_summary.csv` |

---

## APPENDIX B — Files Still Needed

| File | Status | Action |
|---|---|---|
| `src/retraining/extract_causal_summary.py` | 🔴 Does not exist | Create — Phase 0 |
| `src/retraining/drift_analysis.py` | 🔴 Does not exist | Create — Phase 6 |
| `StaticRollingBinsRetrainer` in `retrainers.py` | 🔴 Does not exist | Add — Fix 2 |
| `compute_retrain_efficiency()` in `analysis.py` | 🔴 Does not exist | Add — Fix 6 |
| `compute_stress_robustness()` in `analysis.py` | 🔴 Does not exist | Add — Fix 7 |
| `requirements.txt` | 🟡 Verify complete | Add `statsmodels` if missing |

---

## APPENDIX C — Honest Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| MSM not significant after Holm-Bonferroni | Medium | High | Report honestly; note correction is conservative; lead with practical effect size |
| `static_rolling_bins` closes most of MSM gap | Medium | High | Reframe: "adaptive relabelling drives gains; model adaptation provides additional benefit" |
| MSM signal does not clearly dip at stress events | Low | Critical | This is a data/preprocessing problem in `causal_graphs.pkl`, not a code problem |
| Causal feature sets don't differ stress vs calm | Medium | Medium | Valid null result; report it honestly for the causal feature selection component |
| Very low `spy_parents` counts throughout | Low | Medium | Graph construction may not be connecting SPY properly; check PC algorithm parameters |
