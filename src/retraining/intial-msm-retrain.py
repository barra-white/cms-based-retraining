import pickle
import numpy as np
import pandas as pd
import joblib
from xgboost import XGBClassifier
from sklearn.metrics import f1_score

df = pd.read_csv('data/processed/standardized_data.csv', parse_dates=['Date'])
feature_cols = [col for col in df.columns if col != 'Date']
df = df.dropna(subset=feature_cols).reset_index(drop=True)

TARGET       = 'SPY_lr'
FEATURE_COLS = [c for c in feature_cols if c != TARGET]
N_BINS       = 3
WINDOW       = 504
STEP         = 21

with open('results/causal_graphs.pkl', 'rb') as f:
    all_graphs = pickle.load(f)

params = joblib.load('results/xgboost/xgboost_params.joblib')
params.pop('use_label_encoder', None)


def bin_target(train_vals, all_vals, n_bins=N_BINS):
    edges = np.percentile(train_vals, np.linspace(0, 100, n_bins + 1))
    return np.digitize(all_vals, edges[1:-1]), edges


# CHANGED: MSM redefined as a graph-level stability score.
# For each edge seen in any of the last LOOKBACK windows, compute
# MSM_e = count(e appeared) / LOOKBACK.
# Graph-level MSM = mean(MSM_e) across all those edges.
# This captures full structural shifts rather than individual edge drops,
# which is necessary given SPY-incoming edges are too sparse to monitor
# individually (max global persistence 19.1%).
LOOKBACK  = 4    # ~4 months of lookback
TAU_1     = 0.6  # mean MSM drop threshold — retrain when graph-level
                 # stability falls below this


def compute_graph_msm(all_graphs, current_w, lookback):
    '''
    Computes mean per-edge MSM across all edges seen in the
    last `lookback` windows ending at current_w (inclusive).
    Returns: (mean_msm, dict of {edge: msm_score})
    '''
    start          = max(0, current_w - lookback + 1)
    recent_windows = all_graphs[start: current_w + 1]
    n              = len(recent_windows)

    # union of all edges seen in lookback period
    all_edges = set()
    for g in recent_windows:
        all_edges |= g['edges']

    if not all_edges:
        return 0.0, {}

    edge_scores = {}
    for edge in all_edges:
        count             = sum(1 for g in recent_windows if edge in g['edges'])
        edge_scores[edge] = count / n

    mean_msm = np.mean(list(edge_scores.values()))
    return mean_msm, edge_scores


results      = []
static_model = None
msm_model    = None
prev_msm     = None   # track previous window MSM to detect drops

for w, g in enumerate(all_graphs):
    train_start = g['train_start_idx']
    train_end   = g['train_end_idx']
    test_start  = train_end
    test_end    = test_start + STEP

    if test_end > len(df):
        continue

    X_train    = df[FEATURE_COLS].iloc[train_start:train_end].values
    y_raw      = df[TARGET].iloc[train_start:train_end].values
    y_train, _ = bin_target(y_raw, y_raw)

    X_test     = df[FEATURE_COLS].iloc[test_start:test_end].values
    y_test_raw = df[TARGET].iloc[test_start:test_end].values
    y_test, _  = bin_target(y_raw, y_test_raw)

    # compute current graph-level MSM
    curr_msm, edge_scores = compute_graph_msm(all_graphs, w, LOOKBACK)

    # trigger: MSM has dropped below TAU_1
    # only evaluate trigger after enough windows for a full lookback
    trigger = (w >= LOOKBACK) and (curr_msm < TAU_1)

    # static baseline — trained once on window 1
    if w == 0:
        static_model = XGBClassifier(**params)
        static_model.fit(X_train, y_train)

    static_pred = static_model.predict(X_test)
    static_f1   = f1_score(y_test, static_pred,
                           average='macro', zero_division=0)

    # MSM strategy
    if w == 0 or msm_model is None:
        msm_model = XGBClassifier(**params)
        msm_model.fit(X_train, y_train)
    elif trigger:
        msm_model = XGBClassifier(**params)
        msm_model.fit(X_train, y_train)

    msm_pred = msm_model.predict(X_test)
    msm_f1   = f1_score(y_test, msm_pred,
                        average='macro', zero_division=0)

    results.append({
        'window':      w + 1,
        'date_start':  g['date_start'],
        'date_end':    g['date_end'],
        'total_edges': len(g['edges']),
        'graph_msm':   round(curr_msm, 4),
        'msm_trigger': trigger,
        'static_f1':   round(static_f1, 4),
        'msm_f1':      round(msm_f1, 4),
        'f1_delta':    round(msm_f1 - static_f1, 4)
    })

    trigger_str = 'RETRAIN' if trigger else '       '
    print(f"Window {w+1:>3}  "
          f"edges: {len(g['edges']):>2}  "
          f"graph_msm: {curr_msm:.3f}  "
          f"{trigger_str}  "
          f"static: {static_f1:.3f}  "
          f"msm: {msm_f1:.3f}  "
          f"delta: {msm_f1 - static_f1:+.3f}")

    # print which edges drove the instability when trigger fires
    if trigger:
        unstable = {e: s for e, s in edge_scores.items() if s < TAU_1}
        for (src, tgt, lag), score in sorted(unstable.items(),
                                             key=lambda x: x[1]):
            print(f"           {feature_cols[src]} → {feature_cols[tgt]} "
                  f"lag={lag}  MSM={score:.2f}")

    prev_msm = curr_msm

results_df = pd.DataFrame(results)
results_df.to_csv('results/retraining/msm_preliminary_results.csv', index=False)

n_retrains = results_df['msm_trigger'].sum()
print(f"\n{'='*55}")
print(f"Lookback: {LOOKBACK}  τ1: {TAU_1}")
print(f"Total retrains triggered:        {n_retrains}/{len(results_df)}")
print(f"Mean graph MSM (all windows):    {results_df['graph_msm'].mean():.4f}")
print(f"Mean static F1:                  {results_df['static_f1'].mean():.4f}")
print(f"Mean MSM F1:                     {results_df['msm_f1'].mean():.4f}")
print(f"Mean F1 delta:                   {results_df['f1_delta'].mean():+.4f}")
print(f"Windows where MSM beats static:  "
      f"{(results_df['f1_delta'] > 0).sum()}/{len(results_df)}")

# verify triggers align with known stress periods
print(f"\nRetrains during known stress periods:")
stress = {
    'COVID crash (Feb-Apr 2020)': ('2020-02-01', '2020-04-30'),
    'Fed rate rises (2022)':      ('2022-01-01', '2022-12-31'),
}
for label, (start, end) in stress.items():
    n = results_df[
        results_df['msm_trigger'] &
        (results_df['date_end'] >= start) &
        (results_df['date_end'] <= end)
    ].shape[0]
    print(f"  {label}: {n} retrains")
