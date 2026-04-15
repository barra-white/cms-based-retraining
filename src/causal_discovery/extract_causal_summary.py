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