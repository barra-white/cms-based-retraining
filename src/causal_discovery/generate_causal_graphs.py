import os
import time
import pickle

import numpy as np
import pandas as pd

from tigramite import data_processing as pp
from tigramite.pcmci import PCMCI
from tigramite.independence_tests.robust_parcorr import RobustParCorr

np.random.seed(42)

# read data in
df = pd.read_csv("data/processed/standardized_data.csv",parse_dates=["Date"])
feature_cols = [col for col in df.columns if col != "Date"] # extract feature col names

df = df.dropna(subset=feature_cols).reset_index(drop=True) # drop empty rows after transformation

data = df[feature_cols].to_numpy()
var_names = feature_cols

T, N = data.shape

dates = df["Date"].values

window_size = 504 # 504 observations == 2 trading years
step = 21 # 21 observations == 1 trading month
tau_max = 5 # max time lag to consider for causal links (5 trading days == 1 trading week)
n_windows = (T - window_size) // step

print(f"Total observations: {T}")
print(f"Variables: {N}")
print(f"Window size: {window_size}")
print(f"Step: {step}")
print(f"Number of windows: {n_windows}")


SPY_IDX = var_names.index("SPY_lr")

def extract_edges(graph, tau_max_val):
    '''Extract significant edges from PCMCI+ graph array.
    Returns set of (source_idx, target_idx, lag) tuples.
    Upper triangle convention used for tau=0 (i < j only).
    Includes o-o (undirected contemporaneous) links — direction-aware
    SPY parent extraction handled separately in get_spy_parents().'''
    edges = set()
    for i in range(N):
        for j in range(N):
            for tau in range(0, tau_max_val + 1):
                if tau == 0 and i >= j:
                    continue
                if graph[i, j, tau] not in ['', 'x-x']:
                    edges.add((i, j, tau))
    return edges

def get_spy_parents(graph, tau_max_val, include_undirected=True):
    '''Return directed parents of SPY using graph marks directly.
    Reads graph array to correctly handle upper-triangle convention at tau=0
    where SPY (index 0) is always the row, not the column.'''
    spy_parents = set()
    for i in range(N):
        if i == SPY_IDX:
            continue
        for tau in range(1, tau_max_val + 1):
            if graph[i, SPY_IDX, tau] == '-->':
                spy_parents.add((i, SPY_IDX, tau))
        mark = graph[SPY_IDX, i, 0]
        if mark == '<--':
            spy_parents.add((i, SPY_IDX, 0))
        elif mark == 'o-o' and include_undirected:
            spy_parents.add((i, SPY_IDX, 0))
    return spy_parents

def rolling_window_discovery():
    print(f'\n\n\n=====Rolling Window Causal Discovery (tau_max={tau_max})=====')

    dataframe = pp.DataFrame(
        data=data,
        datatime=np.arange(T),
        var_names=var_names
    )
    ci_test = RobustParCorr(significance='analytic')
    pcmci_full = PCMCI(dataframe=dataframe, cond_ind_test=ci_test, verbosity=0)

    total_start = time.time()

    sliding_results = pcmci_full.run_sliding_window_of(
        method='run_pcmciplus',
        method_args={
            'tau_min': 0,
            'tau_max': tau_max,
            'pc_alpha': None,
            'fdr_method': 'fdr_bh'
        },
        window_step=step,
        window_length=window_size,
        conf_lev=0.9
    )

    total_elapsed = time.time() - total_start
    print(f'    Total runtime: {total_elapsed / 60:.1f} min')

    # CHANGED: p_matrix always present as numpy array — no None fallback needed
    window_graphs = sliding_results['window_results']['graph']
    window_vals   = sliding_results['window_results']['val_matrix']
    window_pvals  = sliding_results['window_results']['p_matrix']

    all_graphs  = []
    window_rows = []

    for w, (graph, val_matrix, p_matrix) in enumerate(
            zip(window_graphs, window_vals, window_pvals)):

        train_start = w * step
        train_end   = train_start + window_size
        date_start  = str(dates[train_start])[:10]
        date_end    = str(dates[train_end - 1])[:10]

        edges     = extract_edges(graph, tau_max)
        spy_edges = get_spy_parents(graph, tau_max)

        all_graphs.append({
            'window':          w + 1,
            'train_start_idx': train_start,
            'train_end_idx':   train_end,
            'date_start':      date_start,
            'date_end':        date_end,
            'graph':           graph.copy(),
            'val_matrix':      val_matrix.copy(),
            'p_matrix':        p_matrix.copy(),
            'edges':           edges
        })

        window_rows.append({
            'window':      w + 1,
            'date_start':  date_start,
            'date_end':    date_end,
            'total_edges': len(edges),
            'spy_parents': len(spy_edges),
            'runtime_s':   round(total_elapsed / len(window_graphs), 1)
        })

    return all_graphs, window_rows



def edge_persistence_analysis(all_graphs):
    print(f'\n\n\n=====Edge Persistence Across All {len(all_graphs)} Windows=====')

    edge_counts = {}
    for g in all_graphs:
        for edge in g['edges']:
            edge_counts[edge] = edge_counts.get(edge, 0) + 1

    sorted_edges = sorted(edge_counts.items(), key=lambda x: -x[1])
    n_w = len(all_graphs)

    print(f'    Unique edges found: {len(sorted_edges)}')
    print(f'\n    Top 20 most persistent edges:')
    print(f'    {"Edge":<40} {"Count":>5} {"Persistence":>11}')
    print(f'    {"-"*56}')

    persistence_rows = []
    for (src, tgt, lag), count in sorted_edges[:20]:
        pct = count / n_w
        edge_str = f'{var_names[src]} --> {var_names[tgt]} (lag={lag})'
        print(f'    {edge_str:<40} {count:>5} {pct:>10.1%}  '
              f'{"█" * int(pct * 30)}')

    for (src, tgt, lag), count in sorted_edges:
        persistence_rows.append({
            'source':       var_names[src],
            'target':       var_names[tgt],
            'lag':          lag,
            'window_count': count,
            'persistence':  round(count / n_w, 4)
        })

    persistence_df = pd.DataFrame(persistence_rows)
    os.makedirs('results/causal_discovery', exist_ok=True)
    persistence_df.to_csv('results/causal_discovery/edge_persistence.csv', index=False)
    print(f'\n    Saved: results/causal_discovery/edge_persistence.csv')

    print(f'\n    Edges directed toward SPY_lr:')
    spy_df = persistence_df[
        persistence_df['target'] == 'SPY_lr'
    ].sort_values('persistence', ascending=False)

    if len(spy_df) > 0:
        for _, row in spy_df.iterrows():
            print(f'    {row["source"]:<16} lag={int(row["lag"])}  '
                  f'count={int(row["window_count"]):>3}  '
                  f'persistence={row["persistence"]:.1%}  '
                  f'{"█" * int(row["persistence"] * 30)}')
    else:
        print('    No edges directed toward SPY_lr found')


if __name__ == '__main__':
    # Resume from a previous manual-loop run or batch runner if available.
    # run_sliding_window_of does not write this file — if it crashes,
    # all progress is lost. Revert to a manual loop if runtime is > 60 min.
    progress_file = 'results/causal_graphs_progress.pkl'
    if os.path.exists(progress_file):
        print('    Loading pre-computed causal graphs...')
        with open(progress_file, 'rb') as f:
            all_graphs = pickle.load(f)
        print(f'    Loaded {len(all_graphs)} window graphs')
        window_rows = []
        for g in all_graphs:
            spy_edges = get_spy_parents(g['graph'], tau_max)
            window_rows.append({
                'window':      g['window'],
                'date_start':  g['date_start'],
                'date_end':    g['date_end'],
                'total_edges': len(g['edges']),
                'spy_parents': len(spy_edges),
                'runtime_s':   0
            })
    else:
        # sensitivity check removed — go straight to rolling window
        all_graphs, window_rows = rolling_window_discovery()

    with open('data/causal_graphs.pkl', 'wb') as f:
        pickle.dump(all_graphs, f)
    print(f'    Causal graphs saved: data/causal_graphs.pkl')

    os.makedirs('results/causal_discovery', exist_ok=True)
    pd.DataFrame(window_rows).to_csv(
        'results/causal_discovery/causal_discovery_summary.csv', index=False)
    print(f'    Summary saved: results/causal_discovery/causal_discovery_summary.csv')

    edge_persistence_analysis(all_graphs)