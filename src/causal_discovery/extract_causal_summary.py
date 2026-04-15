'''
extract_causal_summary.py — Extract per-window causal graph summary from pkl.

Reads data/causal_graphs.pkl and writes:
    results/causal_discovery/causal_discovery_summary.csv

Columns: window, date_start, date_end, n_edges, spy_parents, spy_children
'''

import os
import pickle
import pandas as pd

PKL_PATH = 'data/causal_graphs.pkl'
OUT_DIR  = 'results/causal_discovery'
OUT_PATH = os.path.join(OUT_DIR, 'causal_discovery_summary.csv')

# Must match the variable order in standardized_data.csv
# SPY_lr is at index 0 in the graph
TARGET_NAME = 'SPY_lr'


def main():
    print(f'Loading {PKL_PATH}...')
    with open(PKL_PATH, 'rb') as f:
        graphs = pickle.load(f)
    print(f'  {len(graphs)} windows loaded')

    # Resolve target index from first graph's variable names if available,
    # otherwise fall back to loading from the CSV.
    if 'var_names' in graphs[0]:
        var_names = graphs[0]['var_names']
    else:
        df = pd.read_csv('data/processed/standardized_data.csv', nrows=1)
        var_names = [c for c in df.columns if c != 'Date']

    if TARGET_NAME not in var_names:
        print(f'WARNING: {TARGET_NAME} not in variable names: {var_names}')
        target_idx = 0
    else:
        target_idx = var_names.index(TARGET_NAME)
    print(f'  {TARGET_NAME} at graph index {target_idx}')

    records = []
    for g in graphs:
        edges = g.get('edges', set())

        # Edges directed INTO target (parents)
        spy_parents = sum(1 for e in edges if e[1] == target_idx and e[0] != target_idx)
        # Edges directed FROM target (children)
        spy_children = sum(1 for e in edges if e[0] == target_idx and e[1] != target_idx)

        records.append({
            'window':       g.get('window', None),
            'date_start':   g.get('date_start', None),
            'date_end':     g.get('date_end', None),
            'n_edges':      len(edges),
            'spy_parents':  spy_parents,
            'spy_children': spy_children,
        })

    os.makedirs(OUT_DIR, exist_ok=True)
    pd.DataFrame(records).to_csv(OUT_PATH, index=False)
    print(f'  Saved {len(records)} rows to {OUT_PATH}')


if __name__ == '__main__':
    main()
