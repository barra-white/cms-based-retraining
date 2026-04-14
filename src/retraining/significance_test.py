import ast
import os

import numpy as np
import pandas as pd

from scipy.stats import wilcoxon
from itertools import combinations

def load_results(path='results/experiments/all_results.csv'):
    df = pd.read_csv(path, parse_dates=['date_start', 'date_end'])
    return df

def run_significance_tests(df, model_type='xgboost', alpha=0.05):
    model_df = df[df['model_type'] == model_type]
    # pivot: rows=windows, cols=retrainer, values=f1
    pivot = model_df.pivot_table(
        index=['date_start'], 
        columns='retrainer', 
        values='f1'
    )
    
    pivot = pivot.dropna()
    retrainers = list(pivot.columns)
    results = []
    
    for r1, r2 in combinations(retrainers, 2):
        x = pivot[r1].values
        y = pivot[r2].values
        
        if len(x) < 10:
            print(f"Not enough samples for {r1} vs {r2} (n={len(x)}) - skipping")
            continue
        
        stat, p = wilcoxon(x, y, alternative='two-sided', zero_method='wilcox')
        
        # one sided
        stat_greater, p_greater = wilcoxon(x, y, alternative='greater', zero_method='wilcox')
        
        results.append({
            'model_type': model_type,
            'retrainer_1': r1,
            'retrainer_2': r2,
            'mean_f1_s1': round(np.mean(x), 4),
            'mean_f1_s2': round(np.mean(y), 4),
            'mean_diff': round(np.mean(x) - np.mean(y), 4),
            'n_windows': len(x),
            'statistic': round(stat, 4),
            'p_value': round(p, 4),
            'statistic_greater': round(stat_greater, 4),
            'p_value_greater': round(p_greater, 4),
            'significant_two_sided': p < alpha,
            'significant_greater': p_greater < alpha
        })
        
    results_df = pd.DataFrame(results)
    print(results_df)
    return results_df

if __name__ == "__main__":
    df = load_results()
    os.makedirs('results/analysis', exist_ok=True)
    
    all_tests = []
    for model in df['model_type'].unique():
        tests = run_significance_tests(df, model_type=model)
        all_tests.append(tests)
        print(f'\n{model.upper()} SIGNIFICANCE TESTS:')
        print(tests[['retrainer_1', 'retrainer_2', 'mean_diff', 'p_value', 'significant_two_sided']].to_string(index=False))
    
    pd.concat(all_tests).to_csv('results/analysis/significance_tests.csv', index=False)
    
    