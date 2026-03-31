import os
import pickle

import numpy as np
import pandas as pd

from retrainers import(
    StaticRetrainer,
    FixedScheduleRetrainer,
    PerformanceRetrainer,
    ADWINRetrainer,
    MSMRetrainer
)

# ----- RUN CONFIG -----
# Controls whether full sensitivity grid or default parameters
# False: run with default parameters for each retrainer
# True: run with full grid of parameters for each retrainer (takes much longer)
RUN_SENSITIVITY = True



# ----- SENSITIVITY CONFIGS -----
# Sensitivity retrainer configurations (grid of parameters)
# 1) MSM thresholds
TAU_1_VALUES = [0.5, 0.6, 0.7] 
TAU_2_VALUES = [0.45, 0.55, 0.65] 

# 2) MSM lookback (how many windows to compute MSM)
LOOKBACK = [3, 4, 6]

# 3) Performance drop thresholds
PERFORMANCE_THRESHOLDS = [0.05, 0.1, 0.15] # 5%, 10%, 15% drop in performance triggers retrain

# 4) Performance lookback (how many windows to compute performance drop)
PERFORMANCE_LOOKBACK = [3, 4, 6]

# 5) ADWIN delta values (sensitivity to change)
ADWIN_DELTAS = [0.001, 0.002, 0.005]

# 6) Fixed schedule intervals (in months)
FIXED_INTERVALS = [3, 6, 9, 12]

# default msm config (RUN_SENSITIVITY = False)
MSM_DEFAULT = ("msm_default", MSMRetrainer, {"tau_1": 0.6, "tau_2": 0.55, "lookback": 4})



# ----- CONFIG BUILDERS -----
# static
def build_static_config():
    return [("static", StaticRetrainer, {})]

# fixed schedule
def build_fixed_schedule_configs():
    return [
        (f"fixed_{interval}", FixedScheduleRetrainer, {"retrain_interval": interval})
        for interval in FIXED_INTERVALS
    ]
    
# performance-based
def build_performance_configs():
    configs = []
    for drop in PERFORMANCE_THRESHOLDS:
        for lookback in PERFORMANCE_LOOKBACK:
            name = f'perf_drop{drop}_lookback{lookback}'
            configs.append((name, PerformanceRetrainer, {"drop_threshold": drop, "lookback": lookback}))
    return configs

# ADWIN-based
def build_adwin_configs():
    return [
        (f'adwin_delta{delta}', ADWINRetrainer, {"delta": delta})
        for delta in ADWIN_DELTAS
    ]
    
# MSM-based
def build_msm_configs():
    configs = []
    for tau_1 in TAU_1_VALUES:
        for tau_2 in TAU_2_VALUES:
            if tau_2 >= tau_1:
                continue # skip invalid configs where tau_2 >= tau_1
            for lookback in LOOKBACK:
                name = f'msm_tau1{tau_1}_tau2{tau_2}_lookback{lookback}'
                configs.append((name, MSMRetrainer, {"tau_1": tau_1, "tau_2": tau_2, "lookback": lookback}))
    return configs



# ----- SINGLE EXPERIMENT RUNNER -----
def run_experiment(name, cls, kwargs, base_args, all_graphs):
    print(f'\nRunning: {name}')
    print(f'\tConfig: {kwargs}')
    
    retrainer = cls(**base_args, **kwargs)
    results = retrainer.run(all_graphs)
    
    # tag results with retrainer name for later analysis
    results['retrainer'] = name
    
    # save csv immideiately to avoid losing results if experiment crashes later
    output_file = f"results/experiments/{name}_results.csv"
    results.to_csv(output_file, index=False)
    
    print(f'\n\tmean f1   : {results["f1"].mean():.4f}')
    print(f'\tmean acc   : {results["accuracy"].mean():.4f}')
    print(f'\tretrains   : {results["retrain"].sum()}')
    print(f'saved results to: {output_file}')
    return results



# ----- SUMMARY PRINTING -----
def print_summary(combined):
    summary = (
        combined.groupby('retrainer')
        .agg(
            mean_f1=('f1', 'mean'),
            std_f1=('f1', 'std'),
            retrains=('retrain_triggered', 'sum'),
            windows=('f1', 'count')
        )
        .round(4)
        .sort_values('mean_f1', ascending=False)
    )
    
    print('\n\nExperiment Summary:')
    print(summary.to_string())
    print(f'\nTotal strategies run: {len(summary)}')
    
def main():
    print('Loading data...')
    
    df = pd.read_csv("data/processed/standardized_data.csv", parse_dates=['Date'])
    
    with open("data/causal_graphs.pkl", "rb") as f:
        all_graphs = pickle.load(f)
        
    feature_cols = [col for col in df.columns if col not in ['Date', 'SPY_lr']]
    
    print(f'Observations: {len(df)}')
    print(f'Features: {len(feature_cols)}')
    print(f'Graph windows: {len(all_graphs)}')
    
    # base args: same for all retrainers
    base_args = dict(
        df=df,
        feature_cols=feature_cols,
        target='SPY_lr',
        model_type='xgboost',
        n_bins=3,
        window=504,
        step=21,
        cooldown=3
    )
    
    # build retrainer configs
    configs = []
    configs += build_static_config()
    configs += build_fixed_schedule_configs()
    configs += build_performance_configs()
    configs += build_adwin_configs()
    
    if RUN_SENSITIVITY:
        configs += build_msm_configs()
        print(f'Running full sensitivity grid with {len(configs)} retrainer configurations...')
    else:
        configs += [MSM_DEFAULT]
        print(f'DEV MODE: Running default configurations for each retrainer with {len(configs)} retrainer configurations...')
        
    os.makedirs("results/experiments", exist_ok=True)
    all_results = []
    
    print('Starting experiments...')
    exp_num = 1
    total_exps = len(configs)
    
    for name, cls, kwargs in configs:
        print(f'\n=== Experiment {exp_num}/{total_exps}: {name} ===')
        results = run_experiment(name, cls, kwargs, base_args, all_graphs)
        all_results.append(results)
        exp_num += 1
        
    combined = pd.concat(all_results, ignore_index=True)
    combined.to_csv("results/experiments/all_results.csv", index=False)
    print('All experiments completed. Combined results saved to results/experiments/all_results.csv')
    
    print_summary(combined)
    
if __name__ == "__main__":
    main()