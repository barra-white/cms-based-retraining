import os
import pickle
import traceback

import numpy as np
import pandas as pd

from retrainers import(
    StaticRetrainer,
    FixedScheduleRetrainer,
    PerformanceRetrainer,
    ADWINRetrainer,
    RandomRetrainer,
    MSMRetrainer,
    SPYFocusedMSMRetrainer,
    MSMTimeoutRetrainer,
    CausalFeatureRetrainer
)

# ----- RUN CONFIG -----
# Controls whether full sensitivity grid or default parameters
# False: run with default parameters for each retrainer
# True: run with full grid of parameters for each retrainer (takes much longer)
RUN_SENSITIVITY = True



# ----- SENSITIVITY CONFIGS -----
# Sensitivity retrainer configurations (grid of parameters)
# 1) MSM thresholds
# based off of msm validation script
TAU_1_VALUES = [0.8, 0.83, 0.88] 
TAU_2_VALUES = [0.72, 0.75, 0.78] 

# 2) MSM lookback (how many windows to compute MSM)
LOOKBACK = [3, 4, 6]

# 3) Performance drop thresholds
PERFORMANCE_THRESHOLDS = [0.03, 0.05, 0.1, 0.15, 0.2] # 3%, 5%, 10%, 15%, 20% drop in performance triggers retrain

# 4) Performance lookback (how many windows to compute performance drop)
PERFORMANCE_LOOKBACK = [3, 4, 6]

# 5) ADWIN delta values (sensitivity to change)
ADWIN_DELTAS = [0.002, 0.005, 0.01, 0.05, 0.1]

# 6) Fixed schedule intervals (in months)
FIXED_INTERVALS = [3, 6, 9, 12]

# default msm config (RUN_SENSITIVITY = False)
MSM_DEFAULT = ("msm_default", MSMRetrainer, {"tau_1": 0.83, "tau_2": 0.75, "lookback": 4})



# list of all models to be tested
MODEL_TYPES = ['xgboost', 'lr', 'rf']

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
            name = f'perf_drop_{drop}_lookback_{lookback}'
            configs.append((name, PerformanceRetrainer, {"drop_threshold": drop, "lookback_n": lookback}))
    return configs

# ADWIN-based
def build_adwin_configs():
    return [
        (f'adwin_delta_{delta}', ADWINRetrainer, {"delta": delta})
        for delta in ADWIN_DELTAS
    ]
def build_random_configs():
    return [('random', RandomRetrainer, {})]

def build_spy_msm_configs():
    configs = []
    for tau_1 in TAU_1_VALUES:
        for tau_2 in TAU_2_VALUES:
            if tau_2 >= tau_1 or abs(tau_1 - tau_2) < 0.04:
                continue
            for lookback in LOOKBACK:
                name = f'spy_msm_tau_1_{tau_1}_tau_2_{tau_2}_lb_{lookback}' 
                configs.append((name, SPYFocusedMSMRetrainer, {'tau_1': tau_1, 'tau_2': tau_2, 'lookback': lookback}))
    return configs

def build_timeout_msm_configs():
    configs = []
    for tau_1 in TAU_1_VALUES:
        for tau_2 in TAU_2_VALUES:
            if tau_2 >= tau_1 or abs(tau_1 - tau_2) < 0.04:
                continue
            for lookback in LOOKBACK:
                name = f'timeout_msm_tau_1_{tau_1}_tau_2_{tau_2}_lb_{lookback}'
                configs.append((name, MSMTimeoutRetrainer, {'tau_1': tau_1, 'tau_2': tau_2, 'lookback': lookback}))
    return configs

# MSM-based
def build_msm_configs():
    configs = []
    MIN_TAU_DIFF = 0.04 # ensure tau_1 and tau_2 differ by at least this much to avoid trivial configs
    for tau_1 in TAU_1_VALUES:
        for tau_2 in TAU_2_VALUES:
            if tau_2 >= tau_1:
                continue # skip invalid configs where tau_2 >= tau_1
            if abs(tau_1 - tau_2) < MIN_TAU_DIFF:
                continue # skip configs where tau_1 and tau_2 are too close
            for lookback in LOOKBACK:
                name = f'msm_tau_1_{tau_1}_tau_2_{tau_2}_lb_{lookback}'       # ← 'lookback_' → 'lb_' (consistency)
                configs.append((name, MSMRetrainer, {"tau_1": tau_1, "tau_2": tau_2, "lookback": lookback}))
    return configs



# ----- SINGLE EXPERIMENT RUNNER -----
def get_experiment_type(name: str) -> str:
    if name.startswith('spy_msm'):
        return 'spy_msm'
    if name.startswith('timeout_msm'):
        return 'timeout_msm'
    if name.startswith('msm'):
        return 'msm'
    if name.startswith('causal'):
        return 'causal'
    if name.startswith('fixed'):
        return 'fixed'
    if name.startswith('perf'):
        return 'performance'
    if name.startswith('adwin'):
        return 'adwin'
    if name.startswith('random'):
        return 'random'
    if name.startswith('static'):
        return 'static'
    return 'other'

def run_experiment(name, cls, kwargs, base_args, all_graphs):
    model_type      = base_args['model_type']
    experiment_type = get_experiment_type(name)
    output_dir      = f"results/experiments/{model_type}/{experiment_type}"
    output_file     = f"{output_dir}/{name}_results.csv"
    os.makedirs(output_dir, exist_ok=True)
    if os.path.exists(output_file):
        print(f'\nResults for {name} already exist at {output_file}. Loading existing results...')
        return pd.read_csv(output_file)
    print(f'\nRunning: {name}')
    print(f'\tConfig: {kwargs}')
    
    retrainer = cls(**base_args, **kwargs)
    results = retrainer.run(all_graphs)
    
    # tag results with retrainer name for later analysis
    results['retrainer'] = name
    # tag model type for later analysis
    results['model_type'] = base_args['model_type']
    
    # save csv immideiately to avoid losing results if experiment crashes later
    results.to_csv(output_file, index=False)
    
    print(f'\n\tmean f1    : {results["f1"].mean():.4f}')
    print(f'\tdirectional acc : {results["directional_acc"].mean():.4f}')
    print(f'\tretrains    : {results["retrain_triggered"].sum()}')
    print(f'\tsignals     : {results["signal_fired"].sum()}')
    print(f'\tcooldowns   : {results["cooldown_active"].sum()}')
    print(f'\tretrain rate: {results["retrain_triggered"].mean():.4%}')
    print(f'\twindows     : {len(results)}')
    print(f'saved results to: {output_file}')
    return results



# ----- SUMMARY PRINTING -----
def print_summary(combined):
    summary = (
        combined.groupby(['model_type', 'retrainer'])
        .agg(
            mean_f1=('f1', 'mean'),
            mean_directional_acc=('directional_acc', 'mean'),
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
    base_args_temp = dict(
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
    
    if RUN_SENSITIVITY:
        configs += build_static_config()
        configs += build_fixed_schedule_configs()
        configs += build_performance_configs()
        configs += build_adwin_configs()
        configs += build_random_configs()
        configs += build_msm_configs()
        configs += build_spy_msm_configs()
        configs += build_timeout_msm_configs()
        # Note: CausalFeatureRetrainer needs feature_cols — add after feature_cols is loaded
        spy_idx = feature_cols.index('SPYlr') if 'SPYlr' in feature_cols else 10
        for tau1 in TAU_1_VALUES:
            for tau2 in TAU_2_VALUES:
                if tau2 >= tau1 or abs(tau1 - tau2) < 0.04: continue
                for lookback in LOOKBACK:
                    configs.append((f'causal_tau_1_{tau1}_tau_2_{tau2}_lb_{lookback}', 
                    CausalFeatureRetrainer,
                    {'tau_1': tau1, 'tau_2': tau2, 'lookback': lookback,  'spy_idx': spy_idx, 'all_features': feature_cols}))
    else:
        configs += [MSM_DEFAULT]
        print(f'DEV MODE: Running default configurations for each retrainer with {len(configs)} retrainer configurations...')
        
    os.makedirs("results/experiments", exist_ok=True)
    os.makedirs("results/experiments/xgboost", exist_ok=True)
    os.makedirs("results/experiments/lr", exist_ok=True)
    os.makedirs("results/experiments/rf", exist_ok=True)
    all_results = []
    
    print('Starting experiments...')
    exp_num = 1
    total_exps = len(configs) * len(MODEL_TYPES)
    
    for model_type in MODEL_TYPES:
        base_args = {**base_args_temp, "model_type": model_type}
        for name, cls, kwargs in configs:
            print(f'\n=== Experiment {exp_num}/{total_exps}: {name} ===')
            try:
                results = run_experiment(name, cls, kwargs, base_args, all_graphs)
                all_results.append(results)
                pd.concat(all_results, ignore_index=True).to_csv("results/experiments/all_results_partial.csv", index=False)
            except Exception as e:
                print(f'\nError occurred while running experiment {name}: {e}')
                traceback.print_exc()
            exp_num += 1
        
    
    combined = pd.concat(all_results, ignore_index=True)
    combined.to_csv("results/experiments/all_results.csv", index=False)
    print('All experiments completed. Combined results saved to results/experiments/all_results.csv')
    
    print_summary(combined)
    
if __name__ == "__main__":
    main()