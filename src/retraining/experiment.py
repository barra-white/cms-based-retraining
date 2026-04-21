import os
import pickle
import sys
import traceback
import random
import warnings

warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config as cfg

from retrainers import (
    StaticRetrainer,
    FixedScheduleRetrainer,
    PerformanceRetrainer,
    ADWINRetrainer,
    RandomRetrainer,
    MSMRetrainer,
    SPYFocusedMSMRetrainer,
    MSMTimeoutRetrainer,
    CausalFeatureRetrainer,
    DriftSignalObserverRetrainer,

)

np.random.seed(42)
random.seed(42)

# ----- RUN CONFIG -----
RUN_SENSITIVITY = False


# ----- SENSITIVITY CONFIGS -----
TAU_1_VALUES = [0.8, 0.83, 0.88]
TAU_2_VALUES = [0.72, 0.75, 0.78]
LOOKBACK     = [3, 4, 6]

PERFORMANCE_THRESHOLDS = [0.03, 0.05, 0.1, 0.15, 0.2]
PERFORMANCE_LOOKBACK   = [3, 4, 6]

ADWIN_DELTAS    = [0.002, 0.005, 0.01, 0.05, 0.1]
FIXED_INTERVALS = [3, 6, 9, 12]

MSM_DEFAULT = ("msm_default", MSMRetrainer, {"tau_1": 0.83, "tau_2": 0.75, "lookback": 4})

MODEL_TYPES = ['xgboost', 'lr', 'rf']


# ----- CONFIG BUILDERS -----

def build_static_config():
    # StaticRollingBinsRetrainer removed (see import comment above).
    return [("static", StaticRetrainer, {})]


def build_drift_observer_config():
    '''
    DriftSignalObserver: frozen model, frozen bins, logs MSM at every window.
    Runs once per model. Output feeds lead_lag_analysis.py to produce
    RQ1 evidence (does MSM predictively lead F1 degradation?).
    '''
    return [("drift_observer", DriftSignalObserverRetrainer, {"lookback": 4})]


def build_fixed_schedule_configs():
    return [
        (f"fixed_{interval}", FixedScheduleRetrainer, {"retrain_interval": interval})
        for interval in FIXED_INTERVALS
    ]


def build_performance_configs():
    configs = []
    for drop in PERFORMANCE_THRESHOLDS:
        for lookback in PERFORMANCE_LOOKBACK:
            name = f'perf_drop_{drop}_lookback_{lookback}'
            configs.append((name, PerformanceRetrainer,
                            {"drop_threshold": drop, "lookback_n": lookback}))
    return configs


def build_adwin_configs():
    return [
        (f'adwin_delta_{delta}', ADWINRetrainer, {"delta": delta})
        for delta in ADWIN_DELTAS
    ]


def build_random_configs():
    return [('random', RandomRetrainer, {})]


def build_spy_msm_configs(target_idx_in_graph):
    configs = []
    for tau_1 in TAU_1_VALUES:
        for tau_2 in TAU_2_VALUES:
            if tau_2 >= tau_1 or abs(tau_1 - tau_2) < 0.04:
                continue
            for lookback in LOOKBACK:
                name = f'spy_msm_tau_1_{tau_1}_tau_2_{tau_2}_lb_{lookback}'
                configs.append((
                    name,
                    SPYFocusedMSMRetrainer,
                    {
                        'tau_1':      tau_1,
                        'tau_2':      tau_2,
                        'lookback':   lookback,
                        'target_idx': target_idx_in_graph,
                    }
                ))
    return configs


def build_timeout_msm_configs():
    configs = []
    for tau_1 in TAU_1_VALUES:
        for tau_2 in TAU_2_VALUES:
            if tau_2 >= tau_1 or abs(tau_1 - tau_2) < 0.04:
                continue
            for lookback in LOOKBACK:
                name = f'timeout_msm_tau_1_{tau_1}_tau_2_{tau_2}_lb_{lookback}'
                configs.append((name, MSMTimeoutRetrainer,
                                {'tau_1': tau_1, 'tau_2': tau_2, 'lookback': lookback}))
    return configs


def build_msm_configs():
    configs = []
    MIN_TAU_DIFF = 0.04
    for tau_1 in TAU_1_VALUES:
        for tau_2 in TAU_2_VALUES:
            if tau_2 >= tau_1:
                continue
            if abs(tau_1 - tau_2) < MIN_TAU_DIFF:
                continue
            for lookback in LOOKBACK:
                name = f'msm_tau_1_{tau_1}_tau_2_{tau_2}_lb_{lookback}'
                configs.append((name, MSMRetrainer,
                                {"tau_1": tau_1, "tau_2": tau_2, "lookback": lookback}))
    return configs


# ----- SINGLE EXPERIMENT RUNNER -----

def run_experiment(name, cls, kwargs, base_args, all_graphs):
    model_type      = base_args['model_type']
    experiment_type = cfg.get_experiment_type(name)
    output_dir      = f"results/experiments/{model_type}/{experiment_type}"
    output_file     = f"{output_dir}/{name}_results.csv"
    os.makedirs(output_dir, exist_ok=True)
    if os.path.exists(output_file):
        print(f'\nResults for {name} already exist at {output_file}. Loading.')
        return pd.read_csv(output_file)
    print(f'\nRunning: {name}')
    print(f'\tConfig: {kwargs}')

    retrainer = cls(**base_args, **kwargs)
    results = retrainer.run(all_graphs)

    results['retrainer']  = name
    results['model_type'] = base_args['model_type']

    # Use the shared stress classification (3 events, not 2)
    results['regime'] = pd.to_datetime(results['date_start']).apply(
        lambda d: 'stress' if cfg.in_stress_window(d) else 'calm'
    )
    results['stress_windows_days_at_run'] = cfg.STRESS_WINDOW_DAYS

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


def print_summary(combined):
    summary = (
        combined.groupby(['model_type', 'retrainer'])
        .agg(
            mean_f1=('f1', 'mean'),
            mean_directional_acc=('directional_acc', 'mean'),
            std_f1=('f1', 'std'),
            retrains=('retrain_triggered', 'sum'),
            windows=('f1', 'count'),
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

    # index convention. Without this, every training window is offset by 1.
    feature_cols_for_drop = [c for c in df.columns if c != 'Date']
    df = df.dropna(subset=feature_cols_for_drop).reset_index(drop=True)

    with open("data/causal_graphs.pkl", "rb") as f:
        all_graphs = pickle.load(f)

    # Exclude the primary target AND the secondary target from features.
    # The secondary target is only used in lead_lag_analysis.py.
    FEATURE_EXCLUSIONS = [ 
        'Date',
        'SPY_lr_local_std',  # secondary target for lead-lag analysis
        'SPY_logrv_5d',  # alternative secondary target with same horizon as primary, more noise
        'SPY_logrv_20d',  # alternative secondary target with longer horizon,
    ]
    feature_cols = [c for c in df.columns if c not in FEATURE_EXCLUSIONS]
    # check if removed
    assert cfg.TARGET_PRIMARY not in feature_cols, f"{cfg.TARGET_PRIMARY} should not be in features."
    assert cfg.TARGET_SECONDARY not in feature_cols, f"{cfg.TARGET_SECONDARY} should not be in features."
    print(f'Features (n={len(feature_cols)}): {feature_cols}')
    
    df[feature_cols] = df[feature_cols].shift(1)  # shift features by 1 to prevent lookahead bias
    
    print(f'Observations: {len(df)}')
    print(f'Features: {len(feature_cols)}')
    print(f'Graph windows: {len(all_graphs)}')

    # Graph-index resolution for SPY-focused MSM variants.
    # IMPORTANT: graph variable names must NOT include SPY_lr_local_std
    # (the secondary target didn't exist when causal graphs were built).
    # The original 11 variables in the causal graphs:
    graph_var_names = [
        c for c in df.columns
        if c not in ('Date', cfg.TARGET_SECONDARY)
    ]
    if cfg.TARGET_PRIMARY not in graph_var_names:
        raise ValueError(f"Target '{cfg.TARGET_PRIMARY}' not in columns.")
    target_idx_in_graph = graph_var_names.index(cfg.TARGET_PRIMARY)
    print(f"Target '{cfg.TARGET_PRIMARY}' is at graph index {target_idx_in_graph}")

    base_args_temp = dict(
        df=df,
        feature_cols=feature_cols,
        target=cfg.TARGET_PRIMARY,
        model_type='xgboost',
        n_bins=3,
        window=504,
        step=21,
        cooldown=3,
    )   
    configs = []

    if RUN_SENSITIVITY:
        configs += build_static_config()
        configs += build_drift_observer_config()
        configs += build_fixed_schedule_configs()
        configs += build_performance_configs()
        configs += build_adwin_configs()
        configs += build_random_configs()
        configs += build_msm_configs()
        configs += build_spy_msm_configs(target_idx_in_graph)
        configs += build_timeout_msm_configs()
        for tau1 in TAU_1_VALUES:
            for tau2 in TAU_2_VALUES:
                if tau2 >= tau1 or abs(tau1 - tau2) < 0.04:
                    continue
                for lookback in LOOKBACK:
                    name = f'causal_tau_1_{tau1}_tau_2_{tau2}_lb_{lookback}'
                    configs.append((
                        name,
                        CausalFeatureRetrainer,
                        {
                            'tau_1':           tau1,
                            'tau_2':           tau2,
                            'lookback':        lookback,
                            'target_name':     cfg.TARGET_PRIMARY,
                            'graph_var_names': graph_var_names,
                            'all_features':    feature_cols,
                        }
                    ))
    else:
        configs = [
            ('static', StaticRetrainer, {}),
            MSM_DEFAULT,
        ] 

    os.makedirs("results/experiments", exist_ok=True)
    for m in MODEL_TYPES:
        os.makedirs(f"results/experiments/{m}", exist_ok=True)
    all_results = []

    print('Starting experiments...')
    exp_num = 1
    total_exps = len(configs) * len(MODEL_TYPES)

    for model_type in MODEL_TYPES:
        base_args = {**base_args_temp, "model_type": model_type}
        for name, cls, kwargs in configs:
            print(f'\n=== Experiment {exp_num}/{total_exps}: {name} ({model_type}) ===')
            try:
                results = run_experiment(name, cls, kwargs, base_args, all_graphs)
                all_results.append(results)
                # Write partial results infrequently — each write is O(N) in
                # total rows, so repeated writes dominate runtime.
                if exp_num % 10 == 0:
                    pd.concat(all_results, ignore_index=True).to_csv(
                        "results/experiments/all_results_partial.csv", index=False
                    )
            except Exception as e:
                print(f'\nError in {name}: {e}')
                traceback.print_exc()
            exp_num += 1

    combined = pd.concat(all_results, ignore_index=True)
    combined.to_csv("results/experiments/all_results.csv", index=False)
    print('All experiments completed.')

    print_summary(combined)


if __name__ == "__main__":
    main()