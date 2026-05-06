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
    ADWINRevisedRetrainer,
    StrengthWeightedMSMRetrainer,
    FusedMSMRetrainer
)

np.random.seed(42)
random.seed(42)

# ----- RUN CONFIG -----
RUN_SENSITIVITY = True


# ----- SENSITIVITY CONFIGS -----
TAU_1_VALUES = [0.8, 0.83, 0.88]
TAU_2_VALUES = [0.72, 0.75, 0.78]
LOOKBACK     = [3, 4, 6]

# Thresholds are now in RMSE *rise* units (higher = worse). The target
# SPY_logrv_5d has std ≈ 0.8, so 0.05–0.20 is a reasonable span of triggers.
PERFORMANCE_THRESHOLDS = [0.05, 0.10, 0.15, 0.20, 0.30]
PERFORMANCE_LOOKBACK   = [3, 4, 6]

ADWIN_DELTAS    = [0.002, 0.005, 0.01, 0.05, 0.1]
FIXED_INTERVALS = [3, 6, 9, 12]

# SPYFocusedMSMRetrainer restricts the MSM signal to edges *into SPY_lr* only.
# This is the theoretically correct default: your thesis claims that breakdown
# of SPY_lr's causal structure signals an upcoming volatility regime shift.
MSM_DEFAULT_CLASS  = SPYFocusedMSMRetrainer
MSM_DEFAULT_KWARGS = {"tau_1": 0.83, "tau_2": 0.75, "lookback": 4}


MODEL_TYPES = ['lr', 'rf', 'xgboost']


# ----- CONFIG BUILDERS -----

def build_static_config():
    return [("static", StaticRetrainer, {})]


def build_drift_observer_config():
    '''
    DriftSignalObserver: frozen model, logs MSM at every window. Runs once per
    model. Output feeds lead_lag_analysis.py (RQ1 evidence).
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


def build_weighted_msm_configs():
    configs = []
    # Re-tune thresholds — strength-weighted MSM is smaller-valued than binary
    WEIGHTED_TAU_1 = [0.35, 0.45, 0.55]
    WEIGHTED_TAU_2 = [0.25, 0.35, 0.45]
    for t1 in WEIGHTED_TAU_1:
        for t2 in WEIGHTED_TAU_2:
            if t2 >= t1 or abs(t1 - t2) < 0.04:
                continue
            for lb in LOOKBACK:
                name = f'weighted_msm_tau_1_{t1}_tau_2_{t2}_lb_{lb}'
                configs.append((name, StrengthWeightedMSMRetrainer,
                                {'tau_1': t1, 'tau_2': t2, 'lookback': lb}))
    return configs


def build_fused_msm_configs(spy_idx, vix_idx):
    configs = []
    for t1 in TAU_1_VALUES:
        for t2 in TAU_2_VALUES:
            if t2 >= t1 or abs(t1 - t2) < 0.04:
                continue
            for lb in LOOKBACK:
                name = f'fused_msm_tau_1_{t1}_tau_2_{t2}_lb_{lb}'
                configs.append((
                    name, FusedMSMRetrainer,
                    {
                        'tau_1': t1, 'tau_2': t2, 'lookback': lb,
                        'spy_idx': spy_idx, 'vix_idx': vix_idx
                    }
                ))
    return configs

def build_revised_adwin_configs():
    return [
        (f'adwin_revised_delta_{delta}', ADWINRevisedRetrainer, {"delta": delta})
        for delta in ADWIN_DELTAS
    ]

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

    results['regime'] = pd.to_datetime(results['date_start']).apply(
        lambda d: 'stress' if cfg.in_stress_window(d) else 'calm'
    )
    results['stress_windows_days_at_run'] = cfg.STRESS_WINDOW_DAYS

    results.to_csv(output_file, index=False)

    print(f'\n\tmean rmse  : {results["rmse"].mean():.4f}')
    print(f'\tmean mae   : {results["mae"].mean():.4f}')
    print(f'\tmean r2    : {results["r2"].mean():.4f}')
    print(f'\tmean qlike  : {results["qlike"].mean():.4f}')
    print(f'\tretrains   : {results["retrain_triggered"].sum()}')
    print(f'\tsignals    : {results["signal_fired"].sum()}')
    print(f'\tcooldowns  : {results["cooldown_active"].sum()}')
    print(f'\tretrain rate: {results["retrain_triggered"].mean():.4%}')
    print(f'\twindows    : {len(results)}')
    print(f'saved results to: {output_file}')
    return results


def print_summary(combined):
    summary = (
        combined.groupby(['model_type', 'retrainer'])
        .agg(
            mean_rmse=('rmse', 'mean'),
            mean_mae=('mae', 'mean'),
            mean_r2=('r2', 'mean'),
            std_rmse=('rmse', 'std'),
            retrains=('retrain_triggered', 'sum'),
            windows=('rmse', 'count'),
            mean_qlike=('qlike', 'mean'),
        )
        .round(4)
        .sort_values('mean_rmse', ascending=True)
    )
    print('\n\nExperiment Summary:')
    print(summary.to_string())
    print(f'\nTotal strategies run: {len(summary)}')


def main():
    print('Loading data...')
    df = pd.read_csv("data/processed/standardized_data.csv", parse_dates=['Date'])
    print(f'  dataframe rows: {len(df)}')

    # DO NOT dropna here. The standardised CSV has NaN by design:
    #   - First ~505 rows on features (rolling 504-day standardisation needs
    #     504 days of history before producing a valid value).
    #   - Last 5 rows on SPY_logrv_5d (forward-looking 5-day realised variance).
    # The causal graph cache uses absolute row indices into this full-length
    # dataframe; its first valid window is around January 2019, after the
    # rolling standardisation has stabilised. The retrainer's _impute methods
    # handle NaN cleanup within each training window naturally, and the
    # `if test_end > len(self.df): continue` skip in BaseRetrainer.run()
    # handles the trailing-NaN edge case.

    with open("data/causal_graphs.pkl", "rb") as f:
        all_graphs = pickle.load(f)

    # Sanity check: dataframe must reach at least the last graph's train_end.
    # We do NOT require it to reach train_end + step, because the very last
    # graph window may legitimately have an incomplete test horizon and will
    # be skipped by the run() loop's test_end guard.
    last_graph_train_end = all_graphs[-1]['train_end_idx']
    if len(df) < last_graph_train_end:
        raise ValueError(
            f"Dataframe has {len(df)} rows but graph cache's last window "
            f"requires at least {last_graph_train_end}. "
            f"The graph cache may have been generated on a different "
            f"(longer) version of the standardised data. Regenerate the cache "
            f"or restore the matching CSV."
        )
    print(f"  graph cache last train_end_idx = {last_graph_train_end}, "
        f"dataframe rows = {len(df)} (OK)")

    # Target is SPY_logrv_5d (continuous). Every volatility-labelled column
    # stays excluded from features — they are all derivatives of the same
    # forward-looking series, so including any of them would leak.
    FEATURE_EXCLUSIONS = [
        'Date',
        'SPY_lr_local_std',        # secondary target for lead-lag analysis
        'SPY_logrv_5d',            # PRIMARY regression target
        'SPY_logrv_20d',           # longer-horizon variant
        'SPY_vol_change_5d',       # direction-of-change variant
        'SPY_vol_direction_5d',    # legacy classification label (still in CSV)
    ]
    feature_cols = [c for c in df.columns if c not in FEATURE_EXCLUSIONS]
    assert cfg.TARGET_PRIMARY not in feature_cols, f"{cfg.TARGET_PRIMARY} should not be in features."
    assert cfg.TARGET_SECONDARY not in feature_cols, f"{cfg.TARGET_SECONDARY} should not be in features."
    print(f'Features (n={len(feature_cols)}): {feature_cols}')

    df[feature_cols] = df[feature_cols].shift(1)  # shift features by 1 to prevent lookahead bias

    print(f'Observations: {len(df)}')
    print(f'Features: {len(feature_cols)}')
    print(f'Graph windows: {len(all_graphs)}')

    # Causal graphs were built on the original 11 variables including SPY_lr.
    # SPY_logrv_5d did not exist as a graph node, so MSM continues to monitor
    # SPY_lr's causal structure as the market-regime proxy — deliberately
    # decoupled from the forecast target.
    GRAPH_MONITOR_VAR = 'SPY_lr'

    ORIGINAL_GRAPH_VARS = [
        'SPY_lr', 'GLD_lr', 'UUP_lr', 'USO_lr', 'VIX_ld',
        'OVX', 'MOVE_d', 'T10Y2Y_d', 'BAA10Y_d', 'DGS10_d', 'USEPUINDXD_ld'
    ]
    missing = [v for v in ORIGINAL_GRAPH_VARS if v not in df.columns]
    if missing:
        raise ValueError(f"graph_var_names: expected variables missing from df: {missing}")
    graph_var_names = [v for v in ORIGINAL_GRAPH_VARS if v in df.columns]
    extra = [
        c for c in df.columns if c not in ('Date',) + tuple(ORIGINAL_GRAPH_VARS)
        and c not in (
            cfg.TARGET_SECONDARY,
            'SPY_logrv_5d',
            'SPY_logrv_20d',
            'SPY_vol_change_5d',
            'SPY_vol_direction_5d'
        )
    ]
    if extra:
        print(f"  WARNING: extra columns in df not in original graph vars (ignored): {extra}")
    if len(graph_var_names) != 11:
        raise ValueError(f"Expected 11 graph variables, got {len(graph_var_names)}: {graph_var_names}")

    target_idx_in_graph = graph_var_names.index(GRAPH_MONITOR_VAR)
    print(f"graph_var_names ({len(graph_var_names)}): {graph_var_names}")
    print(f"Causal graph monitoring: '{GRAPH_MONITOR_VAR}' at index {target_idx_in_graph}")
    print(f"Forecast target (regression): '{cfg.TARGET_PRIMARY}'")

    
    VIX_IDX = graph_var_names.index('VIX_ld')
    print(f"VIX_ld index in graph: {VIX_IDX}")
    
    base_args_temp = dict(
        df=df,
        feature_cols=feature_cols,
        target=cfg.TARGET_PRIMARY,
        model_type='xgboost',
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
        configs += build_weighted_msm_configs()
        configs += build_fused_msm_configs(target_idx_in_graph, VIX_IDX)
        configs += build_revised_adwin_configs()
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
                            'target_name':     GRAPH_MONITOR_VAR,
                            'graph_var_names': graph_var_names,
                            'all_features':    feature_cols,
                        }
                    ))
    else:
        MSM_DEFAULT = (
            "msm_default",
            SPYFocusedMSMRetrainer,
            {**MSM_DEFAULT_KWARGS, "target_idx": target_idx_in_graph}
        )
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
