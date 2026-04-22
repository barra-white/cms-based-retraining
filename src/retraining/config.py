import pandas as pd

# Stress events used across experiment, analysis, plotting
STRESS_EVENTS = {
    'covid_crash':        pd.Timestamp('2020-02-20'),
    'fed_hikes_2022':     pd.Timestamp('2022-03-16'),
    'carry_trade_unwind': pd.Timestamp('2024-08-05'),
}
STRESS_WINDOW_DAYS = 60


TRANSITION_WINDOW_DAYS = 10  # ±10 days around event = regime transition zone

def in_transition_window(date, events=None, window_days=None):
    events = events or STRESS_EVENTS
    window_days = window_days or TRANSITION_WINDOW_DAYS
    return any(
        (ev - pd.Timedelta(days=window_days)) <= date <= (ev + pd.Timedelta(days=window_days))
        for ev in events.values()
    )

# Experiment type classification
EXP_TYPE_PREFIXES = [
    'spy_msm', 'timeout_msm', 'msm', 'causal',
    'fixed', 'perf', 'adwin', 'random', 'static', 'drift_observer'
]

# IMPORTANT: perf and adwin are now treated as baselines, not as "signal-driven"
# competitors. Your thesis compares MSM against all existing retrainers.
MSM_TYPES      = {'msm', 'spy_msm', 'timeout_msm', 'causal'}
BASELINE_TYPES = {'static', 'random', 'fixed', 'perf', 'adwin'}
SIGNAL_DRIVEN  = {'msm', 'spy_msm', 'timeout_msm', 'causal', 'adwin', 'perf'}

# Primary forecasting target (REGRESSION)
# 5-day forward log realised variance. Continuous float. The standard
# volatility-forecasting target.
TARGET_PRIMARY = 'SPY_logrv_5d'

# Secondary target for lead-lag analysis (regime-normalised returns).
TARGET_SECONDARY = 'SPY_lr_local_std'

# Retained for diagnostic scripts that still want the raw level.
TARGET_VOL_LEVEL = 'SPY_logrv_5d'

FORECAST_HORIZON = 5

# Helper functions
def get_experiment_type(name):
    for prefix in EXP_TYPE_PREFIXES:
        if name.startswith(prefix):
            return prefix
    return 'other'

def in_stress_window(date, events=None, window_days=None):
    events = events or STRESS_EVENTS
    window_days = window_days or STRESS_WINDOW_DAYS
    return any(
        (ev - pd.Timedelta(days=window_days)) <= date <= (ev + pd.Timedelta(days=window_days))
        for ev in events.values()
    )
