import pandas as pd

# Stress events used across experiment, analysis, plotting
STRESS_EVENTS = {
    'covid_crash':        pd.Timestamp('2020-02-20'),
    'fed_hikes_2022':     pd.Timestamp('2022-03-16'),
    'carry_trade_unwind': pd.Timestamp('2024-08-05'),
}
STRESS_WINDOW_DAYS = 60

BALANCED_WINDOW_MIN_CLASSIFICATION_FRACTION = 0.1  # minimum fraction of each class in a window

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

# Binning configuration — decision from diagnostic_v2
# Fixed thresholds at ±0.3 standardised units give balanced classes
# and regime-sensitive semantics without hiding distributional shifts.
BINNING_SCHEME   = 'integer_labels'
FIXED_THRESHOLD  = 0.3  # in standardised units (standard deviations)

# Primary forecasting target
TARGET_PRIMARY = 'SPY_vol_direction_5d'  # 5-day forward-looking log realized volatility, the main target for RQ1 and RQ2
TARGET_SECONDARY = 'SPY_lr_local_std'  # local volatility regime indicator, the main target for RQ1 lead-lag analysis
TARGET_VOL_LEVEL = 'SPY_logrv_5d'  # the raw log-RV target, used for analysis and sanity checks but not for classification (too unbalanced)

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