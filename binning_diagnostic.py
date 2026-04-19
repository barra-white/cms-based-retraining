'''
binning_diagnostic_v2.py — Final binning and target decision diagnostic.

Respects PCMCI+ stationarity requirement: all tested targets are stationary
transformations of the price series. Standardisation is preserved throughout.

Tests:
  1. Stationarity validation for every candidate target
  2. Binning schemes (quantile, global, fixed-stdev, vol-scaled) on standardised returns
  3. Alternative stationary targets (volatility proxies)
  4. Per-scheme metrics: TVD, macro/weighted F1, stress-vs-calm F1, mutual information
  5. MSM correlation with each target at lags -5 to +5
  6. Granger causality — does MSM predictively lead each target?
  7. Final recommendation combining correlation and Granger results

Usage:
    python src/retraining/binning_diagnostic_v2.py

Outputs:
    results/validation/binning_v2_stationarity.csv
    results/validation/binning_v2_schemes.csv
    results/validation/binning_v2_msm_correlations.csv
    results/validation/binning_v2_granger.csv
    results/validation/binning_v2_recommendation.txt
    results/validation/binning_v2_plots.png
'''

import os
import pickle
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import f1_score
from sklearn.feature_selection import mutual_info_classif
from statsmodels.tsa.stattools import adfuller, kpss, grangercausalitytests

import warnings
warnings.filterwarnings('ignore')

# ---- Configuration ----
STRESS_EVENTS = {
    'covid_crash':        pd.Timestamp('2020-02-20'),
    'fed_hikes_2022':     pd.Timestamp('2022-03-16'),
    'carry_trade_unwind': pd.Timestamp('2024-08-05'),
}
STRESS_WINDOW_DAYS = 60

DATA_PATH   = 'data/processed/standardized_data.csv'
RAW_PATH    = 'data/processed/transformed_data.csv'
GRAPHS_PATH = 'data/causal_graphs.pkl'
OUT_DIR     = 'results/validation'
TARGET      = 'SPY_lr'
N_BINS      = 3
LOOKBACK    = 4

FIXED_STDEV_THRESHOLDS = [0.1, 0.2, 0.3, 0.5, 0.75]
VOL_SCALED_THRESHOLDS  = [0.25, 0.5, 0.75, 1.0]
VOL_WINDOW             = 21
GRANGER_MAX_LAG        = 3


# ---- Stationarity validation ----

def check_stationarity(series, name):
    '''Run ADF and KPSS. Stationary iff ADF rejects AND KPSS does not reject.'''
    clean = pd.Series(series).dropna().values
    if len(clean) < 50:
        return {'target': name, 'stationary': False, 'note': 'insufficient data'}

    try:
        adf_p = adfuller(clean, autolag='AIC')[1]
    except Exception:
        adf_p = np.nan
    try:
        kpss_p = kpss(clean, regression='c', nlags='auto')[1]
    except Exception:
        kpss_p = np.nan

    adf_stationary  = (adf_p < 0.05) if not np.isnan(adf_p) else False
    kpss_stationary = (kpss_p > 0.05) if not np.isnan(kpss_p) else False

    return {
        'target':          name,
        'adf_p':           round(adf_p, 4),
        'kpss_p':          round(kpss_p, 4),
        'adf_stationary':  adf_stationary,
        'kpss_stationary': kpss_stationary,
        'stationary':      adf_stationary and kpss_stationary,
    }


# ---- Candidate targets (all stationary) ----

def build_candidate_targets(df, raw_log_returns):
    targets = {}

    # 1. Current target
    spy_std = df[TARGET].values
    targets['spy_lr_std'] = (spy_std, 'Standardised SPY log returns (current target)')

    # 2. First-diff of absolute log returns (volatility change)
    abs_lr = np.abs(raw_log_returns)
    abs_lr_diff = pd.Series(abs_lr).diff().values
    targets['abs_lr_diff'] = (abs_lr_diff, 'First-diff of |log return| — stationary volatility change')

    # 3. First-diff of log squared returns
    sq_lr = raw_log_returns ** 2
    log_sq_lr = np.log(sq_lr + 1e-10)
    log_sq_diff = pd.Series(log_sq_lr).diff().values
    targets['log_sq_return_diff'] = (log_sq_diff, 'First-diff of log(squared return) — stationary vol proxy')

    # 4. Locally-standardised return (causal rolling z-score)
    vol_21  = pd.Series(raw_log_returns).rolling(21, min_periods=21).std().shift(1)
    mean_21 = pd.Series(raw_log_returns).rolling(21, min_periods=21).mean().shift(1)
    local_std = ((raw_log_returns - mean_21) / vol_21).values
    targets['spy_lr_local_std'] = (local_std, 'Locally-standardised return (causal rolling)')

    return targets


# ---- Binning schemes ----

def apply_static_bins(vals, edges):
    labels = np.full(len(vals), np.nan)
    valid = ~np.isnan(vals)
    labels[valid] = np.digitize(vals[valid], edges[1:-1])
    return labels


def bin_equal_frequency_per_window(target, all_graphs):
    labels = np.full(len(target), np.nan)
    for g in all_graphs:
        train = target[g['train_start_idx']:g['train_end_idx']]
        train = train[~np.isnan(train)]
        if len(train) < 30:
            continue
        sorted_train = np.sort(train)
        n = len(sorted_train)
        edges = np.array([
            -np.inf,
            (sorted_train[n // 3 - 1] + sorted_train[n // 3]) / 2,
            (sorted_train[2 * n // 3 - 1] + sorted_train[2 * n // 3]) / 2,
            np.inf,
        ])
        test_start = g['train_end_idx']
        test_end   = min(test_start + 21, len(target))
        labels[test_start:test_end] = apply_static_bins(target[test_start:test_end], edges)
    return labels


def bin_global_quantile(target, pre_test_end_idx):
    pre_test = target[:pre_test_end_idx]
    pre_test = pre_test[~np.isnan(pre_test)]
    edges = np.array([
        -np.inf,
        np.quantile(pre_test, 1/3),
        np.quantile(pre_test, 2/3),
        np.inf,
    ])
    return apply_static_bins(target, edges), edges


def bin_fixed_stdev(target, threshold):
    edges = np.array([-np.inf, -threshold, threshold, np.inf])
    return apply_static_bins(target, edges), edges


def bin_vol_scaled_on_standardised(target, scale, vol_window=VOL_WINDOW):
    vol = pd.Series(target).rolling(vol_window, min_periods=vol_window).std().shift(1).values
    labels = np.full(len(target), np.nan)
    for i, v in enumerate(target):
        if np.isnan(v) or np.isnan(vol[i]):
            continue
        low  = -scale * vol[i]
        high =  scale * vol[i]
        if v < low:
            labels[i] = 0
        elif v > high:
            labels[i] = 2
        else:
            labels[i] = 1
    return labels


# ---- Per-scheme metrics ----

def class_distribution(labels, dates, dates_in_stress_fn):
    df = pd.DataFrame({'date': dates, 'label': labels}).dropna()
    df['stress'] = df['date'].apply(dates_in_stress_fn)
    out = {}
    for regime_name, regime_df in [('stress', df[df['stress']]), ('calm', df[~df['stress']])]:
        for k, cls_name in [(0, 'down'), (1, 'neutral'), (2, 'up')]:
            out[f'{regime_name}_{cls_name}'] = (regime_df['label'] == k).mean()
        out[f'n_{regime_name}'] = len(regime_df)
    out['tvd'] = 0.5 * sum(
        abs(out[f'stress_{k}'] - out[f'calm_{k}']) for k in ['down', 'neutral', 'up']
    )
    return out


def predictability(features, labels, dates, dates_in_stress_fn):
    valid = ~np.isnan(labels)
    X, y  = features[valid], labels[valid].astype(int)
    d     = dates[valid]

    if len(np.unique(y)) < 2:
        return {'macro_f1': np.nan, 'weighted_f1': np.nan,
                'stress_f1': np.nan, 'calm_f1': np.nan}

    tscv = TimeSeriesSplit(n_splits=5)
    preds_all, y_all, dates_all = np.array([]), np.array([]), []
    for tr, te in tscv.split(X):
        clf = LogisticRegression(max_iter=1000, random_state=42)
        clf.fit(X[tr], y[tr])
        preds_all = np.concatenate([preds_all, clf.predict(X[te])])
        y_all     = np.concatenate([y_all, y[te]])
        dates_all.extend(d.iloc[te].tolist())

    stress_mask = np.array([dates_in_stress_fn(dt) for dt in dates_all])

    result = {
        'macro_f1':    f1_score(y_all, preds_all, average='macro',    zero_division=0),
        'weighted_f1': f1_score(y_all, preds_all, average='weighted', zero_division=0),
    }
    if stress_mask.sum() > 5:
        result['stress_f1'] = f1_score(
            y_all[stress_mask], preds_all[stress_mask], average='macro', zero_division=0
        )
    else:
        result['stress_f1'] = np.nan
    if (~stress_mask).sum() > 5:
        result['calm_f1'] = f1_score(
            y_all[~stress_mask], preds_all[~stress_mask], average='macro', zero_division=0
        )
    else:
        result['calm_f1'] = np.nan
    return result


def mutual_information(features, labels):
    valid = ~np.isnan(labels)
    X, y  = features[valid], labels[valid].astype(int)
    if len(np.unique(y)) < 2:
        return np.nan
    try:
        mi = mutual_info_classif(X, y, random_state=42)
        return float(np.mean(mi))
    except Exception:
        return np.nan


# ---- MSM signal and cross-correlation ----

def compute_msm_series(all_graphs, lookback):
    '''Per-window MSM. 0.0 for empty edge set.'''
    msm = []
    for w in range(len(all_graphs)):
        if w < lookback - 1:
            msm.append(np.nan)
            continue
        recent = all_graphs[max(0, w - lookback + 1):w + 1]
        all_edges = set()
        for g in recent:
            all_edges |= g['edges']
        if not all_edges:
            msm.append(0.0)
            continue
        scores = [sum(1 for g in recent if e in g['edges']) / len(recent)
                  for e in all_edges]
        msm.append(float(np.mean(scores)))
    return np.array(msm)


def per_window_target_signal(target_values, all_graphs, labels=None):
    '''Per-window scalar summary of target: realised std, or KL of class dist.'''
    signals = []
    for g in all_graphs:
        test_start = g['train_end_idx']
        test_end   = min(test_start + 21, len(target_values))
        if labels is None:
            vals = target_values[test_start:test_end]
            signals.append(np.nanstd(vals))
        else:
            train_labels = labels[g['train_start_idx']:g['train_end_idx']]
            test_labels  = labels[test_start:test_end]
            train_labels = train_labels[~np.isnan(train_labels)]
            test_labels  = test_labels[~np.isnan(test_labels)]
            if len(train_labels) == 0 or len(test_labels) == 0:
                signals.append(np.nan)
                continue
            tr_dist = np.array([np.mean(train_labels == k) for k in range(3)]) + 1e-10
            te_dist = np.array([np.mean(test_labels  == k) for k in range(3)]) + 1e-10
            kl = np.sum(te_dist * np.log(te_dist / tr_dist))
            signals.append(kl)
    return np.array(signals)


def cross_correlation(a, b, max_lag=5):
    '''
    Cross-correlation at lags -max_lag to +max_lag.
    With a=msm, b=target: positive lag = MSM LEADS target.
    '''
    valid = ~(np.isnan(a) | np.isnan(b))
    a, b  = a[valid], b[valid]
    if len(a) < max_lag * 3:
        return {}
    corrs = {}
    for lag in range(-max_lag, max_lag + 1):
        if lag > 0:
            x, y = a[:-lag], b[lag:]
        elif lag < 0:
            x, y = a[-lag:], b[:lag]
        else:
            x, y = a, b
        if len(x) > 5:
            corrs[lag] = float(np.corrcoef(x, y)[0, 1])
    return corrs


# ---- Granger causality: does MSM predictively lead the target? ----

def granger_test(msm, target_series, max_lag=GRANGER_MAX_LAG):
    '''
    Does MSM Granger-cause the target? Tests at lags 1..max_lag.
    
    statsmodels convention: grangercausalitytests(data, maxlag) tests whether
    column 2 of `data` Granger-causes column 1. We want "MSM causes target",
    so data = [target, msm].
    
    Returns minimum p-value across tested lags. Low p = MSM predicts target.
    '''
    valid = ~(np.isnan(msm) | np.isnan(target_series))
    if valid.sum() < 20:
        return {'min_p': np.nan, 'note': 'insufficient data'}

    data = np.column_stack([target_series[valid], msm[valid]])

    try:
        results = grangercausalitytests(data, maxlag=max_lag, verbose=False)
        p_values = {lag: results[lag][0]['ssr_ftest'][1] for lag in range(1, max_lag + 1)}
        min_p    = min(p_values.values())
        best_lag = min(p_values, key=p_values.get)
        return {
            'min_p':       round(min_p, 4),
            'best_lag':    best_lag,
            'p_by_lag':    {k: round(v, 4) for k, v in p_values.items()},
            'significant': min_p < 0.05,
        }
    except Exception as e:
        return {'min_p': np.nan, 'note': str(e)}


# ---- Main ----

def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    print('Loading data...')
    df = pd.read_csv(DATA_PATH, parse_dates=['Date'])
    feature_cols = [c for c in df.columns if c != 'Date']
    df = df.dropna(subset=feature_cols).reset_index(drop=True)

    # Load raw (pre-standardisation) log returns for volatility proxies.
    try:
        raw_df = pd.read_csv(RAW_PATH, parse_dates=['Date'])
        raw_df = raw_df.dropna(subset=[TARGET]).reset_index(drop=True)
        merged = df[['Date']].merge(raw_df[['Date', TARGET]], on='Date', how='left',
                                     suffixes=('', '_raw'))
        raw_col = f'{TARGET}_raw' if f'{TARGET}_raw' in merged.columns else TARGET
        raw_log_returns = merged[raw_col].values
        print(f'  Loaded raw log returns from {RAW_PATH}')
    except Exception as e:
        print(f'  Could not load raw returns from {RAW_PATH}: {e}')
        print(f'  Falling back to standardised target for volatility proxies.')
        raw_log_returns = df[TARGET].values

    with open(GRAPHS_PATH, 'rb') as f:
        all_graphs = pickle.load(f)
    print(f'  {len(df)} observations, {len(all_graphs)} causal graphs')

    dates = df['Date']
    pre_test_end_idx = all_graphs[0]['train_end_idx']
    features = df[[c for c in feature_cols if c != TARGET]].fillna(0).values

    def _in_stress(d):
        return any(
            (ev - pd.Timedelta(days=STRESS_WINDOW_DAYS)) <= d <= (ev + pd.Timedelta(days=STRESS_WINDOW_DAYS))
            for ev in STRESS_EVENTS.values()
        )

    # ---- Step 1: stationarity ----

    print('\nStep 1: Stationarity validation of candidate targets')
    targets_with_desc = build_candidate_targets(df, raw_log_returns)

    stationarity_results = []
    for name, (vals, desc) in targets_with_desc.items():
        r = check_stationarity(vals, name)
        r['description'] = desc
        stationarity_results.append(r)
        flag = 'PASS' if r['stationary'] else 'FAIL'
        print(f'  {name:<25s} [{flag}]  ADF p={r["adf_p"]} KPSS p={r["kpss_p"]}')

    pd.DataFrame(stationarity_results).to_csv(f'{OUT_DIR}/binning_v2_stationarity.csv', index=False)

    valid_targets = {
        name: vals for name, (vals, _) in targets_with_desc.items()
        if next(r for r in stationarity_results if r['target'] == name)['stationary']
    }
    print(f'  → {len(valid_targets)} targets passed stationarity')

    # ---- Step 2: build binning schemes ----

    print('\nStep 2: Build binning schemes for spy_lr_std')
    target_std = df[TARGET].values

    schemes = {}
    schemes['A_equal_freq'] = bin_equal_frequency_per_window(target_std, all_graphs)

    labels_d, edges_d = bin_global_quantile(target_std, pre_test_end_idx)
    schemes['D_global_quantile'] = labels_d
    print(f'  D_global_quantile edges: [{edges_d[1]:.3f}, {edges_d[2]:.3f}] (standardised units)')

    for thr in FIXED_STDEV_THRESHOLDS:
        labels, edges = bin_fixed_stdev(target_std, thr)
        schemes[f'B_fixed_stdev_{thr}'] = labels

    for sc in VOL_SCALED_THRESHOLDS:
        schemes[f'C_vol_{sc}'] = bin_vol_scaled_on_standardised(target_std, sc)

    # ---- Step 3: per-scheme metrics ----

    print('\nStep 3: Per-scheme metrics on direction target')
    scheme_records = []
    for name, labels in schemes.items():
        dist = class_distribution(labels, dates, _in_stress)
        pred = predictability(features, labels, dates, _in_stress)
        mi   = mutual_information(features, labels)

        rec = {'scheme': name, 'mi': mi, **dist, **pred}
        rec['stress_calm_f1_delta'] = (pred['stress_f1'] or 0) - (pred['calm_f1'] or 0)
        scheme_records.append(rec)
        print(f'  {name:<25s} TVD={dist["tvd"]:.3f}  '
              f'macro_f1={pred["macro_f1"]:.3f}  wtd_f1={pred["weighted_f1"]:.3f}  '
              f'MI={mi:.3f}')

    pd.DataFrame(scheme_records).to_csv(f'{OUT_DIR}/binning_v2_schemes.csv', index=False)

    # ---- Step 4: MSM vs target correlation ----

    print('\nStep 4: MSM vs target-signal correlation')
    msm = compute_msm_series(all_graphs, LOOKBACK)

    corr_records = []

    for name, vals in valid_targets.items():
        if name == 'spy_lr_std':
            continue
        sig = per_window_target_signal(vals, all_graphs)
        lags = cross_correlation(msm, sig)
        best_lag = max(lags, key=lambda k: abs(lags[k])) if lags else None
        corr_records.append({
            'target_or_scheme': name,
            'type':             'continuous_target',
            'corr_lag_0':       lags.get(0, np.nan),
            'best_lag':         best_lag,
            'best_lag_corr':    lags.get(best_lag, np.nan) if best_lag is not None else np.nan,
            'lags_all':         str({k: round(v, 3) for k, v in lags.items()}),
        })

    for name, labels in schemes.items():
        sig = per_window_target_signal(target_std, all_graphs, labels=labels)
        lags = cross_correlation(msm, sig)
        best_lag = max(lags, key=lambda k: abs(lags[k])) if lags else None
        corr_records.append({
            'target_or_scheme': name,
            'type':             'binning_scheme_kl',
            'corr_lag_0':       lags.get(0, np.nan),
            'best_lag':         best_lag,
            'best_lag_corr':    lags.get(best_lag, np.nan) if best_lag is not None else np.nan,
            'lags_all':         str({k: round(v, 3) for k, v in lags.items()}),
        })

    corr_df = pd.DataFrame(corr_records).sort_values('best_lag_corr', key=abs, ascending=False)
    corr_df.to_csv(f'{OUT_DIR}/binning_v2_msm_correlations.csv', index=False)

    print('  Top MSM-target correlations:')
    for _, r in corr_df.head(8).iterrows():
        print(f'    {r["target_or_scheme"]:<30s} '
              f'corr@best_lag({r["best_lag"]}) = {r["best_lag_corr"]:.3f}')

    # ---- Step 4b: Granger causality ----

    print('\nStep 4b: Granger causality — does MSM predictively lead each target?')
    granger_records = []

    for name, vals in valid_targets.items():
        if name == 'spy_lr_std':
            continue
        sig = per_window_target_signal(vals, all_graphs)
        result = granger_test(msm, sig)
        result['target'] = name
        result['type']   = 'continuous_target'
        granger_records.append(result)
        flag = 'SIG' if result.get('significant') else 'ns '
        print(f'  {name:<25s} [{flag}]  min_p={result.get("min_p")}  '
              f'best_lag={result.get("best_lag")}')

    for name, labels in schemes.items():
        sig = per_window_target_signal(target_std, all_graphs, labels=labels)
        result = granger_test(msm, sig)
        result['target'] = name
        result['type']   = 'binning_scheme_kl'
        granger_records.append(result)
        flag = 'SIG' if result.get('significant') else 'ns '
        print(f'  {name:<25s} [{flag}]  min_p={result.get("min_p")}  '
              f'best_lag={result.get("best_lag")}')

    granger_df = pd.DataFrame(granger_records)
    granger_df.to_csv(f'{OUT_DIR}/binning_v2_granger.csv', index=False)

    # ---- Step 5: Final recommendation ----

    print('\n' + '=' * 80)
    print('  RECOMMENDATION')
    print('=' * 80)

    lines = ['BINNING + TARGET DIAGNOSTIC v2 REPORT', '=' * 80, '']

    # Correlation summary
    best_cont = corr_df[corr_df['type'] == 'continuous_target']
    best_bin  = corr_df[corr_df['type'] == 'binning_scheme_kl']

    if not best_cont.empty:
        top_cont = best_cont.iloc[0]
        lines.append(f'Strongest continuous-target MSM correlation: '
                     f'{top_cont["target_or_scheme"]} at lag {top_cont["best_lag"]} '
                     f'with r = {top_cont["best_lag_corr"]:.3f}')
    if not best_bin.empty:
        top_bin = best_bin.iloc[0]
        lines.append(f'Strongest binning-scheme KL correlation: '
                     f'{top_bin["target_or_scheme"]} at lag {top_bin["best_lag"]} '
                     f'with r = {top_bin["best_lag_corr"]:.3f}')
    lines.append('')

    # Granger-informed decision
    lines.append('GRANGER CAUSALITY TEST RESULTS:')
    lines.append('  (p < 0.05 means MSM contains predictive information about the target')
    lines.append('   beyond what is in the target\'s own past.)')
    lines.append('')
    for r in sorted(granger_records, key=lambda x: x.get('min_p', 1.0) or 1.0):
        flag = 'SIG' if r.get('significant') else 'ns '
        lines.append(f'  [{flag}]  {r["target"]:<25s}  min_p = {r.get("min_p")}  '
                     f'best_lag = {r.get("best_lag")}')
    lines.append('')

    # Identify valid RQ1 secondary targets: Granger-significant AND positive-lead correlation
    sig_targets = granger_df[granger_df.get('significant', False) == True]
    valid_choices = []
    for _, row in sig_targets.iterrows():
        tname = row['target']
        crow = corr_df[corr_df['target_or_scheme'] == tname]
        if crow.empty:
            continue
        best_lag = crow.iloc[0]['best_lag']
        if best_lag is not None and best_lag > 0:
            valid_choices.append({
                'target':    tname,
                'granger_p': row['min_p'],
                'corr':      crow.iloc[0]['best_lag_corr'],
                'lag':       best_lag,
            })

    lines.append('DECISION FOR RQ1 (does MSM precede degradation?):')
    lines.append('')
    if valid_choices:
        best = sorted(valid_choices, key=lambda x: x['granger_p'])[0]
        lines.append(f'→ SECONDARY TARGET for lead-lag analysis: {best["target"]}')
        lines.append(f'  Granger p={best["granger_p"]}, correlation={best["corr"]:.3f} at lag {best["lag"]}')
        lines.append(f'  MSM predictively leads this target. RQ1 is defensible.')
    elif not sig_targets.empty:
        any_sig = sig_targets.iloc[0]
        lines.append(f'→ Granger significance exists ({any_sig["target"]}, p={any_sig["min_p"]})')
        lines.append(f'  but NO target combines significance with positive lead lag.')
        lines.append(f'  RQ1 must be reframed: MSM is a COINCIDENT or LAGGING indicator,')
        lines.append(f'  not a leading one. Use it as confirmation signal.')
    else:
        lines.append('→ No target shows Granger significance.')
        lines.append('  MSM correlates with targets but does not predict them.')
        lines.append('  REFRAME thesis: MSM as interpretable retraining trigger,')
        lines.append('  not a predictive leading indicator.')

    lines.append('')
    lines.append('DECISION FOR PRIMARY TASK (classification binning):')
    lines.append('')
    # Pick binning scheme with best balance of macro and weighted F1, avoiding extreme imbalance
    sdf = pd.DataFrame(scheme_records)
    # Score: macro F1, penalised by |macro - wtd| gap to reject extreme imbalance
    sdf['imbalance_penalty'] = (sdf['weighted_f1'] - sdf['macro_f1']).abs()
    sdf['score']             = sdf['macro_f1'] - 0.5 * sdf['imbalance_penalty']
    sdf_sorted = sdf.sort_values('score', ascending=False)
    top_scheme = sdf_sorted.iloc[0]
    lines.append(f'→ PRIMARY BINNING SCHEME: {top_scheme["scheme"]}')
    lines.append(f'  macro F1 = {top_scheme["macro_f1"]:.3f}, weighted F1 = {top_scheme["weighted_f1"]:.3f}')
    lines.append(f'  TVD = {top_scheme["tvd"]:.3f}, imbalance penalty = {top_scheme["imbalance_penalty"]:.3f}')
    lines.append('')

    lines.append('STATIONARITY STATUS (for PCMCI+ compatibility):')
    for r in stationarity_results:
        flag = '✓' if r['stationary'] else '✗'
        lines.append(f'  {flag} {r["target"]:<25s} — {r["description"]}')
    lines.append('')

    lines.append('BINNING SCHEME COMPARISON:')
    lines.append(f'  {"Scheme":<25s} {"TVD":>8s} {"MacroF1":>8s} {"WtdF1":>8s} '
                 f'{"MI":>6s} {"Δ(S-C)":>8s} {"Score":>7s}')
    for _, r in sdf_sorted.iterrows():
        lines.append(f'  {r["scheme"]:<25s} '
                     f'{r["tvd"]:>8.3f} '
                     f'{r["macro_f1"]:>8.3f} '
                     f'{r["weighted_f1"]:>8.3f} '
                     f'{r["mi"]:>6.3f} '
                     f'{r["stress_calm_f1_delta"]:>+8.3f} '
                     f'{r["score"]:>7.3f}')
    lines.append('')
    lines.append('Score = macro_f1 - 0.5 * |macro_f1 - weighted_f1|')
    lines.append('        (penalises extreme class imbalance)')
    lines.append('Δ(S-C): positive values suggest class-balance artefact')

    report = '\n'.join(lines)
    print(report)

    with open(f'{OUT_DIR}/binning_v2_recommendation.txt', 'w') as f:
        f.write(report)
    print(f'\nSaved: {OUT_DIR}/binning_v2_recommendation.txt')

    # ---- Plots ----

    fig, axes = plt.subplots(2, 2, figsize=(14, 10))

    # Plot 1: MSM correlation with each candidate
    ax = axes[0, 0]
    corr_plot = corr_df.dropna(subset=['best_lag_corr']).copy()
    corr_plot['abs_corr'] = corr_plot['best_lag_corr'].abs()
    corr_plot = corr_plot.sort_values('abs_corr').tail(15)
    colors = ['steelblue' if t == 'continuous_target' else 'coral' for t in corr_plot['type']]
    ax.barh(corr_plot['target_or_scheme'], corr_plot['best_lag_corr'], color=colors)
    ax.axvline(0, color='black', lw=0.8)
    ax.set_xlabel('MSM correlation at best lag')
    ax.set_title('MSM vs target/scheme (blue = continuous, coral = binning KL)')
    ax.grid(alpha=0.3, axis='x')

    # Plot 2: Granger min-p per target
    ax = axes[0, 1]
    gdf = granger_df.dropna(subset=['min_p']).copy()
    gdf = gdf.sort_values('min_p', ascending=True).head(15)
    colors = ['green' if p < 0.05 else 'grey' for p in gdf['min_p']]
    ax.barh(gdf['target'], -np.log10(gdf['min_p'].clip(lower=1e-6)), color=colors)
    ax.axvline(-np.log10(0.05), color='red', ls='--', lw=0.8, label='p = 0.05')
    ax.set_xlabel('-log10(min p-value)  —  higher = stronger Granger causation')
    ax.set_title('Granger causality: does MSM lead each target?')
    ax.legend(fontsize=8)
    ax.grid(alpha=0.3, axis='x')

    # Plot 3: macro vs weighted F1 across schemes
    ax = axes[1, 0]
    x = np.arange(len(sdf_sorted))
    width = 0.35
    ax.bar(x - width/2, sdf_sorted['macro_f1'],    width, label='Macro F1',    alpha=0.8)
    ax.bar(x + width/2, sdf_sorted['weighted_f1'], width, label='Weighted F1', alpha=0.8)
    ax.set_xticks(x)
    ax.set_xticklabels(sdf_sorted['scheme'], rotation=45, ha='right', fontsize=7)
    ax.set_ylabel('F1')
    ax.set_title('Macro vs weighted F1 (large gap = class imbalance)')
    ax.legend()
    ax.grid(alpha=0.3, axis='y')

    # Plot 4: MSM time series with stress events
    ax = axes[1, 1]
    window_dates = [pd.Timestamp(g['date_end']) for g in all_graphs]
    ax.plot(window_dates, msm, color='darkblue', lw=1.2)
    for ev in STRESS_EVENTS.values():
        ax.axvline(ev, color='orange', ls='--', alpha=0.6)
    ax.set_xlabel('Date')
    ax.set_ylabel('MSM')
    ax.set_title(f'MSM time series (lookback={LOOKBACK}, orange = stress events)')
    ax.grid(alpha=0.3)

    plt.tight_layout()
    fig.savefig(f'{OUT_DIR}/binning_v2_plots.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f'Saved: {OUT_DIR}/binning_v2_plots.png')


if __name__ == '__main__':
    main()