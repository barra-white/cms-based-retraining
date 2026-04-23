import json
import os
import sys
import random
import numpy as np
import pandas as pd

from xgboost import XGBRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import Ridge
from sklearn.model_selection import GridSearchCV, TimeSeriesSplit
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from river.drift import ADWIN
from abc import ABC, abstractmethod

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config as cfg

np.random.seed(42)
random.seed(42)

# ---- Model configurations (regression) ----
MODEL_CONFIGS = {
    'xgboost': {
        'class': XGBRegressor,
        'fixed_params': {
            'objective':    'reg:squarederror',
            'random_state': 42,
            'verbosity':    0,
        },
        'grid': {
            'n_estimators':     [100, 200, 300, 500],
            'max_depth':        [3, 5, 7],
            'learning_rate':    [0.01, 0.05, 0.1],
            'subsample':        [0.7, 0.9],
            'colsample_bytree': [0.7, 0.9],
            'min_child_weight': [1, 5],
            'reg_alpha':        [0, 0.1, 1.0],
            'reg_lambda':       [1.0, 5.0],
        },
        'warm_grid_fn': lambda bp: {
            'n_estimators':     sorted({int(max(50, bp['n_estimators'] * f)) for f in [0.75, 1.0, 1.25]}),
            'max_depth':        sorted({max(2, min(9, bp['max_depth'] + d)) for d in [-1, 0, 1]}),
            'learning_rate':    sorted({round(bp['learning_rate'] * f, 4) for f in [0.5, 1.0, 2.0]}),
            'subsample':        sorted({round(max(0.5, min(1.0, bp.get('subsample', 0.8) + d)), 1) for d in [-0.2, -0.1, 0, 0.1, 0.2]}),
            'colsample_bytree': sorted({round(max(0.5, min(1.0, bp.get('colsample_bytree', 0.8) + d)), 1) for d in [-0.1, 0, 0.1]}),
            'min_child_weight': [bp.get('min_child_weight', 1)],
            'reg_alpha':        [bp.get('reg_alpha', 0)],
            'reg_lambda':       [bp.get('reg_lambda', 1.0)],
        },
    },
    'rf': {
        'class': RandomForestRegressor,
        'fixed_params': {'random_state': 42, 'n_jobs': -1},
        'grid': {
            'n_estimators':     [100, 200, 300, 500],
            'max_depth':        [3, 5, 7, 10, 15, None],
            'min_samples_split':[2, 5, 10, 20],
            'min_samples_leaf': [1, 2, 4, 8],
            'max_features':     ['sqrt', 'log2'],
        },
        'warm_grid_fn': lambda bp: {
            'n_estimators':      sorted({max(50, bp['n_estimators'] + d) for d in [-100, 0, 100]}),
            'max_depth':         [bp.get('max_depth', 5)],
            'min_samples_split': [bp.get('min_samples_split', 2)],
            'min_samples_leaf':  [bp.get('min_samples_leaf', 1)],
            'max_features':      [bp.get('max_features', 'sqrt')],
        },
    },
    'lr': {
        # Ridge replaces the previous LogisticRegression slot. alpha = 1/C under
        # L2 — a trimmed grid is plenty for a linear baseline on ~11 features.
        'class': Ridge,
        'fixed_params': {'random_state': 42, 'max_iter': 5000},
        'grid': {
            'alpha':  [0.01, 0.1, 1.0, 10.0, 100.0, 1000.0, 10000.0],
            'solver': ['auto'],
        },
        'warm_grid_fn': lambda bp: {
            'alpha':  sorted({bp['alpha'] * f for f in [0.1, 1.0, 10.0]}),
            'solver': [bp.get('solver', 'auto')],
        },
    },
}


# ---- JSON helpers (unchanged) ----
class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):  return int(obj)
        if isinstance(obj, np.floating): return float(obj)
        if isinstance(obj, np.ndarray):  return obj.tolist()
        return super().default(obj)


def _to_json(obj) -> str:
    return json.dumps(obj, cls=NumpyEncoder)


# ============================================================================
#   BASE RETRAINER  (REGRESSION)
# ============================================================================

class BaseRetrainer(ABC):
    def __init__(self, df, feature_cols, target, model_type='xgboost',
                 window=504, step=21, cooldown=3, horizon=None):
        if model_type not in MODEL_CONFIGS:
            raise ValueError(f"Unsupported model_type '{model_type}'")
        self.df           = df
        self.feature_cols = feature_cols
        self.target       = target
        self.window       = window
        self.step         = step
        self.model_type   = model_type
        self.best_params  = None
        self.cooldown     = cooldown
        self.last_retrain_window = -999
        self.results      = []
        self.horizon      = horizon if horizon is not None else getattr(cfg, 'FORECAST_HORIZON', 5)

    # ---- Model ----

    def build_model(self, tuned_params=None):
        config = MODEL_CONFIGS[self.model_type]
        all_params = config['fixed_params'].copy()
        if tuned_params:
            all_params.update(tuned_params)
        elif self.best_params:
            all_params.update(self.best_params)
        return config['class'](**all_params)

    def _impute(self, X):
        '''Forward-fill NaN. Raise if column is entirely NaN — silent fillna(0)
        corrupts the training signal when it fires on bugs.'''
        X_df = pd.DataFrame(X).ffill()
        if X_df.isna().any().any():
            col_means = X_df.mean(axis=0)
            if col_means.isna().any():
                bad = col_means[col_means.isna()].index.tolist()
                raise ValueError(f"_impute: columns entirely NaN: {bad}")
            X_df = X_df.fillna(col_means)
        return X_df.values

    def _impute_target(self, y):
        '''Forward-fill target NaN. Raise if target is entirely NaN.'''
        y_ser = pd.Series(y).ffill()
        if y_ser.isna().any():
            mean_val = y_ser.mean()
            if pd.isna(mean_val):
                raise ValueError("_impute_target: target entirely NaN in this window")
            y_ser = y_ser.fillna(mean_val)
        return y_ser.astype(float).values

    def _full_grid(self):
        return MODEL_CONFIGS[self.model_type]['grid']

    def _warm_grid(self):
        return MODEL_CONFIGS[self.model_type]['warm_grid_fn'](self.best_params)

    def run_grid_search(self, X_train, y_train, warm=False):
        config = MODEL_CONFIGS[self.model_type]
        base_model = config['class'](**config['fixed_params'])
        tscv = TimeSeriesSplit(n_splits=3)
        param_grid = self._warm_grid() if (warm and self.best_params is not None) else self._full_grid()
        searcher = GridSearchCV(
            estimator=base_model, param_grid=param_grid, cv=tscv,
            scoring='neg_root_mean_squared_error', n_jobs=-1, refit=True, verbose=0,
            error_score=np.nan,
        )
        searcher.fit(X_train, y_train)
        self.best_params = searcher.best_params_
        return searcher.best_estimator_

    def evaluate(self, model, X_test, y_test):
        pred = model.predict(X_test).astype(float)
        y_test = np.asarray(y_test, dtype=float)
        rmse = float(np.sqrt(mean_squared_error(y_test, pred)))
        mae  = float(mean_absolute_error(y_test, pred))
        # R² is undefined for zero-variance windows.
        r2 = float(r2_score(y_test, pred)) if np.var(y_test) > 0 else np.nan

        # QLIKE (Patton 2011). Numerically stable log-space form:
        #   QLIKE = exp(delta) - delta - 1, where delta = log(rv_true) - log(rv_pred)
        # Always ≥ 0; equals 0 iff prediction is exact. Asymmetric: underprediction
        # of variance (delta > 0) is penalised more than overprediction.
        delta = y_test - pred  # log(rv_true) - log(rv_pred) since both in log space
        qlike_per_obs = np.exp(delta) - delta - 1
        qlike = float(np.mean(qlike_per_obs))

        # Sanity assertion — remove after testing if you want speed
        assert qlike >= -1e-10, f"QLIKE must be non-negative, got {qlike}"

        return rmse, mae, r2, qlike, pred, y_test

    def in_cooldown(self, w):
        return (w - self.last_retrain_window) < self.cooldown

    @abstractmethod
    def should_retrain(self, w, **kwargs):
        pass

    # ---- Template Hooks ----
    def _reset_run_state(self):
        pass

    def _get_feature_cols_for_window(self, g):
        return self.feature_cols

    def _compute_window_context(self, w, g, all_graphs):
        return {}

    def _extra_result_fields(self, w, g, triggered, context):
        return {}

    def _post_retrain_hook(self):
        pass

    # ---- Main loop ----

    def run(self, all_graphs):
        self.results = []
        self.last_retrain_window = -999
        self.best_params = None
        self._reset_run_state()
        model = None
        model_feature_cols = self.feature_cols

        for w, g in enumerate(all_graphs):
            train_start = g['train_start_idx']
            train_end   = g['train_end_idx']
            test_start  = train_end
            test_end    = test_start + self.step
            if test_end > len(self.df):
                continue
            # train_end is the last index of the training window; test_start is the first index of the test window.
            # Shift target training cutoff back by the forecast horizon so the last training rows do not peek at the test window.
            H = self.horizon
            train_target_end = train_end - H
            if train_target_end <= train_start + 50:
                continue
            candidate_cols = self._get_feature_cols_for_window(g)
            context   = self._compute_window_context(w, g, all_graphs)
            y_train   = self._impute_target(self.df[self.target].iloc[train_start:train_target_end].values)
            y_r_test  = self._impute_target(self.df[self.target].iloc[test_start:test_end].values)

            triggered, signal_fired, cooldown_active = False, False, False

            if w == 0 or model is None:
                X_train = self._impute(self.df[candidate_cols].iloc[train_start:train_target_end].values)
                model = self.run_grid_search(X_train, y_train, warm=False)
                self.last_retrain_window = w
                model_feature_cols = candidate_cols
            else:
                signal_fired    = self.should_retrain(w, g=g, all_graphs=all_graphs, **context)
                cooldown_active = self.in_cooldown(w)
                if signal_fired and not cooldown_active:
                    X_train = self._impute(self.df[candidate_cols].iloc[train_start:train_target_end].values)
                    model = self.run_grid_search(X_train, y_train, warm=True)
                    self.last_retrain_window = w
                    triggered = True
                    model_feature_cols = candidate_cols
                    self._post_retrain_hook()

            X_test = self._impute(self.df[model_feature_cols].iloc[test_start:test_end].values)
            y_test = y_r_test
            rmse, mae, r2, qlike, pred, y_true = self.evaluate(model, X_test, y_test)

            result = {
                'window':                w + 1,
                'date_start':            g['date_start'],
                'date_end':              g['date_end'],
                'total_edges':           len(g['edges']),
                'retrain_triggered':     triggered,
                'signal_fired':          signal_fired,
                'cooldown_active':       cooldown_active,
                'windows_since_retrain': w - self.last_retrain_window,
                'rmse':                  round(rmse, 6),
                'mae':                   round(mae, 6),
                'r2':                    round(r2, 6) if not np.isnan(r2) else np.nan,
                'qlike':                 round(qlike, 6),
                'y_true':                _to_json(y_true.tolist()),
                'y_pred':                _to_json(pred.tolist()),
                'best_params':           _to_json(self.best_params),
            }
            result.update(self._extra_result_fields(w, g, triggered, context))
            for k, v in {'graph_msm': None, 'spy_msm': None, 'unstable_edges': None, 'active_features': None}.items():
                result.setdefault(k, v)
            self.results.append(result)

        return pd.DataFrame(self.results)


# ============================================================================
#   RETRAINING STRATEGIES
# ============================================================================

class StaticRetrainer(BaseRetrainer):
    def should_retrain(self, w, **kwargs):
        return False



class FixedScheduleRetrainer(BaseRetrainer):
    def __init__(self, *args, retrain_interval=5, **kwargs):
        super().__init__(*args, **kwargs)
        self.retrain_interval = retrain_interval

    def should_retrain(self, w, **kwargs):
        return (w % self.retrain_interval) == 0



class PerformanceRetrainer(BaseRetrainer):
    '''
    Fire when the recent rolling RMSE rises above a trailing baseline by
    drop_threshold. The original classifier version tracked an F1 *drop*;
    for regression, higher RMSE = worse, so we track an RMSE *rise*.
    '''
    def __init__(self, *args, drop_threshold=0.1, lookback_n=5, **kwargs):
        super().__init__(*args, **kwargs)
        self.drop_threshold = drop_threshold
        self.lookback_n = lookback_n

    def should_retrain(self, w, **kwargs):
        smooth_n = max(1, self.lookback_n // 2)
        required = self.lookback_n + smooth_n
        if len(self.results) < required:
            return False
        baseline_rmse = [r['rmse'] for r in self.results[-(self.lookback_n + smooth_n):-smooth_n]]
        baseline      = np.mean(baseline_rmse)
        recent_rmse   = [r['rmse'] for r in self.results[-smooth_n:]]
        current       = np.mean(recent_rmse)
        return (current - baseline) > self.drop_threshold



class RandomRetrainer(BaseRetrainer):
    def __init__(self, *args, p=0.1, seed=42, **kwargs):
        super().__init__(*args, **kwargs)
        self.p = p
        self.seed = seed
        self.rng = np.random.default_rng(seed)

    def _reset_run_state(self):
        self.rng = np.random.default_rng(self.seed)

    def should_retrain(self, w, **kwargs):
        return self.rng.random() < self.p



class ADWINRetrainer(BaseRetrainer):
    '''
    Feeds per-sample |true - pred| into ADWIN (continuous stream). Drift is
    detected when the error distribution shifts. Resets only after the retrain
    actually fires, so cooldown suppression doesn't lose state.
    '''
    def __init__(self, *args, delta=0.002, **kwargs):
        super().__init__(*args, **kwargs)
        self.delta = delta
        self.adwin = ADWIN(delta=delta)
        self._pending_retrain = False

    def _reset_run_state(self):
        self.adwin = ADWIN(delta=self.delta)
        self._pending_retrain = False

    def should_retrain(self, w, **kwargs):
        if not self.results:
            return False
        if self._pending_retrain:
            return True
        last = self.results[-1]
        y_true = json.loads(last['y_true'])
        y_pred = json.loads(last['y_pred'])
        for true, pred in zip(y_true, y_pred):
            self.adwin.update(float(abs(true - pred)))
            if self.adwin.drift_detected:
                self._pending_retrain = True
                return True
        return False

    def _post_retrain_hook(self):
        self.adwin = ADWIN(delta=self.delta)
        self._pending_retrain = False



class ADWINRevisedRetrainer(BaseRetrainer):
    def __init__(self, *args, delta=0.002, clock=10, **kwargs):
        super().__init__(*args, **kwargs)
        self.delta = delta
        self.clock = clock
        self.adwin = ADWIN(delta=delta, clock=clock)
        self._pending_retrain = False
        self._error_scale = None  # rolling MAD scale for bounded input

    def _reset_run_state(self):
        self.adwin = ADWIN(delta=self.delta, clock=self.clock)
        self._pending_retrain = False
        self._error_scale = None

    def should_retrain(self, w, **kwargs):
        if not self.results:
            return False
        if self._pending_retrain:
            return True
        last = self.results[-1]
        y_true = json.loads(last['y_true'])
        y_pred = json.loads(last['y_pred'])
        errors = [abs(float(t) - float(p)) for t, p in zip(y_true, y_pred)]

        # Rolling median-absolute-deviation scale (first few windows set it)
        if self._error_scale is None:
            self._error_scale = max(np.median(errors), 1e-3)
        else:
            self._error_scale = 0.9 * self._error_scale + 0.1 * max(np.median(errors), 1e-3)

        for e in errors:
            # Bounded signal via smooth transform; preserves ordering
            bounded = np.tanh(e / (3 * self._error_scale))
            self.adwin.update(bounded)
            if self.adwin.drift_detected:
                self._pending_retrain = True
                return True
        return False

    def _post_retrain_hook(self):
        self.adwin = ADWIN(delta=self.delta, clock=self.clock)
        self._pending_retrain = False
# ---- MSM-based retrainers ----
class MSMRetrainer(BaseRetrainer):
    def __init__(self, *args, tau_1=0.6, tau_2=0.55, lookback=4, r=2.0, **kwargs):
        super().__init__(*args, **kwargs)
        self.tau_1 = tau_1
        self.tau_2 = tau_2
        self.lookback = lookback
        self.r = r
        self._provisional_flag = False
        self._provisional_edge_scores = {}
        self._expanded_lookback = max(1, int(lookback * r))

    def compute_graph_msm(self, all_graphs, w):
        start = max(0, w - self.lookback + 1)
        recent_windows = all_graphs[start:w + 1]
        n = len(recent_windows)
        all_edges = set()
        for g in recent_windows:
            all_edges |= g['edges']
        if not all_edges:
            return 0.0, {}
        edge_scores = {
            e: sum(1 for g in recent_windows if e in g['edges']) / n
            for e in all_edges
        }
        return np.mean(list(edge_scores.values())), edge_scores

    def compute_expanded_msm(self, all_graphs, w, lookback):
        start = max(0, w - lookback + 1)
        recent_windows = all_graphs[start:w + 1]
        n = len(recent_windows)
        all_edges = set()
        for g in recent_windows:
            all_edges |= g['edges']
        if not all_edges:
            return 0.0, {}
        edge_scores = {
            e: sum(1 for g in recent_windows if e in g['edges']) / n
            for e in all_edges
        }
        return float(np.mean(list(edge_scores.values()))), edge_scores

    def should_retrain(self, w, all_graphs=None, curr_msm=None, **kwargs):
        if w < self.lookback or all_graphs is None:
            return False

        if curr_msm is None:
            curr_msm, edge_scores = self.compute_graph_msm(all_graphs, w)
        else:
            _, edge_scores = self.compute_graph_msm(all_graphs, w)

        if curr_msm < self.tau_1 and not self._provisional_flag:
            self._provisional_flag = True
            self._provisional_edge_scores = edge_scores
            return False

        if self._provisional_flag:
            if w < self._expanded_lookback:
                return False
            expanded_msm, _ = self.compute_expanded_msm(all_graphs, w, self._expanded_lookback)
            if expanded_msm < self.tau_2:
                self._provisional_flag = False
                return True
            if curr_msm >= self.tau_1:
                self._provisional_flag = False
                self._provisional_edge_scores = {}
                return False

        return False

    def _reset_run_state(self):
        self._provisional_flag = False
        self._provisional_edge_scores = {}

    def _compute_window_context(self, w, g, all_graphs):
        curr_msm, edge_scores = (
            self.compute_graph_msm(all_graphs, w)
            if w >= self.lookback else (1.0, {})
        )
        return {'curr_msm': curr_msm, 'edge_scores': edge_scores}

    def _extra_result_fields(self, w, g, triggered, context):
        curr_msm    = context.get('curr_msm', 1.0)
        edge_scores = context.get('edge_scores', {})
        return {
            'graph_msm': round(curr_msm, 4),
            'unstable_edges': _to_json({
                str(e): round(s, 3)
                for e, s in edge_scores.items()
                if s < self.tau_1
            }) if triggered else '{}',
        }

    def _post_retrain_hook(self):
        self._provisional_edge_scores = {}



class SPYFocusedMSMRetrainer(MSMRetrainer):
    def __init__(self, *args, target_idx=0, **kwargs):
        super().__init__(*args, **kwargs)
        self.target_idx = target_idx

    def compute_graph_msm(self, all_graphs, w):
        start = max(0, w - self.lookback + 1)
        recent_windows = all_graphs[start:w + 1]
        n = len(recent_windows)
        all_edges = set()
        for g in recent_windows:
            all_edges |= {e for e in g['edges'] if e[1] == self.target_idx}
        if not all_edges:
            return 0.0, {}
        edge_scores = {
            e: sum(1 for g in recent_windows if e in g['edges']) / n
            for e in all_edges
        }
        return float(np.mean(list(edge_scores.values()))), edge_scores



class MSMTimeoutRetrainer(MSMRetrainer):
    def __init__(self, *args, max_provisional_windows=4, **kwargs):
        super().__init__(*args, **kwargs)
        self.max_provisional_windows = max_provisional_windows
        self._provisional_window_count = 0

    def _reset_run_state(self):
        super()._reset_run_state()
        self._provisional_window_count = 0

    def should_retrain(self, w, all_graphs=None, curr_msm=None, **kwargs):
        if w < self.lookback or all_graphs is None:
            return False
        parent_result = super().should_retrain(w, all_graphs=all_graphs, curr_msm=curr_msm, **kwargs)
        if parent_result:
            self._provisional_window_count = 0
            return True
        if self._provisional_flag:
            self._provisional_window_count += 1
            if self._provisional_window_count >= self.max_provisional_windows:
                self._provisional_flag = False
                self._provisional_window_count = 0
                return True
        else:
            self._provisional_window_count = 0
        return False



class CausalFeatureRetrainer(MSMRetrainer):
    def __init__(self, *args, target_name='SPY_lr', graph_var_names=None,
                 all_features=None, **kwargs):
        super().__init__(*args, **kwargs)
        if graph_var_names is None:
            raise ValueError('CausalFeatureRetrainer requires graph_var_names')
        self.target_name      = target_name
        self.graph_var_names  = list(graph_var_names)
        self.target_idx       = self.graph_var_names.index(target_name)
        self.all_features     = list(all_features) if all_features else list(self.feature_cols)
        self._active_features = list(self.all_features)

    def _reset_run_state(self):
        super()._reset_run_state()
        self._active_features = list(self.all_features)

    def _get_feature_cols_for_window(self, g):
        new_active = self._get_target_parent_features(g)
        if new_active != self._active_features:
            self.best_params = None
        self._active_features = new_active
        return self._active_features

    def _extra_result_fields(self, w, g, triggered, context):
        fields = super()._extra_result_fields(w, g, triggered, context)
        fields['active_features'] = _to_json(self._active_features)
        return fields

    def _get_target_parent_features(self, g):
        parent_graph_indices = {
            e[0] for e in g['edges']
            if e[1] == self.target_idx and e[0] != self.target_idx
        }
        parent_names = [
            self.graph_var_names[idx] for idx in parent_graph_indices
            if 0 <= idx < len(self.graph_var_names)
        ]
        active = [name for name in parent_names if name in self.all_features]
        return sorted(active) if active else list(self.all_features)



class DriftSignalObserverRetrainer(StaticRetrainer):
    '''
    Frozen model. Never retrains. Logs graph-level MSM and SPY-focused MSM at
    every window. Output feeds lead_lag_analysis.py: does MSM predictively
    lead forecast-error (RMSE) rises? Because the model never retrains,
    error degradation is uncorrupted by retraining events.
    '''
    def __init__(self, *args, lookback=4, **kwargs):
        super().__init__(*args, **kwargs)
        self.lookback = lookback

    def should_retrain(self, w, **kwargs):
        return False

    def _compute_window_context(self, w, g, all_graphs):
        if w < self.lookback:
            return {'graph_msm': np.nan, 'spy_msm': np.nan}

        start = max(0, w - self.lookback + 1)
        recent = all_graphs[start:w + 1]
        all_edges = set()
        for gg in recent:
            all_edges |= gg['edges']
        if not all_edges:
            graph_msm = 0.0
        else:
            scores = [sum(1 for gg in recent if e in gg['edges']) / len(recent) for e in all_edges]
            graph_msm = float(np.mean(scores))

        spy_edges = set()
        for gg in recent:
            spy_edges |= {e for e in gg['edges'] if e[1] == 0}
        if not spy_edges:
            spy_msm = 0.0
        else:
            scores = [sum(1 for gg in recent if e in gg['edges']) / len(recent) for e in spy_edges]
            spy_msm = float(np.mean(scores))

        return {'graph_msm': graph_msm, 'spy_msm': spy_msm}

    def _extra_result_fields(self, w, g, triggered, context):
        gm = context.get('graph_msm', np.nan)
        sm = context.get('spy_msm',   np.nan)
        return {
            'graph_msm': round(gm, 4) if not np.isnan(gm) else np.nan,
            'spy_msm':   round(sm, 4) if not np.isnan(sm) else np.nan,
        }



class StrengthWeightedMSMRetrainer(MSMRetrainer):
    """
    Weighs each edge by its |val_matrix| strength from PCMCI+, averaged over
    windows where the edge is present. A dominant edge weakening now counts
    more than a marginal edge flickering near the significance threshold.

    Justification: PCMCI+ already computes edge strength via its CI test's
    val score (partial correlation magnitude). Binary persistence throws this
    away. Empirically, weak edges at threshold are noisier signals of structural
    change than strong edges near threshold.
    """

    def _edge_strength(self, g, edge):
        """|val_matrix| for the given edge in graph g, 0 if edge absent."""
        if edge not in g['edges']:
            return 0.0
        src, tgt, lag = edge
        return abs(float(g['val_matrix'][src, tgt, lag]))

    def compute_graph_msm(self, all_graphs, w):
        start = max(0, w - self.lookback + 1)
        recent_windows = all_graphs[start:w + 1]
        n = len(recent_windows)
        all_edges = set()
        for g in recent_windows:
            all_edges |= g['edges']
        if not all_edges:
            return 0.0, {}

        # Strength-weighted persistence: mean |val| across windows where present,
        # multiplied by presence fraction to penalise flickering edges.
        edge_scores = {}
        for e in all_edges:
            strengths = [self._edge_strength(g, e) for g in recent_windows]
            present = [s > 0 for s in strengths]
            persistence = sum(present) / n
            mean_strength = np.mean([s for s in strengths if s > 0]) if any(present) else 0.0
            # Composite: persistence × normalised strength.
            # Clip strength at 1.0 since |partial corr| ∈ [0, 1].
            edge_scores[e] = persistence * min(mean_strength, 1.0)

        return float(np.mean(list(edge_scores.values()))), edge_scores

    def compute_expanded_msm(self, all_graphs, w, lookback):
        # Same logic on expanded window
        start = max(0, w - lookback + 1)
        recent_windows = all_graphs[start:w + 1]
        n = len(recent_windows)
        all_edges = set()
        for g in recent_windows:
            all_edges |= g['edges']
        if not all_edges:
            return 0.0, {}
        edge_scores = {}
        for e in all_edges:
            strengths = [self._edge_strength(g, e) for g in recent_windows]
            present = [s > 0 for s in strengths]
            persistence = sum(present) / n
            mean_strength = np.mean([s for s in strengths if s > 0]) if any(present) else 0.0
            edge_scores[e] = persistence * min(mean_strength, 1.0)
        return float(np.mean(list(edge_scores.values()))), edge_scores



class FusedMSMRetrainer(MSMRetrainer):
    """
    Monitors two subgraphs in parallel: edges into SPY_lr (returns) and edges
    into VIX_ld (expected volatility). Retrains when the minimum of the two
    MSM scores drops below threshold — either dimension of market structure
    breakdown is sufficient.

    Justification: Your forecast target is realized variance. Monitoring only
    returns' causal structure is theoretically mismatched to what you forecast.
    VIX is the market's expectation of variance, so edges into VIX directly
    capture mechanisms driving volatility expectations. Fusing SPY and VIX
    subgraphs aligns MSM with both directional and variance risk dimensions.
    """

    def __init__(self, *args, spy_idx=0, vix_idx=4, **kwargs):
        super().__init__(*args, **kwargs)
        self.spy_idx = spy_idx
        self.vix_idx = vix_idx

    def _subgraph_msm(self, all_graphs, w, target_idx):
        start = max(0, w - self.lookback + 1)
        recent = all_graphs[start:w + 1]
        n = len(recent)
        edges = set()
        for g in recent:
            edges |= {e for e in g['edges'] if e[1] == target_idx}
        if not edges:
            return 0.0, {}
        scores = {e: sum(1 for g in recent if e in g['edges']) / n for e in edges}
        return float(np.mean(list(scores.values()))), scores

    def compute_graph_msm(self, all_graphs, w):
        spy_msm, spy_scores = self._subgraph_msm(all_graphs, w, self.spy_idx)
        vix_msm, vix_scores = self._subgraph_msm(all_graphs, w, self.vix_idx)
        # Fuse via minimum — trigger when EITHER structure fails.
        fused = min(spy_msm, vix_msm)
        fused_scores = {**spy_scores, **vix_scores}
        return fused, fused_scores

    def compute_expanded_msm(self, all_graphs, w, lookback):
        start = max(0, w - lookback + 1)
        recent = all_graphs[start:w + 1]
        n = len(recent)

        spy_edges = set(); vix_edges = set()
        for g in recent:
            spy_edges |= {e for e in g['edges'] if e[1] == self.spy_idx}
            vix_edges |= {e for e in g['edges'] if e[1] == self.vix_idx}

        def _avg(edge_set):
            if not edge_set:
                return 0.0
            return float(np.mean([sum(1 for g in recent if e in g['edges']) / n
                                  for e in edge_set]))

        return min(_avg(spy_edges), _avg(vix_edges)), {}