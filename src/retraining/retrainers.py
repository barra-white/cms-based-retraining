import numpy as np
import pandas as pd

from xgboost import XGBClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression

from sklearn.model_selection import GridSearchCV, TimeSeriesSplit
from sklearn.metrics import f1_score

from river.drift import ADWIN

from abc import ABC, abstractmethod

# model configurations for grid search
# new models get added here with their fixed and tunable parameters
MODEL_CONFIGS = {
    'xgboost': {
        'class': XGBClassifier,
        'fixed_params': {
            'objective':    'multi:softmax',
            'num_class':    3,
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
            'reg_lambda':       [1.0, 5.0]
        },
        'warm_grid_fn': lambda bp: {
            'n_estimators': sorted({int(max(50, bp['n_estimators'] * f)) for f in [0.75, 1.0, 1.25]}),
            'max_depth':    sorted({max(2, min(9, bp['max_depth'] + d)) for d in [-1, 0, 1]}),
            'learning_rate': sorted({round(bp['learning_rate'] * f, 4) for f in [0.5, 1.0, 2.0]}),
            'subsample':     sorted({round(max(0.5, min(1.0, bp.get('subsample', 0.8) + d)), 1) for d in [-0.1, 0, 0.1]}),
            'colsample_bytree': sorted({round(max(0.5, min(1.0, bp.get('colsample_bytree', 0.8) + d)), 1) for d in [-0.1, 0, 0.1]}),
            'min_child_weight': [bp.get('min_child_weight', 1)],
            'reg_alpha':        [bp.get('reg_alpha', 0)],
            'reg_lambda':       [bp.get('reg_lambda', 1.0)],
        },
    },

    'rf': {
        'class': RandomForestClassifier,
        'fixed_params': {
            'random_state': 42,
            'n_jobs':       -1,
        },
        'grid': {
            'n_estimators':     [100, 200, 300, 500],
            'max_depth':        [3, 5, 7, 10, 15, None],
            'min_samples_split':[2, 5, 10, 20],
            'min_samples_leaf': [1, 2, 4, 8],
            'max_features':     ['sqrt', 'log2'],
            'class_weight':     [None, 'balanced'],
        },
        'warm_grid_fn': lambda bp: {
            'n_estimators':     sorted({max(50, bp['n_estimators'] + d) for d in [-100, 0, 100]}),
            'max_depth':        [bp.get('max_depth', 5)],
            'min_samples_split':[bp.get('min_samples_split', 2)],
            'min_samples_leaf': [bp.get('min_samples_leaf', 1)],
            'max_features':     [bp.get('max_features', 'sqrt')],
            'class_weight':     [bp.get('class_weight', None)],
        },
    },

    'lr': {
        'class': LogisticRegression,
        'fixed_params': {
            'random_state': 42,
            'max_iter':     1000,
            'multi_class':  'multinomial',
        },
        'grid': {
            'C':            [0.0001, 0.001, 0.01, 0.1, 1.0, 10.0, 100.0],
            'solver':       ['lbfgs', 'saga'],
            'penalty':      ['l2'],
            'class_weight': [None, 'balanced'],
        },
        'warm_grid_fn': lambda bp: {
            'C':            sorted({bp['C'] * f for f in [0.1, 1.0, 10.0]}),
            'solver':       [bp.get('solver', 'lbfgs')],
            'penalty':      [bp.get('penalty', 'l2')],
            'class_weight': [bp.get('class_weight', None)],
        },
    },
}



class BaseRetrainer(ABC):
    '''
    Abstract class for all retraining strategies.
    Handles:
        - bin edge freezing
        - evaluation
        - grid searching
        - 
    '''
    def __init__(
        self,
        df,
        feature_cols,
        target,
        model_type='xgboost',
        n_bins=3,
        window=504,
        step=21,
        cooldown=2
    ):
        # check if valid model
        if model_type not in MODEL_CONFIGS:
            raise ValueError(
                f"Unsupported model_type '{model_type}'. "
                f"Supported types: {list(MODEL_CONFIGS.keys())}"
            )
        
        # data
        self.df = df
        self.feature_cols = feature_cols
        self.target = target
        self.n_bins = n_bins
        self.window = window
        self.step = step
        
        # model
        self.model_type = model_type
        self.best_params = None # set after grid search

        # retraining
        self.cooldown = cooldown
        self.last_retrain_window = -999 # to allow retraining at window 0 if needed
        
        # binning
        self.bin_edges = None # frozen at initialization based on first `window` obs
        
        # results
        self.results = []

    
    
    # ----- BINNING ----- #
    def freeze_bin_edges(self, train_vals):
        n = len(train_vals)
        sorted_vals = np.sort(train_vals)

        # Place each boundary at the MIDPOINT between the two sorted values
        # that straddle the quantile boundary — guarantees no ties on the edge
        boundary_idxs = [int(n * k / self.n_bins) for k in range(1, self.n_bins)]
        interior_edges = [
            (sorted_vals[i - 1] + sorted_vals[i]) / 2.0
            for i in boundary_idxs
        ]
        self.bin_edges = np.array([-np.inf] + interior_edges + [np.inf])
        
    def apply_bins(self, vals):
        if self.bin_edges is None:
            raise RuntimeError("Bin edges not frozen yet. Call freeze_bin_edges() first.")
        return np.digitize(vals, self.bin_edges[1:-1])
    
    
    
    # ----- MODEL ----- #
    def build_model(self, tuned_params=None):
        config = MODEL_CONFIGS[self.model_type]
        all_params = config['fixed_params'].copy()
        
        if tuned_params:
            all_params.update(tuned_params)
        elif self.best_params:
            all_params.update(self.best_params)
        return config['class'](**all_params)
    
    
    
    # ----- GRID SEARCH ----- #
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
            estimator=base_model,
            param_grid=param_grid,
            cv=tscv,
            scoring='f1_macro',
            n_jobs=-1,
            refit=True,
            verbose=1,
            error_score=np.nan
        )
        
        searcher.fit(X_train, y_train)
        self.best_params = searcher.best_params_
        return searcher.best_estimator_
    
    
        
    # ----- EVALUATION ----- #
    def evaluate(self, model, X_test, y_test):
        pred = model.predict(X_test)
        f1 = f1_score(y_test, pred, average='macro', zero_division=0)
        return f1, pred, y_test
    
    
    # ----- COOLING ----- #
    def in_cooldown(self, w):
        return (w - self.last_retrain_window) < self.cooldown
    
    
    
    # ----- TRIGGER ----- #
    @abstractmethod
    def should_retrain(self, w, **kwargs):
        '''
        To be implemented by each retraining strategy.
        Should return True if retraining should be triggered at window w, False otherwise.
        '''
        pass
    
    
    
    # ----- MAIN RETRAINING LOOP ----- #
    def run(self, all_graphs):
        '''
        Rolling retraining loop over all_graphs.
        '''
        
        # state reset for new run
        self.results = []
        self.last_retrain_window = -999
        self.best_params = None
        self.bin_edges = None
        model = None
        
        # loop over windows
        for w, g in enumerate(all_graphs):
            train_start = g['train_start_idx']
            train_end   = g['train_end_idx']
            test_start  = train_end
            test_end    = test_start + self.step

            if test_end > len(self.df):
                continue
            
            # slice data
            X_train    = self.df[self.feature_cols].iloc[train_start:train_end].values
            y_r_train  = self.df[self.target].iloc[train_start:train_end].values
            X_test     = self.df[self.feature_cols].iloc[test_start:test_end].values
            y_r_test   = self.df[self.target].iloc[test_start:test_end].values
            
            triggered = False
            signal_fired = False
            cooldown_active = False
            
            if w == 0 or model is None:
                # first window: freeze bins and train initial model
                self.freeze_bin_edges(y_r_train)
                y_train = self.apply_bins(y_r_train)
                model = self.run_grid_search(X_train, y_train, warm=False)
                self.last_retrain_window = w
                
            else:
                signal_fired = self.should_retrain(w, g=g, all_graphs=all_graphs) 
                cooldown_active = self.in_cooldown(w) # check cooldown before allowing retrain
                
                if signal_fired and not cooldown_active: # only retrain if signal fires and we're not in cooldown
                    self
                    y_train = self.apply_bins(y_r_train)
                    model = self.run_grid_search(X_train, y_train, warm=True)
                    self.last_retrain_window = w
                    triggered = True # for recording in results
                    
            # evaluate current model on test set
            y_test = self.apply_bins(y_r_test)
            f1, pred, y_true = self.evaluate(model, X_test, y_test)
            
            # record results
            self.results.append({
                'window': w + 1,
                'date_start': g['date_start'],
                'date_end': g['date_end'],
                'total_edges': len(g['edges']),
                'retrain_triggered': triggered, # did a retrain actually occur at this window?
                'signal_fired': signal_fired, # did the retrain signal fire (even if we were in cooldown and couldn't retrain)?
                'cooldown_active': cooldown_active,
                'windows_since_retrain': w - self.last_retrain_window,
                'f1': round(f1, 4),
                'y_true': y_true.tolist(),
                'y_pred': pred.tolist(),
                'best_params': str(self.best_params)
            })
            
        return pd.DataFrame(self.results)



# ----- RETRAINING STRATEGIES ----- #
class StaticRetrainer(BaseRetrainer):
    '''
    Baseline strategy: train once on first window, never retrain.
    '''
    def should_retrain(self, w, **kwargs):
        return False
    
class FixedScheduleRetrainer(BaseRetrainer):
    '''
    Retrain at fixed intervals (e.g. every 5 windows), regardless of MSM signal.
    '''
    def __init__(self, *args, retrain_interval=5, **kwargs):
        super().__init__(*args, **kwargs)
        self.retrain_interval = retrain_interval
        
    def should_retrain(self, w, **kwargs):
        return (w % self.retrain_interval) == 0
    
class PerformanceRetrainer(BaseRetrainer):
    '''
    Retrain whenever performance drops below a certain threshold.
    '''
    def __init__(self, *args, drop_threshold=0.1, lookback_n=5, **kwargs):
        super().__init__(*args, **kwargs)
        self.drop_threshold = drop_threshold
        self.lookback_n = lookback_n
        
    def should_retrain(self, w, **kwargs):
        # check if previous window's F1 dropped by a certain threshold
        if len(self.results) < self.lookback_n:
            return False # not enough history yet
        
        recent_f1 = [r['f1'] for r in self.results[-self.lookback_n:]]
        baseline = np.mean(recent_f1[:-1]) # mean of all but most recent
        current = recent_f1[-1] 
        
        return (baseline - current) > self.drop_threshold
    
class ADWINRetrainer(BaseRetrainer):
    '''
    Retrain whenever ADWIN detects a drift in the performance metric.
    '''
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.adwin = ADWIN()
        
    def run(self, all_graphs):
        self.adwin = ADWIN() # reset ADWIN state for new run
        return super().run(all_graphs)
    
    def should_retrain(self, w, **kwargs):
        if not self.results:
            return False # no performance data yet
        self.adwin.update(self.results[-1]['f1'])
        return self.adwin.drift_detected
    
class MSMRetrainer(BaseRetrainer):
    '''
    Retrain whenever graph-level MSM drops below a certain threshold.
    '''
    def __init__(self, *args, tau_1=0.6, tau_2 = 0.55, lookback=4, **kwargs):
        super().__init__(*args, **kwargs)
        self.tau_1 = tau_1
        self.tau_2 = tau_2
        self.lookback = lookback
        self._provisional_flag = False # internal flag to track if we're in the provisional period after a drop below tau_1
        
    def compute_graph_msm(self, all_graphs, w):
        '''
        Computes mean per-edge MSM across all edges seen in the
        last `lookback` windows ending at current_w (inclusive).
        Returns: (mean_msm, dict of {edge: msm_score})
        '''
        start = max(0, w - self.lookback + 1)
        recent_windows = all_graphs[start: w + 1]
        n = len(recent_windows)

        # union of all edges seen in lookback period
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
    
    def should_retrain(self, w, all_graphs=None, **kwargs):
        curr_msm, _ = self.compute_graph_msm(all_graphs, w)
        
        if w < self.lookback or all_graphs is None:
            return False # not enough history to compute MSM yet
        
        # stage 1: first window below tau_1, enter provisional period
        if curr_msm < self.tau_1 and not self._provisional_flag:
            self._provisional_flag = True
            return False # wait for next window to confirm drop
        
        # stage 2: if we're in provisional period, check if MSM is still below tau_2
        if self._provisional_flag and curr_msm < self.tau_2:
            self._provisional_flag = False # reset flag after confirming drop
            return True # trigger retrain
        
        # revovery: if MSM goes back above tau_1, exit provisional period
        if curr_msm >= self.tau_1:
            self._provisional_flag = False
            
        return False
    
    def run(self, all_graphs):
        """
        Overrides BaseRetrainer.run() to log MSM-specific fields:
            - graph_msm:      causal stability score per window
            - total_edges:    graph density per window
            - unstable_edges: edges that drove instability at each retrain

        All other logic — bin edges, grid search, evaluation,
        cooldown — is identical to the base class.
        """

        # ── State reset ────────────────────────────────────────────────────────
        # Must repeat base class resets here since we are not calling super().run()
        # Also reset MSM-specific state
        self._provisional_flag   = False
        self.results             = []
        self.best_params         = None
        self.bin_edges           = None
        self.last_retrain_window = -999
        model                    = None

        for w, g in enumerate(all_graphs):

            # ── Window boundaries ──────────────────────────────────────────────
            train_start = g['train_start_idx']
            train_end   = g['train_end_idx']
            test_start  = train_end
            test_end    = test_start + self.step

            if test_end > len(self.df):
                continue

            # slice
            X_train   = self.df[self.feature_cols].iloc[train_start:train_end].values
            y_r_train = self.df[self.target].iloc[train_start:train_end].values
            X_test    = self.df[self.feature_cols].iloc[test_start:test_end].values
            y_r_test  = self.df[self.target].iloc[test_start:test_end].values

            triggered       = False
            signal_fired    = False
            cooldown_active = False

            # compute msm
            curr_msm, edge_scores = (
                self.compute_graph_msm(all_graphs, w)
                if w >= self.lookback else (1.0, {})
            )

            if w == 0 or model is None:
                # Initial training — full grid search, freeze bin edges
                self.freeze_bin_edges(y_r_train)
                y_train = self.apply_bins(y_r_train)
                model   = self.run_grid_search(X_train, y_train, warm=False)
                self.last_retrain_window = w

            else:
                signal_fired    = self.should_retrain(w, all_graphs=all_graphs)
                cooldown_active = self.in_cooldown(w)

                if signal_fired and not cooldown_active:
                    # Refreeze edges to match new training window distribution
                    # then rebin before warm grid search
                    self.freeze_bin_edges(y_r_train)
                    y_train = self.apply_bins(y_r_train)
                    model   = self.run_grid_search(X_train, y_train, warm=True)
                    self.last_retrain_window = w
                    triggered = True

            # evaluate
            # y_test uses current frozen edges — always consistent with model
            y_test           = self.apply_bins(y_r_test)
            f1, pred, y_true = self.evaluate(model, X_test, y_test)

            self.results.append({
                'window':                w + 1,
                'date_start':            g['date_start'],
                'date_end':              g['date_end'],
                'signal_fired':          signal_fired,
                'cooldown_active':       cooldown_active,
                'retrain_triggered':     triggered,
                'windows_since_retrain': w - self.last_retrain_window,
                'f1':                    f1,
                'y_true':                y_true.tolist(),
                'y_pred':                pred.tolist(),
                'best_params':           str(self.best_params),

                # MSM-specific fields — only in MSMRetrainer results CSV
                'total_edges':           len(g['edges']),
                'graph_msm':             round(curr_msm, 4),

                # Which edges drove instability at this retrain point
                # Saved as string dict — parse with ast.literal_eval in analysis.py
                # Empty dict if no retrain triggered this window
                'unstable_edges':        str({
                    str(e): round(s, 3)
                    for e, s in edge_scores.items()
                    if s < self.tau_1
                }) if triggered else '{}',
            })

        return pd.DataFrame(self.results)