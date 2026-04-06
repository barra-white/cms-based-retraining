import numpy as np
import pandas as pd

from xgboost import XGBClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression

from sklearn.model_selection import GridSearchCV, TimeSeriesSplit
from sklearn.metrics import f1_score, accuracy_score

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
            'subsample':     sorted({round(max(0.5, min(1.0, bp.get('subsample', 0.8) + d)), 1) for d in [-0.2, -0.1, 0, 0.1, 0.2]}),
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
        },
        'grid': {
            'C':            [0.0001, 0.001, 0.01, 0.1, 1.0, 10.0, 100.0],
            'solver':       ['saga'],
            'penalty':      ['l2', None],
            'class_weight': [None, 'balanced'],
        },
        'warm_grid_fn': lambda bp: {
            'C':            sorted({bp['C'] * f for f in [0.1, 1.0, 10.0]}),
            'solver':       [bp.get('solver', 'saga')],
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
        cooldown=3
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
    def directional_accuracy(self, y_true, y_pred):
        # For directional accuracy, we can treat the problem as binary classification of "up" vs "down"
        # and ignore the "neutral" class. This is because we're primarily interested in whether the model
        # correctly predicts the direction of movement, rather than exact class.
        mask = (y_true != 1)
        if np.sum(mask) == 0:
            return 1.0 # if no directional samples, consider it perfect directional accuracy
        return accuracy_score(y_true[mask], y_pred[mask])
    
    def evaluate(self, model, X_test, y_test):
        pred = model.predict(X_test)
        f1 = f1_score(y_test, pred, average='macro', zero_division=0)
        directional_acc = self.directional_accuracy(y_test, pred)
        return f1, directional_acc, pred, y_test
    
    
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
                    self.freeze_bin_edges(y_r_train) # refreeze bins to match new training distribution before rebinning
                    y_train = self.apply_bins(y_r_train)
                    model = self.run_grid_search(X_train, y_train, warm=True)
                    self.last_retrain_window = w
                    triggered = True # for recording in results
                    
            # evaluate current model on test set
            y_test = self.apply_bins(y_r_test)
            f1, directional_acc, pred, y_true = self.evaluate(model, X_test, y_test)
            
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
                'directional_acc': round(directional_acc, 4),
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
        '''
        Signal fires when smoothed recent performance drops more than
        `drop_threshold` below the mean of the preceding `lookback_n` windows.

        Concrete example — lookback_n=4, smooth_n=2, 10 results (w=0..9):
        baseline_f1 → self.results[-6:-2] = windows 4, 5, 6, 7
        recent_f1   → self.results[-2:]   = windows 8, 9
        baseline    = mean(F1 at 4,5,6,7)
        current     = mean(F1 at 8,9)
        signal      = (baseline - current) > drop_threshold
        '''
        smooth_n = max(1, self.lookback_n // 2)
        required  = self.lookback_n + smooth_n

        if len(self.results) < required:
            return False

        # Baseline: the lookback_n windows that sit just before the recent block
        baseline_f1 = [r['f1'] for r in self.results[-(self.lookback_n + smooth_n):-smooth_n]]
        baseline    = np.mean(baseline_f1)

        # Current: smoothed mean of the last smooth_n completed windows
        # Averaging over smooth_n windows filters out single-window noise spikes
        recent_f1 = [r['f1'] for r in self.results[-smooth_n:]]
        current   = np.mean(recent_f1)

        return (baseline - current) > self.drop_threshold
class RandomRetrainer(BaseRetrainer):
    '''
    Control strategy: randomly trigger retrains with a fixed probability.
    '''
    def __init__(self, *args, p=0.1, **kwargs):
        super().__init__(*args, **kwargs)
        self.p = p
        
    def should_retrain(self, w, **kwargs):
        return np.random.rand() < self.p
class ADWINRetrainer(BaseRetrainer):
    '''
    Retrain whenever ADWIN detects a drift in the performance metric.
    '''
    def __init__(self, *args, delta=0.002, **kwargs):
        super().__init__(*args, **kwargs)
        self.delta = delta
        self.adwin = ADWIN(delta=delta)
        
    def run(self, all_graphs):
        self.adwin = ADWIN(delta=self.delta) # reset ADWIN state for new run
        return super().run(all_graphs)
    
    def should_retrain(self, w, **kwargs):
        if not self.results:
            return False # no performance data yet
        
        last = self.results[-1] # get most recent window's results
        
        for true, pred in zip(last['y_true'], last['y_pred']):
            self.adwin.update(int(true == pred)) # update ADWIN with 1 for correct, 0 for incorrect
            
            if self.adwin.drift_detected:
                self.adwin = ADWIN(delta=self.delta) # reset after drift detected to avoid repeated triggers
                return True
            
        return False
class MSMRetrainer(BaseRetrainer):
    '''
    Retrain whenever graph-level MSM drops below a certain threshold.
    '''
    def __init__(self, *args, tau_1=0.6, tau_2 = 0.55, lookback=4, r=2.0,**kwargs):
        super().__init__(*args, **kwargs)
        self.tau_1 = tau_1
        self.tau_2 = tau_2
        self.lookback = lookback
        self.r = r # expansion ratio for 2 stage window 
        self._provisional_flag = False # internal flag to track if we're in the provisional period after a drop below tau_1
        self._provisional_edge_scores = {} # to track which edges are unstable during the provisional period
        self._expanded_lookback = max(1, int(lookback * r)) # expanded lookback for stage 2 confirmation
        
        
        
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
    
    
    
    def compute_expanded_msm(self, all_graphs, w, lookback):
        '''
        Computes mean per-edge MSM across all edges seen in the last `lookback` windows.
        Used in stage 2 confirmation for MSMRetrainer.
        '''
        start = max(0, w - lookback + 1)
        recent_windows = all_graphs[start: w + 1]
        n = len(recent_windows)

        all_edges = set()
        for g in recent_windows:
            all_edges |= g['edges']

        if not all_edges:
            return 0.0

        edge_scores = {
            e: sum(1 for g in recent_windows if e in g['edges']) / n
            for e in all_edges
        }
        return float(np.mean(list(edge_scores.values()))), edge_scores
    
    
    
    def should_retrain(self, w, all_graphs=None, curr_msm=None, **kwargs):
        if w < self.lookback or all_graphs is None:
            return False # not enough history to compute MSM yet
        
        if curr_msm is None:
            curr_msm, edge_scores = self.compute_graph_msm(all_graphs, w)
        else:
            _, edge_scores = self.compute_graph_msm(all_graphs, w) # compute edge scores for logging even if curr_msm is provided
        
        # stage 1: first window below tau_1, enter provisional period
        if curr_msm < self.tau_1 and not self._provisional_flag:
            self._provisional_flag = True
            self._provisional_edge_scores = edge_scores # store edge scores for logging
            return False # wait for next window to confirm drop
        
        if self._provisional_flag:
            if w < self._expanded_lookback:
                return False # not enough history to confirm yet
            # stage 2: if we're in provisional period, check if MSM is still below tau_2
            expanded_msm, _ = self.compute_expanded_msm(all_graphs, w, self._expanded_lookback)
            if expanded_msm >= self.tau_2:
                self._provisional_flag = False # reset flag if expanded MSM is above tau_2
                return True # trigger retrain
        
        
        # recovery: if MSM goes back above tau_1, exit provisional period
        if curr_msm >= self.tau_1:
            self._provisional_flag = False
            self._provisional_edge_scores = {} # clear edge scores for logging
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
        self._provisional_edge_scores = {}
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
                signal_fired    = self.should_retrain(w, all_graphs=all_graphs, curr_msm=curr_msm)
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
            f1, directional_acc, pred, y_true = self.evaluate(model, X_test, y_test)

            self.results.append({
                'window':                w + 1,
                'date_start':            g['date_start'],
                'date_end':              g['date_end'],
                'signal_fired':          signal_fired,
                'cooldown_active':       cooldown_active,
                'retrain_triggered':     triggered,
                'windows_since_retrain': w - self.last_retrain_window,
                'f1':                    f1,
                'directional_acc':       directional_acc,
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
            
            if triggered:
                self._provisional_edge_scores = {}
                

        return pd.DataFrame(self.results)
class SPYFocusedMSMRetrainer(MSMRetrainer):
    '''
    Variant of MSM retrainer that focuses on edges connected to SPY.
    Retrain whenever mean MSM of SPY-connected edges drops below tau_1,
    with stage 2 confirmation using expanded lookback and tau_2.
    '''
    def __init__(self, *args, spy_idx=10, **kwargs):
        super().__init__(*args, **kwargs)
        self.spy_idx = spy_idx # index of SPY_lr in feature_cols, used to identify SPY-connected edges in the graph (10)
        
    def compute_graph_msm(self, all_graphs, w):
        '''
        Computes mean MSM of edges connected to SPY in the last `lookback` windows.
        Returns: (mean_msm, dict of {edge: msm_score})
        '''
        start = max(0, w - self.lookback + 1)
        recent_windows = all_graphs[start: w + 1]
        n = len(recent_windows)

        all_edges = set()
        for g in recent_windows:
            all_edges |= {e for e in g['edges'] if e[1] == self.spy_idx} # only consider edges where SPY is the target

        if not all_edges:
            return 0.0, {}

        edge_scores = {
            e: sum(1 for g in recent_windows if e in g['edges']) / n
            for e in all_edges
        }
        return np.mean(list(edge_scores.values())), edge_scores

class MSMTimeoutRetrainer(MSMRetrainer):
    
    
    '''
    if msm sits between tau 1 and tau 2 for max_provisional_windows, trigger retrain anyway even without a confirmed drop below tau_2
    '''
    def __init__(self, *args, max_provisional_windows=4, **kwargs):
        super().__init__(*args, **kwargs)
        self.max_provisional_windows = max_provisional_windows
        self._provisional_window_count = 0 # counts how many windows we've been in the provisional period without confirming a drop below tau_2
        
    def should_retrain(self, w, all_graphs=None, curr_msm=None, **kwargs):
        if w < self.lookback or all_graphs is None:
            return False # not enough history to compute MSM yet
        
        if curr_msm is None:
            curr_msm, edge_scores = self.compute_graph_msm(all_graphs, w)
        else:
            _, edge_scores = self.compute_graph_msm(all_graphs, w) # compute edge scores for logging even if curr_msm is provided
        
        # stage 1: first window below tau_1, enter provisional period
        if curr_msm < self.tau_1 and not self._provisional_flag:
            self._provisional_flag = True
            self._provisional_window_count = 1 # reset counter when we first enter provisional period
            self._provisional_edge_scores = edge_scores # store edge scores for logging
            return False # wait for next window to confirm drop
        
        if self._provisional_flag:
            self._provisional_window_count += 1
            
            # force retrain if stuck for too long
            if self._provisional_window_count >= self.max_provisional_windows:
                self._provisional_flag = False # reset flag after forced retrain
                self._provisional_window_count = 0 # reset counter
                return True # trigger retrain
            
            if curr_msm < self.tau_2:
                self._provisional_flag = False # reset flag after confirming drop
                self._provisional_window_count = 0 # reset counter
                return True # trigger retrain
            
        if curr_msm >= self.tau_1:
            self._provisional_flag = False
            self._provisional_window_count = 0 # reset counter if we exit provisional period due to recovery
            self._provisional_edge_scores = {} # clear edge scores for logging
        return False
    
class CausalFeatureRetrainer(MSMRetrainer):
    '''
    MSM + causal feature selection
    '''
    def __init__(self, *args, spy_idx=10, all_features=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.spy_idx = spy_idx # index of SPY_lr in feature_cols, used to identify SPY-connected edges in the graph (10)
        self.all_features = all_features or self.feature_cols # list of all feature names corresponding to graph node indices, used for logging and interpretation of unstable edges in results analysis
        self._active_features = list(self.all_features) # start with all features
        
    def _get_spy_parent_indices(self, g):
        # returns indices in feature_cols of all features that have an edge to SPY in graph g
        spy_parents = {
            e[0] for e in g['edges']
            if e[1] == self.spy_idx
            and e[0] != self.spy_idx # exclude self-loop on SPY
        }
        if not spy_parents:
            return list(range(len(self.all_features))) # if no parents, consider all features as active
        
        feature_indices = sorted([
            idx for idx in spy_parents
            if 0 <= idx < len(self.all_features)
        ])
        
        return feature_indices if feature_indices else list(range(len(self.all_features))) # if no valid parents, consider all features as active
    
    def run(self, all_graphs):
        self._provisional_flag = False # reset provisional flag for new run
        self._provisional_edge_scores = {} # reset edge scores for new run
        self.results = [] # reset results for new run
        self.best_params = None # reset best params for new run
        self.bin_edges = None # reset bin edges for new run
        self.last_retrain_window = -999 # reset last retrain window for new run
        active_indices = list(range(len(self.all_features))) # start with all features active
        
        for w, g in enumerate(all_graphs):
            # update active features based on current graph structure
            active_indices = self._get_spy_parent_indices(g)
            self._active_features = [self.all_features[idx] for idx in active_indices]
            
            # slice data with active features only
            train_start = g['train_start_idx']
            train_end   = g['train_end_idx']
            test_start  = train_end
            test_end    = test_start + self.step

            if test_end > len(self.df):
                continue
            
            X_train   = self.df[self._active_features].iloc[train_start:train_end].values
            y_r_train = self.df[self.target].iloc[train_start:train_end].values
            X_test    = self.df[self._active_features].iloc[test_start:test_end].values
            y_r_test  = self.df[self.target].iloc[test_start:test_end].values
            
            triggered       = False
            signal_fired    = False
            cooldown_active = False

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
                signal_fired    = self.should_retrain(w, all_graphs=all_graphs, curr_msm=curr_msm)
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
            y_test           = self.apply_bins(y_r_test)
            f1, directional_acc, pred, y_true = self.evaluate(model, X_test, y_test)

            # record results with additional field for which features were active this window
            self.results.append({
                'window':                w + 1,
                'date_start':            g['date_start'],
                'date_end':              g['date_end'],
                'active_features':       str(self._active_features), # log which features were active this window
                'signal_fired':          signal_fired,
                'cooldown_active':       cooldown_active,
                'retrain_triggered':     triggered,
                'windows_since_retrain': w - self.last_retrain_window,
                'f1':                    f1,
                'directional_acc':       directional_acc,
                'y_true':                y_true.tolist(),
                'y_pred':                pred.tolist(),
                'best_params':           str(self.best_params),
                'total_edges':           len(g['edges']),
                'graph_msm':             round(curr_msm, 4),
            })
            
        return pd.DataFrame(self.results)
        