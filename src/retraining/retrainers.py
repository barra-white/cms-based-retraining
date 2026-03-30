import numpy as np
import pandas as pd

from xgboost import XGBClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression

from sklearn.model_selection import GridSearchCV, TimeSeriesSplit
from sklearn.metrics import f1_score

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
            'max_depth':        [3, 4, 5, 6, 7],
            'learning_rate':    [0.005, 0.01, 0.05, 0.1, 0.2],
            'subsample':        [0.6, 0.7, 0.8, 0.9, 1.0],
            'colsample_bytree': [0.6, 0.7, 0.8, 0.9, 1.0],
            'min_child_weight': [1, 3, 5, 7],
            'reg_alpha':        [0, 0.01, 0.1, 0.5, 1.0],
            'reg_lambda':       [0.5, 1.0, 2.0, 5.0],
            'gamma':            [0, 0.1, 0.3, 0.5],
        },
        'warm_grid_fn': lambda bp: {
            'n_estimators':     sorted({max(50, bp['n_estimators'] + d) for d in [-100, 0, 100]}),
            'max_depth':        sorted({max(2, min(9, bp['max_depth'] + d)) for d in [-1, 0, 1]}),
            'learning_rate':    [bp['learning_rate']],
            'subsample':        [bp.get('subsample', 0.8)],
            'colsample_bytree': [bp.get('colsample_bytree', 0.8)],
            'min_child_weight': [bp.get('min_child_weight', 1)],
            'reg_alpha':        [bp.get('reg_alpha', 0)],
            'reg_lambda':       [bp.get('reg_lambda', 1.0)],
            'gamma':            [bp.get('gamma', 0)],
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
        # bin at window 0 and freeze edges for all future windows to ensure consistent labeling
        percentiles = np.linspace(0, 100, self.n_bins + 1)
        self.bin_edges = np.percentile(train_vals, percentiles)
        
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
        tscv = TimeSeriesSplit(n_splits=5)
        
        param_grid = self._warm_grid() if (warm and self.best_params is not None) else self._full_grid()
        
        searcher = GridSearchCV(
            estimator=base_model,
            param_grid=param_grid,
            cv=tscv,
            scoring='f1_macro',
            n_jobs=-1,
            refit=True,
            verbose=1
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