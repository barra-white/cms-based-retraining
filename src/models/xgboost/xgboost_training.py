import os
os.makedirs("results/xgboost", exist_ok=True)

import joblib

import numpy as np
import pandas as pd

from xgboost import XGBClassifier

from sklearn.model_selection import GridSearchCV, TimeSeriesSplit
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score, f1_score


np.random.seed(42)

df = pd.read_csv("data/processed/standardized_data.csv", parse_dates=["Date"])
features = [col for col in df.columns if col != "Date"]
df = df.dropna(subset=features).reset_index(drop=True)

TARGET = "SPY_lr"
FEATURE_COLS = [col for col in features if col != TARGET]



training_window = 504 # 504 observations == 2 trading years
n_bins = 3


# bin target
train_target = df[TARGET].iloc[:training_window].values

percentiles = np.linspace(0, 100, n_bins+1) # [0, 33.33, 66.67, 100]

edges = np.percentile(train_target, percentiles)

print(f"\nBin edges (from first {training_window} obs):")
for i in range(n_bins):
    lo = f"{edges[i]:.4f}" if i > 0 else "-inf"
    hi = f"{edges[i+1]:.4f}" if i < n_bins - 1 else "+inf"
    print(f"\tBin {i}: ({lo}, {hi})")

# assign labels — training set
train_labels = np.digitize(train_target, edges[1:-1])

# assign labels — test set (everything after training window)
test_target = df[TARGET].iloc[training_window:].values
test_labels = np.digitize(test_target, edges[1:-1])

print(f"\nTraining set class distribution:")
for b in range(n_bins):
    count = np.sum(train_labels == b)
    print(f"\tBin {b}: {count} ({count / training_window * 100:.1f}%)")

print(f"\nTest set class distribution:")
n_test = len(test_labels)
for b in range(n_bins):
    count = np.sum(test_labels == b)
    print(f"\tBin {b}: {count} ({count / n_test * 100:.1f}%)")


# separate train and test

X_train = df[FEATURE_COLS].iloc[:training_window].values
y_train = train_labels
print(f'\n\tX_train: {X_train.shape}')


# create param grid for gridsearch
param_grid = {
    'n_estimators': [50, 100, 200],
    'max_depth': [3, 5, 7],
    'subsample': [0.6, 0.8, 1.0],
    'colsample_bytree': [0.6, 0.8, 1.0],
    'learning_rate': [0.01, 0.05, 0.1]
}

# create time series cv split to prevent look ahead bias
tscv = TimeSeriesSplit(n_splits=5)

# create base model
model = XGBClassifier(
    objective='multi:softmax',
    num_class=n_bins,
    random_state=42
)

grid_search = GridSearchCV(
    estimator=model,
    param_grid=param_grid,
    cv=tscv,
    scoring='f1_macro',
    n_jobs=-1
)

grid_search.fit(X_train, y_train)

print(f'\tBest Params: {grid_search.best_params_}')
print(f'\tBest Score: {grid_search.best_score_}')

model = grid_search.best_estimator_

y_train_pred = model.predict(X_train)
train_acc = accuracy_score(y_train, y_train_pred)
train_f1 = f1_score(y_train, y_train_pred, average='macro')

print(f'\n\tTrain Accuracy: {train_acc}')
print(f'\tTrain F1: {train_f1}')
print(f'\nClassification Report: {classification_report(y_train, y_train_pred, target_names=['Down', 'Neutral', 'Up'])}')



importances = model.feature_importances_
importance_df = pd.DataFrame({
    'feature': FEATURE_COLS,
    'importance': importances
}).sort_values('importance', ascending=False)

for _, row in importance_df.iterrows():
    bar = "█" * int(row['importance'] * 50)
    print(f"\t{row['feature']:<16} {row['importance']:.4f}  {bar}")

joblib.dump(model, "results/xgboost/xgboost_model.joblib")

params = grid_search.best_params_.copy()
params['objective']          = 'multi:softmax'
params['num_class']          = n_bins
params['random_state']       = 42
params['verbosity']          = 0
params['use_label_encoder']  = False

joblib.dump(params, "results/xgboost/xgboost_params.joblib")

results_df = pd.DataFrame({
    'model':            ['XGBoost'],
    'best_cv_f1_macro': [round(grid_search.best_score_, 4)],   
    'train_f1_macro':   [round(train_f1, 4)],            
    'train_accuracy':   [round(train_acc, 4)],
    'train_window':     [training_window],
    'n_bins':           [n_bins],
    'best_params':      [str(grid_search.best_params_)]
})
results_df.to_csv("results/xgboost/xgboost_results.csv", index=False)


model.feature_importances_