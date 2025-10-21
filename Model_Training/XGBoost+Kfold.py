import optuna
import pandas as pd
import numpy as np
from sklearn.model_selection import StratifiedKFold
from xgboost import XGBClassifier
from sklearn.metrics import f1_score

# 读取数据
df = pd.read_excel("训练数据.xlsx")
X = df.drop(columns=['Word','Label'])
y = df['Label']

# 5折交叉验证器
K = 5
kf = StratifiedKFold(n_splits=K, shuffle=True, random_state=42)

def objective(trial):
    # 超参数搜索空间
    params = {
        "n_estimators": 1000,
        "max_depth": trial.suggest_int("max_depth", 3, 6),
        "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.1, log=True),
        "subsample": trial.suggest_float("subsample", 0.6, 0.9),
        "colsample_bytree": trial.suggest_float("colsample_bytree", 0.6, 0.9),
        "min_child_weight": trial.suggest_int("min_child_weight", 1, 6),
        "scale_pos_weight": trial.suggest_float("scale_pos_weight", 4, 10),
        "eval_metric": "aucpr",
        "early_stopping_rounds":40,
        "random_state": 42,
    }

    f1_scores = []

    for train_idx, val_idx in kf.split(X, y):
        X_train, X_val = X.iloc[train_idx], X.iloc[val_idx]
        y_train, y_val = y.iloc[train_idx], y.iloc[val_idx]

        model = XGBClassifier(**params)

        model.fit(
            X_train, y_train,
            eval_set=[(X_val, y_val)],
            verbose=False
        )

        y_pred = model.predict(X_val)
        f1 = f1_score(y_val, y_pred, average="macro")  # 用 macro F1
        f1_scores.append(f1)

    return np.mean(f1_scores)


# 创建 Optuna study
study = optuna.create_study(direction="maximize")
study.optimize(objective, n_trials=50)  # 可以调大，比如 100



# 输出最佳参数和分数
print("Best params:", study.best_params)
print("Best macro F1:", study.best_value)

print("\n=== 计算平均最佳迭代次数 ===")
best_params = study.best_params.copy()
# 添加其他固定参数
fixed_params = {
    "n_estimators": 1000,
    "eval_metric": "aucpr",
    "early_stopping_rounds": 40,
    "random_state": 42,
}
best_params.update(fixed_params)

best_iterations = []

for train_idx, val_idx in kf.split(X, y):
    X_train, X_val = X.iloc[train_idx], X.iloc[val_idx]
    y_train, y_val = y.iloc[train_idx], y.iloc[val_idx]
    
    model = XGBClassifier(**best_params)
    model.fit(
        X_train, y_train,
        eval_set=[(X_val, y_val)],
        verbose=False
    )
    
    best_iterations.append(model.best_iteration)

print("Best iterations per fold:", best_iterations)
