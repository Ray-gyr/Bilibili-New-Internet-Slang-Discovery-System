import pandas as pd
import numpy as np
from sklearn.model_selection import StratifiedKFold
from xgboost import XGBClassifier
from sklearn.metrics import accuracy_score, f1_score, classification_report, confusion_matrix
import matplotlib.pyplot as plt

# 读取数据
df = pd.read_excel("训练数据.xlsx")

# 准备特征和标签
X = df.drop(columns=['Word','Label'])
y = df['Label']

# 初始化交叉验证
K = 5
kf = StratifiedKFold(n_splits=K, shuffle=True, random_state=42)

# 初始化列表用于存储指标、最佳迭代次数和 OOF 概率
acc_scores = []
f1_scores = []
best_iterations = []
oof_probs = np.zeros(len(y))  # 用于保存每个样本的验证集预测概率

# 交叉验证循环
for fold_index, (train_idx, val_idx) in enumerate(kf.split(X, y)):
    print(f"\n======= Fold {fold_index+1}/{K} =======")
    
    X_train, X_val = X.iloc[train_idx], X.iloc[val_idx]
    y_train, y_val = y.iloc[train_idx], y.iloc[val_idx]
    
    model = XGBClassifier(
        n_estimators=350,
        max_depth=4,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        eval_metric="logloss",
        early_stopping_rounds=40,
        random_state=42,  # 不同 fold 使用不同种子更稳健
        scale_pos_weight=sum(y_train == 0) / sum(y_train == 1)
    )
    
    model.fit(
        X_train, y_train,
        eval_set=[(X_val, y_val)],
        verbose=False
    )
    
    best_iter = model.best_iteration
    best_iterations.append(best_iter)
    print(f"Best iteration: {best_iter}")
    
    # 验证集预测概率
    val_probs = model.predict_proba(X_val)[:, 1]
    oof_probs[val_idx] = val_probs
    
    # 默认 0.5 阈值预测
    y_pred = (val_probs >= 0.5).astype(int)
    acc = accuracy_score(y_val, y_pred)
    f1 = f1_score(y_val, y_pred, average='weighted')
    acc_scores.append(acc)
    f1_scores.append(f1)
    
    print(f"Fold {fold_index+1} Classification Report:")
    print(classification_report(y_val, y_pred))
    print("Confusion Matrix:")
    print(confusion_matrix(y_val, y_pred))
    print("====================================")

# CV 性能
print("\n======= Cross-Validation Results =======")
print(f"Average Accuracy: {np.mean(acc_scores):.4f} ± {np.std(acc_scores):.4f}")
print(f"Average F1: {np.mean(f1_scores):.4f} ± {np.std(f1_scores):.4f}")
print(f"Best iterations from each fold: {best_iterations}")
median_best_iter = int(np.median(best_iterations))
print(f"Median best iteration: {median_best_iter}")

# -----------------------
# F1-max 阈值搜索
# -----------------------
thresholds = np.linspace(0.1, 0.9, 162)
f1_scores_thresh = []

for t in thresholds:
    preds = (oof_probs >= t).astype(int)
    f1 = f1_score(y, preds)
    f1_scores_thresh.append(f1)

best_idx = np.argmax(f1_scores_thresh)
best_threshold = thresholds[best_idx]
print(f"Best F1-max threshold: {best_threshold:.2f} (F1={f1_scores_thresh[best_idx]:.4f})")

# 绘图
plt.figure(figsize=(8,5))
plt.plot(thresholds, f1_scores_thresh, marker='o', linestyle='-')
plt.title('F1 Score vs Classification Threshold')
plt.xlabel('Threshold')
plt.ylabel('F1 Score')
plt.grid(True)
plt.legend()
plt.show()
