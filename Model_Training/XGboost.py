import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score
from xgboost import XGBClassifier
import matplotlib.pyplot as plt
from sklearn.metrics import f1_score, precision_score, recall_score
import numpy as np
import pickle
import os
from datetime import datetime

# =========================
# 数据读取与拆分
# =========================
df = pd.read_excel("训练数据.xlsx")

X = df.drop(columns=["Word","Label"])  # 特征
y = df["Label"]                        # 标签

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

# =========================
# 模型定义与训练
# =========================
model = XGBClassifier(
    n_estimators=71,
    max_depth=6,
    learning_rate=0.09006,
    subsample=0.60191,
    colsample_bytree=0.86900,
    eval_metric="aucpr",
    scale_pos_weight=7.9461,
    min_child_weight=4
)

model.fit(X_train, y_train)

# =========================
# 模型预测与评估
# =========================
y_pred = model.predict(X_test)

print("Accuracy:", accuracy_score(y_test, y_pred))
print("\nClassification Report:\n", classification_report(y_test, y_pred))
print("\nConfusion Matrix:\n", confusion_matrix(y_test, y_pred))

# =========================
# 特征重要性可视化
# =========================
importances = model.feature_importances_
feat_names = X.columns

plt.figure(figsize=(10, 6))
plt.barh(feat_names, importances)
plt.xlabel("Feature Importance")
plt.ylabel("Features")
plt.title("XGBoost Feature Importance")

# 保存特征重要性图
today = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
basedir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

feature_path = os.path.join(basedir, "xgbModel", f"XGBoost_Feature_Importance_{today}.png")
plt.savefig(feature_path)
plt.close()
print(f"Feature importance plot saved to {feature_path}")

# 保存评估报告
report_path = os.path.join(basedir, "xgbModel", "xgb_evaluation_report.txt")
with open(report_path, "a") as f:
    f.write("\n" + "="*40 + "\n")
    f.write(f"Evaluation Report - {today}\n")
    f.write("Accuracy: {:.4f}\n\n".format(accuracy_score(y_test, y_pred)))
    f.write("Classification Report:\n")
    f.write(classification_report(y_test, y_pred))
    f.write("\nConfusion Matrix:\n")
    f.write(str(confusion_matrix(y_test, y_pred)))
print(f"Evaluation report saved to {report_path}")

# =========================
# 阈值曲线：F1, Precision, Recall
# =========================
y_proba = model.predict_proba(X_test)[:, 1]  # 获取类别1的概率

thresholds = np.linspace(0.0, 1.0, 101)
f1_scores, precisions, recalls = [], [], []

for t in thresholds:
    y_thresh = (y_proba >= t).astype(int)
    f1_scores.append(f1_score(y_test, y_thresh))
    precisions.append(precision_score(y_test, y_thresh, zero_division=0))
    recalls.append(recall_score(y_test, y_thresh))

plt.figure(figsize=(8, 5))
plt.plot(thresholds, f1_scores, label="F1 Score", marker="o")
plt.plot(thresholds, precisions, label="Precision", marker="x")
plt.plot(thresholds, recalls, label="Recall", marker="s")
plt.xlabel("Threshold")
plt.ylabel("Score")
plt.title("F1 / Precision / Recall vs Threshold")
plt.legend()
plt.grid(True)

# 保存阈值曲线图
curve_path = os.path.join(basedir, "xgbModel", f"xgb_metrics_vs_threshold_{today}.png")
plt.savefig(curve_path)
plt.close()
print(f"Metrics vs Threshold plot saved to {curve_path}")

# 输出最佳阈值
best_idx = np.argmax(f1_scores)
best_threshold = thresholds[best_idx]
best_f1 = f1_scores[best_idx]
print(f"Best F1: {best_f1:.4f} at threshold = {best_threshold:.2f}")

# =========================
# 保存模型
# =========================
model_path = os.path.join(basedir, "xgbModel", f"xgb_model_{today}.pkl")
with open(model_path, "wb") as f:
    pickle.dump(model, f)
print(f"Model saved to {model_path}")
