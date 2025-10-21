# Bilibili Comment Analysis & New Word Discovery System

A comprehensive system for collecting, analyzing, and discovering new words from Bilibili comments — powered by Playwright, XGBoost, and Flask for continuous model improvement.


## 🎥 Demo Video
*Demo video not yet uploaded — coming soon*


## ⚙️ Environment Setup

### Prerequisites
- Python 3.7+
- Chrome/Chromium browser
- SQLite (auto-created automatically)

## Installation Steps
### 1. Clone the repository
```
git clone <https://github.com/Ray-gyr/Bilibili-New-Internet-Slang-Discovery-System.git>
cd bilibili-new-word-discovery
```
### 2. Create and activate virtual environment
```
python -m venv venv
source venv/bin/activate      # On Windows: venv\Scripts\activate
```
### 3. Install Python dependencies
```
pip install -r requirements.txt
```
### 4. Install Playwright browser
```
playwright install chromium
Initialize Database
```

## 🧠Model Training Summary

The model was trained in two iterative phases, both validated using 5-fold cross-validation. Hyperparameter optimization was conducted via Optuna for the XGBoost model.

**Performance Comparison**
| Phase   | Samples | Accuracy | Recall | F1-score |
|---------|--------|---------|--------|----------|
| Phase 1 | 555    | 0.955   | 0.95   | 0.93     |
| Phase 2 | 1,205  | 0.851   | 0.72   | 0.70     |
---
### Phase 1 — Foundation Model (5-Fold Cross Validation)
- Training data consists of **555 obviously true / obviously false words** to establish a reliable baseline

**Performance Metrics (5-Fold Cross Validation**):

Accuracy: 0.9550
Classification Report:
```

              precision    recall  f1-score   support
           0       0.99      0.96      0.97        89
           1       0.84      0.95      0.89        22
    accuracy                           0.95       111
   macro avg       0.91      0.95      0.93       111
weighted avg       0.96      0.95      0.96       111

Confusion Matrix:
[[85  4]
[ 1 21]]
```
### Phase 2 — Enhanced Model (Extended Dataset)
- Phase 2 expands training data by using the **Phase 1 model to identify borderline candidates** with confidence around 0.5.  
- These uncertain words were **manually labeled** to enlarge the dataset to **1,205 samples**, improving generalization.

**Performance Metrics (5-Fold Cross Validation)**:

Accuracy: 0.8506
Classification Report:
   ```

	             precision    recall  f1-score   support
	          0       0.93      0.90      0.91       209
	          1       0.45      0.53      0.49        32
	   accuracy                           0.85       241
	  macro avg       0.69      0.72      0.70       241
weighted avg    0.86      0.85      0.86       241

Confusion Matrix:
[[188  21]
[ 15 17]]
```
**Optimized XGBoost Parameters (via Optuna)**
```
{
  "n_estimators": 71,
  "max_depth": 6,
  "learning_rate": 0.09006,
  "subsample": 0.60191,
  "colsample_bytree": 0.86900,
  "scale_pos_weight": 7.9461,
  "min_child_weight": 4
}
```
**Optimal Threshold Selection (0.27)**

The threshold of 0.27 represents a tradeoff between F1-score and Recall. Since the system prioritizes catching all potential slang words, higher recall is crucial, while maintaining a reasonable F1-score.

![F1-score and Recall vs Classification Threshold](image/xgb_metrics_vs_threshold_v2.png)

📊 Model Feature Importance

![Weight of each feature in the XGBoost model](image/XGBoost_Feature_Importance_v2.png)

## 🌐 Web Interface Overview
**Running the Website Locally**
```
$env:FLASK_APP = "Webapp.app.py"
$env:FLASK_ENV = "development"
flask run
```
**Main Interface**
- Trending Terms Cloud: Shows popular slang with frequency-based sizing

- Dictionary Search: Search validated slang entries

**Annotation Interface (Logged-in Users)**
- Batch Annotation: Presents candidate term

- Context Display: Shows example sentences and usage contexts

- Model Transparency: Displays model scores and extracted features

- Progress Tracking: Tracks daily & total annotations

**Administrator Review Panel**
- Login: Default account → admin / Asdf1234

- Review Queue: View terms with enough annotations

- Vote Statistics: Inspect yes/no ratios and individual votes

- Batch Approval: Approve/reject multiple entries at once

- Quality Control: Originally required 3+ human annotations per term (set to 1+ in demo)
