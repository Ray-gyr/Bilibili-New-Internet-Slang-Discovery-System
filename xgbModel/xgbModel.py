import pickle
import pandas as pd
import os
import json

XGB_PATH=os.path.join(os.path.dirname(os.path.abspath(__file__)),"xgb_model_v2.pkl")

class xgbModel:
    def __init__(self, model_path=XGB_PATH):
        with open(model_path, "rb") as f:
            self.model = pickle.load(f)

        self.feature_names = {'Length','log_freq','PMI','LeftEnt','RightEnt','tfidf','hot_video_ratio'}
        self.filtered_candidates = []

    def predict(self, candidates, threshold=0.5):
        features = [{k: v for k, v in c.items() if k in self.feature_names} for c in candidates]
        X_new = pd.DataFrame(features)
        threshold = float(threshold)
        y_proba = self.model.predict_proba(X_new)[:, 1]
        y_pred = (y_proba >= threshold).astype(int)
        

        for i, c in enumerate(candidates):
            machine_score=float(f"{y_proba[i]:.2f}")
            if machine_score <=0.1:
                continue
            
            c['status'] = 'pending'
            c['machine_label'] = int(y_pred[i])
            c['machine_score'] = machine_score
            self.filtered_candidates.append(c)
    
    def return_tuple_list(self):
        words_list = []
        for c in self.filtered_candidates:
            words_list.append((
                c['word'],
                json.dumps(c['sample']),
                c['log_freq'],
                c['PMI'],
                c['tfidf'],
                c['LeftEnt'],
                c['RightEnt'],
                c['hot_video_ratio'],
                c['machine_score'],
                c['machine_label'],
                c['status']
            ))
        return words_list
    

        
        
    
