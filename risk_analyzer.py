import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest

# 파일 불러오기
df_w1 = pd.read_csv("user_stats_week1.csv")
df_w2 = pd.read_csv("user_stats_week2.csv")

# 사용할 특성
features = [
    "total_outings",
    "avg_outing_time",
    "avg_sleep_time",
    "avg_intermediate_awakenings",
    "avg_pir",
    "avg_radar"
]

# 사용자 기준 정렬 및 인덱싱
week1 = df_w1.set_index("user_name")[features].sort_index()
week2 = df_w2.set_index("user_name")[features].sort_index()

# feature별 평균과 표준편차 계산
mu = week1.mean(axis=0)     # 각 feature의 평균
sigma = week1.std(axis=0)   # 각 feature의 표준편차

# Z-score 계산
Z1 = (week1 - mu) / sigma
Z2 = (week2 - mu) / sigma

# 변화량 계산
D = Z2 - Z1
D.columns = [f"{col}_delta" for col in D.columns]

# Isolation Forest 입력 구성
X_input = pd.concat([Z2, D], axis=1)

# Isolation Forest 모델 학습 및 예측
model = IsolationForest(n_estimators=300, contamination="auto", random_state=42)
model.fit(X_input)
risk_score = model.decision_function(X_input)
anomaly_score = model.predict(X_input)
risk_label = np.where(anomaly_score == -1, "HighRisk", "Normal")

# 결과 정리
X_result = X_input.copy()
X_result["risk_score"] = risk_score
X_result["anomaly_score"] = anomaly_score
X_result["risk_label"] = risk_label
X_result = X_result.reset_index().round(2)

# 결과 저장 및 출력
X_result.to_csv("zscore_feature_based_risk_analysis.csv", index=False)
print("위험군 분석 완료: zscore_feature_based_risk_analysis.csv")
print(X_result[["user_name", "risk_score", "anomaly_score", "risk_label"]].sort_values(by="risk_score"))