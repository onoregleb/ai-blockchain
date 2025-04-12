#k=8

import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
from sklearn.metrics import silhouette_score
import seaborn as sns

# --- Загрузка данных ---
# Загружаем CSV файл с данными о кошельках
data = pd.read_csv("ethereum_clustering_dataset.csv")

# Проверяем данные
print(data.head())

# --- Предобработка данных ---
# Выбираем числовые признаки для кластеризации
features = ['balance', 'tx_count', 'active_days', 'token_interactions', 'avg_tx_frequency', 'holding_period', 'incoming_tx_count', 'outgoing_tx_count', 'avg_incoming_volume', 'avg_outgoing_volume', 'unique_counterparties']

# Удаляем строки с пропущенными значениями (если есть)
data = data.dropna(subset=features)

# Масштабируем данные, чтобы все признаки имели одинаковый масштаб
scaler = StandardScaler()
scaled_features = scaler.fit_transform(data[features])

silhouette_scores = []
k_values = range(2, 11)  # Минимум 2 кластера для силуэта

for k in k_values:
    kmeans = KMeans(n_clusters=k, random_state=42)
    labels = kmeans.fit_predict(scaled_features)
    score = silhouette_score(scaled_features, labels)
    silhouette_scores.append(score)

# Визуализация
plt.figure(figsize=(8, 5))
plt.plot(k_values, silhouette_scores, marker='o')
plt.title('Метод силуэта для выбора количества кластеров')
plt.xlabel('Количество кластеров')
plt.ylabel('Силуэтный коэффициент')
plt.show()
