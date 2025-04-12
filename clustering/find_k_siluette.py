import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
import matplotlib.pyplot as plt
import seaborn as sns

# --- Загрузка данных ---
data = pd.read_csv(r"C:\Users\Gleb Onore\Desktop\ai_blockchain\ethereum_0x514910771AF9Ca656af840dff83E8264EcF986CA_clustering_dataset.csv")
print(data.head())

# --- Предобработка данных ---

# Преобразование дат в timestamp
data['first_token_tx_date'] = pd.to_datetime(data['first_token_tx_date'], errors='coerce')
data['last_token_tx_date'] = pd.to_datetime(data['last_token_tx_date'], errors='coerce')
data['first_token_tx_date_ts'] = data['first_token_tx_date'].apply(lambda x: x.timestamp() if pd.notnull(x) else None)
data['last_token_tx_date_ts'] = data['last_token_tx_date'].apply(lambda x: x.timestamp() if pd.notnull(x) else None)

# Преобразование флагов в числовые значения
completeness_mapping = {
    'full': 1.0,
    'partial_10k_limit': 0.0
}
data['data_completeness'] = data['data_completeness'].map(completeness_mapping)

# Список признаков
features = [
    'token_balance',
    'data_completeness',
    'token_tx_count',
    'token_active_days',
    'avg_token_tx_frequency',
    'holding_period',
    'incoming_token_tx_count',
    'outgoing_token_tx_count',
    'avg_incoming_token_volume',
    'avg_outgoing_token_volume',
    'unique_token_counterparties',
    'first_token_tx_date_ts',
    'last_token_tx_date_ts',
    'token_interactions'
]

# Удаление пропусков
data = data.dropna(subset=features)

# Масштабирование признаков
scaler = StandardScaler()
scaled_features = scaler.fit_transform(data[features])

# --- Silhouette Score для определения количества кластеров ---
silhouette_scores = []
k_values = range(2, 11)  # silhouette score применим от k=2

for k in k_values:
    kmeans = KMeans(n_clusters=k, random_state=42)
    labels = kmeans.fit_predict(scaled_features)
    score = silhouette_score(scaled_features, labels)
    silhouette_scores.append(score)

# Визуализация
plt.figure(figsize=(8, 5))
plt.plot(k_values, silhouette_scores, marker='o')
plt.title('Silhouette Score для определения количества кластеров')
plt.xlabel('Количество кластеров')
plt.ylabel('Silhouette Score')
plt.show()
