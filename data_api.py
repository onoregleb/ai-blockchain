import os
import dotenv
import requests
from datetime import datetime, timedelta
import time
import pandas as pd
from tqdm import tqdm

dotenv.load_dotenv()

ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY")
MAX_ADDRESSES = 10000
DAYS_BACK = 90

# Рассчет временного диапазона
END_DATE = datetime.now()
START_DATE = END_DATE - timedelta(days=DAYS_BACK)


def datetime_to_block(dt):
    """Конвертирует дату в примерный номер блока через Etherscan API"""
    url = "https://api.etherscan.io/api"
    params = {
        "module": "block",
        "action": "getblocknobytime",
        "timestamp": int(dt.timestamp()),
        "closest": "before",
        "apikey": ETHERSCAN_API_KEY
    }
    response = requests.get(url, params=params)
    return int(response.json()["result"]) if response.ok and response.json()["status"] == "1" else None


def fetch_balance(address):
    """Получает баланс ETH для адреса"""
    url = "https://api.etherscan.io/api"
    params = {
        "module": "account",
        "action": "balance",
        "address": address,
        "tag": "latest",
        "apikey": ETHERSCAN_API_KEY
    }
    response = requests.get(url, params=params)
    if response.ok and response.json()["status"] == "1":
        return int(response.json()["result"]) / 1e18  # Конвертация из wei в ETH
    return 0.0


def fetch_token_transactions(address):
    """Получает транзакции с токенами для адреса"""
    url = "https://api.etherscan.io/api"
    params = {
        "module": "account",
        "action": "tokentx",
        "address": address,
        "startblock": 0,
        "endblock": 99999999,
        "sort": "asc",
        "apikey": ETHERSCAN_API_KEY
    }
    response = requests.get(url, params=params)
    if response.ok and response.json()["status"] == "1":
        return response.json()["result"]
    return []


def fetch_active_addresses():
    """Получает список активных адресов через анализ блоков"""
    start_block = datetime_to_block(START_DATE)
    end_block = datetime_to_block(END_DATE)

    if not start_block or not end_block:
        return []

    addresses = set()
    current_block = start_block

    with tqdm(desc="Сбор активных адресов", unit=" блоков") as pbar:
        while current_block <= end_block and len(addresses) < MAX_ADDRESSES:
            # Получаем блок
            url = "https://api.etherscan.io/api"
            params = {
                "module": "proxy",
                "action": "eth_getBlockByNumber",
                "tag": hex(current_block),
                "boolean": "true",  # Включить транзакции
                "apikey": ETHERSCAN_API_KEY
            }
            response = requests.get(url, params=params)
            if response.ok and "result" in response.json():
                block = response.json()["result"]
                transactions = block.get("transactions", [])

                for tx in transactions:
                    if isinstance(tx, dict):  # Проверка формата транзакции
                        if "from" in tx:
                            addresses.add(tx["from"])
                        if "to" in tx:
                            addresses.add(tx["to"])

                        # Остановка при достижении лимита
                        if len(addresses) >= MAX_ADDRESSES:
                            break

            current_block += 1
            pbar.update(1)
            time.sleep(0.1)  # Задержка для соблюдения лимитов API

    return list(addresses)[:MAX_ADDRESSES]


def get_wallet_metrics(address):
    """Собирает метрики для одного кошелька"""
    metrics = {
        "address": address,  # Адрес кошелька
        "balance": 0.0,  # Баланс
        "tx_count": 0,  # Общее количество транзакций
        "active_days": 0,  # Количество активных дней
        "token_interactions": 0,  # Количество взаимодейтсвий с токенами
        "avg_tx_frequency": 0.0,  # средняя частота транзакций/день
        "holding_period": 0.0,  #  Время холда
        "incoming_tx_count": 0,  # Количество входящих транзакций
        "outgoing_tx_count": 0,  # Количество исходящих транзакций
        "avg_incoming_volume": 0.0,  # Средний объем входящих транзакций
        "avg_outgoing_volume": 0.0,  # Средний объем исходящих транзакций
        "unique_counterparties": 0,  # Количество уникальных контрагентов
    }

    url = "https://api.etherscan.io/api"
    params = {
        "module": "account",
        "action": "txlist",
        "address": address,
        "startblock": 0,
        "endblock": 99999999,
        "sort": "asc",
        "apikey": ETHERSCAN_API_KEY
    }
    response = requests.get(url, params=params)

    if response.ok:
        data = response.json()
        if data.get("status") == "1":
            txs = data.get("result", [])
        else:
            print(f"Ошибка API для адреса {address}: {data.get('message')}")
            txs = []
    else:
        txs = []

    # Фильтрация по времени
    filtered_txs = []
    for tx in txs:
        if isinstance(tx, dict) and "timeStamp" in tx:
            try:
                tx_time = datetime.fromtimestamp(int(tx["timeStamp"]))
                if START_DATE <= tx_time <= END_DATE:
                    filtered_txs.append(tx)
            except (ValueError, TypeError):
                continue

    if not filtered_txs:
        return None

    # Расчет метрик
    timestamps = [int(tx["timeStamp"]) for tx in filtered_txs]
    first_activity = datetime.fromtimestamp(min(timestamps))
    last_activity = datetime.fromtimestamp(max(timestamps))

    metrics["tx_count"] = len(filtered_txs)
    metrics["active_days"] = (last_activity - first_activity).days
    metrics["avg_tx_frequency"] = metrics["tx_count"] / ((last_activity - first_activity).days or 1)
    metrics["holding_period"] = (END_DATE - first_activity).days

    # Баланс
    metrics["balance"] = fetch_balance(address)

    # Взаимодействие с токенами
    token_txs = fetch_token_transactions(address)
    metrics["token_interactions"] = len(token_txs)

    # --- Новые метрики ---
    incoming_volumes = []  # Объемы входящих транзакций
    outgoing_volumes = []  # Объемы исходящих транзакций
    counterparties = set()  # Уникальные контрагенты

    for tx in filtered_txs:
        value = int(tx.get("value", 0)) / 1e18  # Конвертация из wei в ETH
        sender = tx.get("from", "").lower()
        receiver = tx.get("to", "").lower()

        if sender == address.lower():
            # Исходящая транзакция
            metrics["outgoing_tx_count"] += 1
            outgoing_volumes.append(value)
            counterparties.add(receiver)
        elif receiver == address.lower():
            # Входящая транзакция
            metrics["incoming_tx_count"] += 1
            incoming_volumes.append(value)
            counterparties.add(sender)

    # Подсчет средних объемов
    metrics["avg_incoming_volume"] = sum(incoming_volumes) / len(incoming_volumes) if incoming_volumes else 0.0
    metrics["avg_outgoing_volume"] = sum(outgoing_volumes) / len(outgoing_volumes) if outgoing_volumes else 0.0

    # Количество уникальных контрагентов
    metrics["unique_counterparties"] = len(counterparties)

    return metrics


# --- Основной поток выполнения ---
if __name__ == "__main__":
    # 1. Сбор активных адресов
    active_addresses = fetch_active_addresses()
    print(f"Найдено {len(active_addresses)} активных адресов")

    # 2. Сбор данных
    wallet_data = []
    for address in tqdm(active_addresses, desc="Обработка кошельков"):
        metrics = get_wallet_metrics(address)
        if metrics:
            wallet_data.append(metrics)
        time.sleep(0.1)  # Соблюдение лимитов API

    # 3. Сохранение данных
    df = pd.DataFrame(wallet_data)
    df.to_csv("ethereum_clustering_dataset.csv", index=False)
    print("Данные сохранены в ethereum_clustering_dataset.csv")