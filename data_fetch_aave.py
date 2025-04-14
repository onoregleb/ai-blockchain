import math
import os
import dotenv
import requests
from datetime import datetime, timedelta, time as dt_time # Импортируем time как dt_time
import time as os_time
import pandas as pd
from tqdm import tqdm
import sys
dotenv.load_dotenv()
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY")
if not ETHERSCAN_API_KEY:
    print("Ошибка: ETHERSCAN_API_KEY не найден в переменных окружения.")
    print("Пожалуйста, убедитесь, что у вас есть файл .env с ETHERSCAN_API_KEY=ВАШ_КЛЮЧ")
    sys.exit(1)

TARGET_TOKEN_CONTRACT_ADDRESS = "0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9" # AAVE
DAYS_BACK = 15
API_DELAY = 0.2

END_DATE_DT = datetime.now()
START_DATE_DT = END_DATE_DT - timedelta(days=DAYS_BACK)

def etherscan_request(params):
    """Отправляет запрос к Etherscan API с обработкой ошибок и задержкой."""
    url = "https://api.etherscan.io/api"
    params["apikey"] = ETHERSCAN_API_KEY
    max_retries = 4
    retry_delay = 5

    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            if data.get("status") == "1":
                os_time.sleep(API_DELAY)
                return data["result"]
            elif data.get("status") == "0":
                message = data.get("message", "")
                result_val = data.get("result")

                if "Result window is too large" in message:
                    print(f"\n[Лимит Дня] Предупреждение: Достигнут лимит API Etherscan 10k ({message}) для запроса: {params}. Данные за этот день будут неполными.")
                    os_time.sleep(API_DELAY)
                    return "10k_limit"

                elif "Max rate limit reached" in message:
                     print(f"\nПредупреждение: Достигнут лимит запросов ({message}). Повтор через {retry_delay * (attempt + 2)} сек...")
                     os_time.sleep(retry_delay * (attempt + 2))
                     continue # Переход к следующей попытке

                elif "No transactions found" in message or "No records found" in message:
                    os_time.sleep(API_DELAY)
                    return None

                elif "Invalid address format" in message:
                    print(f"\nПредупреждение: Неверный формат адреса в запросе: {params}")
                    os_time.sleep(API_DELAY)
                    return None

                elif "Query Timeout" in message:
                     print(f"\nПредупреждение: Таймаут запроса Etherscan ({message}). Повтор через {retry_delay * (attempt + 1)} сек...")
                     os_time.sleep(retry_delay * (attempt + 1))
                     continue # Переход к следующей попытке

                else:
                    print(f"\nОшибка API Etherscan (Status 0): {message} | Result: {result_val} | Params: {params}")
                    os_time.sleep(API_DELAY)
                    return None
            else:
                print(f"\nНеожиданный формат ответа API Etherscan: {data}")
                os_time.sleep(API_DELAY)
                return None

        except requests.exceptions.RequestException as e:
            print(f"\nСетевая или HTTP ошибка во время запроса к Etherscan: {e}")
            if attempt < max_retries - 1:
                print(f"Повтор через {retry_delay * (attempt + 1)} секунд...")
                os_time.sleep(retry_delay * (attempt + 1))
            else:
                print("Достигнуто максимальное количество попыток для сетевой/HTTP ошибки. Пропуск запроса.")
                return None
        except Exception as e:
             print(f"\nПроизошла неожиданная ошибка при обработке API запроса: {e}")
             return None

    print("\nНе удалось получить успешный ответ после максимального количества попыток.")
    return None

def datetime_to_block(dt, closest="before"):
    """Конвертирует datetime объект в примерный номер блока Ethereum."""
    params = {
        "module": "block",
        "action": "getblocknobytime",
        "timestamp": int(dt.timestamp()),
        "closest": closest
    }
    os_time.sleep(API_DELAY / 2)
    result = etherscan_request(params)
    if result and result != "10k_limit":
        try:
            return int(result)
        except (ValueError, TypeError):
            print(f"\nОшибка: Не удалось конвертировать результат номера блока '{result}' в целое число для {dt}.")
            return None
    else:
        if result == "10k_limit":
             print(f"\nПредупреждение: Не удалось получить номер блока для {dt} из-за лимита. Результат: {result}")
        return None

def fetch_token_decimals(contract_address):
    """Получает количество десятичных знаков для токена."""
    print(f"Получение информации о токене (десятичные знаки) для {contract_address}...")
    params_tx = {
        "module": "account",
        "action": "tokentx",
        "contractaddress": contract_address,
        "page": 1, "offset": 1, "sort": "desc"
    }
    tx_result = etherscan_request(params_tx)
    if tx_result and tx_result != "10k_limit" and isinstance(tx_result, list) and len(tx_result) > 0:
        try:
            decimals = int(tx_result[0].get('tokenDecimal', '18'))
            print(f"Успешно получены десятичные знаки из транзакции: {decimals}")
            return decimals
        except (ValueError, TypeError, KeyError) as e:
             print(f"\nНе удалось извлечь десятичные знаки из данных транзакции: {e}. Принимаем 18.")
             return 18
    else:
        print(f"\nПредупреждение: Не удалось найти транзакции для токена {contract_address}, чтобы определить десятичные знаки. Принимаем 18.")
        return 18

def fetch_transactions_daily_chunks(contract_address, start_date_dt, end_date_dt):
    """
    Получает транзакции токена, разбивая период на дневные интервалы.
    Возвращает список всех транзакций, множество уникальных адресов и список дат с достигнутым лимитом 10k.
    """
    print(f"\nПолучение транзакций токена {contract_address} по дням за период с {start_date_dt.date()} по {end_date_dt.date()}...")
    all_transactions = []
    unique_addresses = set()
    days_with_10k_limit = []
    total_days = (end_date_dt.date() - start_date_dt.date()).days + 1

    current_date = start_date_dt.date()

    with tqdm(total=total_days, desc="Обработка дней", unit=" день") as pbar_days:
        while current_date <= end_date_dt.date():
            day_start_dt = datetime.combine(current_date, dt_time.min)
            day_end_dt = datetime.combine(current_date, dt_time.max)


            day_start_block = datetime_to_block(day_start_dt, closest="before")
            day_end_block = datetime_to_block(day_end_dt, closest="before")

            if day_start_block is None or day_end_block is None or day_end_block < day_start_block:
                print(f"\nПредупреждение: Не удалось определить корректные блоки для даты {current_date}. Пропуск этого дня.")
                current_date += timedelta(days=1)
                pbar_days.update(1)
                continue

            page = 1
            offset = 1000
            daily_tx_count = 0
            hit_limit_today = False

            while True:
                params = {
                    "module": "account", "action": "tokentx",
                    "contractaddress": contract_address,
                    "startblock": day_start_block, "endblock": day_end_block,
                    "page": page, "offset": offset, "sort": "asc"
                }
                transactions_page = etherscan_request(params)

                if transactions_page == "10k_limit":
                    hit_limit_today = True
                    days_with_10k_limit.append(current_date)
                    print(f"-> Лимит 10k достигнут для {current_date} на странице {page}.")
                    break

                if not transactions_page or not isinstance(transactions_page, list):
                    break

                page_added_count = 0
                for tx in transactions_page:
                     if isinstance(tx, dict) and tx.get("contractAddress", "").lower() == contract_address.lower():
                        try:
                            timestamp = int(tx["timeStamp"])
                            tx_time = datetime.fromtimestamp(timestamp)
                            if day_start_dt <= tx_time <= day_end_dt:
                                all_transactions.append(tx)
                                page_added_count += 1
                                sender = tx.get("from")
                                receiver = tx.get("to")
                                if sender and sender != "0x0000000000000000000000000000000000000000":
                                    unique_addresses.add(sender)
                                if receiver and receiver != "0x0000000000000000000000000000000000000000":
                                    unique_addresses.add(receiver)
                        except (ValueError, TypeError, KeyError):
                            continue

                daily_tx_count += page_added_count

                if len(transactions_page) < offset:
                    break

                page += 1
                if page > 15:
                    print(f"\nПредупреждение: Достигнуто >15 страниц для дня {current_date}. Принудительный выход из пагинации дня.")
                    hit_limit_today = True # Считаем это как потенциальный лимит
                    if current_date not in days_with_10k_limit:
                         days_with_10k_limit.append(current_date)
                    break

            current_date += timedelta(days=1)
            pbar_days.update(1)

    print(f"\n--- Завершено получение транзакций по дням. ---")
    print(f"Всего найдено транзакций за период: {len(all_transactions)}")
    print(f"Всего найдено уникальных адресов: {len(unique_addresses)}")
    if days_with_10k_limit:
        print(f"Предупреждение: Лимит Etherscan в 10,000 транзакций был достигнут для следующих дат:")
        for dt in days_with_10k_limit:
            print(f"- {dt.strftime('%Y-%m-%d')}")
        print("Данные за эти дни могут быть неполными.")
    else:
        print("Лимит Etherscan в 10,000 транзакций за день не был достигнут.")

    return all_transactions, list(unique_addresses), days_with_10k_limit


def calculate_period_metrics(address, all_period_transactions, token_decimals, start_dt, end_dt):
    """
    Рассчитывает метрики для ОДНОГО адреса на основе списка ВСЕХ транзакций за период.
    (Эта функция остается без изменений по логике расчета)
    """
    metrics = {
        "address": address,
        "period_total_tx_count": 0, "period_incoming_tx_count": 0, "period_outgoing_tx_count": 0,
        "period_total_volume_in": 0.0, "period_total_volume_out": 0.0,
        "period_avg_volume_in": 0.0, "period_avg_volume_out": 0.0,
        "period_unique_counterparties": 0,
        "period_first_tx_date": None, "period_last_tx_date": None, "period_active_days": 0,
        "current_link_balance": 0.0
    }

    address_lower = address.lower()
    address_transactions = []
    timestamps_for_address = []
    incoming_volumes = []
    outgoing_volumes = []
    counterparties = set()

    for tx in all_period_transactions:
        sender = tx.get("from", "").lower()
        receiver = tx.get("to", "").lower()
        if sender == address_lower or receiver == address_lower:
            # Дополнительно убедимся, что транзакция точно в периоде (на случай неточности при сборе)
            try:
                 timestamp = int(tx["timeStamp"])
                 tx_time = datetime.fromtimestamp(timestamp)
                 if start_dt <= tx_time <= end_dt:
                     address_transactions.append(tx)
                     timestamps_for_address.append(timestamp)
            except (ValueError, TypeError, KeyError):
                continue

    metrics["period_total_tx_count"] = len(address_transactions)
    if not address_transactions:
        raw_balance = fetch_token_balance(address, TARGET_TOKEN_CONTRACT_ADDRESS)
        metrics["current_link_balance"] = raw_balance / (10 ** token_decimals) if token_decimals else 0.0
        return metrics

    timestamps_for_address.sort()
    metrics["period_first_tx_date"] = datetime.fromtimestamp(timestamps_for_address[0])
    metrics["period_last_tx_date"] = datetime.fromtimestamp(timestamps_for_address[-1])

    unique_days = set(datetime.fromtimestamp(ts).date() for ts in timestamps_for_address)
    metrics["period_active_days"] = len(unique_days)

    for tx in address_transactions:
        try:
            value_raw = int(tx.get("value", '0'))
            value_adjusted = value_raw / (10 ** token_decimals)
        except (ValueError, TypeError): value_adjusted = 0.0

        sender = tx.get("from", "").lower()
        receiver = tx.get("to", "").lower()
        if sender == address_lower:
            metrics["period_outgoing_tx_count"] += 1
            outgoing_volumes.append(value_adjusted)
            if receiver != address_lower: counterparties.add(receiver)
        elif receiver == address_lower:
            metrics["period_incoming_tx_count"] += 1
            incoming_volumes.append(value_adjusted)
            if sender != address_lower: counterparties.add(sender)

    metrics["period_total_volume_in"] = sum(incoming_volumes)
    metrics["period_total_volume_out"] = sum(outgoing_volumes)
    metrics["period_avg_volume_in"] = metrics["period_total_volume_in"] / metrics["period_incoming_tx_count"] if metrics["period_incoming_tx_count"] > 0 else 0.0
    metrics["period_avg_volume_out"] = metrics["period_total_volume_out"] / metrics["period_outgoing_tx_count"] if metrics["period_outgoing_tx_count"] > 0 else 0.0
    metrics["period_unique_counterparties"] = len(counterparties)

    raw_balance = fetch_token_balance(address, TARGET_TOKEN_CONTRACT_ADDRESS)
    metrics["current_link_balance"] = raw_balance / (10 ** token_decimals) if token_decimals else 0.0
    return metrics

def fetch_token_balance(address, contract_address):
    """Получает текущий баланс токена ERC-20 для адреса."""
    params = {
        "module": "account", "action": "tokenbalance",
        "contractaddress": contract_address, "address": address, "tag": "latest"
    }
    result = etherscan_request(params)
    if result and result != "10k_limit":
         try: return int(result)
         except (ValueError, TypeError): return 0
    else: return 0

if __name__ == "__main__":
    print("--- Запуск анализа транзакций токена ERC-20 (по дням) ---")
    print(f"Токен: LINK ({TARGET_TOKEN_CONTRACT_ADDRESS})")
    print(f"Анализируемый период: последние {DAYS_BACK} дней")
    print(f"Начало периода: {START_DATE_DT.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Конец периода: {END_DATE_DT.strftime('%Y-%m-%d %H:%M:%S')}")
    print("-" * 60)


    token_decimals = fetch_token_decimals(TARGET_TOKEN_CONTRACT_ADDRESS)
    print("-" * 60)

    all_transactions, unique_addresses, days_hit_limit = fetch_transactions_daily_chunks(
        TARGET_TOKEN_CONTRACT_ADDRESS, START_DATE_DT, END_DATE_DT
    )

    if not unique_addresses:
        print("\nНе найдено адресов, взаимодействовавших с токеном в указанный период. Выход.")
        sys.exit(0)

    print(f"\nНайдено {len(unique_addresses)} уникальных адресов для анализа.")
    addresses_to_process = unique_addresses # Обрабатываем все найденные адреса
    print("-" * 60)

    all_wallet_metrics = []
    print(f"\n--- Расчет метрик для {len(addresses_to_process)} адресов ---")
    for address in tqdm(addresses_to_process, desc="Обработка кошельков", unit=" кошелек"):
        metrics = calculate_period_metrics(address, all_transactions, token_decimals, START_DATE_DT, END_DATE_DT)
        if metrics:
             all_wallet_metrics.append(metrics)

    print("\n--- Завершен расчет метрик ---")
    print("-" * 60)

    if not all_wallet_metrics:
        print("Нет данных для сохранения в CSV.")
    else:
        df = pd.DataFrame(all_wallet_metrics)
        column_order = [
            "address", "current_link_balance",
            "period_total_tx_count", "period_incoming_tx_count", "period_outgoing_tx_count",
            "period_total_volume_in", "period_total_volume_out", "period_avg_volume_in", "period_avg_volume_out",
            "period_unique_counterparties", "period_active_days",
            "period_first_tx_date", "period_last_tx_date",
        ]
        df = df.reindex(columns=[col for col in column_order if col in df.columns])
        token_symbol = "LINK"
        timestamp_str = END_DATE_DT.strftime("%Y%m%d_%H%M%S")
        filename = f"link_wallet_activity_{DAYS_BACK}d_daily_{timestamp_str}.csv"

        try:
            df.to_csv(filename, index=False, date_format='%Y-%m-%d %H:%M:%S', float_format='%.8f')
            print(f"\nДанные успешно сохранены в файл: {filename}")
            print(f"Сохранено {len(df)} строк.")
            if days_hit_limit:
                print("\n*** ВАЖНОЕ ПРЕДУПРЕЖДЕНИЕ ***")
                print("Из-за достижения лимита Etherscan в 10,000 транзакций для некоторых дней,")
                print("общий список транзакций и рассчитанные метрики могут быть НЕПОЛНЫМИ.")
                print("Даты с потенциально неполными данными:")
                for dt in days_hit_limit: print(f"- {dt.strftime('%Y-%m-%d')}")
                print("****************************")

        except Exception as e:
            print(f"\nОшибка при сохранении данных в CSV: {e}")

    print("\n--- Скрипт завершен ---")