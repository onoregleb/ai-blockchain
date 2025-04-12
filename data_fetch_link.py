import math
import os
import dotenv
import requests
from datetime import datetime, timedelta
import time
import pandas as pd
from tqdm import tqdm

dotenv.load_dotenv()

ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY")
if not ETHERSCAN_API_KEY:
    raise ValueError("ETHERSCAN_API_KEY not found in environment variables.")

TARGET_TOKEN_CONTRACT_ADDRESS = "0x514910771AF9Ca656af840dff83E8264EcF986CA" # LINK


MAX_ADDRESSES = 5000 #ограничение по адресам (1k/час примерно)
DAYS_BACK = 90
API_DELAY = 0.21 # delay для api запросов

END_DATE = datetime.now()
START_DATE = END_DATE - timedelta(days=DAYS_BACK)

def etherscan_request(params):
    """Makes a request to the Etherscan API with error handling and delay."""
    url = "https://api.etherscan.io/api"
    params["apikey"] = ETHERSCAN_API_KEY
    max_retries = 3
    retry_delay = 5 # seconds

    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params)
            response.raise_for_status() # отлов ошибок
            data = response.json()

            if data.get("status") == "1":
                time.sleep(API_DELAY)
                return data["result"]
            elif data.get("status") == "0":
                message = data.get("message", "")
                if "Result window is too large" in message:
                    print(f"Warning: Etherscan API limit reached (10k results) for query: {params}. Partial data may result.")
                    time.sleep(API_DELAY)
                    return "10k_limit"
                elif "No transactions found" in message or \
                     "No records found" in message or \
                     "Invalid address format" in message:
                     time.sleep(API_DELAY)
                     return None
                else:
                    print(f"Etherscan API Error: {message} | Result: {data.get('result')} | Params: {params}")
                    return None
            else:
                print(f"Unexpected Etherscan API response format: {data}")
                return None # Unexpected format

        except requests.exceptions.RequestException as e:
            print(f"Network or HTTP Error during Etherscan request: {e}")
            if attempt < max_retries - 1:
                print(f"Retrying in {retry_delay * (attempt + 1)} seconds...")
                time.sleep(retry_delay * (attempt + 1))
            else:
                print("Max retries reached. Skipping request.")
                return None # Max retries failed
        except Exception as e:
             print(f"An unexpected error occurred during API request: {e}")
             return None # Catch any other unexpected error

    return None


def datetime_to_block(dt):
    """Converts a datetime object to an approximate Ethereum block number."""
    params = {
        "module": "block",
        "action": "getblocknobytime",
        "timestamp": int(dt.timestamp()),
        "closest": "before"
    }
    result = etherscan_request(params)
    return int(result) if result and result != "10k_limit" else None


def fetch_token_info(contract_address):
    """Fetches token information (like decimals)"""
    print(f"Attempting to fetch decimals for {contract_address}...")
    params = {
        "module": "account",
        "action": "tokentx",
        "contractaddress": contract_address,
        "page": 1,
        "offset": 1,
        "sort": "desc"
    }
    result = etherscan_request(params)
    if result and result != "10k_limit" and isinstance(result, list) and len(result) > 0:
        try:
            decimals = int(result[0].get('tokenDecimal', 18))
            print(f"Successfully fetched decimals: {decimals}")
            return decimals
        except (ValueError, TypeError, KeyError) as e:
             print(f"Could not parse decimals from transaction data: {e}")
             return 18 # Fallback
    else:
        print(f"Warning: Could not find any transactions for token {contract_address} to determine decimals. Assuming 18.")
        return 18


def fetch_token_balance(address, contract_address):
    """Fetches the balance of a specific ERC-20 token for an address."""
    params = {
        "module": "account",
        "action": "tokenbalance",
        "contractaddress": contract_address,
        "address": address,
        "tag": "latest"
    }
    result = etherscan_request(params)
    return int(result) if result and result != "10k_limit" else 0


def fetch_active_addresses_for_token(contract_address, start_block, end_block):
    """Fetches addresses that transferred the specific token within the block range."""
    print(f"Fetching active addresses for token {contract_address} from block {start_block} to {end_block}...")
    addresses = set()
    page = 1
    offset = 1000
    fetched_count = 0

    max_page = math.ceil(10000 / offset) + 1

    with tqdm(desc="Fetching token transfers", unit=" addresses") as pbar:
        while len(addresses) < MAX_ADDRESSES and page <= max_page:
            params = {
                "module": "account",
                "action": "tokentx",
                "contractaddress": contract_address,
                "startblock": start_block,
                "endblock": end_block,
                "page": page,
                "offset": offset,
                "sort": "asc"
            }
            transactions = etherscan_request(params)

            if transactions == "10k_limit":
                 print(f"\nWarning: Hit 10k transaction limit while fetching addresses for token {contract_address}. May not find all addresses.")
                 break

            if not transactions or not isinstance(transactions, list):
                if transactions is None:
                    print(f"\nNo more token transactions found for page {page} or error occurred.")
                    break
                elif transactions is not None:
                     print(f"\nUnexpected data type received while fetching addresses: {type(transactions)}. Stopping.")
                     break

            if not transactions and isinstance(transactions, list):
                 print(f"\nNo token transactions found on page {page}. Stopping.")
                 break

            current_page_count = 0
            for tx in transactions:
                 if isinstance(tx, dict):
                     sender = tx.get("from")
                     receiver = tx.get("to")
                     addr_added = False
                     if sender and sender != "0x0000000000000000000000000000000000000000":
                        if len(addresses) < MAX_ADDRESSES and sender not in addresses:
                           addresses.add(sender)
                           addr_added = True

                     if receiver and len(addresses) < MAX_ADDRESSES and receiver not in addresses:
                        if not addr_added or sender != receiver:
                           addresses.add(receiver)
                           addr_added = True

                     if addr_added:
                           pbar.update(1)
                           fetched_count += 1
                           current_page_count +=1


                 if len(addresses) >= MAX_ADDRESSES:
                     print(f"\nReached MAX_ADDRESSES limit ({MAX_ADDRESSES}).")
                     break

            if len(addresses) >= MAX_ADDRESSES:
                 break

            print(f"\nFetched page {page}, added {current_page_count} new addresses (Total unique: {len(addresses)}).")

            if len(transactions) < offset:
                 print("\nReached end of token transaction results.")
                 break

            page += 1
            if page > 1000:
                print("\nWarning: Reached arbitrary maximum page limit (1000). Stopping address fetch.")
                break

    return list(addresses)


def get_wallet_metrics_for_token(address, contract_address, token_decimals, start_block_for_period, end_block_for_period):
    """Collects metrics for a single wallet based on its interaction with a specific token."""
    metrics = {
        "address": address, "token_balance": 0.0, "token_tx_count": 0,
        "token_active_days": 0, "token_interactions": 0, "avg_token_tx_frequency": 0.0,
        "holding_period": 0.0, "incoming_token_tx_count": 0, "outgoing_token_tx_count": 0,
        "avg_incoming_token_volume": 0.0, "avg_outgoing_token_volume": 0.0,
        "unique_token_counterparties": 0, "first_token_tx_date": None,
        "last_token_tx_date": None, "data_completeness": "full"
    }

    first_tx_params = {
        "module": "account", "action": "tokentx", "address": address,
        "contractaddress": contract_address, "startblock": 0, "endblock": 99999999,
        "page": 1, "offset": 1, "sort": "asc"
    }
    first_tx_result = etherscan_request(first_tx_params)
    first_activity_overall_ts = None
    if first_tx_result and first_tx_result != "10k_limit" and isinstance(first_tx_result, list) and len(first_tx_result) > 0:
        try:
            first_activity_overall_ts = int(first_tx_result[0]["timeStamp"])
            metrics["first_token_tx_date"] = datetime.fromtimestamp(first_activity_overall_ts)
            metrics["holding_period"] = max(0, (END_DATE - metrics["first_token_tx_date"]).days)
        except (ValueError, TypeError, KeyError, IndexError) as e:
            print(f"Warning: Could not parse timestamp from first tx data for {address}: {e}")
            metrics["first_token_tx_date"] = None
            metrics["holding_period"] = -1

    all_token_txs_in_period = []
    page = 1
    offset = 1000
    max_page = math.ceil(10000 / offset) + 1
    hit_10k_limit_in_period = False


    while page <= max_page:
        params = {
            "module": "account", "action": "tokentx", "address": address,
            "contractaddress": contract_address,
            "startblock": start_block_for_period,
            "endblock": end_block_for_period,
            "page": page, "offset": offset, "sort": "asc",
        }
        txs = etherscan_request(params)

        if txs == "10k_limit":
             hit_10k_limit_in_period = True
             metrics["data_completeness"] = "partial_10k_limit" # Mark data as incomplete
             print(f"Warning: Hit 10k transaction limit for address {address} within the {DAYS_BACK}-day period. Activity metrics will be incomplete.")
             break # Stop fetching more pages

        if txs and isinstance(txs, list):
            all_token_txs_in_period.extend(txs)
            if len(txs) < offset:
                break
            page += 1
        else:
             if txs is None:
                 if page > 1: print(f"Stopped fetching period txs for {address} due to error or no further data.")
             else:
                 print(f"Stopped fetching period txs for {address} due to unexpected data type: {type(txs)}")
             break

    filtered_txs = []
    for tx in all_token_txs_in_period:
        if isinstance(tx, dict) and "timeStamp" in tx and "contractAddress" in tx:
             if tx.get("contractAddress", "").lower() == contract_address.lower():
                try:
                    timestamp = int(tx["timeStamp"])
                    tx_time = datetime.fromtimestamp(timestamp)
                    if START_DATE <= tx_time <= END_DATE:
                         filtered_txs.append(tx)
                except (ValueError, TypeError):
                    continue

    raw_balance = fetch_token_balance(address, contract_address)
    metrics["token_balance"] = raw_balance / (10 ** token_decimals)

    if not filtered_txs:
        return metrics

    # Calculate Metrics based on filtered_txs (activity within DAYS_BACK)
    timestamps_in_period = [int(tx["timeStamp"]) for tx in filtered_txs]
    # These are now guaranteed to be within the period
    first_activity_in_period = datetime.fromtimestamp(min(timestamps_in_period))
    last_activity_in_period = datetime.fromtimestamp(max(timestamps_in_period))

    metrics["last_token_tx_date"] = last_activity_in_period

    metrics["token_tx_count"] = len(filtered_txs)
    metrics["token_interactions"] = metrics["token_tx_count"]
    metrics["token_active_days"] = max(1, (last_activity_in_period - first_activity_in_period).days)
    metrics["avg_token_tx_frequency"] = metrics["token_tx_count"] / metrics["token_active_days"]

    incoming_volumes = []
    outgoing_volumes = []
    counterparties = set()

    for tx in filtered_txs:
        try:
            value_raw = int(tx.get("value", 0))
            value_adjusted = value_raw / (10 ** token_decimals)
        except (ValueError, TypeError):
            value_adjusted = 0.0

        sender = tx.get("from", "").lower()
        receiver = tx.get("to", "").lower()
        address_lower = address.lower()

        if sender == address_lower:
            metrics["outgoing_token_tx_count"] += 1
            outgoing_volumes.append(value_adjusted)
            if receiver != address_lower: counterparties.add(receiver)
        elif receiver == address_lower:
            metrics["incoming_token_tx_count"] += 1
            incoming_volumes.append(value_adjusted)
            if sender != address_lower: counterparties.add(sender)

    metrics["avg_incoming_token_volume"] = sum(incoming_volumes) / len(incoming_volumes) if incoming_volumes else 0.0
    metrics["avg_outgoing_token_volume"] = sum(outgoing_volumes) / len(outgoing_volumes) if outgoing_volumes else 0.0
    metrics["unique_token_counterparties"] = len(counterparties)

    return metrics


if __name__ == "__main__":
    print("--- Starting ERC-20 Token Analysis Script ---")
    print(f"Target Token Contract: {TARGET_TOKEN_CONTRACT_ADDRESS}")
    print(f"Analysis Period: {START_DATE.strftime('%Y-%m-%d')} to {END_DATE.strftime('%Y-%m-%d')} ({DAYS_BACK} days)")
    print(f"Max Addresses to Analyze: {MAX_ADDRESSES}")

    token_decimals = fetch_token_info(TARGET_TOKEN_CONTRACT_ADDRESS)
    print(f"Using token decimals: {token_decimals}")

    start_block = datetime_to_block(START_DATE)
    end_block = datetime_to_block(END_DATE) # Use current block implicitly in API calls? Let's get it.

    if not start_block: # end_block might be None if current time fails, handle later
        print("Error: Could not fetch start block number from Etherscan. Exiting.")
        exit()
    # Fetch current block if end_block failed or for better accuracy
    current_block_params = {"module": "proxy", "action": "eth_blockNumber"}
    current_block_hex = etherscan_request(current_block_params)
    if current_block_hex:
        end_block = int(current_block_hex, 16)
        print(f"Using current block number for end block: {end_block}")
    elif not end_block:
         print("Error: Could not determine end block number. Using 99999999.")
         end_block = 99999999 # Fallback if both methods fail
    else:
         print(f"Using calculated end block number: {end_block}")

    print(f"Block Range for Analysis: {start_block} to {end_block}")

    active_addresses = fetch_active_addresses_for_token(TARGET_TOKEN_CONTRACT_ADDRESS, start_block, end_block)

    if not active_addresses:
        print("No active addresses found for this token in the specified period. Exiting.")
        exit()
    print(f"\nFound {len(active_addresses)} unique addresses interacting with the token.")


    wallet_data = []
    print("\n--- Collecting Wallet Metrics ---")
    for address in tqdm(active_addresses, desc="Processing wallets"):
        metrics = get_wallet_metrics_for_token(address, TARGET_TOKEN_CONTRACT_ADDRESS, token_decimals, start_block, end_block)
        if metrics:
            wallet_data.append(metrics)

    if not wallet_data:
        print("No wallet data collected. Not saving CSV.")
    else:
        df = pd.DataFrame(wallet_data)
        column_order = [
            "address", "token_balance", "data_completeness",
            "token_tx_count", "token_active_days",
            "avg_token_tx_frequency", "holding_period", "incoming_token_tx_count",
            "outgoing_token_tx_count", "avg_incoming_token_volume",
            "avg_outgoing_token_volume", "unique_token_counterparties",
            "first_token_tx_date", "last_token_tx_date", "token_interactions"
        ]
        df = df.reindex(columns=[col for col in column_order if col in df.columns])
        filename = f"dataset_link.csv"
        df.to_csv(filename, index=False, date_format='%Y-%m-%d %H:%M:%S')
        print(f"\nData saved successfully to {filename}")

    print("\n--- Script Finished ---")
