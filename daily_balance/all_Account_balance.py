import pandas as pd
import numpy as np
from binance.client import Client
from datetime import datetime, timezone
import time
import os
import mysql.connector

# Function to safely get keys and secrets with error handling
def get_client(api_key_env, api_secret_env):
    api_key = os.environ.get(api_key_env)
    api_secret = os.environ.get(api_secret_env)

    if not api_key or not api_secret:
        raise ValueError(f"Missing API key or secret for {api_key_env}")

    return Client(api_key, api_secret)

# Initialize Binance clients
clients = [
    get_client('AF1_key', 'AF1_secret'),
    get_client('AF2_key', 'AF2_secret'),
    get_client('AF5_key', 'AF5_secret'),
    get_client('MIRRORX1_key', 'MIRRORX1_secret'),
    get_client('MIRRORX2_key', 'MIRRORX2_secret'),
    get_client('MIRRORX3_key', 'MIRRORX3_secret'),
    get_client('MIRRORX4_key', 'MIRRORX4_secret'),
    get_client('MIRRORX5_key', 'MIRRORX5_secret'),
    get_client('MIRRORXFUND_key', 'MIRRORXFUND_secret'),
    get_client('OFFICE_key', 'OFFICE_secret'),
    get_client('TEAM_key', 'TEAM_secret')
]

# Initialize Account Names
account_names = [
    "AF1", "AF2", "AF5", "MIRRORX1", "MIRRORX2", 
    "MIRRORX3", "MIRRORX4", "MIRRORX5", "MIRRORXFUND", "OFFICE", "TEAM"
]

# MySQL database connection
db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="password",
    database="binance_balance_data"
)

# Get total wallet balance
def wallet_balance(client):
    wallet_account = client.futures_account()
    return float(wallet_account['totalWalletBalance'])

# Get total margin balance
def margin_balance(client):
    wallet_account = client.futures_account()
    total_balance = float(wallet_account['totalWalletBalance'])
    unrealizedPnl = float(wallet_account['totalUnrealizedProfit'])
    return total_balance + unrealizedPnl

# Fetch balances with retry mechanism
def fetch_balance_with_retry(account_name, client):
    retries = 5
    for attempt in range(retries):
        try:
            wallet = wallet_balance(client)
            margin = margin_balance(client)
            print(f"{account_name} balance data successfully fetched!")
            return wallet, margin
        except Exception as e:
            print(f"{account_name} balance data fetching failed, trying again... (Attempt {attempt + 1})")
            time.sleep(2)  # Wait before retrying
    print(f"Failed to fetch balance for {account_name} after {retries} attempts.")
    return None, None

# Process the functions and its received data to send in MySQL database
def fetch_and_save_balance():
    print("Fetching data from Binance...")

    # Prepare the dictionary for holding balances
    balance_data = {
        '`utc_time`': datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    }

    # Loop through all account names and clients
    for account_name, client in zip(account_names, clients):
        wallet, margin = fetch_balance_with_retry(account_name, client)
        if wallet is not None and margin is not None:
            balance_data[f"{account_name}_wallet_balance"] = wallet  # No backticks needed here
            balance_data[f"{account_name}_margin_balance"] = margin  # No backticks needed here

    # Prepare the SQL query to insert into the table
    cursor = db.cursor()

    # Create dynamic SQL query with backticks for `utc_time` only
    columns = ', '.join(balance_data.keys())  # Account balances and time
    placeholders = ', '.join(['%s'] * len(balance_data))  # Same number of placeholders as columns

    balance_query = f"""
    INSERT INTO binance_data_table ({columns})
    VALUES ({placeholders})
    """

    # Execute the query with balance data values
    cursor.execute(balance_query, tuple(balance_data.values()))

    # Commit to the database
    db.commit()
    cursor.close()

    print("Data saved in the database.")

    # Save balance data to files (same as before)
    save_balance_to_files(balance_data)

    local_time = datetime.now()
    print(f"Data saved at local time: {local_time} | UTC: {datetime.now(timezone.utc)}")

# Save balances to CSV and text files
def save_balance_to_files(data):
    df = pd.DataFrame([data])  # Convert to DataFrame for saving
    # Save to CSV
    df.to_csv('C:/Users/User/Documents/daily_balance/csv_daily_balance.csv', mode='a', header=not os.path.isfile('C:/Users/User/Documents/daily_balance/csv_daily_balance.csv'), index=False)

    # Save to text file in the desired format
    with open('C:/Users/User/Documents/daily_balance/txt_daily_balance.txt', 'a') as f:
        # Write UTC Time and account balances for each row
        f.write(f"UTC Time: {data['`utc_time`']}\n")
        for account_name in account_names:
            wallet_balance = data.get(f"{account_name}_wallet_balance", 0)
            margin_balance = data.get(f"{account_name}_margin_balance", 0)
            f.write(f"{account_name} - Wallet Balance: {wallet_balance}, Margin Balance: {margin_balance}\n")
        f.write("\n")  # New line for separation between entries

# Run the function to fetch and save balances
fetch_and_save_balance()
