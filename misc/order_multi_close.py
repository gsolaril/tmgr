# Import required system and data processing libraries
import os, sys, numpy, requests, time, sqlalchemy as sql
from concurrent.futures import as_completed, ThreadPoolExecutor as Pool
from pandas import read_sql_query, Series, DataFrame, Index, Timestamp
from argparse import ArgumentParser
from urllib.parse import urlencode

# Add the current directory to Python path
sys.path.append("./")
from core.utils import *

# Initialize database connection
connDB = sql.create_engine(DB_URL)
# Define API URL template for MT5 orders
URL = MT_API_URL_5.format(protocol = "http", version = 5) + "Order{action}?{params}"

# Module description
DESCRIPTION = "This will just close all of the orders in the different target accounts that have \"active = 1\" in DB."

#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
def close_all_orders(alias_regex: str = None):
    """
    Close all open orders for trading accounts matching the specified alias pattern.
    
    Inputs: (str) Regular expression pattern to filter account aliases.
            If None, matches all accounts.
    
    Output: (DataFrame) Contains results of closing operations with columns:
            - server: Trading server name
            - id: Account ID
            - n_orders: Number of orders processed
            - errors: Count of failed close operations
            - orders: Detailed order closing results
    """
    
    # Set default regex pattern to match all accounts if none provided
    if (alias_regex is None): alias_regex = ".*"
    # Query active accounts matching the alias pattern
    query = f"""SELECT alias, server, id, token, mtver
        FROM {TABLE_ACCS} WHERE (alias ~ '{alias_regex}')
        AND (active = true)
    """
    # Load matching accounts into DataFrame
    df_close = read_sql_query(query, connDB, index_col = "alias")
    # Construct base URLs for API endpoints
    url_base = MT_API.BASE_URL.format(protocol = "http", version = "{mtver}")
    url_orders = "{0[url_base]}OpenedOrdersTickets?id={0[token]}"
    url_close = "{0[url_base]}OrderClose?id={0[token]}&ticket="

    # Define progress display format
    verbose_main = "\rAccount \"{account}\" (#{n}/%d): " % len(df_close)
    # Generate API URLs for each account
    df_close["url_base"] = df_close.agg(url_base.format, axis = "columns")
    df_close["url_orders"] = df_close.agg(url_orders.format, axis = "columns")
    df_close["url_close"] = df_close.agg(url_close.format, axis = "columns")

    # Initialize columns for order data
    df_close[["n_orders", "orders"]] = None
    verbose = verbose_main + "Getting orders..."

    # Fetch open orders for each account
    for n, (account, row) in enumerate(df_close.iterrows(), 1):
        print(verbose.format(n = n, account = account), end = "\t\t")
        orders = dict.fromkeys(requests.get(row["url_orders"]).json())
        df_close.at[account, "n_orders"] = len(orders)
        df_close.at[account, "orders"] = orders

    print()
    # Calculate total number of orders to process
    df_close["n_orders"] = df_close["orders"].str.len()
    n_order, n_orders_all = 0, df_close["n_orders"].sum()
    verbose = "Closing order #{ticket} (#{n_ord}/%d)..."
    verbose: str = verbose_main + verbose % n_orders_all
    # Initialize error counter
    df_close["errors"] = 0

    # Process order closing for each account
    for n, (account, row) in enumerate(df_close.iterrows(), 1):
        for ticket in dict.keys(orders := row["orders"]):
            print(verbose.format(account = account, ticket = ticket,
                n_ord = (n_order := n_order + 1), n = n), end = "\t\t")
            # Send close request for each order
            close_result = requests.get(row["url_close"] + str(ticket))
            orders[ticket] = {"code": (code := close_result.status_code),
                "test": close_result.text}
            # Increment error counter if close operation failed
            if (code == 200): continue
            else: df_close.at[account, "errors"] += 1

    print()
    return df_close[["server", "id", "n_orders", "errors", "orders"]]

#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████

if (__name__ == "__main__"):
    df = close_all_orders()
    df_json = df.to_dict(orient = "index")
    print(json.dumps(df_json, indent = 4))