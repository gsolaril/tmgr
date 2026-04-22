import os, sys, numpy, requests, time, sqlalchemy as sql
from concurrent.futures import as_completed, ThreadPoolExecutor as Pool
from pandas import read_sql_query, Series, DataFrame, Index, Timestamp
from argparse import ArgumentParser
from urllib.parse import urlencode

sys.path.append("./")
from core.utils import *

connDB = sql.create_engine(DB_URL)
URL = MT_API_URL_5.format(protocol = "http", version = 5) + "Order{action}?{params}"

DESCRIPTION = "This will just shoot a couple of orders in source accounts, then modify and finally close them. See arguments."

#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
def get_accounts_urls(symbol: str = "USDJPY", price: float = 0.0, 
    buy: bool = True, lot: float = 0.1, variation: float = 0.001,
    comment: str = "storm_orders"):

    query = f"""SELECT alias, token FROM {TABLE_TSRS}
        WHERE (alias ~ '^storm_test') AND (category = 0)
    """
    df_send = read_sql_query(query, connDB, index_col = "alias")

    sign = {True: 1, False: -1}[buy]
    order = {True: "Buy", False: "Sell"}[buy]
    if (price != 0.0): order = order + "Stop"
    factor = sign * abs(numpy.random.randn() * variation)
    price_open = round(price * (1 + 1 * factor), 3)
    price_sl = round(price_open * (1 - 1 * factor), 3)
    price_tp = round(price_open * (1 + 2 * factor), 3)

    params = {"symbol": symbol, "comment": comment, "placedType": "Mobile",
        "operation": order, "volume": lot, "price": price_open, "stoploss": price_sl}
    
    print("Order properties:", params, sep = "\n")
    
    params = urlencode(params) + "&id="
    url = URL.format(params = params, action = "Send")
    df_send["url_send"] = url + df_send["token"]

    params = urlencode({"takeprofit": price_tp}) + "&ticket={ticket}&id="
    url = URL.format(params = params, action = "Modify")
    df_send["url_modify"] = url + df_send["token"]

    params = "ticket={ticket}&id="
    url = URL.format(params = params, action = "Close")
    df_send["url_close"] = url + df_send["token"]
    return df_send

#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████

VERBOSE_FORMAT = "Account \"{alias}\" order {action} (#{ticket}) {result}:\n{response}".format

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
def execute(account: dict, delay_min: float = 0.01, delay_max: float = 1.0):

    delay_width = delay_max - delay_min
    verbose = {"alias": account["alias"]}
    
    time.sleep(delay_min + numpy.random.random() * delay_width)

    url_send: str = account["url_send"]
    response = requests.get(url_send)
    ticket = response.json().pop("ticket", 0)
    verbose.update(response = response.text,
          action = "send", ticket = ticket)
    
    if (response.status_code != 200) or (ticket is None):
        return print(VERBOSE_FORMAT(**verbose, result = "failed"))
    else: print(VERBOSE_FORMAT(**verbose, result = "success"))

    time.sleep(delay_min + numpy.random.random() * delay_width)

    url_modify: str = account["url_modify"]
    url_modify = url_modify.format(ticket = ticket)
    response = requests.get(url_modify)
    verbose.update(response = response.text,
          action = "modify", ticket = ticket)

    if (response.status_code != 200) or (ticket is None):
        return print(VERBOSE_FORMAT(**verbose, result = "failed"))
    else: print(VERBOSE_FORMAT(**verbose, result = "success"))

    time.sleep(delay_min + numpy.random.random() * delay_width)

    url_close: str = account["url_close"]
    url_close = url_close.format(ticket = ticket)
    response = requests.get(url_close)
    verbose.update(response = response.text,
          action = "close", ticket = ticket)

    if (response.status_code != 200) or (ticket is None):
        return print(VERBOSE_FORMAT(**verbose, result = "failed"))
    else: print(VERBOSE_FORMAT(**verbose, result = "success"))

#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████

if (__name__ == "__main__"):

    args = ArgumentParser(description = DESCRIPTION)
    
    args.add_argument("-s", "--symbol", default = "USDJPY", type = str)
    args.add_argument("-l", "--lot", default = 1.0, type = float)
    args.add_argument("-p", "--price", default = 0.0, type = float)
    args.add_argument("-b", "--buy", default = True, action = "store_true")
    args.add_argument("-v", "--variation", default = 0.001, type = float)
    args.add_argument("-dl", "--mindelay", default = 0.01, type = float)
    args.add_argument("-dh", "--maxdelay", default = 0.01, type = float)
    args.add_argument("-c", "--cycles", default = 1, type = int)

    args = args.parse_args()
    
    cycles = getattr(args, "cycles")
    min_delay = getattr(args, "mindelay")
    max_delay = getattr(args, "maxdelay")

    accounts = get_accounts_urls(variation = getattr(args, "variation"),
            symbol = getattr(args, "symbol"), buy = getattr(args, "buy"),
            price = getattr(args, "price"), lot = getattr(args, "lot"),
            comment = "c=%02ddl%.2fdh%d" % (cycles, min_delay, max_delay))

    accounts = accounts.reset_index().to_dict("index")
    n_accounts = len(accounts := [*accounts.values()])


    #████████████████████████████████████████████████

    processes = dict()
    with Pool(n_accounts * cycles) as pool:
        for cycle in range(n_accounts * cycles):
            account = accounts[cycle % n_accounts]
            args = (account, min_delay, max_delay)
            process = pool.submit(execute, *args)
            processes[process] = args[0]
        
        for process in as_completed(processes):
            account = processes[process]
            try: process.result()
            except Exception as EXC:
                exc = "Process for \"%s\" failed:\n%s"
                print(exc % (account, EXC.__repr__()))