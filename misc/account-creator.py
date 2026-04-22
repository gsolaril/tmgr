import sys, requests, json
from pandas import Series, DataFrame
from argparse import ArgumentParser
from urllib.parse import quote
import sqlalchemy as sql
from uuid import uuid4

sys.path.append("./")
from core.utils import *

connDB = sql.create_engine(DB_URL)
URL = MT_API_URL_5.format(protocol = "https", version = 5) + "{endpoint}?{params}"

DESCRIPTION = "Write a json where keys are server names, and values are number of accounts. This will create them."

#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
def locate_servers(servers: list):

    url_temp = URL.format(params = "company={server}", endpoint = "Search")
    VERBOSE: str = "\rFetching server #{n}/%d: \"{server}\"" % len(servers)

    results = list()
    for n, server in enumerate(servers, 1):
        print(VERBOSE.format(n = n, server = server), end = "")
        url = url_temp.format(server = server)
        df = DataFrame(requests.get(url).json())
        results = results + df["results"].to_list()

    results = DataFrame(results)
    results = DataFrame(results.apply(Series)).stack()
    results = results.apply(Series).set_index("name")
    results: Series = results["access"].str[0].rename("ip")
    return results.rename_axis("server")

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
def create_accounts(servers: DataFrame):

    n_accounts = servers["accounts"].sum()
    params_1 = "host={host}&port={port}"
    params_2 = params_1 + "&id={login}&password={password_url}"
    VERBOSE: str = "\rAccount #{n}/%d: Server \"{server}\" @ \"{ip}\"" % n_accounts

    url_temp_1 = URL.format(endpoint = "GetDemo", params = params_1)
    url_temp_2 = URL.format(endpoint = "Connect", params = params_2)
    url_temp_3 = URL.format(endpoint = "AccountSummary", params = "id={token}")

    ERROR_TIMEOUT = "Account connection error:\n{0} \nURL: {1}"
    FINANCE_KEYS = ["balance", "equity", "margin", "leverage"]
    FINAL_COLUMNS = ["id", "password", "server", "ip", "token", *FINANCE_KEYS]

    n_account, accounts, hmap = 0, list(), dict()

    for server, row in servers.iterrows():

        ip: str = row["ip"]
        host, port = ip.split(":")
        n_accounts = row["accounts"]
        args = dict(host = host, port = port)
        hmap[host] = server

        for _ in range(n_accounts):
            
            print(VERBOSE.format(n = (n_account := n_account + 1),
                              server = server, ip = ip), end = "")
            result = requests.get(url_temp_1.format(**args))
            row = dict(server = server, **args, **result.json())
            row["password_url"] = quote(row["password"])
            result = requests.get(url_temp_2.format(**row))

            try: assert (result.status_code == 200)
            except: raise AssertionError(ERROR_TIMEOUT.format(
                    result.json(), url_temp_2.format(**row)))

            row["token"] = result.json()["token"]
            result = requests.get(url_temp_3.format(**row)).json()
            for key in FINANCE_KEYS: row[key] = result[key]
            accounts.append(row)

    accounts = DataFrame(accounts).dropna(subset = ["id"])
    accounts = accounts.reset_index(drop = True).reset_index()
    accounts["server"] = accounts["host"].map(hmap)
    accounts["id"] = accounts["id"].astype(int)
    ip_format = "{0[host]}:{0[port]}".format
    accounts["ip"] = accounts.agg(ip_format, axis = "columns")
    
    return accounts[FINAL_COLUMNS]

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
def upload_accounts(accounts: DataFrame, alias_prefix: str, offset: int = 0):
    
    digits_n = len(str(accounts.shape[0]))
    digits_n = "{0:0%dd}" % digits_n
    accounts = accounts.reset_index()

    accounts["index"] = (accounts["index"] + offset).map(digits_n.format)
    accounts.index = (alias_prefix + accounts["index"]).rename("alias")
    accounts = accounts.drop(columns = "index")

    accounts["mtver"], accounts["category"], accounts["active"] = 5, 2, False
    accounts.to_sql(TABLE_ACCS, con = DB_URL, if_exists = "append")

#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
    
if (__name__ == "__main__"):

    args = ArgumentParser(description = DESCRIPTION)
    args.add_argument("-j", "--json", default = "{\"MetaQuotes-Demo\": 2}", type = str)
    args.add_argument("-p", "--prefix", default = uuid4().hex[: 6].upper(), type = str)
    args.add_argument("-o", "--offset", default = 0, type = int)

    args = args.parse_args()

    _json = getattr(args, "json")
    _json = _json.replace("'", "\"")
    _json = json.loads(_json)
    prefix = getattr(args, "prefix")
    offset = getattr(args, "offset")

    servers = locate_servers([*_json]).reset_index()
    servers = servers.set_index("server")
    servers["accounts"] = servers.index.map(_json)

    accounts = create_accounts(servers)

    upload_accounts(accounts, prefix, offset)
