import os, sys
sys.path.append("./")
from core.account import Account
from core.receiver import StreamReceiver
from core.utils import *

########################################################################################
########################################################################################
########################################################################################

if (__name__ == "__main__"):

    account = {
        4: Account(mtver = MT_API.VersionMT.MT4, category = Account.Category.SOURCE,
                    alias = "test_account_mt4", id = 61262628, password = "algo99990",
                    server = "RoboForex-Demo"),
        5: Account(mtver = MT_API.VersionMT.MT5, category = Account.Category.SOURCE,
                    alias = "test_account_mt5", id = 100510, password = "Ly!4NcTh",
                    server = "MyServerGlobal-Server") }

    filename = "tests/sample_event_WS_order{o}_{m}_mt{n}.json"
    index = "order{o}_{m}_mt{n}"

    combinations = [
        dict(o = "send", m = "market", n = 4),
        dict(o = "send", m = "market", n = 5),
        dict(o = "send", m = "pending", n = 4),
        dict(o = "send", m = "pending", n = 5),
        dict(o = "modify", m = "market", n = 4),
        dict(o = "modify", m = "market", n = 5),
        dict(o = "modify", m = "pending", n = 4),
        dict(o = "modify", m = "pending", n = 5),
        dict(o = "close", m = "market", n = 4),
        dict(o = "close", m = "market", n = 5),
        dict(o = "close", m = "pending", n = 4),
        dict(o = "close", m = "pending", n = 5),
    ]

    results = dict()

    for args in combinations:
        with open(filename.format(**args), "r") as file: content = file.read()
        result = StreamReceiver.parse_message_ws(content, account[args["n"]])
        results[index.format(**args)] = result
    
    results = DataFrame.from_dict(results, orient = "index")
    print(results)
