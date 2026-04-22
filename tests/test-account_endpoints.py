import os, sys, asyncio
sys.path.append("./")
from pandas import DataFrame, Timedelta
from core.account import Account
from core.actions import *
from core.utils import *

########################################################################################
########################################################################################
########################################################################################

if (__name__ == "__main__"):

    account = {
        4: Account(mtver = MT_API.VersionMT.MT4, category = Account.Category.TARGET,
                    alias = "test_account_mt4", id = 61262628, password = "algo99990",
                    server = "RoboForex-Demo"),
        5: Account(mtver = MT_API.VersionMT.MT5, category = Account.Category.TARGET,
                    alias = "test_account_mt5", id = 100509, password = "@2LsXjDj",
                    server = "MyServerGlobal-Server") }
    
    ####################################################################################
    
    title = "Testing \"/Connect\""
    print("", "-" * 140, title, "-" * 140, sep = "\n")
    asyncio.run(account[4].connect_get_token())
    asyncio.run(account[5].connect_get_token())
    asyncio.run(account[4].update_state_vars())
    asyncio.run(account[5].update_state_vars())

    ####################################################################################
    
    title = "Testing \"/AccountSummary\""
    print("", "-" * 140, title, "-" * 140, sep = "\n")
    df4 = df5 = None
    print("", "Results MT4:", "-" * 12, sep = "\n")
    try: print(asyncio.run(account[4].get_current_state()))
    except Exception as EXC: Log.exception(EXC)
    print("", "Results MT5:", "-" * 12, sep = "\n")
    try: print(asyncio.run(account[5].get_current_state()))
    except Exception as EXC: Log.exception(EXC)

    ####################################################################################

    title = "Testing \"OrderHistory\""
    print("", "-" * 140, title, "-" * 140, sep = "\n")
    df4 = df5 = None
    args = {"delta": Timedelta(days = 10)}    
    print("", "Results MT4:", "-" * 12, sep = "\n")
    try: print(asyncio.run(account[4].get_closed_trades(**args)))
    except Exception as EXC: Log.exception(EXC)
    print("", "Results MT5:", "-" * 12, sep = "\n")
    try: print(asyncio.run(account[5].get_closed_trades(**args)))
    except Exception as EXC: Log.exception(EXC)

    ####################################################################################

    title = "Testing \"OpenedOrders\""
    print("", "-" * 140, title, "-" * 140, sep = "\n")
    df4 = df5 = None
    print("", "Results MT4:", "-" * 12, sep = "\n")
    try: print(asyncio.run(account[4].get_active_trades()))
    except Exception as EXC: Log.exception(EXC)
    print("", "Results MT5:", "-" * 12, sep = "\n")
    try: print(asyncio.run(account[5].get_active_trades()))
    except Exception as EXC: Log.exception(EXC)

    ####################################################################################

    title = "Testing \"OrderSend\""
    print("", "-" * 140, title, "-" * 140, sep = "\n")

    order = OrderSend(source = "manual",
        symbol = "EURUSD", comment = "TEST|123|rest",
        action = OrderRequest.Action.OPEN,
        t_send = int(time.time() * 1e6),
        is_market = True, lots = 0.1,
        is_buy = False, is_stop = False, 
        stopLoss = 0.0, takeProfit = 0.0)

    order.ticket = 123
    order.base_value = 1
    order.base_point = 1e-5
    order.t_sign = order.t_send + 1e3
    order.t_recv = order.t_sign + 1e3

    #print("", "Results MT4:", "-" * 12, sep = "\n")
    #try: print(resp_4 := asyncio.run(account[4] \
    #                .execute(order, factor = 0.4)))
    #except Exception as EXC: Log.exception(EXC)
    print("", "Results MT5:", "-" * 12, sep = "\n")
    try: print(resp_5 := asyncio.run(account[5] \
                    .execute(order, factor = 0.5)))
    except Exception as EXC: Log.exception(EXC)
    
    ####################################################################################
