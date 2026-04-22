import os, sys, numpy, asyncio, uuid, time
from pandas import read_sql, to_datetime, Timestamp
from argparse import ArgumentParser
import sqlalchemy as sql
sys.path.append("./")

from core.account import *
from core.actions import *
from core.utils import *

from apscheduler.schedulers.blocking import BlockingScheduler as Scheduler
from apscheduler.triggers.interval import IntervalTrigger as Trigger

connDB = sql.create_engine(DB_URL, isolation_level = "AUTOCOMMIT")

#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
#████████████████████████████████████████████████████████████████████████████████████████████████████████████   Base class   ███
#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████

#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
        
#▄▄▄▄▄▄▄▄▄▄▄▄▄
class Strategy:

    MAX_LEN_HIST = 5

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, symbols: dict, jobs: dict, name: str = None, **args):

        self.name = name
        if (self.name is None): self.name = self.__class__.__name__
        for key, value in args.items(): self.__setattr__(key, value)

        self._jobs = jobs
        self._symbols = symbols
        self._orders_history = list()
        self._orders_current = list()
        self.time_init = time.time()

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @property
    def orders_history(self): return self._orders_history
    @property
    def orders_current(self): return self._orders_current
    @property
    def symbols(self): return self._symbols

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def reg_response(self, response: OrderResponse): 

        class_name = self.__class__.__name__
        name = class_name + "/" + self.name

        response_dict = response.__dict__.copy()
        response_dict.pop("state", None)
        response_dict.pop("_exception", None)
        self._orders_history.append(response_dict)
        
        if (len(self._orders_history) >= self.MAX_LEN_HIST):
            orders_history = DataFrame(self._orders_history)
            orders_history = orders_history.set_index("t_resp")
            orders_history["alias_source"] = name
            orders_history["nzmq"] = 0
            self._orders_history = list()

            n_orders = orders_history.shape[0]
            try:
                orders_history.to_sql(
                    name = TABLE_RESP, con = connDB,
                    index = True, if_exists = "append")
                Log.success(f"{name} added {n_orders} responses.")
            except Exception as EXC:
                Log.exception(EXC)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def react(self, data: DataFrame = None):

        return None
    
#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
#█████████████████████████████████████████████████████████████████████████████████████████████████████████   Example (Ali)   ███
#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class RandomTrade(Strategy):

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, name: str, symbols: dict,
        lots: float = 0.01, freq_rand: int = 1,
        dist_stop: int = 20, threshold: float = 0):

        jobs = {self.send_random_order: freq_rand}
        super().__init__(name = name, symbols = symbols, jobs = jobs,
            lots = lots, freq_rand = freq_rand, dist_stop = dist_stop,
            threshold = threshold)

        self.time_init_str = Timestamp.utcnow().strftime("%X")

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def react(self, data: DataFrame = None):

        return None
    
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def send_random_order(self, data: DataFrame):

        #random_1 = 2 * numpy.random.rand() - 1

        #if (random_1 < self.threshold): return list()

        random_2 = 2 * numpy.random.rand() - 1

        sign = int(numpy.sign(random_2))
        order_type = OrderRequest.TYPE_INT_TO_ENUM[sign]

        symbol = numpy.random.choice([*self._symbols])
        last_prices = data.loc[symbol]
        price_ask = last_prices["ask"]
        price_bid = last_prices["bid"]

        try:
            dig = self._symbols[symbol]["digits"]
            point = self._symbols[symbol]["point"]
            mstop = self._symbols[symbol]["mstop"]
        except:
            mstop = int(price_ask / 3)
            dig = 5 - round(numpy.log10(price_ask))
            point = pow(10, - dig)
            debug_verbose = "{symbol} specs... mstop: {mstop:.0f}, point: {point:.4f}"
            Log.debug(debug_verbose.format(symbol = symbol, mstop = mstop, point = point))

        delta = round(self.dist_stop * mstop * point, dig) * sign
        entry = {-1: price_bid, +1: price_ask}[sign]
        close = {+1: price_bid, -1: price_ask}[sign]
        price_sl, price_tp = close - delta, close + delta
    
        t = int(time.time() * 1e6)
        comment = self.time_init_str
        comment = comment.split("-")[0]
        comment = comment.upper()

        verbose = "{type} {lots:.2f} {symbol} @ {price:.4f} - "
        verbose += "SL {sl:.4f}, TP {tp:.4f} (delta: {delta:.4f})"
        verbose = verbose.format(type = order_type.name, lots = self.lots,
            symbol = symbol, price = entry, sl = price_sl, tp = price_tp,
            delta = delta)
        Log.info(f"RandomTrade, trying to send test order: \"{verbose}\"")

        return [OrderSend(**{"action": OrderRequest.Action.OPEN,
            "type": order_type, "lots": self.lots, "price": entry,
            "comment": comment, "source": self.name, "symbol": symbol,
            "stopLoss": price_sl, "takeProfit": price_tp, "ticket": 0,
            "t_sign": t, "t_recv": t})]
        
#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
#███████████████████████████████████████████████████████████████████████████████████████████████████████████   Manual test   ███
#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████

if (__name__ == "__main__"):

    arg_parser = ArgumentParser(description = "Trade Emulator (Random test)")
    arg_parser.add_argument("-a", "--account", default = "", type = str)
    arg_parser.add_argument("-s", "--symbols", default = "", type = str)
    arg_parser.add_argument("-l", "--lots", default = 0.02, type = float)
    arg_parser.add_argument("-t", "--thr", default = 0.0, type = float)
    arg_parser.add_argument("-d", "--dist", default = 1.2, type = float)
    arg_parser.add_argument("-f", "--freq", default = 20, type = int)

    args = arg_parser.parse_args()
    lots = getattr(args, "lots")
    freq = getattr(args, "freq")
    dist = getattr(args, "dist")
    threshold = getattr(args, "thr")
    symbols = getattr(args, "symbols")
    account = getattr(args, "account")

    if not symbols: symbols = str({
        "AUDUSD":   {"point": 1e-5, "digits": 5, "mstop": 10},
        "EURUSD":   {"point": 1e-5, "digits": 5, "mstop": 10},
        "GBPUSD":   {"point": 1e-5, "digits": 5, "mstop": 10},
        "USDCAD":   {"point": 1e-5, "digits": 5, "mstop": 10},
        "USDCHF":   {"point": 1e-5, "digits": 5, "mstop": 10},
    })
    if not account: account = "rwtest00"

    symbols = eval(symbols)
    strategy = RandomTrade(name = "rtest", symbols = symbols,
            lots = lots, freq_rand = freq, dist_stop = dist,
            threshold = threshold)

    alias = "rtest00"
    accounts = read_sql(TABLE_ACCS, con = DB_URL, index_col = "alias")
    account_info = accounts.loc[alias].dropna().drop("active").to_dict()
    account_info["category"] = Account.CATEGORIES[account_info.pop("category")]
    account_info["mtver"] = MT_API.MT_VERSIONS[account_info.pop("mtver")]
    account = Account(alias = alias, **account_info)
    asyncio.run(account.connect_get_token())
    asyncio.run(account.update_state_vars())
    verbose = "SOURCE \"%s\" token: \"%s\""
    Log.info(verbose % (alias, account.token))

    #███████████████████████████████████████████████████████████████████
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def test(strategy: RandomTrade):

        url = MT_API.get_base_url(
            version = MT_API.VersionMT.MT5,
            protocol = MT_API.ProtocolURL.HTTP)

        data = list()
        
        for symbol in [*strategy.symbols]:

            args = {"url": url + "GetQuote",
                "verbose": f"Last market data for \"{symbol}\"",
                "data": {"id": account.token, "symbol": symbol}}
            
            data.append(asyncio.run(MT_API.request(**args)))
            
        data = DataFrame(data).set_index("symbol")
        try: data["time"] = to_datetime(data["time"],
                        format = "mixed", utc = True)
        except Exception as EXC: Log.error(EXC)
        orders: OrderSend = strategy.send_random_order(data)

        if not orders: return Log.info(
            "Random order skipped... wait %d seconds" % strategy.freq_rand)
        order: OrderRequest = orders.pop(0)
        order.base_value, order.base_point = 1.0, \
            strategy.symbols[order.symbol]["point"]
        try:
            response = asyncio.run(account.execute(order, factor = 1))
            if (response is None): return Log.warning("Order not sent...")
            Log.info(f"Order sent. Resp:\n -> {response.__dict__}")
            strategy.reg_response(response)
        except Exception as EXC:
            Log.exception(EXC)
        
    #███████████████████████████████████████████████████████████████████

    time.sleep(10)

    scheduler = Scheduler()
    scheduler.add_job(test, name = "send_random_order",
        trigger = Trigger(seconds = strategy.freq_rand),
        args = (strategy,), )

    Log.warning("Everything ready...")

    try: scheduler.start()
    except KeyboardInterrupt:
        scheduler.shutdown()
