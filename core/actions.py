#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
"""
Trading Actions Module

Implements order request and response handling for trading operations.
Provides standardized interfaces for:
- Order creation and modification requests
- Trade execution responses
- Error handling and validation
- Cross-platform compatibility (MT4/MT5)
"""
import os, sys, time, asyncio
from pandas import Timestamp
from enum import IntEnum
from numpy import sign

sys.path.append("./")

from core.utils import *

#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
#██████████████████████████████████████████████████████████████████████████████████████████████████████████████  Base types  ███
#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class OrderResponse:
    """
    Handles order response processing and validation.
    """

    class Requote(BaseException): pass
    class InvalidOrder(BaseException): pass
    class InvalidStops(BaseException): pass
    class TooManyPositions(BaseException): pass
    class NotEnoughBalance(BaseException): pass
    class NotEnoughMargin(BaseException): pass
    class InvalidVolume(BaseException): pass
    class InvalidPrice(BaseException): pass

    COMMON_TO_MSG = {
        MT_API.VersionMT.MT5: dict(
            ticket = "ticket", symbol = "symbol", comment = "comment", placed_as = "placedType",
            lots = "lots", price_open = "openPrice", price_close = "closePrice", stop_loss = "stopLoss",
            take_profit = "takeProfit", commission = "commission", swap = "swap", order_type = "orderType",
            point_value = "profitRate", fee = "fee", t_exec = "openTime"),
        MT_API.VersionMT.MT4: dict(
            ticket = "ticket", symbol = "symbol", comment = "comment", placed_as = "placedType",
            lots = "lots", price_open = "openPrice", price_close = "closePrice", stop_loss = "stopLoss",
            take_profit = "takeProfit", commission = "commission", swap = "swap", order_type = "type",
            point_value = "rateOpen", fee = "fee", t_exec = "openTime"), }

    #▄▄▄▄▄▄▄▄▄▄▄
    @classmethod
    def _verify_response(cls, response: str):
        """
        Verifies the order response and raises the according exception if it is invalid.
        The response is a string from the MT API and cannot be decoded as a JSON object.
        The included error strings are identical to some of the error codes in the MT protocol
        (or at least the most recurrent ones related to the trading operations themselves).
        """
        resp_lower = response.lower()
        if resp_lower.__contains__("requote"): return cls.Requote(response)
        elif resp_lower.__contains__("not found"): return cls.InvalidOrder(response)
        elif resp_lower.__contains__("been closed"): return cls.InvalidOrder(response)
        elif resp_lower.__contains__("balance"): return cls.NotEnoughBalance(response)
        elif resp_lower.__contains__("limit_pos"): return cls.TooManyPositions(response)
        elif resp_lower.__contains__("money"): return cls.NotEnoughMargin(response)
        elif resp_lower.__contains__("volume"): return cls.InvalidVolume(response)
        elif resp_lower.__contains__("stops"): return cls.InvalidStops(response)
        elif resp_lower.__contains__("price"): return cls.InvalidPrice(response)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, alias: str, json_request: dict, json_response: dict,
                        mtver: MT_API.VersionMT = MT_API.VersionMT.MT5):
        """
        Initializes the OrderResponse object with the given parameters.
        """
        # Message pattern and keys may vary according to the MT version.
        common_to_msg: dict = self.COMMON_TO_MSG[mtver].copy()

        self.alias = alias
        self.alias_source = json_request["source"]
        self.ticket_source = json_request["ticket"]
        self.action = json_request["action"].name
        self.server = json_request["server"]
        self.account = json_request["account"]
        self.t_sign = json_request["t_sign"]
        self.t_recv = json_request["t_recv"]
        self.t_send = json_request["t_send"]
        # Time given in UNIX microseconds.
        self.t_resp = int(1e6 * time.time())
        self.t_exec = self.t_resp
        # Price is zero for market orders where not specified.
        self.price_sign = json_request.pop("price", 0.0)
        self._exception = None

        if isinstance(json_response, dict):
            self.success = True
            # Get all info related to the order response from the API.
            self.ticket = json_response.get(common_to_msg.get("ticket", None), None)
            self.t_exec = json_response.get(common_to_msg.get("t_exec", None), None)
            self.symbol = json_response.get(common_to_msg.get("symbol", None), None)
            self.placed_as = json_response.get(common_to_msg.get("placed_as", None), None)
            self.comment = json_response.get(common_to_msg.get("comment", None), None)
            if (self.comment is None): self.comment = json_request["comment"]

            self.lots = json_response.get(common_to_msg.get("lots", None), None)
            self.price_open = json_response.get(common_to_msg.get("price_open", None), None)
            self.price_close = json_response.get(common_to_msg.get("price_close", None), None)
            self.stop_loss = json_response.get(common_to_msg.get("stop_loss", None), None)
            self.take_profit = json_response.get(common_to_msg.get("take_profit", None), None)
            self.point_value = json_response.get(common_to_msg.get("point_value", None), None)
            self.commission = json_response.get(common_to_msg.get("commission", None), None)
            self.order_type = json_response.get(common_to_msg.get("order_type", None), None)
            self.swap = json_response.get(common_to_msg.get("swap", None), None)
            self.fee = json_response.get(common_to_msg.get("fee", None), None)
            self.t_exec = int(Timestamp(self.t_exec).timestamp() * 1e6)

        elif (json_response == "OK"):
            # Sometimes the response may not hold useful info but still be successful.
            self.success, self.comment = True, "OK"

        else:
            # If no valid response, assign the correct exception case.
            self.success, self.comment = False, str(json_response)
            self._exception = self._verify_response(self.comment)
        
        # Truncate the comment to 250 characters.
        self.comment = self.comment[: 250]
        # TODO: REMOVE WHEN FINISHED "WATCHER"
        # self.state: dict = dict()
        
#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class OrderRequest:
    """
    Handles order request processing and validation.
    """
    KEYS_WS_TO_API = dict()

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    class Type(IntEnum):
        """Enum representing different order types. Buys are positive, sells are negative."""
        BUY, SELL, BUY_LIMIT, SELL_LIMIT, BUY_STOP, SELL_STOP = +1, -1, +2, -2, +3, -3

    TYPE_STR_TO_ENUM, TYPE_INT_TO_ENUM = Type._member_map_, Type._value2member_map_
    TYPE_STR_TO_ENUM = {K.replace("_", ""): V for K, V in TYPE_STR_TO_ENUM.items()}

    #▄▄▄▄▄▄▄▄▄▄▄
    @classmethod
    def invert(cls, order_type: Type):
        """Inverts the order type."""
        inverted_int = - order_type.value
        return cls.TYPE_INT_TO_ENUM[inverted_int]

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    class Action(IntEnum):
        """Enum on different actions. Market orders are positive, pending orders are negative."""
        OPEN, OPEN_PEND, MODIFY, MODIFY_PEND, CLOSE, CLOSE_PEND, SL, TP = +1, -1, +2, -2, +3, -3, -4, +4

    ACTION_INT_TO_ENUM = Action._value2member_map_
    # MT4/5 enum naming to TM enum naming.
    ACTION_STR_TO_ENUM = dict(
        MarketOpen = Action.OPEN, PositionOpen = Action.OPEN, PendingOpen = Action.OPEN_PEND,
        MarketClose = Action.CLOSE, PositionClose = Action.CLOSE, PendingClose = Action.CLOSE_PEND,
        MarketModify = Action.MODIFY, PositionModify = Action.MODIFY, PendingModify = Action.MODIFY_PEND,
        MarketCloseBy = Action.CLOSE, PartialClose = Action.CLOSE,
        OnStopLoss = Action.SL, OnTakeProfit = Action.TP)

    # Some event classifications.
    STOP_EVENTS = [Action.SL, Action.TP]
    CLOSE_EVENTS = [Action.CLOSE, Action.CLOSE_PEND] + STOP_EVENTS
    OPEN_EVENTS = [Action.OPEN, Action.OPEN_PEND]
    MODIFY_EVENTS = [Action.MODIFY, Action.MODIFY_PEND]

    #▄▄▄▄▄▄▄▄▄▄▄▄
    @staticmethod
    def is_market(enum: Type): return (abs(enum.value) == 1)
    @staticmethod
    def is_limit(enum: Type): return (abs(enum.value) == 2)
    @staticmethod
    def is_stop(enum: Type): return (abs(enum.value) == 3)
    @staticmethod
    def is_buy(enum: Type): return (enum.value > 0)
    @staticmethod
    def is_sell(enum: Type): return (enum.value < 0)

    BOOL_SELECTORS = [is_buy, is_market, is_stop]

    #▄▄▄▄▄▄▄▄▄▄▄
    @classmethod
    def enum_to_bools(cls, enum: Type):
        """Decomposes order type into boolean flags.
        For binary-encoding trade message on source side."""
        if isinstance(enum, str): enum = cls.TYPE_STR_TO_ENUM[enum]
        return {BS.__name__: BS(enum) for BS in cls.BOOL_SELECTORS}
    
    #▄▄▄▄▄▄▄▄▄▄▄
    @classmethod
    def bools_to_enum(cls, bools: dict):
        """Composes order type from boolean flags.
        For decoding binary message to trade on target side."""
        value = 1 if bools["is_buy"] else -1
        if bools["is_market"]: return cls.TYPE_INT_TO_ENUM[value * 1]
        elif bools["is_stop"]: return cls.TYPE_INT_TO_ENUM[value * 3]
        else: return cls.TYPE_INT_TO_ENUM[value * 2]
    
    BOOL_SELECTOR_NAMES = [BS.__name__ for BS in BOOL_SELECTORS]
    
    VERBOSE = "{source} -> {target} (#{ticket})"

    #███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████

    COMMENT_MAXLEN = 48

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, **kwargs):
        """Initializes the OrderRequest object with the given specifics,
        directly from the decoded message on target side."""
        if ("t_send" not in kwargs):
            kwargs["t_send"] = int(time.time() * 1e6)

        for key, value in kwargs.items():
            # Map the keys to the correct ones for the API.
            key = self.KEYS_WS_TO_API.get(key, key)
            setattr(self, key, value)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def set_ticket(self, ticket: int):
        """Sets the ticket, for modifying/closing target orders."""
        self.ticket = ticket

#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
#████████████████████████████████████████████████████████████████████████████████████████████████████████████████  Subtypes  ███
#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class OrderSend(OrderRequest):
    """Handles order sending requests."""
    DEF_BASE_POINT = 0.00001
    VERBOSE = "{source} (#{ticket}) -> x{factor} -> {target} ({symbol} - {type} {lots:.2f})"
    KEYS_WS_TO_API = dict(symbol = "symbol", type = "operation", lots = "volume", comment = "comment",
                          openPrice = "price", stopLoss = "stoploss", takeProfit = "takeprofit")
    
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, **kwargs):

        if set(self.BOOL_SELECTOR_NAMES).issubset(kwargs):

            type_bools = {BS: kwargs.pop(BS, None) for BS in self.BOOL_SELECTOR_NAMES}
            # Compose the order type from the boolean flags from the source binary message.
            kwargs["type"] = self.bools_to_enum(type_bools)

        # For market orders, the price is not specified. API needs a zero price.
        if ("openPrice" not in kwargs): kwargs["openPrice"] = 0.0

        if ("symbol" in kwargs) and ("symbol_info" in kwargs):
            # Tries to gather the base and quote currencies from symbol specs.
            self.base = dict.get(kwargs["symbol_info"], "base")
            self.quote = dict.get(kwargs["symbol_info"], "quote")
        else:
            # If no symbol info, parse the symbol manually (not recommended).
            self.base = kwargs["symbol"][-3 :]
            self.quote = kwargs["symbol"][: -3]
            self.base_point = self.DEF_BASE_POINT

        super().__init__(**kwargs)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def exposure(self, leverage: float = 100):
        """Calculates the exposure of the order in USD."""
        value = self.price * self.volume / self.base_point
        exp = value * sign(self.operation.value) / leverage
        exp = round(exp, 2)
        # Quote increases exp, Base decreases exp.
        return {self.quote: exp, self.base: -exp}

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class OrderClose(OrderRequest):
    """Handles order closing requests. Lots specified for partial closes."""
    VERBOSE = "{source} -> {target} (#{ticket}) due to {action}"
    KEYS_WS_TO_API = dict(ticket = "ticket", slippage = "slippage",
                          closePrice = "price", lots = "lots")
    
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, **kwargs):

        # For closing orders, the price is not specified. API needs a zero price.
        if ("closePrice" not in kwargs): kwargs["closePrice"] = 0.0

        super().__init__(**kwargs)

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class OrderModify(OrderRequest):
    """Handles order modification requests. Needs at least one of the kwargs (OP/SL/TP)."""
    KEYS_WS_TO_API = dict(ticket = "ticket", openPrice = "price", stopLoss = "stoploss",
                          takeProfit = "takeprofit", expiration = "expiration")

#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class OrderRequest(OrderRequest):
    """Handles order request processing and validation."""
    # General patterns reused for multiple repeated cases:
    ZMQ_FORMAT_HEADER = ["action", "ticket", "source", "t_send", "t_sign"]
    ZMQ_FORMAT_OPEN = ["symbol", "base_value", "lots", "openPrice", "stopLoss", "takeProfit", "comment"]

    ZMQ_FORMATS = { # Sequential structure of the binary message created on source side, for each case
        
        OrderRequest.Action.OPEN_PEND:      ZMQ_FORMAT_HEADER + OrderRequest.BOOL_SELECTOR_NAMES + ZMQ_FORMAT_OPEN,
        OrderRequest.Action.OPEN:           ZMQ_FORMAT_HEADER + OrderRequest.BOOL_SELECTOR_NAMES + ZMQ_FORMAT_OPEN,
        OrderRequest.Action.MODIFY_PEND:    ZMQ_FORMAT_HEADER + ["openPrice", "stopLoss", "takeProfit"],
        OrderRequest.Action.MODIFY:         ZMQ_FORMAT_HEADER + ["stopLoss", "takeProfit"],
        OrderRequest.Action.CLOSE_PEND:     ZMQ_FORMAT_HEADER + ["is_buy", "closePrice", "lots"],
        OrderRequest.Action.CLOSE:          ZMQ_FORMAT_HEADER + ["is_buy", "closePrice", "lots"],
        OrderRequest.Action.SL:             ZMQ_FORMAT_HEADER + ["is_buy", "closePrice", "lots"],
        OrderRequest.Action.TP:             ZMQ_FORMAT_HEADER + ["is_buy", "closePrice", "lots"],
    }

    ACTION_ENUM_TO_SUB = {# Conversion from action enum to the correct OrderRequest subclass.
        
        OrderRequest.Action.OPEN: OrderSend,
        OrderRequest.Action.MODIFY: OrderModify,
        OrderRequest.Action.CLOSE: OrderClose,
        OrderRequest.Action.OPEN_PEND: OrderSend,
        OrderRequest.Action.MODIFY_PEND: OrderModify,
        OrderRequest.Action.CLOSE_PEND: OrderClose,
        OrderRequest.Action.SL: OrderClose,
        OrderRequest.Action.TP: OrderClose,
    }

    DF_MSG_TO_COMMON = { # Conversion from API's JSON format to common keys.
        MT_API.VersionMT.MT4: dict(ticket = "ticket", symbol = "symbol", type = "order",
            lots = "lots", rateOpen = "point_value", comment = "comment", openPrice = "price"),
        MT_API.VersionMT.MT5: dict(ticket = "ticket", symbol = "symbol", orderType = "order",
            lots = "lots", profitRate = "point_value", comment = "comment", openPrice = "price") }

    DF_COMMON_TO_MSG = { # Conversion from common keys to API's JSON format.
        MT_API.VersionMT.MT4: dict(ticket = "ticket", symbol = "symbol", order = "type",
            lots = "lots", point_value = "rateOpen", comment = "comment", price = "openPrice",
            action = "action", time = "openTime"),
        MT_API.VersionMT.MT5: dict(ticket = "ticket", symbol = "symbol", order = "orderType",
            lots = "lots", point_value = "profitRate", comment = "comment", price = "openPrice",
            action = "type", time = "openTime"), }

#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
#█████████████████████████████████████████████████████████████████████████████████████████████████████████████  Manual test  ███
#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
    
if (__name__ == "__main__"):
    
    params = {
        "action": OrderRequest.Action.OPEN,
        "ticket": 56283114,
        "source": "coco_test_oanda_5",
        "t_send": int(time.time() * 1e6),
        "is_buy": False,
        "is_market": True,
        "is_stop": False,
        "symbol": "EURUSD",
        "openPrice": 1.23456,
        "lots": 0.01,
        "stopLoss": 0.0,
        "takeProfit": 0.0,
        "comment": "",
    }
    order = OrderSend(**params)
    print(order.exposure())