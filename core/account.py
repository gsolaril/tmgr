#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
"""
Trading Account Management System

This module implements a comprehensive trading account management system that interfaces 
with MetaTrader 4 and 5 platforms. It handles account operations, trade execution,
risk management, and real-time monitoring through WebSocket connections.
"""
import os, sys, asyncio, websockets
from numpy import sign, inf as INF
from asyncio.exceptions import *
from typing import Callable
from pandas import Series, concat
from pandas import Timestamp, Timedelta
from pandas import DatetimeIndex

sys.path.append("./")

from core.utils import *
from core.actions import *

#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
#████████████████████████████████████████████████████████████████████████████████████████████████████████████  Account data  ███
#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████

#▄▄▄▄▄▄▄▄▄▄▄▄
class Account:
    """
    A class representing a trading account with comprehensive management capabilities.
    This class handles all aspects of trading account management including:
    - Account authentication and connection
    - Order execution and monitoring
    - Risk management
    - Real-time state tracking
    - WebSocket communication
    """

    # Standard exposure level and token length constants
    STD_EXP_LEV = 100
    TOKEN_LENGTH = 36

    class Category(IntEnum):
        """
        Enumeration of possible account categories.
        
        Categories:
            SOURCE: Creates the trades
            TARGET: Copies the trades
            TEST: Testing/simulation account
        """
        SOURCE, TARGET, TEST = 0, 1, 2

    # Mapping between category values and enum members
    CATEGORIES = { 0: Category.SOURCE, 1: Category.TARGET, 2: Category.TEST }
    
    #▄▄▄▄▄▄▄▄
    @property
    def __dict__(self):
        """
        Returns a dictionary containing essential account attributes.

        Output: (dict) Core account properties including alias, category, ID, and server details
        """
        return {"alias": self.alias, "category": self.category.name, "id": self.id,
                  "ip": self.ip, "server": self.server, "ssuffix": self.ssuffix}

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __repr__(self):
        """
        Creates a detailed string representation of the account.

        Output: (str) Formatted string with account details and WebSocket connection status.
        """
        string = "Account({alias}, cat: {category}, id: {id}, "
        string += "server: {server} @ {ip}, suffix: \"{ssuffix}\""
        if self.active_ws: string += ", active_ws"
        return f"({string})".format(**self.__dict__)
    
    #▄▄▄▄▄▄▄▄
    @property
    def string(self):
        """
        Provides a concise string identifier for the account for verbose purposes.

        Output: (str)  Short format account identifier
        """
        return f"{self.alias} ({self.server} #{self.id})"

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def verbose(self, subject: str):
        """
        Creates a verbose message for summarizing responses to HTTP requests.

        Inputs:
            subject (str): The action or event being logged

        Output (str): Formatted message including category and account details
        """
        category = self.category.name
        return f"/{subject} for {category} \"{self.string}\""
    
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, alias: str, id: int, password: str, server: str,
                 category: Category, mtver: MT_API.VersionMT = 5, **kw):
        """
        Initializes a new generic account instance.
        The initialization process includes setting up:
        - Authentication credentials
        - Connection parameters
        - Trade tracking variables
        - Risk management settings

        Inputs:
            alias (str): Unique identifier for the trading account
            id (int): Broker-assigned account number
            password (str): Account password for authentication
            server (str): Trading server hostname
            category (Category): Account classification (SOURCE/TARGET/TEST)
            mtver (MT_API.VersionMT): MetaTrader version (defaults to MT5)
            **kw: Additional configuration parameters
            
        """
        # Authentication and identification attributes
        self.token = None
        self.id = id
        self.password = password
        self.category = self.CATEGORIES[category]
        self.mtver = MT_API.MT_VERSIONS[mtver]
        self.server = server
        self.alias = alias
        self.owner = None

        # Server connection configuration
        self.ip: str = kw.pop("ip", None)
        self.websocket = None
        self.callback_ws = None
        self.active_ws = False

        # Update account's specific parameters
        self.reconfig(**kw)

        # Trade tracking counters
        self.n_ord_buy: int = 0
        self.n_ord_sell: int = 0
        self.balance: float = None
        self.eqstart: float = None
        self.equity: float = None
        self.margin: float = None
        self.leverage: float = None
        self.lots_net: float = None
        
        # Recent activity tracking dictionaries. These are mostly used by master accounts
        # to track activity patterns (repeated prices, net lot count, etc.) and further
        # restrict trading activity if needed.
        self.recent_order_map = dict()
        self.recent_prc_bands = dict()
        self.recent_lot_count = dict()
        self.recent_pos_count = dict()
        self.recent_exposures = dict()

        self.last_exec = Timestamp(0, tz = "UTC")

        # Validate MetaTrader version
        if self.mtver not in MT_API.MT_VERSIONS.values():
            Log.warning(f"Unknown version: {self.mtver}. "
                f"Using default, {MT_API.VER_DEFAULT.name}.")
            self.mtver = MT_API.VER_DEFAULT

        async def get_server_ip():
            """
            Gets the IP address based on the server label in the MT network.
            """
            self.ip = await MT_API.get_server_ip(self.server, self.mtver)
            
        # Initialize server IP if not provided
        if (self.ip is None): asyncio.run(get_server_ip())
        
    #███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████

    # Default risk management system parameters
    PARAMETERS_RMS = {
        "sym_suffix": "",       # Symbol suffix for broker-specific instruments
        "max_npos": 84,         # Maximum number of positions allowed
        "invert_trades": False, # Whether to invert trade directions
        "enforce_sl": 0,        # Overridden stop loss, distance in points from exec price
        "max_lots_sig": INF,    # Maximum lot size per single trade
        "max_lots_net": INF,    # Maximum total lot size overall
        "max_marg_usd": INF,    # Maximum margin usage in USD
        "max_loss_usd": INF,    # Maximum loss in USD since last reset
        "max_loss_prc": INF,    # Maximum loss as percentage since last reset
        "max_exposure": INF,    # Maximum exposure in USD measured per quote currency 
        "max_pricerange": 0,    # Maximum allowed price range in points. 0 means no restriction.
        "max_timerange": 0      # Maximum tolerance for unexecuted/delayed trades (in seconds)
    }

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def reconfig(self, **kw):
        """
        Reconfigures the account's risk management parameters.

        Inputs: Keyword arguments containing new parameter values.
        Uses default values from "PARAMETERS_RMS" if not specified.
        """
        self.recent_tickets = set()
        # Just go over each attribute and update with the new value if provided.
        # Otherwise, use the default value from "PARAMETERS_RMS".
        for label, default in self.PARAMETERS_RMS.items():
            value = kw.pop(label, default)
            if (value is None): value = default
            if (label == "sym_suffix"): label = "ssuffix"
            setattr(self, label, value)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def track_state(self, **kw):
        """
        Updates and tracks the account's current state. Updates critical account metrics,
        Including:
            - Balance and equity
            - Margin and leverage
            - Net position size
            - Profit and margin level

        Inputs: Optional state values as "kwargs". If empty, fetches current state from
        server and calculates positions.            
        """
        # Fetch current state if not provided, keep it for RMS
        if (len(kw) == 0):
            kw = await self.get_current_state()
            trades = await self.get_active_trades()
            is_buy = trades["order"].str.lower().eq("buy")
            is_sell = trades["order"].str.lower().eq("sell")
            lots_buy = trades.loc[is_buy, "lots"].sum()
            lots_sell = trades.loc[is_sell, "lots"].sum()
            kw["lots_net"] = lots_buy - lots_sell
            kw["eqstart"] = kw.get("equity", None)
            
        # Update account state variables
        self.balance = kw.get("balance")
        self.equity = kw.get("equity")
        self.margin = kw.get("margin")
        self.leverage = kw.get("leverage", None)
        self.lots_net = kw.get("lots_net", None)
        # Firstmost equity value, for later PNL calc.
        self.eqstart = kw.get("eqstart", None)
        
        # Calculate UPNL and margin level
        self.profit = self.equity - self.balance
        if (self.margin == 0): self.marg_lvl = 0.0
        else: self.marg_lvl = self.equity / self.margin

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def reset_token(self, token: str = None):
        """
        Resets or establishes the authentication token for the trading connection.
        Raises "AssertionError" if the token format is invalid or doesn't meet requirements.
        May initializes account state if not already done (not needed - mostly for outside tests)

        Inputs:
            token (str, optional): Specific token to validate. If None, requests a new one from the server.
        """
        # Request new token if starting from scratch
        # (none provided or previous error)
        if not isinstance(token, str):
            host_port = self.ip.split(":")
            IP = dict(zip(["host", "port"], host_port))
            if ("port" not in IP): IP["port"] = "443"

            # Prepare verbose string for logging.
            verbose = self.verbose(endpoint := "Connect") + "$"
            url = MT_API.get_base_url(self.mtver) + endpoint
            
            # Execute request and get token if possible
            data = {"user": self.id, "server": self.server, "password": self.password, **IP}
            if self.server.startswith("ACY"): data.pop("server") # Skip - issue from broker side.
            token = await MT_API.request(url, verbose, data, timeout = TOKEN_TIMEOUT)

        # Validate token format. Will retry later if still None.
        assert isinstance(token, str), token # Must be a string
        assert not token.startswith("#"), token # Must not be an error code
        assert (len(token) == self.TOKEN_LENGTH), token # Must be UUID4.
    
        self.token = token

        # Query account state if still unknown. RMS must be ready.
        if (self.balance is None): await self.track_state()

    #███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
    
    SUBS_CHANNELS = ["SubscribeOpenedOrdersTickets", "SubscribeOrderUpdate"]

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def peek(self, message: str):
        """
        Print a shortened JSON for WebSocket communication inspection.
        Mostly for knowing what's happening inside WebSocket stuff.

        Inputs:
            message (str): The WebSocket message to inspect

        Output: (str) Formatted message string, truncated if too long
        """

        peek = f"WS message @ \"{self.string}\""
        peek += f" ({len(message)} chars):\n => %s \n"
        if (len(message) < 450): return peek % message
        return peek % (message[: 200] + " ... " + message[-200 :])
        # Example:
        # WS message @ "MT5_API" (1000 chars):
        # => {"event": "order", "order": {"id": 1234567890, ..., "comment": "Test order"}}
        
    
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def connect_ws(self):
        """
        Establishes and maintains a WebSocket connection for real-time trading updates:
        - Validates account category and token availability
        - Subscribes to trading update channels
        - Maintains active connection and processes messages
        - Handles disconnection and cleanup

        Note:
            - Only source accounts need to be aware of trading activity
            - Requires valid token and callback function ("on_message")
            - Automatically handles reconnection on channel subscription failures
        """
        # Only source accounts need to be aware of trading activity.
        if (self.category != self.Category.SOURCE):
            return Log.warning(self.verbose("WS not needed")[1 :])

        # Needs the account's token first.
        if (self.token is None):
            verbose = self.verbose("Unavailable token")[1 :]
            verbose += ": Can't request for state variables" 
            if self.active_ws: return Log.error(verbose)
            else: return Log.info(verbose + " yet...")
        
        # Validate callback function
        if not isinstance(self.callback_ws, Callable):
            verbose = self.verbose("Callback is invalid for")[1 :]
            return Log.error(verbose + ": " + str(self.callback_ws))
        
        # Log callback information
        verbose = self.verbose("Callback being used")[1 :]
        Log.info(verbose + ": " + self.callback_ws.__name__)

        # Subscribe to trading channels
        channels_failed = set()
        for channel in self.SUBS_CHANNELS:

            verbose = self.verbose(endpoint := channel)
            url = MT_API.get_base_url(self.mtver) + endpoint
            data = {"id": self.token}

            response = await MT_API.request(url = url, data = data, verbose = verbose)
            is_response_ok = isinstance(response, list) | (response == "OK")
            if not is_response_ok:
                channels_failed.add(channel)
                # This means the token has gone obsolete. Create a new one.
                if (response.__contains__("#201 > Created(Client with id")):
                    Log.warning(self.verbose("Will reset token from")[1 :])
                    await self.reset_token()

        # Display failed channel subscriptions.
        if channels_failed:
            verbose = self.verbose("Unsuccessful WS connection")[1 :]
            verbose += f" - Rejected channels: {sorted(channels_failed)}"
            return Log.error(verbose)
        
        ########################################################################### IF ALL OK...
        
        # Get WS URL according to MT version, channels and token.
        url = MT_API.get_base_url(self.mtver, MT_API.ProtocolURL.WS) + ("Events?id=" + self.token)

        websocket: websockets.client.WebSocketClientProtocol = None
        async with websockets.connect(url, max_size = 10485760) as websocket:

            Log.success(self.verbose("WS connection")[1 :] + " successful.")
            self.active_ws = True
            while self.active_ws:
                try: # Get the last message and react to it.
                    message = await websocket.recv()
                    await self.callback_ws(self, message)
                except Exception as EXC:
                    Log.exception(EXC); self.active_ws = False

        Log.success(self.verbose("WS disconnection")[1 :] + " successful.")

        # Clean up connection
        verbose = self.verbose(endpoint := "Disconnect")
        # Disconnection endpoint URL.
        url = MT_API.get_base_url(self.mtver) + endpoint
        # Disconnecting cleans up API cache and enables a new token to be emitted later.
        await MT_API.request(url, data = {"id": self.token}, verbose = verbose) # holds retries in "request()"

    #███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████

    # Verbose message templates for state tracking and error reporting
    VERBOSE_STATE = "State of \"{alias}\" after {action}..." \
                  + " equity: {equity:.2f}, margin: {margin:.2f}," \
                  + " lots_net: {lots_prev:.2f} -> {lots_net:.2f}"
    
    VERBOSE_ERROR_MODIFY = "\"{alias}\", {action} with no real arguments: {args}"
    FORCE_RETRIES = 5

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def execute(self, order: OrderRequest, factor: float = None, override_symbol: str = None):
        """
        Executes a trading order with risk management controls:
        - Applies risk management rules before execution
        - Handles different order types (OPEN, MODIFY, CLOSE)
        - Manages retries on failure
        - Updates account state after successful execution

        Inputs:
            order (OrderRequest): The order form to be executed.
            factor (float, optional): Size multiplier for the order, coming from database.
            override_symbol (str, optional): Needed for symbols with unspecified suffixes.

        Output: (OrderResponse) The response from the trading server
        """
        # Prepare endpoint and order data
        endpoint = order.__class__.__name__
        # Order object in JSON format.
        json_order = {"id": self.token, **order.__dict__}
        url = MT_API.get_base_url(self.mtver)
        # Process the effective lot size based on:
        # - current account state and management rules.
        lots = self.RMS(order, factor)

        # Set account-specific parameters
        json_order["account"], json_order["server"] = self.id, self.server

        #######################################################

        # Prepare request JSON for the HTTP request.
        json_request = {"id": json_order["id"]}
        for key in order.KEYS_WS_TO_API.values():
            if key not in json_order: continue
            json_request[key] = json_order[key]

        if override_symbol: json_request["symbol"] = override_symbol

        # Store current position size for tracking
        lots_prev = sum(self.recent_lot_count.values())
        action: OrderRequest.Action = json_order.get("action", None)
        order_type: OrderRequest.Type = json_order.get("operation", None)

        # Handle OPEN orders
        if (action in order.OPEN_EVENTS):

            if (lots is None): return None
            symbol: str = json_request["symbol"]
            # Remove the suffix for compatibility with all accounts.
            symbol = symbol.rstrip(self.ssuffix)
            # Assures symbols will always have the needed suffix.
            json_request["symbol"] = symbol + self.ssuffix
            if self.invert_trades:
                # SLs and TPs become invalid so:
                json_request.pop("stoploss", None)
                json_request.pop("takeprofit", None)
                order_type = OrderRequest.invert(order_type)

            # For URL, enum to string.
            order_type_str: str = order_type.name.title()
            json_request["operation"] = order_type_str.replace("_", "")
            json_order["volume"] = json_request["volume"] = lots
            # Shorten comment for MT5 platform display.
            comment = json_request["comment"][: order.COMMENT_MAXLEN]
            json_request["comment"] = comment

        # Handle MODIFY orders
        if (action in order.MODIFY_EVENTS):
            needed = {"price", "stoploss", "takeprofit"}
            # Keep only the needed URL arguments.
            real_modify = needed.intersection(json_request)
            # If no arguments are left, Order Modify is meaningless.
            if not real_modify: return Log.debug(self.VERBOSE_ERROR_MODIFY.format(
                    alias = self.alias, action = action.name, args = json_request))

        # Handle CLOSE orders
        if (action in order.CLOSE_EVENTS):
            # Order Close implies the opposite of its parent.
            # (Buy -> Sell, Sell -> Buy)... just for printing.
            order_type = int(pow(-1, json_order["is_buy"]))
            order_type = order.TYPE_INT_TO_ENUM[- order_type]
            is_mt4 = (self.mtver == MT_API.VersionMT.MT4)
            is_pending = (action == order.Action.CLOSE_PEND)
            # OrderDelete is MT4-only, for pending orders.
            if is_pending and is_mt4: endpoint = "OrderDelete"
            json_request.pop("price"), json_request.pop("lots")

        # Don't specify execution price for market orders.
        if (action == order.Action.OPEN): json_request.pop("price")

        ################################################################

        # Prepare verbose description for logging
        description = " | " + order.VERBOSE.format(target = self.alias,
                        ticket = json_order["ticket"], factor = factor,
                        type = getattr(order_type, "name", order_type),
                        source = json_order["source"], lots = lots,
                        action = getattr(action, "name", action),
                        symbol = json_order.get("symbol", ""))
        
        ################################################################

        # Execute order with retries
        force_retries = 0
        while (force_retries < self.FORCE_RETRIES):

            force_retries = force_retries + 1 # Next retry.
            # Only print error in last retry to save space in logs.
            ignore = (force_retries < self.FORCE_RETRIES)
            verbose = f"{endpoint} [rt: {force_retries}]" + description
            # Execute the HTTP request on the order placement.
            json_response: dict = await MT_API.request(url = url + endpoint,
                verbose = verbose, ignore_errors = ignore, data = json_request,
                timeout = TRADE_TIMEOUT)

            # Process the HTTP response and create the response object accordingly,
            response = OrderResponse(self.alias, json_order, json_response, self.mtver)

            # Check if order from source account had SL/TP touched.
            already_closed_by_sl_tp = (force_retries == self.FORCE_RETRIES)
            already_closed_by_sl_tp &= (action in order.STOP_EVENTS)
            already_closed_by_sl_tp &= (is_order_invalid := isinstance(
                        response._exception, OrderResponse.InvalidOrder))                
            
            # if SL/TP already touched, ignore the trade-not-found error.
            if already_closed_by_sl_tp: response.success = True

            if response.success: break # No need to retry.
            # if there's any error, wait a bit before retrying.
            elif action in order.OPEN_EVENTS: await asyncio.sleep(0.5)
            elif action in order.MODIFY_EVENTS: await asyncio.sleep(0.5)
            elif action in order.CLOSE_EVENTS: await asyncio.sleep(0.5)

        ################################################################

        # Update account state after successful execution
        if response.success and (order_type is not None):
            order_int = getattr(order_type, "value", None)
            order_letter = "b" if (order_int > 0) else "s"
            # Again, standardize state on symbol without suffix.
            order.symbol = response.symbol.replace(self.ssuffix, "")
            
            # Update account info needed for RMS, when closing position.
            # E.g.: number of buy/sell/overall trades goes down,
            # net lot goes on direction opposite from original sign, etc.
            if (action in order.CLOSE_EVENTS):
                self.n_ord_buy = self.n_ord_buy - (order_int > 0)
                self.n_ord_sell = self.n_ord_sell - (order_int < 0)
                symbol_npos_key = f"{order.symbol}_{order_letter}"
                npos = self.recent_pos_count.get(symbol_npos_key, 0)
                self.recent_pos_count[symbol_npos_key] = npos - 1
                lots_new = json_order.pop("lots") * sign(order_int)
                self.lots_net = self.lots_net - lots_new

            # Update account info needed for RMS, when new position.
            # E.g.: number of trades goes up, net lot and exposure goes up, etc.
            elif (action in order.OPEN_EVENTS):
                self.last_exec = Timestamp.utcnow()
                # Add to buy (positive enum) or sell (negative enum) counter.
                self.n_ord_buy = self.n_ord_buy + (order_int > 0)
                self.n_ord_sell = self.n_ord_sell + (order_int < 0)
                symbol_npos_key = f"{order.symbol}_{order_letter}"
                npos = self.recent_pos_count.get(symbol_npos_key, 0)
                self.recent_pos_count[symbol_npos_key] = npos + 1
                lots_new = json_response.pop("lots") * sign(order_int)
                exp_new = order.price * lots_new / order.base_point
                # On buy: quote exp goes up, base exp goes down.
                exp_Q = self.recent_exposures.get(order.quote, 0.0)
                exp_B = self.recent_exposures.get(order.base, 0.0)
                self.recent_exposures[order.quote] = exp_Q + exp_new
                self.recent_exposures[order.base] = exp_B - exp_new
                self.lots_net = self.lots_net + lots_new

            # Print changes on account state after update.
            Log.info(self.VERBOSE_STATE.format(alias = self.alias,
                action = getattr(action, "name", action),
                equity = self.equity, lots_prev = lots_prev,
                margin = self.margin, lots_net = self.lots_net))

        return response

    #███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████

    # Risk Management System constants
    LOT_VALUE_MIN, LOT_VALUE_MAX = 0.01, 30.0
    VERBOSE_RMS_NPOS = "Already reached {npos}."
    VERBOSE_RMS_FREQ = "Wait until {next:%H:%M:%S} (max freq: {freq} secs)"

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def compress(cls, prev: float, plus: float, limit: float):
        """
        Calculates a compression factor for position sizing within limits.

        Inputs:
            prev (float): Previous/current value
            plus (float): Additional value to be added
            limit (float): Maximum allowed value

        Output: (float) Compression factor between 0.0 and 1.0
        """
        factor = (sign(plus) * limit - prev) / plus
        return max(0.0, min(factor, 1.0))

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def RMS(self, order: OrderSend, factor: float = None):
        """
        Main Risk Management System function that validates and adjusts order sizes.
        Naturally, the target trade size should be "lots = order.volume * factor".
        However, the actual trade size might be adjusted by the risk management system
        to ensure that the trade size does not exceed anyone from the imposed limits:
        Max number of trades, time since source trade was received, max net lot size,
        max exposure, max loss and max margin.

        Inputs:
            order (OrderSend): The order object to be validated. It contains the source trade size.
            factor (float): Size multiplier for the order coming from database.

        Output: (float) Adjusted target lot size that meets all risk criteria,
        or None if order should be rejected.
        """
        # Basic order validation
        if not isinstance(order, OrderSend): return
        
        # Calculate initial lot size
        if (factor is None): factor = 1.0
        if (factor <= 0): lots = abs(factor)
        else: lots = order.volume * factor

        # Check position count limits
        if not self.rms_npos(order):
            verbose = self.VERBOSE_RMS_NPOS.format(npos = self.max_npos)
            return Log.warning(self._rms_verbose(order) + verbose)

        # Check if source trade is already too old.
        if not self.rms_freq(order):
            next = self.last_exec + Timedelta(seconds = self.max_timerange)
            verbose = self.VERBOSE_RMS_FREQ.format(next = next, freq = self.max_timerange)
            return Log.warning(self._rms_verbose(order) + verbose)

        # Apply lot size boundaries based on symbol specifics from broker
        lots = min(max(self.LOT_VALUE_MIN, lots), self.LOT_VALUE_MAX)

        # Apply various risk management checks based on a max sum / total / net
        if (lots_rms_lots := self.rms_lots(lots, order)) < self.LOT_VALUE_MIN: return
        if (lots_rms_expo := self.rms_expo(lots, order)) < self.LOT_VALUE_MIN: return
        if (lots_rms_loss := self.rms_loss(lots, order)) < self.LOT_VALUE_MIN: return
        if (lots_rms_marg := self.rms_marg(lots, order)) < self.LOT_VALUE_MIN: return

        # Calculate final lot size
        lots = round(min([lots, lots_rms_lots, lots_rms_expo]), 2)
        return min(max(self.LOT_VALUE_MIN, lots), self.LOT_VALUE_MAX)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def rms_freq(self, order: OrderSend):
        """
        Verify if not more than a certain number of seconds ("max timerange") have
        passed since the source trade was received through the WebSocket.

        Inputs:
            order (OrderSend): The order to be validated

        Output: (bool) True if frequency limits are met, False otherwise
        """
        if (self.max_timerange is None): return False
        next_exec = self.last_exec + Timedelta(seconds = self.max_timerange)
        return (Timestamp.utcnow() > next_exec)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def rms_npos(self, order: OrderSend):
        """
        Validates order against maximum position count limits.
        Tracks positions separately for buy and sell sides.

        Inputs: order (OrderSend): The order to be validated

        Output: (bool) True if position count limits are met, False otherwise
        """
        side = order.operation.name.lower()[0]
        entry = f"{order.symbol}_{side}"
        npos = self.recent_pos_count.get(entry, 0)
        Log.debug(f"RMS-NPOS \"{self.string}\"... npos: {npos}, max: {self.max_npos}")
        if (self.max_npos == INF): return True
        else: return (self.max_npos > npos)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def rms_lots(self, lots: float, order: OrderSend):
        """
        Validates and adjusts lot size against position size limits.
        Checks both single position and net position limits

        Inputs:
            lots (float): Proposed lot size
            order (OrderSend): The order to be validated

        Output: (float) Adjusted lot size that meets position size limits.
        """
        verbose: str = self._rms_verbose(order)
        lots_sig = lots * sign(order.operation.value)
        lots_net = sum(self.recent_lot_count.values())

        # Check signal (single position) limit
        if (self.max_lots_sig != INF):
            lots = min(lots, self.max_lots_sig)
            verbose += f" \"{order.symbol}\" over signal limit "
            verbose += f"({lots:.2f} > {self.max_lots_sig:.2f})"
            # Lot too small or negative is already past the limit, even compressed.
            if (lots < self.LOT_VALUE_MIN): Log.warning(verbose)
                
        # Check net position limit
        if (self.max_lots_net != INF):
            # Use the "compress" function to try shrinking the lot if it exceeds.
            lots *= self.compress(lots_net, lots_sig, self.max_lots_net)
            verbose += f" \"{order.symbol}\" over net limit "
            verbose += f"({lots_net:.2f} + {lots:.2f} > {self.max_lots_net:.2f})"
            # Lot too small or negative is already past the limit, even compressed.
            if (lots < self.LOT_VALUE_MIN): Log.warning(verbose)
                
        return lots

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def rms_expo(self, lots: float, order: OrderSend):

        if (self.max_exposure != INF):

            side = sign(order.operation.value)
            cur_Q, cur_B = order.quote, order.base
            # Calculate exposure for the order, in USD units.
            value_order = order.price * lots / order.base_point
            exp_Q_order = + side * value_order / self.STD_EXP_LEV
            exp_B_order = - side * value_order / self.STD_EXP_LEV
            # Exposure on individual currencies will be compared with equity.
            equity = self.recent_exposures.get("_equity", self.equity)

            exp_Q_last = self.recent_exposures.get(cur_Q, 0.0) / self.STD_EXP_LEV
            exp_B_last = self.recent_exposures.get(cur_B, 0.0) / self.STD_EXP_LEV
            exp_Q_next = exp_Q_last + exp_Q_order
            exp_B_next = exp_B_last + exp_B_order
            # Max exposure as percentage of equity, in USD units:
            exp_max = equity * self.max_exposure 
            #factor_Q = self.compress(exp_Q_last, exp_Q_order, exp_max)
            #factor_B = self.compress(exp_B_last, exp_B_order, exp_max) 

            verbose = f"{self.string} exposure on {order.operation.name} #{order.ticket} =>"
            verbose = verbose + f" {cur_Q} {exp_Q_last:+.0f}{exp_Q_order:+.0f}={exp_Q_next:+.0f},"
            verbose = verbose + f" {cur_B} {exp_B_last:+.0f}{exp_B_order:+.0f}={exp_B_next:+.0f}"
            Log.debug(verbose + f" / {exp_max:.0f}")

            # If exposure on any currency is over the max, reject the order.
            if (abs(exp_Q_next) > exp_max) or (abs(exp_B_next) > exp_max):
                Log.warning(self._rms_verbose(order) + f"exposure over max")
                lots = 0
        
        return lots
    
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def rms_loss(self, lots: float, order: OrderSend):

        return lots # Not active for the time being
        if (order.price is None) or (order.price == 0.0): return lots
        if (order.stoploss is None) or (order.stoploss == 0.0): return lots
        
        # Calculate the value of the lot in the base currency.
        lot_value = order.base_value / order.base_point
        # Calculate the stop loss width in pips.
        sl_width = abs(order.price - order.stoploss)
        # Calculate the loss in the base currency.
        lot_to_loss = lot_value * sl_width
        
        if (self.max_loss_usd != INF) or (self.max_loss_prc != INF):

            loss_max = self.max_loss_prc * self.eqstart
            loss_max = min(self.max_loss_usd, loss_max)

            loss_max -= self.eqstart - self.equity
            lots = min(lots, loss_max / lot_to_loss)

        return lots
            
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def rms_marg(self, lots: float, order: OrderSend):

        return lots # Not active for the time being
        if (order.price is None) or (order.price == 0.0): return lots
        if (self.leverage is None) or (self.leverage == 0.0): return lots

        # Calculate the value of the lot in the base currency.      
        lot_value = order.base_value / order.base_point
        # Calculate the leverage factor.
        lev_factor = order.price / self.leverage
        # Calculate the margin required for the lot.
        lot_to_marg = lot_value * lev_factor

        if (self.max_marg_usd != INF) or (self.max_marg_prc != INF):

            marg_max = self.max_marg_prc * self.equity
            marg_max = min(self.max_marg_usd, marg_max)

            marg_max -= self.margin
            lots = min(lots, marg_max / lot_to_marg)      
        
        return lots
    
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _rms_verbose(self, order):
        
        return f"RMS blocked \"{order.source}, {order.ticket} -> {self.alias}\" ({order.operation.name} @ {order.symbol}) => "

#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
#█████████████████████████████████████████████████████████████████████████████████████████████████  Account state retrieval  ███
#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████

    HIST_DEFAULT_DELTA = Timedelta(hours = 4)
    HIST_DEFAULT_UTC = Timedelta(hours = 3)

    COLUMNS_ORDERS_MAIN = {
        MT_API.VersionMT.MT4: dict(
            symbol = "symbol", lots = "lots", type = "order", openTime = "t_entry", closeTime = "t_exit",
            openPrice = "p_entry", closePrice = "p_exit", stopLoss = "p_sl", takeProfit = "p_tp", profit = "value",
            comment = "comment"),
        MT_API.VersionMT.MT5: dict(
            symbol = "symbol", lots = "lots", orderType = "order", openTime = "t_entry", closeTime = "t_exit",
            openPrice = "p_entry", closePrice = "p_exit", stopLoss = "p_sl", takeProfit = "p_tp", profit = "value",
            comment = "comment"),
    }
    
    SAMPLE_SERVER = "MyServerGlobal-Server"
    TEMPLATE_ORDERS = DataFrame(columns = COLUMNS_ORDERS_MAIN[MT_API.VersionMT.MT5].values()).rename_axis("ticket")
    VERBOSE_ERROR_NODF = "Trades' dict is not a dict. Check whether previous exception disabled the trade retrieval."
    MILLIY_SUFFIXES_QUERY = f"SELECT DISTINCT(sym_suffix) FROM {TABLE_ACCS} WHERE (server = '{SAMPLE_SERVER}')"
    MILLIY_SUFFIXES = read_sql(MILLIY_SUFFIXES_QUERY, DB_URL)["sym_suffix"].to_list()

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def get_current_state(self):
        """
        Retrieves the current values for the account state variables.
        
        Output: (dict) Current account state including balance, equity, margin, etc.
        """
        
        if (self.token is None):
            verbose = self.verbose("Unavailable token")[1 :]
            verbose += ": Can't request for state variables" 
            return Log.error(verbose)

        data = {"id": self.token}
        verbose = self.verbose(endpoint := "AccountSummary")
        url = MT_API.get_base_url(self.mtver) + endpoint
        response = await MT_API.request(url = url, data = data,
                    verbose = verbose, timeout = TOKEN_TIMEOUT)

        if not isinstance(response, dict):
            error = self.verbose("Failed in requesting state values")
            Log.error(self.verbose(error)[1 :]); series = dict()
            # This means the token has gone obsolete. Create a new one.
            if (response.__contains__("#201 > Created(Client with id")):
                Log.warning(self.verbose("Will reset token from")[1 :])
                await self.reset_token()
        else:
            self.leverage = response["leverage"]
            self.balance = response["balance"]
            self.equity = response["equity"]
            self.margin = response["margin"]
            # Starting value for equity. Useful for measuring PNL.
            if self.eqstart is None: self.eqstart = self.equity

            series = dict(server = self.server,
                alias = self.alias, account = self.id,
                timestamp = int(time.time() * 1e6),
                balance = round(self.balance, 2),
                equity = round(self.equity, 2),
                margin = round(self.margin, 2),
                leverage = self.leverage)

        return Series(series)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def get_active_trades(self):
        """
        Retrieves the current active trades for the account.

        Output: (DataFrame) Current active trades with specified columns.
        """
        
        if (self.token is None):
            verbose = self.verbose("Unavailable token")[1 :]
            verbose += ": Can't request for active trades" 
            return Log.error(verbose)

        verbose = self.verbose(endpoint := "OpenedOrders")
        url = MT_API.get_base_url(self.mtver) + endpoint

        response: dict = await MT_API.request(url = url,
            data = {"id": self.token}, verbose = verbose)

        if not isinstance(response, (list, dict)):
            error = self.verbose("Failed in requesting state values")
            Log.error(self.verbose(error)[1 :]); response = dict()
            # This means the token has gone obsolete. Create a new one.
            if (response.__contains__("#201 > Created(Client with id")):
                Log.warning(self.verbose("Will reset token from")[1 :])
                await self.reset_token()
        
        # When no active trades, return an empty DataFrame but with the right columns.
        if (len(response) == 0): return self.TEMPLATE_ORDERS.copy()
        # Return the trades as a DataFrame with the right columns.
        return self.process_trades(response)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def get_closed_trades(self, **kwargs):
        """
        Retrieves the closed trades for the account. Not used nowadays.

        Output: (DataFrame) Closed trades with specified columns.
        """
        
        if (self.token is None):
            verbose = self.verbose("Unavailable token")[1 :]
            verbose += ": Can't request for closed trades"
            return Log.error(verbose)

        until: Timestamp = kwargs.pop("until", Timestamp.utcnow())
        delta: Timedelta = kwargs.pop("delta", self.HIST_DEFAULT_DELTA)
        since: Timestamp = kwargs.pop("since", until - delta)
        until += self.HIST_DEFAULT_UTC

        # Convert the timestamps to MT5 API format.
        until_str = until.strftime(MT_API.DT_FORMAT)
        since_str = since.strftime(MT_API.DT_FORMAT)

        verbose = self.verbose(endpoint := "OrderHistory")
        url = MT_API.get_base_url(self.mtver) + endpoint
        
        response: dict = await MT_API.request(url, verbose = verbose,
            data = {"id": self.token, "from": since_str, "to": until_str})

        if isinstance(response, str): return
        if (len(response) == 0): return
        if (self.mtver == MT_API.VersionMT.MT5):
            response = response.pop("orders", None)
        if (response is None) or (len(response) == 0): return

        # Return the trades as a DataFrame with the right columns.
        return self.process_trades(response)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def process_trades(self, trades: DataFrame):

        try:
            # Columns selected to parse the trades based on the MT version.
            msg_to_common = self.COLUMNS_ORDERS_MAIN[self.mtver].copy()
            # Check if the response is even convertible to DataFrame first.
            assert isinstance(trades, (list, dict)), self.VERBOSE_ERROR_NODF
            trades: DataFrame = DataFrame(trades)
            # Ticket will always be the index as it is unique.
            trades.index = trades.pop("ticket").astype(str).sort_index()
            # Select the columns and rename them to the common format.
            trades: DataFrame = trades[list(msg_to_common)].rename(
                        errors = "ignore", columns = msg_to_common)
            # Convert the timestamps to UNIX microsecond format.
            ms = lambda ts: Timestamp(ts).timestamp() * 1e6
            
            suffixes = [self.ssuffix] # Remove suffix from symbols to standardize across accounts
            if (self.ssuffix in self.MILLIY_SUFFIXES): suffixes = self.MILLIY_SUFFIXES.copy()
            for suffix in suffixes: trades["symbol"] = trades["symbol"].str.rstrip(suffix)
            
            trades.loc[trades["t_entry"].str.startswith("0"), "t_entry"] = "1970-01-01"
            trades.loc[trades["t_exit"].str.startswith("0"), "t_exit"] = "1970-01-01"
            # Convert the timestamps into large integers to avoid overflow.
            trades["t_entry"] = DatetimeIndex(trades["t_entry"]).map(ms).astype("int64")
            trades["t_exit"] = DatetimeIndex(trades["t_exit"]).map(ms).astype("int64")
            # Missing or negative timestamps may imply active (not closed) trades.
            trades.loc[trades["t_exit"] < 0, "t_exit"] = None
            trades[["alias_s", "ticket_s", "exit"]] = None
            try: comm = trades.pop("comment").str.split("|")
            except: comm = Series("", index = trades.index)

            if (self.category == self.Category.TARGET):
                # Extract the alias, ticket, and comment from the comment column.
                # For tracking trade-to-trade relationships between source and target.
                trades["alias_s"] = comm.str[0]
                trades["ticket_s"] = comm.str[1]
                comment = comm.str[2].fillna("")
                # Keep real comment from source trade. "exit" holds exit reason.
                trades["comment"], trades["exit"] = comment, ""
                # Comment may have been overwritten if source trade closed by SL/TP.
                sl = comment.str.contains("\[sl\]$", regex = True)
                tp = comment.str.contains("\[tp\]$", regex = True)
                mc = comment.str.contains("\[.*close.*\]$", regex = True)
                trades.loc[sl, "exit"], trades.loc[tp, "exit"] = "sl", "tp"
                trades.loc[mc, "exit"] = "manual"

            else: trades["comment"] = comm.str[0]

        except Exception as EXC:
            Log.exception(EXC)
            # Return an empty DataFrame with the right columns.
            trades = self.TEMPLATE_ORDERS.copy()

        return trades

#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
#████████████████████████████████████████████████████████████████████████████████████████████████████████████████  Run test  ███
#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
            
if (__name__ == "__main__"):

    alias = "bbox10"
    DB_HOST_DEF = "127.0.0.1" # DB_HOST
    DB_URL_DEF = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST_DEF}:{DB_PORT}/{DB_NAME}"
    accounts: DataFrame = read_sql(TABLE_ACCS, con = DB_URL_DEF, index_col = "alias")
    
    account = accounts.loc[alias].to_dict()
    version = MT_API.MT_VERSIONS[account.pop("mtver")]
    category = Account.CATEGORIES[account.pop("category")]
    account = Account(alias = alias, **account,
          category = category, mtver = version)
    
    order = OrderSend(source = "manual",
        symbol = "EURUSD", comment = "?",
        action = OrderRequest.Action.OPEN,
        t_send = int(time.time() * 1e6),
        is_market = True, lots = 0.01,
        is_buy = False, is_stop = False, 
        stopLoss = 0.0, takeProfit = 0.0)

    async def query_and_send_order():
        await account.reset_token()
        TD = {"delta": Timedelta(days = 2)}
        state = await account.get_current_state()
        active = await account.get_active_trades()
        #closed = await account.get_closed_trades(**TD)
        active["alias"] = account.alias
        #summary = TMReshape.trades_to_lot_exposures(active \
        #        .reset_index().set_index(["alias", "ticket"]))
        #active = active.to_string(max_cols = active.shape[1], max_rows = 100)
        #closed = closed.to_string(max_cols = closed.shape[1], max_rows = 100)
        #summary = summary.to_string(max_cols = summary.shape[1], max_rows = 100)
        #print(summary)
        #print("Current state:", state, "", sep = "\n")
        #print("Recent active:", active, "", sep = "\n")
        #print("Recent closed:", closed, "", sep = "\n")
        #print("Summary active:", summary, "", sep = "\n")
        # resp = await account.execute(order, factor = 1.0)
        # Log.debug("Test order response:\n%s" % resp.__dict__)

    asyncio.run(query_and_send_order())

    if False:
        
        async def on_message(account: Account, message: str):
            Log.debug(f"From \"{account.alias}\":\n{message}")

        account.callback_ws = on_message
        asyncio.run(account.run())