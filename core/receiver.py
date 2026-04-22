#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
"""
    This file contains the StreamExecutor class, which is a subclass of StreamBase.
    It handles the execution of orders and the processing of responses from the source accounts. It doesn't manage
    tokens because it's better to have an outsider single-responsibility node for that (Watcher). Also it's the
    only node that actually needs WebSocket support because the relevant trading activity to be copied is coming
    from the source accounts (not the other way around).
"""
import sys, time, msgpack
from pandas import DataFrame

sys.path.append("./")

from core.account import *
from core.actions import *
from core.base import *
from core.utils import *

#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
#████████████████████████████████████████████████████████████████████████████████████████████████████████████████  Receiver  ███
#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
            
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class StreamReceiver(StreamBase):

    ACCOUNT_CATEGORIES_STR = str(Account.Category.SOURCE.value)
    SPECS_FIELDS = ["standard", "point"]
    AUTO_UPDATE_STATE_VARS = True
    SUFFIX_PARAMETERS = "freq_"
    TOKEN_MANAGER = False
    OUTER_STREAM = False

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, name: str):
        """
        Initialize the StreamExecutor instance. It's a subclass of StreamBase.
        It handles the execution of orders and the processing of responses from the source accounts.
        It doesn't manage tokens because it's better to have an outsider single-responsibility node for that (Watcher).
        Needs WebSocket support because the relevant trading activity to be copied is coming from the source accounts.

        Inputs:
            name (str): The name of the executor instance.
            nzmq (int): The number of ZMQ watchers to create.
        """

        super().__init__(name, mode_zmq = "PUB")
        self.last_action = Timestamp.utcnow() - Timedelta(minutes = 5)
        self.last_msg = time.time() * 1e9 # Last WS message timestamp, whichever source.
        self.tasks.update({ # Cron tasks focused on WS connection and source account RMS.
            self._ping:                 {"next_run": time.time(), "freq": self.freq_ws_ping},
            self._unstash_prc_bands:    {"next_run": time.time(), "freq": self.freq_update_summary},
            self._unstash_pos_count:    {"next_run": time.time(), "freq": self.freq_update_summary},
        })
        
    #██████████████████████████████████████████████████████████████████████████████████████████████████████████████  Websocket

    VERBOSE_IGNORE = "Rejecting trade from {account}: {symbol} is rejected by config (ignoring: {ignore_symbols})"    
    ERROR_RESEND = "Rejecting message from {account}: #{ticket} previously executed (possible duplicate). Skipping..."
    VERBOSE_MESSAGE = "\"{account}\", WS message {did} through ZMQ ({delay:.0f} ns, {bytes:.0f} bytes):\n"
    
    VERBOSE_WARNING = "Warning: \"{account}\" order #{ticket}: "

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def _on_message_ws(self, account: Account, raw: str):
        """
        Handle incoming messages from the WebSocket server. This callback replaces the one
        from the base class, because it's specific to the source accounts. Parses the message,
        validates its content according to the account's context, discriminates it based on RMS
        rules, and if applicable, sends it to the ZMQ server.

        Inputs:
            account (Account): The account that sent the message.
            raw (str): The raw message from the WebSocket server.
        """

        self.last_msg = time.time() * 1e9 # Last WS message timestamp, whichever source.
        if not self.is_enabled_ws_recv: return # If WebSocket disabled from global config, skip.
        # Messages from MT5 WS larger than 6000 chars are valid orders. More than 1.5MB imply errors.
        if (len(raw) > 6000): Log.debug(account.peek(raw)) # If debug enabled, print truncated message.
        if (len(raw) > 1.5e6): return Log.error("JSON too long!") # Can't interpret too long messages.
        message: dict = await self.parse_message_ws(raw, account) # Parse the message based on account's context.
        if (message is None) or not len(message): return # Reject messages denied by parser (e.g.: invalid order type).

        ########################################### Getting ready for ZMQ

        # Get symbol and action from message.
        symbol = message.get("symbol", None)
        action = message.get("action", None)
        action = OrderRequest.Action(action)

        # Standard form of symbol name without suffixes or weird things.
        if (symbol is not None): message["symbol"] = self.map_symbol(
                symbol = symbol, server = account.server, std = True)

        symbol = message.get("symbol", None)
        action = message.get("action", None)
        max_npos = getattr(account, "max_npos", INF)

        # Get previous value of position count for the symbol.
        recent_pos_count: dict = getattr(account, "recent_pos_count")
        npos_before = npos_after = recent_pos_count.get(symbol, 0)
        if (action in OrderRequest.OPEN_EVENTS): npos_after = npos_before + 1
        elif (action in OrderRequest.CLOSE_EVENTS): npos_after = npos_before - 1

        recent_pos_count[symbol] = max(npos_after, 0) # Update position count on symbol.
        verbose_count = f"{npos_before} -> {npos_after} / {max_npos}\n{recent_pos_count}"
        setattr(account, "recent_pos_count", recent_pos_count.copy())
        Log.debug(verbose_count)

        if (symbol in self.set_ignore_symbols) and (action in OrderRequest.OPEN_EVENTS):
            return Log.warning(self.VERBOSE_IGNORE.format(account = account.string,
                        symbol = symbol, ignore_symbols = self.set_ignore_symbols))

        # Apply RMS rules to the message based on source account config and stashables.
        message = self.global_order_control(message, account, symbol) # comment out if error
        if (message is None) or not len(message): return # Reject messages denied by RMS rules.
        message = {key: message[key] for key in message.pop("_form")} # comment out if error
        # The parser function will purposefully insert the order fields required by ZMQ; inside "_form" key.
        if (message is None) or not len(message): return # Reject messages missing anyone of "_form" fields.

        message_list = list(message.values())
        # Pack the message into a bytes object for ZMQ.
        encoded: bytes = msgpack.packb(message_list)
        # The "executor" node on the other side will unpack it and rebuild
        # the message dict because it will know the required "_form" fields.
        verbose_args = {"delay": time.time() * 1e9 - self.last_msg,
                    "bytes": len(encoded), "account": account.string}
        
        # Print the processed message about to be sent through ZMQ.
        verbose = self.VERBOSE_MESSAGE.format(**verbose_args,
            did = "sent" if self.is_enabled_zmq_recv else "not sent")
        
        # Send the message through ZMQ if enabled from global config.
        if self.is_enabled_zmq_recv: await self.socket.send(encoded)
        Log.info(verbose + str(message) + "\nPosition counter: " + verbose_count)

    #▄▄▄▄▄▄▄▄▄▄▄
    @classmethod
    async def parse_message_ws(cls, message: str, account: Account):
        """
        Parse the message from the WebSocket server. This is a class method because
        it may be necessary to test with different messages manually copied from outside
        sources or terminals (e.g.: API swaggers or documentation examples).

        The message is expected to be a JSON-like string, and will go through various
        validation filters and conditions to assure that it implies a valid trade action
        (order send, order modify, order close, order delete, etc.).

        Depending on the trade action, the required fields for the ZMQ encoding will be
        different. The "_form" key is introduced in the message to ease the transition by
        including these appropriate fields, in the correct order.

        Inputs:
            message (str): The raw message from the WebSocket server.
            account (Account): The account that sent the message.

        Output: (dict) The parsed message with the required "_form" fields and the correct
            value formats. If the message is None, it means that it was rejected by those
            validation filters and conditions, and won't be taken into account.
        """
        # Parse the message as JSON.
        try: message: dict = json.loads(message)
        except Exception as EXC:
            Log.error(f"On \"{account.string}\"...")
            return Log.exception(EXC)

        # Copy the list of ZMQ field sets that may be required for easier
        # access. It will introduce the right one in "_form" at the end.
        msg_to_common = OrderRequest.DF_MSG_TO_COMMON[account.mtver].copy()
        common_to_msg = OrderRequest.DF_COMMON_TO_MSG[account.mtver].copy()

        ts_event = message.pop("timestampUTC", None) # Timestamp of the message, in ms.
        field_base_value = common_to_msg["point_value"] # Point value of the symbol.
        field_order_type = common_to_msg["order"] # Order type (Buy, BuyStop, Sell, etc.)
        field_price_open = common_to_msg["price"] # Price at which the order was opened.
        field_comment = common_to_msg["comment"] # Original comment of the source order.
        field_action = common_to_msg["action"] # Action to be performed (Open, Close, etc.)
        field_time = common_to_msg["time"] # Timestamp of order execution from server side.

        reason = message.pop("type", None) # Event that gave birth to the message.
        message = message.pop("data", None) # Get more order-specific (inner) data from message.
        if (reason != "OrderUpdate"): return # We need updates on any activity on source account.
        if (message is None): return # Message without data is not trade-related, hence rejected.

        # Outer level of the JSON holds recent values of state variables (balance, equity, etc.)
        states: dict = message.copy() 
        # Info about active trades. Good for immediate update of local counters (lot count, etc.)
        orders = states.pop("openedOrders") 
        # Actual content related to the most recent trade activity that triggered this message.
        message: dict = states.pop("update")
        await account.track_state(**states) # Update local state variable values with "states".
        
        orders: DataFrame = DataFrame(orders) # Convert list of active orders into a DataFrame
        if orders.empty: # If no active orders, set all counters (lot count, exposure) to zero.
            account.n_ord_buy = 0
            account.n_ord_sell = 0
            account.lots_net = 0.0
        else: # Process the dataframe and recount current trading activity.
            orders = orders[list(msg_to_common.keys())]
            orders = orders.rename(columns = msg_to_common)
            orders = orders.set_index("ticket").sort_index()
            # Use "buy" and "sell" strings for positive or negative lots.
            order_type = orders["order"].str.lower()
            is_buy = order_type.str.startswith("buy")
            is_sell = order_type.str.startswith("sell")
            lots_buy = orders.loc[is_buy, "lots"].sum()
            lots_sell = orders.loc[is_sell, "lots"].sum()
            # Update local counters with the new values.
            account.lots_net = lots_buy - lots_sell
            account.n_ord_buy = is_buy.sum()
            account.n_ord_sell = is_sell.sum()

        try: # Verify validity of trade action.
            action = message[field_action] # Open, Close, Modify, etc.
            # Convert to enum for compatibility with OrderRequest class.
            action_enum = OrderRequest.ACTION_STR_TO_ENUM[action]
            # Get the "_form" fields in the right order for ZMQ encoding.
            message_form = OrderRequest.ZMQ_FORMATS[action_enum]
        except Exception as EXC:
            Log.error(f"On \"{account.string}\"...")
            return Log.exception(EXC)
        
        # Get specific order data from the message. (SL, TP, lot size, etc.)
        message = message.pop("order")
        # Get the ticket number of the order.
        ticket = message.get("ticket")
        
        # Update the message with the processed action and base value.
        message.update(action = action_enum.value,
            base_value = message[field_base_value])

        # If the action is an open event, check for certain conditions.
        if (action_enum in OrderRequest.OPEN_EVENTS):
            warning = cls.VERBOSE_WARNING.format(
                account = account.string, ticket = ticket)
            # Price must be non-zero, otherwise order signal is not real.
            if ((price_open := message.get(field_price_open)) == 0.0):
                return Log.warning(warning + f"\"{field_price_open} == 0\"")
            # Reject already executed orders (API may sometimes give duplicates).
            if (ticket in account.recent_tickets): # Tickets need to be unique.
                account.recent_tickets.remove(ticket) # Dupes happen only once: don't accumulate.
                return Log.warning(warning + "Order already executed")
            else: account.recent_tickets.add(ticket) # Add new ticket to the already-executed set.

        try:
            # Get the order type from the message and convert to enum.
            order_type = str.upper(message[field_order_type])
            order_enum = OrderRequest.TYPE_STR_TO_ENUM[order_type]
            # Convert enum to boolean flags for easier processing.
            order_dict = OrderRequest.enum_to_bools(order_enum)
            # If the account is configured to invert trades, invert the order type.
            if account.invert_trades: # Source account configured to invert trades.
                order_dict["is_buy"] = not order_dict["is_buy"] # Invert on boolean encoding.
                message["stopLoss"] = message["takeProfit"] = 0 # SL & TP are not valid anymore.
            message.update(**order_dict) # Update the message with the corrected order information.

        except KeyError:
            Log.error(f"On \"{account.string}\"...")
            return Log.error("Order type not found")
        except Exception as EXC:
            Log.error(f"On \"{account.string}\"...")
            return Log.exception(EXC)

        if (ts_event is None):
            # If no timestamp is provided, use the server's timestamp.
            # Those are only needed for measuring server-side latency.
            ts_event = Timestamp(message[field_time])
            ts_event = ts_event.timestamp() * 1e3

        # Insert the definitive contextual information...
        # ...including the ZMQ-required "_form" fields.
        message.update({"source": account.alias,
                "t_send": int(time.time() * 1e6),
                "t_sign": int(ts_event * 1e3),
                "_form": message_form })
        
        # If the comment is not provided, set it to an empty string.
        if message[field_comment] is None: message[field_comment] = ""
        return message # {key: message[key] for key in message_form} # uncomment if error

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _ping(self):
        """Measure amount of time between messages from the WebSocket server, alert if needed.
        Channel \"OpenedOrdersTickets\" in Account objects serve as frequent heartbeat, then
        it should assure a minimal, steady and constant rate of message income."""
        if ((idle_time := time.time() - self.last_msg / 1e9) < self.freq_ws_ping): return
        Log.error(f"WebSocket has caught no message for at least %.1f secs!" % idle_time)

#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
#████████████████████████████████████████████████████████████████████████████████████████████████████  Global order control  ███
#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████

    WARNING_LIMITER_PRICE_REF = " ({price:.5f} <= {bp1:.5f}-{bp2:.5f})"
    WARNING_LIMITER_PR_ASSIGN = "Next order ({account}, {side} {symbol}) settles avoidable price band" + WARNING_LIMITER_PRICE_REF
    WARNING_LIMITER_PR_BLOCK = "Next order ({account}, {side} {symbol}) avoided by previous price band" + WARNING_LIMITER_PRICE_REF
    WARNING_LIMITER_PR_CLEAR = "Next order ({account}, {side} {symbol}) clears previous price band " + WARNING_LIMITER_PRICE_REF
    ERROR_LIMITER_PR_NOSYMBOL = "Exit order ({account}, {side} {symbol}) applies to a symbol not being traded. Did you reset TM?"
    WARNING_LIMITER_NOLIMIT = "Price-limiting bandwidth for \"{account}\" is 0. Last order executed as it is."
    WARNING_LIMITER_ENFORCE_SL = "Next order ({account}, {side} {symbol}) @ {price} enforces SL... {old} -> {new} ({enforce_sl})"
    WARNING_LIMITER_POS_COUNT = "Next order from {account} avoided because trade count {npos} has already reached max {nmax}"

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _unstash_prc_bands(self):
        """Unstash the price bands from the cache. To be used as cron task."""

        stashable = self.Stashable.PRC_BANDS ; label = stashable.name
        # Skip and let some time pass if the last action was recent enough.
        delta = (Timestamp.utcnow() - self.last_action).total_seconds()
        # This avoids that the local StreamBase pos-count dataframe which
        # may already be updated, is overridden by the unstashed data.
        if (delta < self.freq_update_summary): return Log.warning(
            self.VERBOSE_STASH_SKIP, header = label, delta = delta,
            obj = self.__class__.__name__)
        # Unstash the position count data. Keep as dict.
        prc_bands: dict = self.unstash(stashable)
        data: dict = None; pbands_raw: str = None
        
        # For each Redis outer index-hashmap pair...
        for alias, data in prc_bands.items():
            # Inactive accounts don't need update.
            if not (alias in self.accounts): continue
            account: Account = self.accounts[alias]
            # Configured max tolerance for price bandwidth on source account.
            pband_width = getattr(account, "max_pricerange") # given in points.
            # Price is actually given as an integer (= price / point-size)
            if pband_width: pband_width = int(pband_width) # assure integer.
            else: continue

            for symbol, pbands_raw in data.items():
                pbands = set()
                for pband in pbands_raw.split(" "):
                    # When re-converted to float, the result become discrete
                    # (dividing an integer value by another integer value)
                    pband = float(pband) / pband_width
                    # So it actually works as a "band"!
                    pbands.add(int(pband) * pband_width)
                data[symbol] = pbands # Update discretized price band.
            
            # The recent price-band buffer is actually an account instance attribute.
            setattr(account, "recent_prc_bands", data)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _unstash_pos_count(self):
        """Unstash the position count from the cache. To be used as cron task."""

        stashable = self.Stashable.POS_COUNT ; label = stashable.name
        # Skip and let some time pass if the last action was recent enough.
        delta = (Timestamp.utcnow() - self.last_action).total_seconds()
        # This avoids that the local StreamBase pos-count dataframe which
        # may already be updated, is overridden by the unstashed data.
        if (delta < self.freq_update_summary): return Log.warning(
            self.VERBOSE_STASH_SKIP, header = label, delta = delta,
            obj = self.__class__.__name__)
        # Unstash the position count data. Keep as dict.
        pos_count = self.unstash(stashable)
        if not len(pos_count): return

        # Convert the dict into a DataFrame full of symbol/account counters.
        pos_count = DataFrame.from_dict(pos_count, orient = "index")
        # Stashable originally separates into buy and sell counters. (e.g.: EURUSD_b, EURUSD_s). 
        pos_count.columns = pos_count.columns.str.split("_").str[0]
        # This is only needed for monitoring. Here we will group-by and sum for each symbol anyway.
        pos_count = pos_count.rename_axis("symbol", axis = "columns")
        pos_count = pos_count.rename_axis("alias").stack("symbol")
        pos_count = pos_count.groupby(["alias", "symbol"]).sum()
        pos_count = pos_count.unstack("symbol") # Account aliases are indexes, symbols are columns.

        # Any source account missing in the unstashed data just hasn't traded yet.
        mis_index = sorted(set(self.accounts).difference(pos_count.index))
        # Add the missing source accounts with counters equal to zero.
        mis_count = DataFrame(0, columns = pos_count.columns, index = mis_index)
        pos_count: DataFrame = concat((pos_count, mis_count)).fillna(0.0)
        pos_count = pos_count.sort_index().astype(int)

        #if stashable == self.Stashable.POS_COUNT:
            #print("#" * 100, "\n ---- UNSTASHING:\n")
            #print(pos_count.to_string(), "\n" + "#" * 100)

        for alias, counters in pos_count.items():
            # Inactive accounts don't need update.
            if not (alias in self.accounts): continue
            account: Account = self.accounts[alias]
            # Only source accounts are monitored.
            if (account.category != Account.Category.SOURCE): continue
            # The recent position counter is actually an account instance attribute.
            setattr(account, "recent_pos_count", counters)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def global_order_control(self, message: dict, account: Account, symbol: str):
        """Apply source-specific RMS measures to the message."""

        symbol_info = self.specs.loc[account.server].loc[symbol].to_dict()
        message = self.global_limiter_pricerange(message, account, symbol_info)
        message = self.global_limiter_tradecount(message, account)
        message = self.global_limiter_enforce_sl(message, account)
        
        return message

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def global_limiter_tradecount(cls, message: dict, account: Account):
        """
        RMS measure: limit the number of trades per symbol.
        
        Inputs:
        - message: (dict) the message to be controlled.
        - account: (Account) the source account to be controlled.

        Outputs: (dict) the validated message, or none if filtered out.
        """
        if (abs(message["action"]) != 1): return message
        # Recent position counter from the account instance attribute.
        counters: dict = getattr(account, "recent_pos_count")
        # Configured max number of positions from the account instance.
        max_count: int = getattr(account, "max_npos")
        all_count = sum(counters.values()) # Sum for all symbols.
        # TODO: filter out by symbol as well. Not needed right now...

        verbose = cls.WARNING_LIMITER_POS_COUNT.format(
            account = account.string, nmax = max_count, npos = all_count)
        if (all_count > max_count): Log.warning(verbose); message = None

        return message
    
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def global_limiter_enforce_sl(cls, message: dict, account: Account):
        """
        RMS measure: enforce a stop-loss on the message. Not a filter: just
        modifies the message if the "enforce_sl" is set in the account config.
        
        Inputs:
        - message: (dict) the message to be controlled.
        - account: (Account) the source account to be controlled.

        Outputs: (dict) the validated message, or none if filtered out.
        """
        if message is None: return message
        # If no stop-loss enforced or equal to 0, keep as it is.
        if account.enforce_sl is None: return message
        if (account.enforce_sl == 0.0): return message
        # Only new trades are considered (OrderSend events)
        if abs(message.get("action")) > 1: return message
        point_size = pow(10.0, - message.get("digits"))
        # Convert stop-loss points into signed price difference.
        new_sld = abs(account.enforce_sl * point_size)
        side = +1 if message.get("is_buy") else -1
        side_str = "BUY" if (side > 0) else "SELL"
        price = message.get("openPrice")
        # Calculate the actual enforced stop-loss price.
        old_sl = message.get("stopLoss", 0.0)
        old_sld = abs(old_sl - price)
        # If the old stop-loss implies less risk, keep it.
        adj_sld = min(new_sld, old_sld)
        adj_sl = round(price - side * adj_sld, 5)
        message["stopLoss"] = adj_sl # Update the message.
        verbose = cls.WARNING_LIMITER_ENFORCE_SL.format(
            account = account.string, side = side_str, price = price,
            symbol = message["symbol"], old = old_sl, new = adj_sl,
            enforce_sl = account.enforce_sl)
        Log.warning(verbose)
        return message
    
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def global_limiter_pricerange(cls, message: dict, account: Account, symbol_info: dict):
        """
        RMS measure: filter out trades where the price is too close to an already active trade
        of the same type (buy/sell). The actual distance tolerance is given by the account's
        "max_pricerange" attribute.

        Inputs:
        - message: (dict) the message to be controlled.
        - account: (Account) the source account to be controlled.
        - symbol_info: (dict) the symbol information.

        Output: (dict) the validated message, or none if filtered out.
        """
        pband_width: int = getattr(account, "max_pricerange")
        if (pband_width == 0): # No max price-range set: no need to filter.
            Log.warning(cls.WARNING_LIMITER_NOLIMIT.format(account = account.string))
            return message
        
        if (message is None): return None
        action: int = message.get("action", None)
        price = message.get("openPrice", None)
        is_buy = message.get("is_buy", None)
        # If price is not set, trade is probably invalid.
        if not bool(price): return message

        symbol, point = symbol_info["standard"], symbol_info["point"]
        verbose_dict = {"symbol": symbol, "account": account.alias}
        verbose = None

        sign = +1 if is_buy else -1
        pband: int = int(price / point) # Get band back as integer.
        # Store the price as negative if it's a selling trade. Bands
        # need to be distinct for each side, so as to allow hedging.
        # (So that a buy doesn't necessarily block a sell of same price.)
        pband = sign * int(pband / pband_width) * pband_width
        verbose_dict["side"] = "buy" if is_buy else "sell"
        # Calculate the price of the band edges as real float prices.
        verbose_dict["bp2"] = (pband + pband_width) * point # Upper edge
        verbose_dict["bp1"] = pband * point # Lower edge
        # Keep the original source price for verbose.
        verbose_dict["price"] = price

        if (abs(action) == 1): # When opening new trades...
            # ...a new band needs to be included into the set.

            if not (symbol in account.recent_prc_bands):
                # If symbol not yet in set, create it with the new band.
                account.recent_prc_bands[symbol] = {pband}
                verbose: str = cls.WARNING_LIMITER_PR_ASSIGN
            elif not (pband in account.recent_prc_bands[symbol]):
                # If symbol already in set, but band is absent, append it.
                set.add(account.recent_prc_bands[symbol], pband)
                verbose: str = cls.WARNING_LIMITER_PR_ASSIGN
            # Price of source trade lies inside a band already:
            # occupied by another... trade needs to be rejected.
            else: verbose, message = cls.WARNING_LIMITER_PR_BLOCK, None
        
        elif (abs(action) == 3): # When closing trades...
            # ...its band turns available: remove from set.

            if not (symbol in account.recent_prc_bands):
                # Weird case: trade is not in set, but is still closing. May
                # happen during TradeManager reset without cleaning Redis first..
                verbose = cls.ERROR_LIMITER_PR_NOSYMBOL
            elif (pband in account.recent_prc_bands[symbol]):
                # If band is present in set, remove it.
                set.remove(account.recent_prc_bands[symbol], pband)
                verbose = cls.WARNING_LIMITER_PR_CLEAR

        if verbose: Log.warning(verbose.format(**verbose_dict))
        return message

#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
#████████████████████████████████████████████████████████████████████████████████████████████████████████████████  Run test  ███
#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
    
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄
async def main():
    obj = StreamReceiver(name = SESSION_NAME)
    await asyncio.gather(*obj.coroutines.values())

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
if (__name__ == "__main__"):
    
    try: asyncio.run(main())
    except KeyboardInterrupt:
        Log.warning("Ctrl+C, interrupted")
        Log.success("See you next time :)")
        
