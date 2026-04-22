#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
"""
    This file contains the StreamExecutor class, which is a subclass of StreamBase.
    It handles the execution of orders and the processing of responses from the target accounts.
    It doesn't manage tokens because it's better to have an outsider single-responsibility node for that (Watcher).
    Doesn't need WebSocket support because it reacts to ZMQ messages from the executor which is already reading WS.
"""
import os, sys, msgpack, uuid, asyncio
from pandas import Series, DataFrame
from pandas import concat, Timestamp
from numpy import sign

sys.path.append("./")

from core.utils import *
from core.account import *
from core.actions import *
from core.base import *

#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
#█████████████████████████████████████████████████████████████████████████████████████████████████████████████  Stream node  ███
#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
            
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class StreamExecutor(StreamBase):
    """
    This class is a subclass of StreamBase.
    It handles the execution of orders and the processing of responses from the target accounts.
    It doesn't manage tokens because it's better to have an outsider single-responsibility node for that (Watcher).
    Doesn't need WebSocket support because it reacts to ZMQ messages from the executor which is already reading WS.
    """
    ACCOUNT_CATEGORIES_STR = str(Account.Category.TARGET.value)
    SPECS_FIELDS = ["standard", "point", "base", "quote"]
    AUTO_UPDATE_STATE_VARS = True
    SUFFIX_PARAMETERS = "freq_"
    PRINT_ORDMAP_UPDATES = 5
    TOKEN_MANAGER = False
    OUTER_STREAM = False

    #████████████████████████████████████████████████████████████████████████████████████████████████████████████  Constructor

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, name: str, nzmq: int = 3):
        super().__init__(name, mode_zmq = "SUB", nzmq = nzmq)
        self.last_action = Timestamp.utcnow() - Timedelta(minutes = 5)
        self.mult, self.responses = DataFrame(), list()
        # Unstash relevant stashables that source accounts need.
        self._unstash_order_map() # Order map - outer indexes are target tickets themselves.
        self._unstash_lot_count() # Lot count - RMS will restrict trades to be copied if target lot size above limit.
        self._unstash_exposures() # Exposures - RMS will restrict trades to be copied if target exposure above limit.
        self.watchers = 0

        self.tasks.update({
            # Update the lot size multipliers between source and target accounts.
            self._update_multipliers: {"next_run": time.time(), "freq": self.freq_query_accounts},
            # Update the responses from the source accounts.
            self._update_responses: {"next_run": time.time(), "freq": self.freq_update_responses},
            # Unstash the order map.
            self._unstash_order_map: {"next_run": time.time(), "freq": self.freq_update_summary},
            # Unstash the position count.
            self._unstash_pos_count: {"next_run": time.time(), "freq": self.freq_update_summary},
            # Unstash the lot count.
            self._unstash_lot_count: {"next_run": time.time(), "freq": self.freq_update_summary},
            # Unstash the exposures.
            self._unstash_exposures: {"next_run": time.time(), "freq": self.freq_update_summary},
        })
        # Create the ZMQ "watchers": they are just coroutine runners that work as separate threads.
        for n in range(1, nzmq + 1): self.coroutines[f"zmq{n}"] = asyncio.create_task(self._watch(n))

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def _watch(self, n: int):
        """
        This method is called when a new ZMQ watcher is created. It's a coroutine that runs as a separate thread.
        It receives messages from the source accounts and parses them. This is done to multithread the ZMQ message
        handling so as to allow persistence and avoid thread-blocking when a lot of messages are received at once.

        Inputs:
            n (int): The identifying number of the watcher.
        """
        self.watchers += 1
        while self.active:
            # Read last message from the source accounts.
            message = await self.socket.recv()
            # Deserialize the message to the original string.
            message = msgpack.unpackb(message)
            # Parse the message and react according to it.
            try: await self.parse_message_zmq(message)
            # Most errors are handled accordingly. So in case
            # an unknown or fatal error occurs, stop the watcher.
            except Exception as EXC:
                self.active = False
                return Log.exception(EXC)
    
    #█████████████████████████████████████████████████████████████████████████████████████████████████████████████████ Unstash

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _unstash_order_map(self):
        """Unstash the order map stashable. Used as cron task wrapper"""
        # Dictionary is used as it is. Not turned into a DataFrame
        # because the so-called "columns" are actually the target
        # and may not be equal across all "rows" (source tickets).
        # E.g.: Account T2 copies from S2, account T1 copies from S1.
        self.order_map: dict = self.unstash(self.Stashable.ORDER_MAP)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _unstash_pos_count(self):
        """Unstash the position count stashable. Used as cron task wrapper"""

        stashable = self.Stashable.POS_COUNT ; label = stashable.name
        # Skip and let some time pass if the last action was recent enough.
        delta = (Timestamp.utcnow() - self.last_action).total_seconds()
        # This avoids that the local StreamBase pos-count dataframe which
        # may already be updated, is overridden by the unstashed data.
        if (delta < self.freq_update_summary): return Log.warning(
            self.VERBOSE_STASH_SKIP, header = label, delta = delta,
            obj = self.__class__.__name__)
        # Unstash the position count data. Keep as dict.
        pos_count: dict = self.unstash(stashable)
        # For each Redis outer index-hashmap pair...
        for alias, data in pos_count.items():
            if not (alias in self.accounts): continue # Inactive accounts don't need update
            # The recent position counter is actually an account instance attribute.
            setattr(self.accounts[alias], "recent_pos_count", data)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _unstash_lot_count(self):
        """Unstash the lot count stashable. Used as cron task wrapper"""

        stashable = self.Stashable.LOT_COUNT ; label = stashable.name
        # Skip and let some time pass if the last action was recent enough.
        delta = (Timestamp.utcnow() - self.last_action).total_seconds()
        # This avoids that the local StreamBase lot-count dataframe which
        # may already be updated, is overridden by the unstashed data.
        if (delta < self.freq_update_summary): return Log.warning(
            self.VERBOSE_STASH_SKIP, header = label, delta = delta,
            obj = self.__class__.__name__)
        # Unstash the lot count data. Keep as dict.
        lot_count: dict = self.unstash(stashable)
        # For each Redis outer index-hashmap pair...
        for alias, data in lot_count.items():
            if not (alias in self.accounts): continue # Inactive accounts don't need update
            # The recent lot counter is actually an account instance attribute.
            setattr(self.accounts[alias], "recent_lot_count", data)
        
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _unstash_exposures(self):
        """Unstash the exposure stashable. Used as cron task wrapper"""

        stashable = self.Stashable.EXPOSURES ; label = stashable.name
        # Skip and let some time pass if the last action was recent enough.
        delta = (Timestamp.utcnow() - self.last_action).total_seconds()
        # This avoids that the local StreamBase exposures dataframe which
        # may already be updated, is overridden by the unstashed data.
        if (delta < self.freq_update_summary): return Log.warning(
            self.VERBOSE_STASH_SKIP, header = label, delta = delta,
            obj = self.__class__.__name__)
        # Unstash the exposures data. Keep as dict.
        exposures: dict = self.unstash(stashable)
        # For each Redis outer index-hashmap pair...
        for alias, data in exposures.items():
            if not (alias in self.accounts): continue # Inactive accounts don't need update
            # The recent exposures are actually an account instance attribute.
            setattr(self.accounts[alias], "recent_exposures", data)

    #████████████████████████████████████████████████████████████████████████████████████████████████████████████████████  ZMQ
    
    VERBOSE_MESSAGE = "Received message ({will} trade):\n{params}"
    WARNING_WATCH = "Executor \"%s\" started watcher #%d."
    ERROR_TRADE_EXISTS = "New source trade from \"{source}\" already exists: \"#{ticket}\""
    ERROR_TRADE_NOT_FOUND = "Source trade from \"{source}\" during \"{action_str}\" not found: \"#{ticket}\""

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def parse_message_zmq(self, message: list):
        """Parse the ZMQ message and react according to it."""
        
        t_recv = int(1e6 * time.time())
        # Extract the action type and the source ticket first.
        action_int, ticket_source = message[0], int(message[1])
        # Convert the action type to an enum. (e.g.: "Action.SEND")
        action_enum = OrderRequest.ACTION_INT_TO_ENUM[action_int]
        # Get the order subclass for the action type. (e.g.: OrderSend)
        OrderSubclass = OrderRequest.ACTION_ENUM_TO_SUB[action_enum]
        # Get the relevant fields' keys for the action type.
        action_keys = OrderRequest.ZMQ_FORMATS[action_enum]
        # Create a dict with the relevant fields as keys and the values from the message.
        params: dict = dict(zip(action_keys, message))
        # Add the action type as a key.
        params["action"] = action_enum
        Log.info(self.VERBOSE_MESSAGE.format(params = params,
            will = "will" if self.is_enabled_zmq_exec else "won't"))
        # Timestamp of message reception to measure delay afterwards.
        params["t_recv"] = t_recv
        # If the executor is disabled, just don't continue.
        if not self.is_enabled_zmq_exec: return

        try: # Use right order subclass and response according to action type.
            if (OrderSubclass is OrderSend): await self.orders_send(params)
            elif (ticket_source in self.order_map): await self.orders_modify(params)
            # If the action is to modify an order that doesn't exist, return an error.
            else: return Log.error(self.ERROR_TRADE_NOT_FOUND.format(**params,
                action_str = action_enum.name))
            
        except Exception as EXC:
            return Log.exception(EXC)

    #████████████████████████████████████████████████████████████████████████████████████████████████████████████████████

    VERBOSE_UPD_RESPONSES = "Uploaded {n} responses, since \"{t_first}\" (delay = {delay} ns)."

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _update_responses(self):
        """Assimilate the most recently kept responses into one
        timeseries-like DataFrame, then write to the database."""
        if self.responses:

            t_start = time.time() * 1e9
            # Create timeseries-like DataFrame from response objects.
            responses = DataFrame(self.responses).set_index("t_resp")
            # Get earliest response's timestamp for further verbose.
            t_first = Timestamp.fromtimestamp(responses.index[0] / 1e6)
            # Drop these columns. Already included with other labels.
            responses = responses.drop(["state", "_exception"],
                         axis = "columns", errors = "ignore")
            # Add number of ZMQ watchers (analyze correlation with delay).
            responses["nzmq"] = self.nzmq
            # Write to the database.
            with self.connDB.connect() as conn:
                responses.to_sql(name = TABLE_RESP, con = conn,
                            index = True, if_exists = "append")
            
            self.responses = list() # Renew the list.
            t_first = t_first.strftime("%Y/%m/%d %X")
            
            Log.info(self.VERBOSE_UPD_RESPONSES.format(
                n = len(self.responses), t_first = t_first,
                delay = int(time.time() * 1e9 - t_start)))
            
        else: Log.debug("No order-copying responses")

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _update_multipliers(self):
        """Update the local table of lot multipliers
        between source and target accounts."""
        t_start = time.time() * 1e9
        # Read the multipliers from the database.
        mult = read_sql(TABLE_MULT, con = DB_URL)
        # Set the index to the source and target accounts.
        mult = mult.set_index(["source", "target"])
        # Fill NaNs with 0.0 and sort by index. Any target trade which
        # has 0 as multiplier with the source trade will be later avoided.
        mult = mult["factor"].fillna(0.0).sort_index()
        
        if not self.mult.empty:
            # Store specific changes for verbose.
            mult = {"before": self.mult, "now": mult}
            mult: DataFrame = concat(mult, axis = "columns")
            changes = mult["now"].ne(mult["before"])
            # Replace old multipliers with new ones.
            self.mult = mult["now"].rename("factor")
            changes = mult.loc[changes].reset_index()
        else: self.mult = mult; changes = Series()

        name = self.__class__.__name__
        delay = int(time.time() * 1e9 - t_start)
        # Format and print the list of changes for verbose.
        FORMAT = " -> {0[source]}/{0[target]}: {0[before]} to {0[now]}"
        verbose = f"\"{name}\" updated multipliers (delay = {delay} ns)."
        if changes.empty: Log.debug(verbose + " No changes.")
        else:
            changes = str.join("\n", changes.agg(FORMAT.format, axis = "columns"))
            Log.info(verbose + " %d changes:\n%s" % (len(changes), changes))

    #█████████████████████████████████████████████████████████████████████████████████████████████████████████  Event handling

    ERROR_NO_SPEC = "Symbol properties \"{server} / {symbol}\" not found in database."
    ERROR_NO_MULT = "Lot multiplier between \"{source}\" and \"{target}\" is zero or not found."
    WARNING_OFFLINE = "Tried to close trade #{ticket} from currently inactive account \"{target}\"."
    COMMENT_FORMAT = "{source}|{ticket}|{comment}"
            
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def orders_send(self, params: dict):
        """
        Send an order to all target accounts. This may handle the complete
        lifecycle of the order: creation, emission, execution and response.

        Inputs:
            params (dict): The parameters of the order.
        """
        source = params["source"] # Source account alias.
        ticket = params["ticket"] # Source ticket.
        symbol = params["symbol"] # Source symbol.
        orders, errors = dict(), dict()

        if (ticket in self.order_map):
            # API may sometimes send JSONs twice.
            error = self.ERROR_TRADE_EXISTS.format(**params)
            # Duplicate tickets mean duplicate trades. Not allowed.
            return Log.error(error)

        # Get the quote and base of the source symbol from the specs...
        params["quote"] = self.specs.at[(Account.SAMPLE_SERVER, symbol), "quote"]
        params["base"] = self.specs.at[(Account.SAMPLE_SERVER, symbol), "base"]
        # Add the source account alias and ticket to the comment, this way:
        # "source|ticket|comment". (allows tracking of target trade in MT5)
        params["comment"] = self.COMMENT_FORMAT.format(**params)
        # Create a fresh new instance of the order object accordingly.
        order: OrderSend = OrderSend(**params)

        async def on_iter_order():
            """Iterate over all target accounts and send the target orders concurrently."""
            account: Account = None
            accounts_traded = set()
            source_symbol = order.symbol
            processes = dict() # Generic array of concurrent processes.

            for target, account in self.accounts.items():

                # Source accounts are not supposed to be used as targets.
                if (account.category == Account.Category.SOURCE): continue

                try:
                    # Map the source symbol to the target account's server.
                    target_symbol = self.map_symbol(source_symbol, account.server, std = False)
                    # Get the base point size of the target symbol from the specs.
                    # Needed for any type of correction or adjustment in the SL/TP values,
                    # as well as for the calculation of the exposure in USD.
                    order.base_point = self.specs.at[(account.server, target_symbol), "point"]
                except (KeyError, AssertionError): # If the symbol is not in the specs, or if specs are
                    # incomplete for it, use the symbol name and point size from source symbol directly.
                    errors[target] = self.ERROR_NO_SPEC.format(
                        server = account.server, symbol = source_symbol)
                    order.base_point, target_symbol = None, source_symbol

                try: assert ((factor := self.mult.at[source, target]) != 0)
                # If the multiplier is not found in the multipliers table, or is
                # zero, source and target are unrelated: skip this trade whatsoever.
                except (KeyError, AssertionError):
                    Log.warning(self.ERROR_NO_MULT.format(
                        source = source, target = target)) ; continue

                # Skip if the account has already traded this source order.
                if (account.alias in accounts_traded): continue
                # Otherwise, add the account to the set of traded accounts.
                else: accounts_traded.add(account.alias) # Mostly for verbose.

                # Execute the order on the target account.
                processes[target] = account.execute(order, factor, override_symbol = target_symbol)

            # Force the concurrent execution of the order on all target accounts.
            responses = await asyncio.gather(*processes.values())
            # Gather and process responses altogether as one response dataset.
            responses = dict(zip(processes.keys(), responses))

            response: OrderResponse = None
            for target, response in responses.items():
                if (response is None): continue
                # Add new ticket to local order map.
                if response.success: 
                    orders[target] = response.ticket
                # Process the response for the target account.
                # That is: update local copy of stashables...
                # (recent lot count, exposures, order map, etc.)
                self.process_response(response)

        await on_iter_order()

        if len(errors):
            header = "Error, order send (\"%s\", #%d)" % (source, ticket)
            errors_str = "\n".join([" -> %s: %s" % KV for KV in errors.items()])
            Log.error(header + "\n" + errors_str)

        self.order_map[ticket] = orders # Store the modified version of the order map.
    
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def orders_modify(self, params: dict):
        """
        Modify an existing order on all target accounts. This
        includes the closing, deletion and SL/TP change of orders.

        Inputs:
            params (dict): The parameters of the order.
        """
        # Get the subclass of the order request action.
        action_enum: OrderRequest.Action = params["action"]
        # Get the source account alias and ticket from the parameters.
        source, ticket = params.get("source"), params.pop("ticket")
        # Get the subclass of the order request action.
        OrderSubclass = OrderRequest.ACTION_ENUM_TO_SUB[action_enum]
        # Check if trade needs to close whatsoever (not to be modified).
        is_close = (action_enum in OrderRequest.CLOSE_EVENTS)
        # Check if the trade needs to close because of the source touching SL/TP.
        is_stop = (action_enum in OrderRequest.STOP_EVENTS)
        # Initialize a dictionary to store errors for each target account.
        errors = dict()

        # Get all of the target tickets that depend on the source trade.
        subtickets: dict = self.order_map[ticket]
        # If the order map didn't initially contain the source ticket,
        # Then there are not related target trades. Skip this one.
        if (subtickets is None): return
        # Make a deep copy of the subtickets dictionary.
        subtickets_new: dict = subtickets.copy()

        verbose = "Target tickets for \"%s #%s\":\n -> %s"
        Log.info(verbose % (source, ticket, subtickets))

        async def on_iter_order():
            """
            Async coroutine to iterate over all target accounts and
            modify the dependent orders, then return the responses.
            """
            processes = list() # Generic array of concurrent processes.

            for target, ticket in subtickets.items():
                # If the target account is offline, skip.
                if target not in self.accounts:
                    Log.warning(self.WARNING_OFFLINE.format(
                        target = target, ticket = ticket)); continue
                # Get the account object.
                account: Account = self.accounts[target]
                # Create a new order request object.
                order = OrderSubclass(**params, ticket = ticket)
                # Execute the order on the target account.
                processes.append(account.execute(order = order))

            # Force the concurrent execution of the order on all target accounts.
            responses = await asyncio.gather(*processes)
            # Gather and process responses altogether as one response dataset.
            responses = dict(zip(self.accounts, responses))

            response: OrderResponse = None
            for target, response in responses.items():
                if (response is None): continue
                # Check if the order was successful or if it needs to be closed.
                ok = (response.success or is_stop)
                if ok and is_close: subtickets_new.pop(target, None)
                # If the order was not successful, add the error to the errors dictionary.
                elif not ok: errors[target] = response.comment
                # Process the response for the target account.
                self.process_response(response)

        await on_iter_order() # Execute the order on all target accounts.

        # Order map may need to be changed only if source tickets changed.
        # Thus: in cases where any trade has been deleted/closed.
        if is_close: self.order_map[ticket] = subtickets_new

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def process_response(self, response: OrderResponse):
        """
        Process the response from an order execution. This updates the
        recent lot count and exposures for the account, and creates the
        response object which holds the consequences of the executor's
        function calls themselves.

        Inputs:
            response (OrderResponse): The response from an order execution.
        """
        # Append this most recent response to the list of responses.
        self.responses.append(response.__dict__.copy())
        # If the account is not in the accounts dictionary, return.
        if not (response.alias in self.accounts): return
        # Get the account object that gave birth to this response.
        account: Account = self.accounts[response.alias]
        # If the account is not found or the order was not successful, return.
        if (account is None) or not response.success: return

        # Get the action enum from the response.
        action = OrderRequest.Action._member_map_[response.action]
        # Check if the action is a closing or opening event.
        is_close = OrderRequest.CLOSE_EVENTS.__contains__(action)
        is_open = OrderRequest.OPEN_EVENTS.__contains__(action)
        # In case the action is just a modification, no need to
        # touch any local stashable (lot count, exposures, etc).
        if not (is_close or is_open): return

        order_str: str = response.order_type # API JSON response returns strings only.
        order_str = order_str.upper().replace("_", "") # make string convertible to enum.
        order = OrderRequest.TYPE_STR_TO_ENUM[order_str] # order enum from type string.
        lot, price = response.lots, response.price_open # order context specifics.
        # Convert the response's symbol label to its legitimate standard.
        symbol: str = self.map_symbol(response.symbol, account.server, True)
        # Get the specs for the symbol: base, quote and point size.
        specs: dict = self.specs.loc[Account.SAMPLE_SERVER, symbol].to_dict()
        point, base, quote = specs["point"], specs["base"], specs["quote"]
        if (quote is None): quote = symbol[: -3] # e.g. EURUSD -> EUR
        if (base is None): base = symbol[-3 :] # e.g. EURUSD -> USD
        # Get the sign of the order and the action to calculate the exposure.
        # Signs should be: Open buy +, open sell -, close buy -, close sell +.
        sign_order, sign_action = sign(order.value), pow(-1, is_close)
        exp = (lot := lot * sign_order * sign_action) * (price / point)

        # Create a dictionary of the response's context and effect for verbose.
        data = {"ticket": response.ticket, "action": action, "order": order,
                "symbol": response.symbol, "standard": symbol, "quote": quote,
                "base": base, "lot": lot, "exp": exp}
        
        VERBOSE_PROCESS_RESPONSE = f"Processed trade response @ \"{account.alias}\"...\n* Content: {data}" \
        f"\n* Lot count: {account.recent_lot_count} => %s\n * Exposures: {account.recent_exposures} => %s" \

        # If the symbol is not in the stashable, set it to 0.0, will be updated below.
        if not (symbol in account.recent_lot_count): account.recent_lot_count[symbol] = 0.0
        if not (quote in account.recent_exposures): account.recent_exposures[quote] = 0.0
        if not (base in account.recent_exposures): account.recent_exposures[base] = 0.0

        # Update account attributes that depend on local stashables: lot count and exposures.
        # Mainly for short-term awareness of the account RMSs. Symbol guaranteed to be present.
        account.recent_lot_count[symbol] = round(account.recent_lot_count[symbol] + lot, 2)
        account.recent_exposures[quote] = round(account.recent_exposures[quote] + exp, 2)
        account.recent_exposures[base] = round(account.recent_exposures[base] - exp, 2)
        
        # Log.debug(VERBOSE_PROCESS_RESPONSE % (account.recent_lot_count, account.recent_exposures))
        self.last_action = Timestamp.utcnow()
        
#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
#████████████████████████████████████████████████████████████████████████████████████████████████████████████████  Run test  ███
#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄       
async def main():
    obj = StreamExecutor(name = SESSION_NAME, nzmq = 30)
    await asyncio.gather(*obj.coroutines.values())

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
if (__name__ == "__main__"):
    try: asyncio.run(main())
    except KeyboardInterrupt:
        Log.warning("Ctrl+C, interrupted")
        Log.success("See you next time :)")