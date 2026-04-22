#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
"""
    This file contains the base class for all stream nodes.
    It provides core functionality for data streaming and message handling,
    including ZMQ connections, WebSocket support, and Redis caching.
"""

import os, sys, json, time, msgpack
import asyncio, zmq, zmq.asyncio
import sqlalchemy as sql
from redis import Redis
from enum import Enum

sys.path.append("./")

from core.utils import *
from core.account import *

#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
#█████████████████████████████████████████████████████████████████████████████████████████████████████████████  Stream node  ███
#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
            
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class StreamBase:
    """
    Base class for handling data streaming operations with ZMQ and WebSocket connections.
    Manages account configurations, symbol mappings, and provides core functionality for
    data streaming and message handling. Supports both publisher (PUB) and subscriber (SUB)
    ZMQ modes.

    Notes:
        - Uses Redis for caching data which is needed for quick access (e.g.: tickets of recent trades)
        - Maintains database connections and tables for configuration and symbol lookups.
        - Handles automatic token management and account updates through the Account object instances
        - Supports WebSocket connections for source accounts.
    """

    SUFFIX_PARAMETERS = "freq_"
    AUTO_UPDATE_STATE_VARS = False
    ACCOUNT_CATEGORIES_STR = str.join(", ", map(str, Account.CATEGORIES))
    SPECS_KEYS, SPECS_FIELDS = ["server", "symbol"], "*"
    TOKEN_MANAGER = OUTER_STREAM = False

    VERBOSE_WAIT = "Instance of \"{obj}\" will be waiting during {secs} seconds for valid API tokens."

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, name = "test", mode_zmq = None, nzmq: int = 1):
        """
        Initialize a new StreamBase instance.

        Inputs:
            name (str): Instance identifier used for configuration and logging
            mode_zmq (str): ZMQ socket mode ("PUB" or "SUB")
            nzmq (int): Number of ZMQ connections to establish
        """

        self.nzmq = nzmq
        self.name = name
        self.active = False
        # PUB or SUB
        self.mode_zmq = mode_zmq
        self.initialized = False
        self.tasks = dict()
        # self.is_enabled_ws = False
        
        self.accounts, self.series = dict(), dict()
        # Create a Redis connector for the stashable datasets.
        self.redis = Redis(REDIS_HOST, REDIS_PORT, decode_responses = True)
        # Create a PostgreSQL connector to retrieve and write data to the key tables.
        self.connDB = sql.create_engine(DB_URL, isolation_level = "AUTOCOMMIT")
        # If the StreamBase subclass is meant to manage tokens, make the API create fresh new tokens first.
        if self.TOKEN_MANAGER: self._update_accounts()
        # Update the instance configuration and initialize the ZMQ connection.
        self._update_config(), self._init_zmq()
        # If the StreamBase subclass is meant to receive data from other instances,
        # update the specs from the database.
        if self.OUTER_STREAM: self.tasks = {
            # Update symbol specifications. "outer stream" means that this instance
            # is working isolated from the TradeManager itself (for testing purposes)
            self._update_specs:     {"next_run": time.time(), "freq": self.freq_query_specs}, }
        else: self.tasks = {
            # Update global configuration settings.
            self._update_config:    {"next_run": time.time(), "freq": self.freq_query_config},
            # Update account credentials, tokens, state variables and custom risk settings.
            self._update_accounts:  {"next_run": time.time(), "freq": self.freq_query_accounts},
            # Update symbol specifications.
            self._update_specs:     {"next_run": time.time(), "freq": self.freq_query_specs}, }
            
        self.coroutines = {"cron": asyncio.create_task(self._run_cron_tasks())}

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __repr__(self, n_sep = 140):
        """Generate a string representation of the current instance.
        Usually printed at the initialization phase for general info."""
        active = {True: "active", False: "inactive"}[self.active]

        return str.join("\n", [ "",
            "=" * n_sep, f"{self.__class__.__name__} \"{self.name}\" ({active})",
            "-" * n_sep, f"Accounts ({len(self.accounts)}):", self.str_accounts(), 
            "", "Config:", self.str_config(), "=" * n_sep, "" ])
    
    #███████████████████████████████████████████████████████████████████████████████████████████████████████████  Cron tasks
    
    QUERY_SPECS = "SELECT {fields} FROM " + TABLE_SYMS + " ORDER BY {keys}"

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def _run_cron_tasks(self):
        """
        Every instance of a StreamBase subclass will have a set of "cron tasks"
        which need to be executed periodically and in a non-blocking loop. This
        function basically runs in the background and executes the tasks in time
        whenever scheduled at a given constant frequency (adjustable through DB).
        """

        Log.warning(f"Tasks started for \"{self.__class__.__name__}\"")
        
        self.active = True
        while self.active:
            now = time.time()
            # Iterate over tasks and 
            for task, cron in self.tasks.items():
                # Get parameters from the task.
                next_run, freq = cron.values()
                if (next_run is None):
                    # Update next run time.
                    next_run = time.time()
                if (now < next_run): continue
                # Execute them when the time comes
                try: task(); next_run += freq
                # Any fatal error shall stop the TM.
                except Exception as EXC:
                    Log.exception(EXC); break
                # Store next run for the next time.
                cron["next_run"] = next_run

            # Wait for the next cycle. No function shall
            # really need to run more than once per second.
            await asyncio.sleep(1)

    #███████████████████████████████████████████████████████████████████████████████████████████████████████████  DB, symbols

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _update_specs(self):
        """
        Update symbol specifications from database.
        Retrieves and updates symbol mappings, including standard symbol formats
        and server-specific symbol names. Maintains bidirectional mappings between
        standard and server-specific symbols.
        """

        t_start = time.time() * 1e9
        keys = self.SPECS_KEYS.copy()
        keys_str = str.join(", ", keys)
        if (self.SPECS_FIELDS is None): return
        elif (self.SPECS_FIELDS[0] == "*"): fields = ["*"]
        else: fields = {*self.SPECS_KEYS, *self.SPECS_FIELDS}
        # Get the necessary fields from the symbol specs.
        fields = str.join(", ", fields) # Convert to SQL format.

        # Create the complete query and get the specs table from the database.
        query = self.QUERY_SPECS.format(fields = fields, keys = keys_str)
        self.specs = read_sql(query, con = self.connDB, index_col = keys)
        # Get the list of involved server names (brokers and their hosts).
        servers = self.specs.index.get_level_values("server").unique().to_list()
        self.specs = self.specs.sort_index()

        # Create mapping dictionaries that turn the symbol names with suffixes into
        # their authentic standard names and vice versa. (e.g.: "EURUSD-ecn" -> "EURUSD")
        specs = self.specs["standard"].reset_index() # "Standard" holds the "true" symbol names.
        self.map_sym_to_std = specs.groupby(["server", "symbol"])["standard"].first()
        self.map_std_to_sym = specs.groupby(["server", "standard"])["symbol"].first()

        delay = int(time.time() * 1e9 - t_start)
        # Print a brief update report with the given info.
        verbose = "Updated {n_syms} symbols from {n_serv} servers (delay = {delay} ns). Servers:\n"
        verbose = verbose.format(n_serv = len(servers), n_syms = self.specs.shape[0], delay = delay)
        Log.info(verbose + str.join(", ", servers))

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def map_symbol(self, symbol: str, server: str, std: bool = True):
        """
        Convert between standard and server-specific symbol formats. It is just a
        wrapper with some error handling for the usage of the symbol-mapping dicts.

        Inputs:
            symbol (str): Symbol to convert
            server (str): Server identifier
            std (bool): True to convert to standard format, False for server-specific

        Output (str): Converted symbol name
        """

        try: # Return the converted symbol name.
            if std: return self.map_sym_to_std[server][symbol]
            else: return self.map_std_to_sym[server][symbol]
        except KeyError as EXC:
            # If the symbol is not found, return the original one.
            error: str = "While mapping" if std else "While unmapping"
            error += " - Unknown symbol \"{symbol}\" or server \"{server}\" -> {exc}"
            Log.error(error.format(exc = str(EXC), symbol = symbol, server = server))
            return symbol
    
    #███████████████████████████████████████████████████████████████████████████████████████████████████████████  DB, accounts

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _update_accounts(self):
        """
        Update active trading accounts configuration.
        Retrieves account information from database and manages WebSocket connections
        for source accounts. Handles token management and account state updates.
        """

        categories = self.ACCOUNT_CATEGORIES_STR
        # Get the list of active accounts from the database.
        query = f"""SELECT * FROM {TABLE_ACCS} WHERE (active = true)
                    AND category IN ({categories}) ORDER BY alias;"""
        df: DataFrame = read_sql(query, self.connDB, index_col = "alias")
        # Get the Redis key for the most recently stashed tokens.
        key_tokens = SESSION_NAME + "|" + REDIS_KT
        # Account alias-to-token mapper from Redis (stashable).
        tokens = self.redis.hgetall(key_tokens)

        # No longer needed: always true.
        df = df.drop(columns = ["active"])
        # Get the list of old account aliases.
        accounts_old = set(self.accounts.keys())
        accounts_old = accounts_old.difference(df.index)
        # Get the list of new account aliases.
        accounts_new = df.index.difference(self.accounts)

        ########################################################

        async def _update_accounts():
            """
            Async context function which iterates over each entry and creates,
            updates or destroys the "Account" object depending on the case.
            """

            account: Account = None       
            for alias in accounts_old:
                # Destroy accounts that are old (no longer active)
                account = self.accounts.pop(alias)
                self.redis.hdel(key_tokens, alias)
                # Stop their WS (if they are sources)
                account.active_ws = False

            processes = list()
            for alias in accounts_new:
                # Create new accounts for the ones recently activated in database.
                account = Account(alias, **df.loc[alias]) # Account instantiation.
                is_source = (account.category == Account.Category.SOURCE)
                # Source accounts need to receive trading activity through WS.
                if is_source: account.callback_ws = self._on_message_ws
                # If this instance is a token manager, this will
                # force the API to create token from scratch.
                if self.TOKEN_MANAGER: token = None 
                # Otherwise reuse the token from Redis (if available).
                else: token = tokens.get(alias, None)
                # Append the coroutine as a future parallel process.
                processes.append(account.reset_token(token))
                # Update the accounts dict.
                self.accounts[alias] = account

            # Run all the account creations in parallel.
            await asyncio.gather(*processes)

            
            for alias, account in self.accounts.items():
                # Reconfigure the accounts that have always been active, if needed.
                account.reconfig(**df.loc[alias])
                # Source accounts need to receive trading activity through WS.
                is_source = (account.category == Account.Category.SOURCE)
                # Token managers shall get new tokens from scratch if needed.
                if self.TOKEN_MANAGER and (account.token is not None):
                    self.redis.hset(key_tokens, alias, account.token)
                # Source accounts need to receive trading activity through WS. This may
                # be done in either recently disconnected or new accounts in the program.
                elif is_source and not account.active_ws:
                    task = asyncio.create_task(account.connect_ws())
                    # Keep the WS callback function as a permanent coroutine from now
                    # onwards until the account is disconnected or the program ends.
                    self.coroutines["ws-" + alias] = task 

            ####################################################

            any_enabled = (len(accounts_old) > 0)
            any_disabled = (len(accounts_new) > 0)
            report = f"\"{self.__class__.__name__}\" updated accounts."
            # Print a report with the new and old accounts under any specific event...
            if any_enabled: report += "\n -> New accounts: " + str.join(", ", accounts_new)
            if any_disabled: report += "\n -> Old accounts: " + str.join(", ", accounts_old)
            # Print the report only if there are any change events.
            if any_enabled or any_disabled: Log.info(report)
            else: Log.debug(report + " No changes.")

            if not self.initialized:
                # Flag to consider the instance as ready to trade.
                self.initialized = True
                name = self.__class__.__name__
                # Print the stream description ("__repr__" above).
                Log.info("\"%s\" config:\n%s" % (name, repr(self)))

        ########################################################
        
        # Run the coroutine in the background.
        asyncio.create_task(_update_accounts())

    ############################################################

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def str_accounts(self):
        """Enlist and print all the active accounts in
        the instance with their "__repr__" outputs."""
        bullet = " -> "
        separator = "\n" + bullet
        str_accounts = list()
        # Iterate over the accounts and print their "repr" strings.
        for KV in self.accounts.items():
            str_accounts.append("{}: {}".format(*KV))
        return bullet + str.join(separator, str_accounts)

    #█████████████████████████████████████████████████████████████████████████████████████████████████████████████  DB, config
                    
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _update_config(self):
        """
        Update instance configuration from database.
        Retrieves and applies configuration parameters, including WebSocket settings
        and symbol ignore lists. Tracks and logs configuration changes.
        """

        changes = dict()
        # Get the global configuration parameters from the database.
        query = f"SELECT * FROM {TABLE_CONF} WHERE (instance = '{self.name}');"
        config = read_sql(query, con = self.connDB, index_col = "instance")
        config = config.loc[self.name].to_dict()
        # Iterate over the configuration parameters and update the instance.
        for key in config:
            old = getattr(self, key, None)
            if ((new := config[key]) != old):
                changes[key] = f"{old} to {new}"
                setattr(self, key, config[key])

        # List with the trading symbols to skip whatsoever.
        ignore_symbols_list = str.split(self.ignore_symbols, " ")
        self.set_ignore_symbols = set(ignore_symbols_list)

        # If this instance is a token manager, print a report with the changes.
        if self.TOKEN_MANAGER:
            report = f"Updated config."
            if (len(changes) <= 0):
                Log.debug(report + " No changes.")
            else:
                report += " Changes: "
                for KV in changes.items():
                    report += f"\n -> %s: %s" % KV
                Log.info(report)

        # Set the WS-enabling flag based on the ZMQ mode. Basically:
        # - PUB -> receiver node -> on master accounts -> will need WS
        # - SUB -> executor node -> on source accounts -> won't need WS
        self.is_enabled_ws = dict( # These flags are set in the database.
            PUB = config["is_enabled_ws_recv"],
            SUB = config["is_enabled_ws_exec"],
        ).get(self.mode_zmq, False)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def str_config(self):
        """Custom printing for the instance's global
        configuration. Used only during start-up."""
        separator = "\n" + (bullet := " -> ")
        str_config = [f"nzmq: {self.nzmq}"]
        # Iterate over the instance parameters and print their values.
        for par in dir(self):
            # Actually only need enlisting the cron task frequencies.
            if par.startswith(self.SUFFIX_PARAMETERS):
                str_config.append(f"{par}: {getattr(self, par)}")
        return bullet + str.join(separator, str_config)

    #████████████████████████████████████████████████████████████████████████████████████████████████████████████████████  ZMQ

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _init_zmq(self):
        """
        Initialize ZMQ socket connection.
        Sets up ZMQ publisher or subscriber socket based on configured mode.
        Basically:
         - PUB for "receiver" node (receives activity from master accounts).
         - SUB for "executor" node (executes orders on source accounts).
        Establishes connection to specified ZMQ URL.
        """

        obj = self.__class__.__name__
        if (self.mode_zmq is None): return Log.warning(
            f"No ZMQ channel for \"{obj}\" instances...")

        # Config string to ZMQ enum (predefined integer actually).
        int_mode_zmq = {"PUB": zmq.PUB, "SUB": zmq.SUB}[self.mode_zmq]
        Log.info(f"{obj} \"{self.name}\" connecting as {self.mode_zmq} to \"{ZMQ_URL}\"...")
        try:
            # Create a ZMQ context and socket for the instance.
            self.connMQ = zmq.asyncio.Context()
            self.socket = self.connMQ.socket(int_mode_zmq)
            # If the instance is a SUB, listen to the ZMQ URL.
            if (int_mode_zmq == zmq.SUB):
                self.socket.connect(ZMQ_URL)
                self.socket.subscribe(b"")
            # Otherwise, bind the socket as a publisher.
            else: self.socket.bind(ZMQ_URL)

        except Exception as EXC: 
            Log.exception(EXC)

    #███████████████████████████████████████████████████████████████████████████████████████████████████████████████████ Redis
    
    # Info pieces that need to be short-term stashed and updated. # Hence, will be considered "stashable".
    class Stashable(Enum): TOKENS, ORDER_MAP, PRC_BANDS, POS_COUNT, LOT_COUNT, EXPOSURES = range(6)

    # Data types for the DataFrame-like dicts:
    STASHABLE_DTYPES = { # row (hashmap outer key), column (hashmap inner key), value
        Stashable.TOKENS: (str, str, str), # alias, "token" column name, token
        Stashable.ORDER_MAP: (int, str, int), # source ticket, target alias, target ticket
        Stashable.PRC_BANDS: (str, str, str), # source alias, symbol, comma-separated occupied prices
        Stashable.POS_COUNT: (str, str, int), # target alias, symbol, position count
        Stashable.LOT_COUNT: (str, str, float), # target alias, symbol, net lot total
        Stashable.EXPOSURES: (str, str, float), # target alias, symbol, exposure in USD
    }
    VERBOSE_STASH_IN = "Stashed \"{header}\" by \"{obj}\" with {ni} indexes and {ne} entries (delay = {delay} ns)."
    VERBOSE_STASH_OUT = "Unstashed \"{header}\" on \"{obj}\" with {ni} indexes and {ne} entries (delay = {delay} ns)."
    VERBOSE_STASH_SKIP = "Skipped unstash on \"{obj}\" of \"{header}\" due to being only {delta:.1f} seconds ago."

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def stash(self, stashable: Stashable, data: DataFrame):
        """
        Store data frame in Redis cache.
        Serializes and stores DataFrame content in Redis using hierarchical keys.
        Supports different information contents through the Stashable enum.

        Basically: Header -> Row index (outer index) -> Column-value pairs (inner hashmap).
         - A "header" key stores a Redis set with the outer indexes of the stashable.
         - Each one of these outer indexes is equal to a row label in the DataFrame/dict,
           which is itself equal to another Redis key that points to a hashmap.
         - Each hashmap contains the column-value pairs of the row label.

        Inputs:
            stashable (Stashable): Enum which specifies which specific type of info is being stored.
                The actual structure of the hashmap inside Redis will greatly depend on this.
            data (DataFrame): Data to cache in Redis, usually in the form of a DataFrame-like dict.
                That is: a dict with dicts inside, usually the latter (inner) one having the columns.
        """

        t_start = time.time() * 1e9
        # Redis enums are uppercase but hash keys are lowercase.
        label = stashable.name.lower()
        # Header contains TradeManager instance name + stashable label.
        header = f"{self.name}|{label}" 
        n_indexes, n_entries = 0, 0
        self.redis.delete(header)

        # Proceed with the stash.
        if not data.empty:

            n_indexes = data.shape[0]
            self.redis.delete(header) # Clear the previous stashable content.
            for index, entry in data.iterrows(): # Iterate over the rows of the DataFrame.
                entry = entry[entry != 0].dropna() # Drop the rows with no useful data.
                entry = dict(entry.loc[entry != 0]) # Series (row) to inner dict (column).
                n_entries = n_entries + len(entry) # Get number of columns per row
                hash = f"{header}|{index}" # In-hashmap key for column-value pairs.
                self.redis.delete(hash) # Clear the previous in-hashmap content.
                # Outer indexes (rows) of the hashmap (column-value pairs) that stores the...
                self.redis.sadd(header, str(index)) # ...stashable are stored in a Redis set.
                for key, value in entry.items(): # Iterate over the columns of the row.
                    if not bool(value): continue # No need to store empty column-value pairs.
                    key, value = str(key), str(value) # Encode to strings (Redis only accepts strings).
                    self.redis.hset(hash, key, value) # Set the column-value pair in the in-hashmap.

        label = stashable.name
        # Print the stash operation details.
        Log.info(self.VERBOSE_STASH_IN.format(label = label, ni = n_indexes,
                  ne = n_entries, delay = int(time.time() * 1e9 - t_start),
                  obj = self.__class__.__name__, header = header))

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def unstash(self, stashable: Stashable):
        """
        Retrieve data from Redis cache. Basically the opposite of the "stash" method.
        Deserializes and reconstructs data previously stored as a DataFrame-like dict.
        Handles type conversion based on Stashable enum.
        Read the "stash" method for more structural details.

        Inputs:
            stashable (Stashable): Type of data to retrieve

        Output: (dict) Retrieved and reconstructed data
        """

        t_start = time.time() * 1e9
        # Redis enums are uppercase but hash keys are lowercase.
        label = stashable.name.lower()
        # Header contains TradeManager instance name + stashable label.
        header = f"{self.name}|{label}"
        # Get the data types for the DataFrame-like dict.
        dtypes = self.STASHABLE_DTYPES[stashable]
        type_index, type_key, type_value = dtypes
        # Indexes towards the hashmaps that store the stashable
        # column-value pairs are stored in a Redis set.
        indexes = self.redis.smembers(header)
        indexes = [*map(type_index, indexes)]
        n_indexes, n_entries = len(indexes), 0
        df, data = "", dict()

        for index in indexes: # over each outer index (row label).
            hash = f"{header}|{index}" # Key towards hashmap.
            # Dict that stores the stashable column-value pairs.
            data_i = self.redis.hgetall(hash)
            n_entries = n_entries + len(data_i)
            if not bool(data_i): continue
            else: data[index] = dict()
            for key, value in data_i.items():
                # Rebuild the column-value pairs as DataFrame.
                key, value = type_key(key), type_value(value)
                if value: data[index][key] = value

        label = stashable.name
        Log.info(self.VERBOSE_STASH_OUT.format(label = label, ni = n_indexes,
                    ne = n_entries, delay = int(time.time() * 1e9 - t_start),
                    obj = self.__class__.__name__, header = header) + df)
        
        return data # Return the reconstructed DataFrame-like dict.
    
    #██████████████████████████████████████████████████████████████████████████████████████████████████████████████  Websocket

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def _on_message_ws(self, account: Account, message: str):
        """
        Handle incoming WebSocket messages.
        Process and log messages received through WebSocket connections
        from trading accounts. This function is just a placeholder in this class;
        it is meant to be overridden by the child classes, mainly the "Receiver".
        Inputs:
            account (Account): Account that received the message
            message (str): Raw message content to be decoded into JSON.
        """

        message = json.dumps(json.loads(message), indent = 4)
        n_lines = len(message_lines := message.split("\n"))
        fragment = min(10, n_lines // 3)
        message_upper = message_lines[: + fragment]
        message_lower = message_lines[- fragment :]
        message_lines = [*message_lower, "...", *message_upper]
        verbose = f"WS message from \"{account.alias}\":\n"
        Log.debug(verbose + str.join("\n", message_lines))
        
#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
#████████████████████████████████████████████████████████████████████████████████████████████████████████████████  Run test  ███
#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
     
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄       
async def main():
    obj = StreamBase(name = "test")
    await asyncio.gather(*obj.coroutines.values())

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
if (__name__ == "__main__"):
    try: asyncio.run(main())
    except KeyboardInterrupt:
        Log.warning("Ctrl+C, interrupted")
        Log.success("See you next time :)")
