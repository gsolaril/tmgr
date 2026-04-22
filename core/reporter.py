#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
"""
   AccountReporter is a subclass of StreamBase. However, its just symbolic: it works as an outer node which monitors the
   current state of the accounts and sends reports to Telegram. It doesn't need WebSocket support because it doesn't react
   to any messages from the accounts. It only needs access to database, Redis and Telegram API. Currently not being used.
"""
import sys, time
from pandas import Series, DataFrame, concat, read_csv
from pandas import Index, Timedelta, Timestamp
from pandas import DatetimeIndex, TimedeltaIndex

from core.account import *
from core.base import *
from core.utils import *
from misc.reports_basic import TMReport

# ███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
# ████████████████████████████████████████████████████████████████████████████████████████████████████████████████  Reporter  ███
# ███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████

class AccountReporter(StreamBase):
    """
    This class is a subclass of StreamBase.
    It works as an outer node which monitors the current state of the accounts and sends reports to Telegram.
    It doesn't need WebSocket support because it doesn't react to any messages from the accounts.
    It only needs access to database, Redis and Telegram API. Currently not being used.
    """
    SUFFIX_PARAMETERS = "freq_"
    AUTO_UPDATE_STATE_VARS = False
    ACCOUNT_CATEGORIES_STR = ", ".join(map(str, Account.CATEGORIES))
    SPECS_KEYS, SPECS_FIELDS = ["server", "symbol"], "*"
    OUTER_STREAM = True # Satellite service, doesn't need to intervene with the main stream.
    TOKEN_MANAGER = False

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, name: str):

        super().__init__(name)
        self.started = Timestamp.utcnow()
        self.prev_request = None
        self.total_equity = None
        self._get_last_state()
        self._send_summary()
        self._read_requests()

        self.tasks.update({
            self._send_update: {"next_run": time.time(), "freq": self.freq_rep_send_update},
            self._send_summary: {"next_run": time.time(), "freq": self.freq_rep_send_reports},
            self._read_requests: {"next_run": time.time(), "freq": self.freq_rep_read_requests},
        })

    # ███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████

    ACCOUNT_TAG_SEP = " :: "

    FIELDS_STATES = ["balance", "equity", "margin", "lots_net", "n_cop_buy", "n_cop_sell", "n_ord_buy", "n_ord_sell"]
    QUERY_STATES = "SELECT DISTINCT ON (alias) alias, {fields} FROM {table} ORDER BY alias, timestamp DESC".format(
        fields = str.join(", ", FIELDS_STATES), table = TABLE_TSRS
    )
    FIELDS_ACCOUNT_TAG = ["server", "id", "owner"]
    QUERY_ACCOUNTS = "SELECT alias, {fields}, max_lots_net FROM {table} WHERE active = true;".format(
        fields = str.join(", ", FIELDS_ACCOUNT_TAG), table = TABLE_ACCS
    )
    ACCOUNT_TAG = str.join(ACCOUNT_TAG_SEP, map("{{0[{}]}}".format, FIELDS_ACCOUNT_TAG))

    BROKER_TAGS = ["ACY", "Milliy", "Fusion", "Juno", "VT", "XM"]
    
    ERROR_NO_TABLE_CACT = f"\"{TABLE_CACT}\" could not be found. Probably it is being" \
        " refreshed by \"AccountWatcher\" right now. Check connection just in case..."

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _get_last_state(self):
        """
        Get the last state of the accounts from the database in terms of the
        state variables (balance, equity, etc.), exposures and active orders.
        Such "recent" state entries are stored as attributes of the instance
        to be later used in the Telegram report.
        """
        until = int(time.time() * 1e6)
        # Read the most recent active orders on all target accounts.
        try: self.orders = read_sql(TABLE_CACT, con = self.connDB)
        except Exception as EXC: Log.error(self.ERROR_NO_TABLE_CACT)
        # Read the most recent state variables on all target accounts.
        self.states = read_sql(self.QUERY_STATES, con = self.connDB)
        # Current config on the account RMSs on all target accounts.
        self.accounts = read_sql(self.QUERY_ACCOUNTS, con = self.connDB)
        self.accounts = self.accounts.set_index("alias") # Account alias as index
        # Reduce the broker names (remove stuff like "-Demo", "-PAMM", etc.)
        for tag in self.BROKER_TAGS: self.accounts["server"] = \
            self.accounts["server"].str.replace(tag + ".*", tag, regex = True)

        self.accounts["owner"] = self.accounts["owner"].fillna("")
        # Create a descriptive label for each target account by aggregating its attributes.
        self.accounts["tag"] = self.accounts.agg(self.ACCOUNT_TAG.format, axis = "columns")
        self.accounts, max_lots_net = self.accounts["tag"], self.accounts["max_lots_net"]
        # Set the maximum allowed lots for each target account.
        self.states["lots_max"] = self.states["alias"].map(max_lots_net)
        self.states["lots_max"] = self.states["lots_max"].fillna(1000.00)
        # Map the account aliases to the corresponding broker tags.
        self.states["target"] = self.states.pop("alias").map(self.accounts)
        # Calculate exposure's current state as ratio vs. the maximum allowed.
        self.states["exp"] = self.states["lots_net"] / self.states["lots_max"]
        # Replace aliases with ID numbers for easier visualization in Telegram.
        self.orders["source"] = self.orders.pop("alias_s").map(self.accounts)
        self.orders["target"] = self.orders.pop("alias").map(self.accounts)
        # Replace timestamp with time since execution, for easier interpretation.
        self.orders["ago"] = until - self.orders["t_entry_s"]
        self.orders = self.orders.set_index("target")
        self.states = self.states.set_index("target")
        self.states = self.states.loc[self.states.index.notna()]

    # ███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████

    QUERY_RESIZE = "UPDATE " + TABLE_MULT + " SET factor = {factor} WHERE source = {source} AND target = {target};"

    FIELDS_SERIES = ["equity", "margin", "n_cop_buy", "n_cop_sell"]
    QUERY_SERIES = "SELECT alias, timestamp, {fields} FROM {table}".format(
        fields = str.join(", ", FIELDS_SERIES), table = TABLE_TSRS) \
        + " WHERE timestamp BETWEEN {since} AND {until};"

    VERBOSE_REPORT_FAILED = "Report {n} ({tag}) failed. Response:\n{resp}"
    VERBOSE_REPORT_SUCCESS = "Report {n} ({tag}) sent to \"T#{chat}\""
    SEND_ARGS = {"token": TGRAM_TOKEN, "chat": TGRAM_CHATS} # defaults
    IGNORE_ACCOUNTS = ["TM_POKE", SOURCE_ALIAS := "Ali"]

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _send_summary(self):
        """
        Send a summary of the current state of the accounts to Telegram. Consists on a
        single message containing global TM's balance, equity, active trade count, etc.
        """
        mins = self.freq_rep_send_reports // 60 # Minutes since last report

        # Get the most recent state variables on all target accounts.
        states: DataFrame = self.states.copy().loc[self.states.index.notnull()]
        # Irrelevant accounts with active trades: demos, tests, etc. Focus on investor accounts.
        states = states.loc[~ self.states.index.str.contains(str.join("|", self.IGNORE_ACCOUNTS))]

        # Get the source account's active trades count. For now, intended for a single source.
        source = self.states.loc[self.states.index.str.contains(self.SOURCE_ALIAS)].iloc[0]
        active_trades = source["n_ord_buy"] + source["n_ord_sell"]

        # Unsynced accounts which have loose trades (targets
        # with no source) will be enlisted in the message...
        is_unsync = states["n_cop_buy"].ne(source["n_ord_buy"])
        is_unsync |= states["n_cop_sell"].ne(source["n_ord_sell"])
        unsync = states.loc[is_unsync].index
        field_pnl = f"Total PNL ({mins} min)"

        summary = {
            "Total balance": round(states["balance"].sum(), 2),
            "Total equity": round(states["equity"].sum(), 2),
            "Total margin": round(states["margin"].sum(), 2),
            field_pnl: None, "Total trading lots": round(states["lots_net"].sum(), 2), 
            "Active trades": int(active_trades), "Check accounts": list(unsync)
        }
        if (self.total_equity is not None):
            # Track PNL as difference between current equity
            # and the one from the previous report...
            total_equity = summary["Total equity"]
            total_pnl = round(total_equity - self.total_equity, 2)
            summary[field_pnl] = total_pnl

        message = summary.copy()
        message.pop("Check accounts")
        if (len(unsync) > 5): # If unsynced list is too long, truncate it.
            unsync = Index([*unsync[: 5], f"...({len(unsync) - 5} more)"])
        # Give style to the Telegram message (bold, underline, bullets, etc.)
        message["<u>Check accounts</u>"] = "\n" + str.join("\n", " • " + unsync)
        message = str.join("\n", [f"{K}: <b>{V}</b>" for K, V in message.items()])
        async def send(): await TGram_API.send(**self.SEND_ARGS, message = message)
        asyncio.create_task(send()) # Send message; no need to wait for response.

        # Keep current equity as previous on next report.
        self.total_equity = summary["Total equity"]

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _send_reports(self, which: str = None):
        """
        Send a report of the current state of the accounts to Telegram.
        Consists on a single message containing the timeseries of the PNL,
        the distribution of the trading lots, and the grid of the active orders.
        """
        since = (until := time.time() * 1e6) - self.freq_rep_send_reports * 6e6
        query = self.QUERY_SERIES.format(since = since, until = until)
        # Read the timeseries of the PNL, margin, etc.
        timeseries = read_sql(query, con = self.connDB)
        # Read the grid of the active orders.
        order_grid = self.orders.reset_index()

        timestamp = timeseries.pop("timestamp") * 1e3
        # Timestamps for the x-axis of the timeseries' plots.
        timeseries["timestamp"] = DatetimeIndex(timestamp, tz = "UTC")
        timeseries["timestamp"] = timeseries["timestamp"].dt.round("30s")
        timeseries["tag"] = timeseries.pop("alias").map(self.accounts)
        timeseries = timeseries.set_index(["tag", "timestamp"]).sort_index()
        timeseries["profit"] = timeseries.pop("equity").groupby("tag").diff()
        timeseries["profit"] = timeseries.pop("profit").groupby("tag").cumsum()
        timeseries = timeseries.unstack("tag") # One timeseries per account.

        # Pop the timeseries' relevant columns, prepare them for the plots.
        profit, margin = timeseries.pop("profit"), timeseries.pop("margin")
        orders = timeseries.pop("n_cop_buy") + timeseries.pop("n_cop_sell")
        timeseries = timeseries.index
        until = timeseries[-1] # Last timestamp in timeseries as x-limit.

        # Rebuild the grid of the active orders:
        columns_grid = ["source", "ticket_s", "target"]
        # Rows are source tickets, columns are target accounts.
        order_grid[["ticket", "ticket_s"]] = order_grid[["ticket", "ticket_s"]].astype(str)
        # Cells contain the target tickets and time since exec (will appear between brackets).
        order_grid = order_grid.set_index(columns_grid).sort_index()[["ticket", "ago"]]
        order_grid, hours_ago = order_grid["ticket"], order_grid["ago"]

        hours_ago = hours_ago.groupby(columns_grid[: -1]).first()
        # Time since execution expressed in number of hours.
        hours_ago = DataFrame(hours_ago / 3.6e9).round(1).transpose()
        hours_ago.index = Index(["Hours since execution"], name = "target")
        order_grid = order_grid.unstack(columns_grid[: -1]).fillna("")
        order_grid = concat((order_grid, hours_ago), axis = "index")

        # Keep only the accounts with largest exposure or margin.
        relevant_accounts = TMReport.compress_accounts(margin)
        # Reduce timeseries to selected relevant accounts.
        profit: DataFrame = profit[relevant_accounts]
        margin: DataFrame = margin[relevant_accounts]
        orders: DataFrame = orders[relevant_accounts]

        num = 0; reports = list()

        if (which == (num := 1)) or (which is None):
            # Create plot of the cumulative PNL timeseries.
            path, desc = TMReport.report_series(profit, margin, orders)
            reports.append((num, path, desc, "timeseries"))
        if (which == (num := 2)) or (which is None):
            # Create plot of the net lots, moving between "+/-" boundaries.
            path, desc = TMReport.report_lot_dist(self.states, until)
            reports.append((num, path, desc, "lots & orders"))
        if (which == (num := 3)) or (which is None):
            # Create table image of the active orders grid.
            path, desc = TMReport.report_order_grid(order_grid, until)
            reports.append((num, path, desc, "order grid"))

        async def send():

            send_args = self.SEND_ARGS.copy()
            for num, path, desc, tag in reports:
                # Cycle through each created image and its path. Format message.
                send_args["message"] = f"<b><u>Chart {num}</u></b>:\n" + desc
                # Send message with image copied from path, through Telegram.
                ok, resp = await TGram_API.send(**send_args, filepath = path)
                # Log success or failure of the report.
                verb_args = {"n": num, "tag": tag, "chat": TGRAM_CHATS, "resp": resp}
                if ok: Log.success(self.VERBOSE_REPORT_SUCCESS.format(**verb_args))
                else: Log.error(self.VERBOSE_REPORT_FAILED.format(**verb_args))
                await asyncio.sleep(self.MESSAGE_COOLDOWN_SHORT)

        asyncio.create_task(send()) # Parallelize the sending of the reports.

    # ▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _send_update(self):

        pass


# ███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
# ███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
# ███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████

    COLUMNS_TABLE_MULT = ["source", "target", "factor"]
    MESSAGE_COOLDOWN_SHORT, MESSAGE_COOLDOWN_LONG = 3, 15
    REQUEST_APPEARED = "\"{sender}\" from \"{chat}\" requested: \"{message}\""
    REQUEST_RESOLVED = "Provided {message} as requested by \"{sender}\"."
    REQUEST_FAILED = "Last command failed:\n\"%s\""
    
    # ▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _read_requests(self):
        
        self._get_last_state()

        async def react():

            query = list()
            last_update = await TGram_API.last(TGRAM_TOKEN, False)
            
            if (last_update is None): return
            if (len(last_update) == 0): return
            if (last_update["ts"] < self.started): return
            if (last_update["id"] == self.prev_request): return
            if (last_update["chat_id"] != int(TGRAM_CHATS)): return

            self.prev_request = last_update.pop("id")
            sender: str = last_update.pop("user_name")
            message: str = last_update.pop("message")
            if not isinstance(message, str): return
            if not message.startswith("/"): return
            message = message.lstrip("/")
            
            Log.info(self.REQUEST_APPEARED.format(
                sender = sender, message = message,
                chat = TGRAM_CHATS))
            
            command = message.split(" ")[0]
            args = message.replace(command, "").lstrip(" ")

            if (command == "report"):
                if not args: args = None
                else: args = int(args)
                try:
                    self._send_reports(args)
                    message = self.REQUEST_RESOLVED.format(
                        message = message, sender = sender)
                except Exception as EXC:
                    Log.exception(EXC)
                    message = self.REQUEST_FAILED % EXC

            elif (command == "resize"):
                for line in message.split(","):
                    line = line.strip(" ").split(" ")
                    if (len(line) != 3): continue
                    args = dict(zip(self.COLUMNS_TABLE_MULT, line))
                    query.append(self.QUERY_RESIZE.format(**args))

            if len(query):
                with self.connDB.connect() as conn:
                    query = sql.text(str.join("\n", query))
                    try: conn.execute(query); Log.success(message := f"Success:\n\"{message}\"")
                    except Exception as EXC: Log.exception(EXC); message = self.REQUEST_FAILED % EXC 

            await asyncio.sleep(self.MESSAGE_COOLDOWN_LONG)
            await TGram_API.send(**self.SEND_ARGS, message = message)

        asyncio.create_task(react())

# ███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
# ███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
# ███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████

if (__name__ == "__main__"):

    async def main():
        Log.remove(1)
        accr = AccountReporter(name = SESSION_NAME)
        await asyncio.gather(*accr.coroutines.values())

    try: asyncio.run(main())
    except KeyboardInterrupt:
        Log.warning("Ctrl+C, interrupted")
        Log.success("See you next time :)")
