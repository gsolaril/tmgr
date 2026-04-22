#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
"""
    This file contains the AccountWatcher class, which is a subclass of StreamBase.
    It's responsible for actually managing and recording the state of the accounts at all times.
    This includes: requesting and writing state variables, exposures and stashables to database and Redis,
    and also keeping the tokens updated and valid at all times, Hence, it's the actual "TOKEN_MANAGER" node.
"""
import sys, time
from pandas import Series, DataFrame, concat, to_datetime
from pandas import Index, MultiIndex, Timedelta, Timestamp
from sqlalchemy.exc import IntegrityError

sys.path.append("./")

from core.account import *
from core.base import *
from core.utils import *
from misc.reshapes_basic import TMReshape

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#█████████████████████████████████████████████████████████████████████████████████████████████████████████████████  Watcher  ███
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class AccountWatcher(StreamBase):
    """
    This class is responsible for actually managing and recording the state of the accounts at all times.
    This includes: requesting and writing state variables, exposures and stashables to database and Redis,
    and also keeping the tokens updated and valid at all times, Hence, it's the actual "TOKEN_MANAGER" node.
    """
    SPECS_FIELDS = ["standard", "quote", "base"]
    SUFFIX_PARAMETERS = "freq_"
    AUTO_UPDATE_STATE_VARS = False
    TOKEN_MANAGER = True
    OUTER_STREAM = False

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, name: str):

        super().__init__(name)
        self.last_states, self.last_active = DataFrame(), DataFrame()
        self.last_summary = Timestamp.utcnow() - Account.HIST_DEFAULT_DELTA
        # Last summary timestamp sets the window since when to request historical data.

        self.tasks.update({
            self._update_summary:       {"next_run": time.time(), "freq": self.freq_update_summary},
        })

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
                    
    VERBOSE_UPD_SUMMARY = "Updated {ns} account states... \n -> Also " \
                    "updated {na} (active) + {nc} (closed) trades to " \
                    "DB since {last} (delay: {delay} ns)"
    
    INDEX_SUMMARY_TRADES = Index(["alias", "ticket"])
    VERBOSE_DT_FORMAT = "%Y/%m/%d %X"

    VERBOSE_UPD_RET = "Retrieved {var} from {len} accounts."

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _update_summary(self):
        """
        This method is the Watcher's main and only cron task. It's responsible for updating the summary of the accounts.
        It retrieves the state variables, exposures and stashables from the database and Redis. Also if there's any need
        to reset the token, the Watcher will be in charge of doing so, and recording it to the Redis stashable so that
        the rest of the nodes can use them for their account objects without having to request them through API.
        """
        index_names = [*self.INDEX_SUMMARY_TRADES]

        #▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
        async def on_iter_account():
            """
            Async coroutine that iterates over all accounts and concurrently retrieves all of the necessary data
            from the associated endpoints within the MT API, then standardizes it and stores it in the database.
            Finally, it updates the last summary timestamp and stores the stashables into Redis.
            """
            t = time.time()
            t_start = t * 1e9
            states: DataFrame = dict()
            active: DataFrame = dict()
            closed: DataFrame = dict()
            categories: dict = dict()
            account: Account = None
            dpsits = DataFrame()

            args = {
                "until": Timestamp.utcnow(), "since": self.last_summary,
                "delta": Timedelta(seconds = self.freq_update_summary),
            }

            #▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
            processes: list = list() # Future list of concurrent processes.
            # Coroutine for each account to get the current state variables.
            for alias, account in self.accounts.items():
                categories[alias] = account.category
                while not account.token: await asyncio.sleep(0.1)
                processes.append(account.get_current_state()) # Create coroutine.

            if (not processes): return Log.warning("No account states available")
            try: responses = await asyncio.gather(*processes) # Make coroutines concurrent.
            except Exception as EXC: return Log.exception(EXC) # Stop if any major error.
            for alias in self.accounts.keys(): # Order of responses same as account dict.
                response: dict = responses.pop(0)
                # Collect responses and store their results in the state dict.
                if (response is not None): states[alias] = response.copy()
            
            Log.debug(self.VERBOSE_UPD_RET.format(len = len(states), var = "states"))

            #▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
            processes: list = list() # Future list of concurrent processes.
            # Coroutine for each account to get the list of active trades.
            for alias, account in self.accounts.items():
                categories[alias] = account.category
                while not account.token: await asyncio.sleep(0.1)
                processes.append(account.get_active_trades()) # Create coroutine.

            if (not processes): return Log.warning("No active trades available")
            try: responses = await asyncio.gather(*processes) # Make coroutines concurrent.
            except Exception as EXC: return Log.exception(EXC) # Stop if any major error.
            for alias in self.accounts.keys(): # Order of responses same as account dict.
                response: dict = responses.pop(0)
                # Collect responses and store their results in the active dict.
                if (response is not None): active[alias] = response.copy()
            
            Log.debug(self.VERBOSE_UPD_RET.format(len = len(active), var = "trades"))

            #▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
            if (len(active) == 0): active = DataFrame()
            else:
                # Vertically stack all tables of active trades.
                active = concat(active, names = index_names)
                # Standardize symbols (quotes/bases) and add specs.
                active: DataFrame = self.standardize(active)
                active["is_closed"] = False

            if (len(closed) == 0): closed = DataFrame()
            else:
                # Vertically stack all tables of closed trades.
                closed = concat(closed, names = index_names)
                # Extract transfers and keep only trades with valid exit time and volume.
                dpsits = closed.loc[closed["order"] == "Balance"] # Transfers.
                closed = closed.loc[closed["order"] != "Balance"] # Closed trades.
                closed = closed.loc[closed["t_exit"].gt(0.0)] # No exit time.
                closed = closed.loc[closed["lots"].gt(0.0)] # No volume.
                # Standardize symbols (quotes/bases) and add specs.
                closed: DataFrame = self.standardize(closed)
                closed["is_closed"] = True

            #▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
            next_summary = Timestamp.utcnow()

            try:
                # Compute the lot count and exposures per account alias and currency.
                lot_count = TMReshape.trades_to_lot_exposures(active)
                exposures = TMReshape.trades_to_usd_exposures(active)
                # Vertically stack active and closed trades to create a DataFrame with an
                # homogeneous format. They are still distinguishable by "is_closed" column.
                trades = concat((active, closed), axis = "index")
                # Vertically stack states so as to create a global multi-indexed DataFrame.
                states: DataFrame = concat(states, axis = "index")
                # Recount the buy and sell trades together with their lot sizes.
                states = self.order_recount(states, lot_count)
                # Compute the price bands for future stashable.
                price_bands = self.order_pbands(active, categories)
                # Generate the order map for future stashable, to be used by the Executor. Target
                # trades UNMatched with any source trade is considered loose and will be alerted.
                unm, trades = self.order_rematch(trades, categories) 

                pos_count = lot_count.reset_index() # Create new recent "pos-count" stashable.
                # Separate counters by symbol and buy/sell side (e.g.: "EURUSD_b", "EURUSD_s").
                pos_count["side"] = pos_count["sign"].map({+1: "_b", -1: "_s"})
                pos_count["symbol"] = pos_count["symbol"] + pos_count["side"]

                # Group by alias and symbol to create a DataFrame with the
                # total count of buy and sell trades for each symbol and alias.
                pos_count = pos_count.groupby(["alias", "symbol"])
                pos_count = pos_count["count"].sum().unstack("symbol")
                pos_count = pos_count.fillna(0).astype(int)

                # Group by alias and symbol to create a DataFrame with the
                # total lot size of buy and sell trades for each symbol and alias.
                lot_count = lot_count.groupby(["alias", "symbol"])
                lot_count = lot_count["sum"].sum().unstack("symbol")

                # Keep the most recent state variable values for each alias.
                self.last_states = states.groupby("alias").last()
                # Active and closed trades to be re-separated.
                is_closed = trades.pop("is_closed")

                if not active.empty: # If not active trades
                    #... keep empty dataframe but with the correct columns.
                    is_active_column = ~ active.columns.str.contains("exit")
                    active = active.loc[:, active.columns[is_active_column]]
                    
                closed = trades.loc[is_closed] # Keep closed trades.
                active = trades.loc[~ is_closed] # Keep active trades.
                order_map = self.order_mapping(active) # Generate the order map for future stashable.
                self.last_active = active.copy() # Keep the last active trades for future use.

                if not dpsits.empty: # If not transfers
                    #... keep empty dataframe but with the correct columns.
                    dpsits = dpsits[["t_entry", "value", "comment"]]
                    dpsits = dpsits.rename(columns = {"t_entry": "index"})
                    dpsits = dpsits.reset_index().set_index("index")
                
            except Exception as EXC: return Log.exception(EXC)

            #Log.debug(f"Up to now:\nstates:\n{states}\nclosed:\n{closed}\nactive:\n{active}")

            # flag to define if any data was written to database.
            uploaded_to_DB = True
            # counters for the number of rows written to each table.
            n_active = n_closed = n_states = 0
            with self.connDB.connect() as conn:
                args = {"con": conn, "index": True}
                try:
                    #Log.debug("Writing to database...")
                    if not states.empty: n_states = states.to_sql(TABLE_TSRS, if_exists = "append", **args)
                    if not closed.empty: n_closed = closed.to_sql(TABLE_CCLS, if_exists = "append", **args)
                    if not active.empty: n_active = active.to_sql(TABLE_CACT, if_exists = "replace", **args)
                    if not dpsits.empty: n_dpsits = dpsits.to_sql(TABLE_DPST, if_exists = "append", **args)
                except IntegrityError: Log.warning("Warning: repeated trades")
                except Exception as EXC: Log.exception(EXC); uploaded_to_DB = False

            # Log database-writing action to the terminal.
            last_str = self.last_summary.strftime(self.VERBOSE_DT_FORMAT)
            verbose_args = {"ns": n_states, "na": n_active, "nc": n_closed,
                "delay": int(time.time() * 1e9 - t_start), "last": last_str}
            Log.info(self.VERBOSE_UPD_SUMMARY.format(**verbose_args))
            # Update the last summary timestamp if any data was written.
            if uploaded_to_DB: self.last_summary = next_summary

            # Add the current equity of each account to the exposures' stashable.
            # As if a distinct currency: useful for calculating exposure ratios.
            exposures["_equity"] = self.last_states["equity"]

            # Add missing accounts to the pos-count stashable, with zero counts
            mis_index = sorted(set(self.accounts).difference(pos_count.index))
            mis_count = DataFrame(0, columns = pos_count.columns, index = mis_index)
            pos_count: DataFrame = concat((pos_count, mis_count)).fillna(0.0)
            pos_count = pos_count.sort_index().astype(int)

            #print("#" * 123, "\nPOS_COUNT")
            #print(pos_count.to_string(), "\n" + "#" * 123)
            # Stash all of the data in its corresponding Redis key.
            self.stash(self.Stashable.ORDER_MAP, order_map)
            self.stash(self.Stashable.PRC_BANDS, price_bands)
            self.stash(self.Stashable.POS_COUNT, pos_count)
            self.stash(self.Stashable.LOT_COUNT, lot_count.round(2))
            self.stash(self.Stashable.EXPOSURES, exposures.round(2))
            # Force exit of any UNMatched trades which may be loose.
            try: await self._order_force_exit(unm.reset_index())
            except Exception as EXC: Log.exception(EXC)

        #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
        # Run the async coroutine within the cron task.
        asyncio.create_task(on_iter_account())

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀

    OUTER_ACCOUNTS = ["bbox40", "bbox41", "bbox42", "bbox43"]

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def standardize(self, trades: DataFrame):
        """
        Standardize the symbols of the trades and remove any kind of suffixes belonging to specific servers.
        Also append the quote and base currencies, needed for exposure calculations later on.
        """

        index_names = ["alias", "symbol"]

        # Get the server for each account alias.
        acc_server = {alias: getattr(account, "server") for alias, account in self.accounts.items()}
        # Get the unique symbols for each alias.
        acc_syms = trades.reset_index("alias")[index_names].drop_duplicates(subset = index_names)

        # Get the server for each account alias.
        acc_syms["server"] = acc_syms["alias"].map(acc_server)
        # Get the unique symbols for each server.
        server_syms = acc_syms[["server", "symbol"]].drop_duplicates()
        server_syms = MultiIndex.from_frame(server_syms).sort_values()
        available_symbols = self.specs.index.intersection(server_syms)
        server_syms: DataFrame = self.specs.loc[available_symbols, ["standard"]]
        # Get the quote and base currencies for each symbol.
        mapper_qb = self.specs.loc[Account.SAMPLE_SERVER].set_index("standard")

        # Merge the server symbols with the quote and base currencies.
        server_syms["quote"] = server_syms["standard"].map(mapper_qb["quote"])
        server_syms["base"] = server_syms["standard"].map(mapper_qb["base"])
        acc_syms = DataFrame.merge(acc_syms, server_syms, on = ["server", "symbol"])

        # Associate the correct mapping from server-symbol to standard-symbol for each account.
        acc_syms = acc_syms[[*index_names, "standard", "quote", "base"]]

        # Merge the trades with the standardized symbols.
        trades = DataFrame.merge(trades.reset_index(), acc_syms, on = index_names)
        # Remove the server-symbol column and rename the standardized symbol column.
        trades = trades.drop(columns = ["symbol"]).rename(columns = {"standard": "symbol"})
        # Return the trades with the standardized symbols.
        return trades.set_index([*self.INDEX_SUMMARY_TRADES])

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
    
    COLUMNS_COPIES_TARGET = dict(alias_s = "alias_s", ticket_s = "ticket_s", lots = "lots", t_entry = "t_entry",
              t_exit = "t_exit", p_entry = "p_entry", p_exit = "p_exit", exit = "exit", is_closed = "is_closed")
    
    COLUMNS_COPIES_SOURCE = dict(symbol = "symbol", lots = "lots_s", order = "order_type", t_entry = "t_entry_s",
            t_exit = "t_exit_s", p_entry = "p_entry_s", p_exit = "p_exit_s", exit = "exit_s", comment = "comment")
    
    COLUMNS_COPIES_SOURCE = dict(quote = "quote", base = "base", **COLUMNS_COPIES_SOURCE)
    
    COLUMNS_COPIES_ALL = [*INDEX_SUMMARY_TRADES, *COLUMNS_COPIES_TARGET.values(), *COLUMNS_COPIES_SOURCE.values()]

    TEMPLATE_SUMMARY_TRADES = DataFrame(columns = COLUMNS_COPIES_ALL).set_index([*INDEX_SUMMARY_TRADES])

    COLUMNS_SUMMARY_TRADES = ["lots_buy", "lots_sell", "lots_net", "n_ord_buy", "n_ord_sell", "n_cop_buy", "n_cop_sell"]

    COLUMNS_SUMMARY_INDEX = ["timestamp", "alias", "account", "server"]

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def order_recount(cls, states: DataFrame, lot_count: DataFrame):
        """
        Recount the number of orders and the global net lot for each account. But before,
        initialize the summary columns to zero, avoiding errors in inactive accounts.

        Inputs:
            states: (DataFrame) with the states of the accounts.
            lot_count: (DataFrame) with the lot count for each account.

        Output: (DataFrame) with the recounted number of orders and the global net lot for each account.
        """

        if states.empty: return states

        # Rename the alias and field columns.
        states = states.rename_axis(["alias", "field"])
        # Unstack the field column and set the alias as the index.
        states = states.unstack("field").set_index("alias")
        # Initialize the summary columns to zero.
        states[cls.COLUMNS_SUMMARY_TRADES] = 0.0

        if not lot_count.empty:

            # Distinguish between buy and sell trades.
            # ORD = Any order, COP = Copied order.
            ord_buy = lot_count.loc[lot_count["sum"].gt(0.0)]
            ord_sell = lot_count.loc[lot_count["sum"].lt(0.0)]
            cop_buy = lot_count.loc[lot_count["sum_c"].gt(0.0)]
            cop_sell = lot_count.loc[lot_count["sum_c"].lt(0.0)]
            # Recount the number of orders for each account.
            states["n_ord_buy"] = ord_buy.groupby("alias")["count"].sum()
            states["n_ord_sell"] = ord_sell.groupby("alias")["count"].sum()
            states["n_cop_buy"] = cop_buy.groupby("alias")["count_c"].sum()
            states["n_cop_sell"] = cop_sell.groupby("alias")["count_c"].sum()
            # Recount the global buy, sell and net lot for each account.
            states["lots_buy"] = ord_buy.groupby("alias")["sum_c"].sum()
            states["lots_sell"] = ord_sell.groupby("alias")["sum_c"].sum()
            states["lots_net"] = states["lots_buy"] + states["lots_sell"]

        # Standard lot sizes have a precision of 2 decimal places.
        states["lots_buy"] = states["lots_buy"].fillna(0).astype(float).round(2)
        states["lots_sell"] = states["lots_sell"].fillna(0).astype(float).round(2)
        states["lots_net"] = states["lots_net"].fillna(0).astype(float).round(2)

        # Position counters are integers.
        states["n_ord_buy"] = states["n_ord_buy"].fillna(0).astype(int)
        states["n_cop_buy"] = states["n_cop_buy"].fillna(0).astype(int)
        states["n_ord_sell"] = states["n_ord_sell"].fillna(0).astype(int)
        states["n_cop_sell"] = states["n_cop_sell"].fillna(0).astype(int)
        return states.reset_index().set_index(cls.COLUMNS_SUMMARY_INDEX)
    
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def order_rematch(cls, trades: DataFrame, categories: dict):
        """
        Separate trades among different accounts based on their category (source or target), then rebuild
        the trade-to-trade dependency relationship considering that the source account's alias and ticket
        is present in the target trade's comment. This creates the future order-map stashable.

        Inputs:
            trades: (DataFrame) with the trades.
            categories: (dict) with the categories of the accounts.

        Output: (tuple) with the unmatched trades and the rematched trades, separated.
        """
        # Create empty pair of dataframes to be returned if no trading activity was found.
        empty_template = (
            cls.TEMPLATE_SUMMARY_TRADES.copy(),
            cls.TEMPLATE_SUMMARY_TRADES.copy())
        
        categories: Series = Series(categories)
        # Get aliases of target and source accounts.
        targets = categories.loc[categories == 1].index
        sources = categories.loc[categories == 0].index

        if trades.empty: return empty_template
        # Get aliases of accounts with trading activity.
        alias_with_trades = trades.index.get_level_values("alias")
        # Split real trades' dataframe by targets and sources.
        targets = targets.intersection(alias_with_trades)
        sources = sources.intersection(alias_with_trades)

        # Return empty templates if no trading
        # activity was found on either side
        if targets.empty: return empty_template
        if sources.empty: return empty_template

        # Need to create a dataframe where each row holding a trade relationship will have
        # to hold alias of both source and target, and ticket of both source and target.
        # Therefore, we need a suffix for source items to distinguish among columns.
        index_t = cls.INDEX_SUMMARY_TRADES + ""
        index_s = cls.INDEX_SUMMARY_TRADES + "_s"
        trades_t = trades.loc[targets].rename_axis(index_t)
        trades_s = trades.loc[sources].rename_axis(index_s)

        # Rows with missing ticket values are trades
        # from outside the scope of the Trade Manager.
        trades_t = trades_t.dropna(subset = ["ticket_s"])
        trades_s = trades_s.drop(columns = index_s)
        if trades_t.empty: return empty_template

        # Keep and rename the columns needed for the order-mapping.
        trades_t = trades_t[list(cls.COLUMNS_COPIES_TARGET)].reset_index()
        trades_s = trades_s[list(cls.COLUMNS_COPIES_SOURCE)].reset_index()
        trades_t = trades_t.rename(columns = cls.COLUMNS_COPIES_TARGET)
        trades_s = trades_s.rename(columns = cls.COLUMNS_COPIES_SOURCE)
        # Merge the source and target dataframes, already having identified
        # the source/target alias/ticket pairs for each matchable trade entry.
        rematched = DataFrame.merge(trades_s, trades_t, how = "right", on = [*index_s])

        rematched["ticket"] = rematched["ticket"].astype(int)
        rematched["ticket_s"] = rematched["ticket_s"].astype(int)
        rematched = rematched.set_index(list(index_t)).sort_index()
        # Unmatched active trades are those which have no valid source ticket.
        unmatched = rematched["t_entry_s"].isna() & ~ rematched["is_closed"]
        # Separate matched/unmatched. Unmatched trades will be handled afterwards.
        unmatched = rematched.loc[unmatched, list(index_s)]
        return unmatched, rematched.dropna(subset = ["t_entry_s"])

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def order_mapping(cls, trades: DataFrame):
        """
        Create the order-map stashable from the rematched trades' dataframe. Basically, it creates a
        one-to-one relationship between the source and target ticket, but also including the alias of
        the target account so as to be able to access and manipulate the trade through API later.

        Inputs:
            trades: (DataFrame) with the trades.

        Output: (DataFrame) with the order-map stashable.
        """
        index_map = ["ticket_s", "alias"]
        if trades.empty: return DataFrame()
        # Multi-indexed series that does the following mapping:
        # (source ticket ; target account) -> target ticket.
        trades = trades.reset_index().groupby(index_map)["ticket"].last()
        return trades.unstack("alias").fillna(0).astype(int)
        # In Redis, the same mapping occurs:
        # source ticket (key in order-map set)
        # -> target account (hash in order-map hashmap)
        # -> target ticket (value in order-map hashmap)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def order_pbands(cls, trades: DataFrame, categories: dict):
        """
        Create the pbands stashable from the trades' dataframe. Basically, it creates a multi-indexed
        dataframe of the shape based on each source accounts' symbols. Each value is a set of the different
        unique prices found in all active trades.

        Inputs:
            trades: (DataFrame) with the trades.
            categories: (dict) with the categories of the accounts.

        Output: (DataFrame) with the p-bands' stashable.
        """
        if trades.empty: return DataFrame()
        # Get the alias of the source accounts.
        alias: Series = trades.index.to_frame()["alias"]
        # Filter the trades by source accounts.
        is_source = alias.map(categories).eq(Account.Category.SOURCE)
        pbands = trades.loc[is_source, ["symbol", "p_entry", "point", "order"]].copy()
        # Get the side of the trades, then set price values for sells as negative.
        pbands["side"] = pbands.pop("order").str.lower().map({"buy": 1, "sell": -1})
        pbands = pbands.dropna(subset = "side") # Drop weird stuff (e.g.: transfers.)
        pbands["p_entry"] *= pbands.pop("side")
        # Create the p-band values. Discretize them into points-based integers.
        # (e.g.: EURUSD, 1.23456 / 1e-5 -> 123456... XAUUSD, 1723.45 / 0.01 -> 172345...)
        pbands["pband"] = (pbands["p_entry"] / pbands["point"]).astype(int).astype(str)
        # Group the p-bands by alias and symbol, and return as series of sets.
        pbands = pbands.groupby(["alias", "symbol"])["pband"].apply(set).sort_index()
        return pbands.map(" ".join).unstack("symbol")

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀

    """
    INTERVENTION_KEYS = dict(sz = "safety_zone", mgr = "max_global_risk", mgl = "max_global_marg")
    QUERY_INTERV = f"SELECT source, target, interv FROM {TABLE_MULT} WHERE interv IS NOT NULL"
    VERBOSE_INTERV_PRE = "\rChecking relationship #{n}/%d ({index}): \"{line}\""
    VERBOSE_INTERV_POST = "Orders closed through \"{key}\" from {ni} accounts..."
    VERBOSE_INTERV_ORDER = "\"{account}\" ({interv}). Closed: {nc}/{n} ({closed}), Failed: {nf}/{n} ({failed})"

    #▄▄▄▄▄▄▄▄▄▄▄
    @classmethod
    def keys_to_dict(cls, line: str):

        interv = dict()
        for segment in line.split(","):
            segment = segment.strip(" ")
            key, *segment = segment.split(" ")
            label = cls.INTERVENTION_KEYS[key]
            interv[label] = [*map(eval, segment)]
            
        return interv

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _order_intervention(self):

        if self.last_states.empty: return

        results = {key: list() for key in self.INTERVENTION_KEYS.values()}
        df_interv: DataFrame = read_sql(self.QUERY_INTERV, self.connDB)
        df_interv = df_interv.set_index(["source", "target"])["interv"]
        verbose = self.VERBOSE_INTERV_PRE % df_interv.shape[0]

        if df_interv.empty: return

        for n, (index, line) in enumerate(df_interv.iterrows(), 1):

            verbose_args = dict(n = n, index = index, line = line)
            print(verbose.format(**verbose_args), end = "\t" * 4)
            if (line is None) or (line == ""): continue
            values = self.keys_to_dict(line)

            try:
                if (args := values.pop("safety_zone", None)):
                    results["safety_zone"].append(self._react_safety_zone(*index, *args))
                if (args := values.pop("max_global_risk", None)):
                    results["max_global_risk"].append(self._react_global_risk(*index, *args))
                if (args := values.pop("max_global_marg", None)):
                    results["max_global_marg"].append(self._react_global_marg(*index, *args))

            except Exception as EXC: Log.exception(EXC); continue
        
        verbose = ["Finished with order intervention processes."]
        
        for key, intervs in results.items():  
            verbose.append(" -> " + self.VERBOSE_INTERV_POST.format(ni = df_interv.shape[0], key = key))
            for interv in intervs: verbose.append("    -> " + self.VERBOSE_INTERV_ORDER.format(**interv))

        if (len(verbose) > 1): print(); Log.info(str.join("\n", verbose))"""


#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀

    """LOT_NET_MAX_GLOBAL = 999.0

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _react_safety_zone(self, alias_source: str, alias_target: str, *args):

        source: Account = self.accounts[alias_source]
        target: Account = self.accounts[alias_target]
        safe_lot_ratio: float = list.pop(args, 0)

        states_source = self.last_states.loc[self.last_states["alias"].eq(alias_source)]
        states_target = self.last_states.loc[self.last_states["alias"].eq(alias_target)]
        lots_net_source_prev: float = states_source.iloc[-2]["lots_net"]
        lots_net_source_last: float = states_source.iloc[-1]["lots_net"]
        lots_net_target_prev: float = states_target.iloc[-2]["lots_net"]
        lots_net_target_last: float = states_target.iloc[-1]["lots_net"]
        lots_net_source_max, lots_net_target_max = source.max_lots_net, target.max_lots_net
        if (lots_net_source_max is None): lots_net_source_max = self.LOT_NET_MAX_GLOBAL
        if (lots_net_target_max is None): lots_net_target_max = self.LOT_NET_MAX_GLOBAL
        lot_ratio_source_prev = abs(lots_net_source_prev) / (lots_net_source_max + 0.01)
        lot_ratio_source_last = abs(lots_net_source_last) / (lots_net_source_max + 0.01)
        lot_ratio_target_prev = abs(lots_net_target_prev) / (lots_net_target_max + 0.01)
        lot_ratio_target_last = abs(lots_net_target_last) / (lots_net_target_max + 0.01)

        is_safe_prev = (lot_ratio_source_prev < safe_lot_ratio)
        is_safe_last = (lot_ratio_source_last < safe_lot_ratio)
        if is_safe_prev or not is_safe_last: return

        is_order_source = self.last_active["alias_s"].eq(alias_source)
        active_orders: DataFrame = self.last_active.loc[is_order_source]
        
        closed, failed = list(), list()
        for ticket, order in active_orders.iterrows():
            try:
                order = OrderClose(lots = order["lot"], ticket = ticket)
                response: OrderResponse = target.execute(order)
                closed.append(ticket) if response.success else failed.append(ticket)
            except Exception as EXC: Log.exception(EXC); failed.append(ticket); continue

        return {"account": target.string, "interv": "sz = %.4f" % safe_lot_ratio,
                "n": active_orders.shape[0], "nc": len(closed), "nf": len(failed),
                "closed": closed, "failed": failed}
        """
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀

    WAIT_FORCE_EXIT = 3
    VERBOSE_FORCE_EXIT = f"The following trades are unmatched and need to close in {WAIT_FORCE_EXIT} seconds:\n%s"
    USELESS_BUT_NECESSARY = {"is_buy": 0, "lots": 0}

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def _order_force_exec(self, trades: DataFrame):

        # TODO:
        # * match source actives with target actives.
        # * see if any SOURCE active remains UNMATCHED.
        # * send ZMQ message as "OrderSend" as if from StreamReceiver.
        # (if and only if a certain time threshold hasn't been crossed)

        pass

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def _order_force_exit(self, trades: DataFrame):
        """
        Force-closes all target trades that had a source ticket and alias in their comment,
        but where the source ticket is not present in the source account's active orders.
        This doesn't use ZMQ to communicate with the "Executor" node, but just executes
        them on demand through the Account.execute() method by itself as an isolated action.

        Inputs:
            trades: (DataFrame) with the trades.
        """
        if trades.empty: return
        now = int(Timestamp.utcnow().timestamp() * 1e6)

        trades_str: DataFrame = trades.copy()
        # Create a multi-indexed dataframe of the
        # shape (target, source) -> (alias, ticket).
        trades_str.columns = MultiIndex.from_product(
            (("target", "source"), ("alias", "ticket")))
        
        Log.warning(self.VERBOSE_FORCE_EXIT % trades_str)
        await asyncio.sleep(self.WAIT_FORCE_EXIT)

        async def force_exit(*args):
            # Get the target alias, ticket, source alias and ticket.
            alias_t, ticket_t, alias_s, _ = args
            # Get the source and target accounts.
            source: Account = self.accounts[alias_s]
            target: Account = self.accounts[alias_t]
            # Force-close the target trade.
            await target.execute(OrderClose(is_buy = 0,
                source = source.alias, ticket = ticket_t,
                lots = 0, t_sign = now, t_recv = now,
                action = OrderRequest.Action.CLOSE))

        processes = list() # Generic list to store concurrent processes.
        for _, trade in trades.iterrows(): # Iterate over the trades.
            try: processes.append(force_exit(*trade)) # Force-close the trade.
            except Exception as EXC: Log.exception(EXC) # Log any exceptions.

        await asyncio.gather(*processes) # Generate concurrency in coroutines.
        
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀

if (__name__ == "__main__"):

    async def main():
        # Log.remove(1)
        accw = AccountWatcher(name = SESSION_NAME)
        await asyncio.gather(*accw.coroutines.values())

    try: asyncio.run(main())
    except KeyboardInterrupt:
        Log.warning("Ctrl+C, interrupted")
        Log.success("See you next time :)")
