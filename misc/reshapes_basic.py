#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
# TODO: ADD COMMENTS AND DOCSTRINGS.

import os, sys, numpy
from numpy import log10
from pandas import Series, DataFrame, MultiIndex, concat, read_csv
from pandas import date_range, Timestamp, Timedelta, DatetimeIndex

sys.path.append("./")

from core.utils import *
from core.account import *
from core.base import *
from core.actions import *

# ███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
# ███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
# ███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class TMReshape:
    """
    A class that contains mathematical formulas for reshaping the trades dataframes
    into various measurements of state definition (e.g.: current exposure).
    """
    INDEX_EXPOSURES_USD = ["alias", "cur"]
    TEMPLATE_LOT_COUNT = DataFrame(columns = ["alias", "symbol", "sum", "sum_c", "count", "count_c"])
    TEMPLATE_LOT_COUNT = TEMPLATE_LOT_COUNT.set_index(["alias", "symbol"])

    BASIC_POINT_MAP_QUERY = f"SELECT symbol, point FROM {TABLE_SYMS} WHERE (server ~ 'MyServer')"
    BASIC_POINT_MAP = read_sql(BASIC_POINT_MAP_QUERY, con = DB_URL, index_col = "symbol")["point"]
    
    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def trades_to_lot_exposures(cls, trades: DataFrame):
        """
        Reshape the trades dataframe into a dataframe of lot exposures.
        Values provided per account and symbol, with separate columns for
        the net lot number based on all trades and on only copied trades.

        Inputs: (DataFrame) The trades dataframe.
        Output: (DataFrame) The lot exposures dataframe.
        """
        if (trades is None) or trades.empty:
            return cls.TEMPLATE_LOT_COUNT.copy()

        # Prepare the dataframe for the calculation.
        trades = trades.rename_axis(["alias", "ticket"])
        trades = trades.reset_index("alias", drop = False)
        # Only keep trades which are active (i.e. no exit time).
        df_pos = trades.loc[~ trades["t_exit"].gt(0.0)].copy()

        # Distinguish between buy (positive) and sell (negative) orders,
        # extract the sign of the trade and whether it is copied or not.
        is_buy = df_pos["order"].str.contains("buy", case = False)
        is_sell = df_pos["order"].str.contains("sell", case = False)
        df_pos["sign"] = is_buy.astype(int) - is_sell.astype(int)
        df_pos["copied"] = df_pos["ticket_s"].notna()

        grouper = ["alias", "symbol", "sign"]

        # Calculate the sum and count of the lot number for each group.
        df_pos = df_pos.groupby([*grouper, "copied"])["lots"]
        df_pos = df_pos.agg(["sum", "count"]).reset_index("sign")
        df_pos["sum"] = df_pos["sum"] * df_pos["sign"]

        # Extract the sum and count of the lot number for the copied trades.
        try: df_pos_c = df_pos.xs(key = True, level = "copied")
        except KeyError as EXC:
            df_pos_c = df_pos.iloc[0 : 0].copy()
            df_pos_c = df_pos_c.reset_index("copied", drop = True)
            Log.error("No actively copied trades: %s" % EXC)

        # Set the index and rename the columns.
        df_pos_c = df_pos_c.set_index("sign", append = True)
        df_pos_c.columns = df_pos_c.columns + "_c"
        df_pos_t = df_pos.groupby(grouper).sum()
        df_pos = concat(objs = (df_pos_t, df_pos_c), axis = "columns").fillna(0)

        # Convert the count columns to integers and round the sum columns.
        df_pos[["count", "count_c"]] = df_pos[["count", "count_c"]].astype(int)
        df_pos[["sum", "sum_c"]] = df_pos[["sum", "sum_c"]].round(2)
        return df_pos

    #▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod#█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def trades_to_usd_exposures(cls, trades: DataFrame):
        """
        Reshape the trades dataframe into a dataframe of USD exposures.
        Net values provided per account and currency - NOT per symbol
        (symbols are split into base and quote currencies).

        Inputs: (DataFrame) The trades dataframe.
        Output: (DataFrame) The USD exposures dataframe.
        """
        if (trades is None) or trades.empty: return DataFrame()
        
        sign: dict = {"buy": +1, "sell": -1}
        # Extract the base and quote currencies from the symbol.
        trades["base"] = trades["symbol"].str.slice(3, 6)
        trades["quote"] = trades["symbol"].str.slice(0, 3)
        # Extract the sign of the trade.
        trades["sign"] = trades["order"].str.lower().map(sign)
        # Extract the point value of the trade.
        trades["point"] = trades["symbol"].map(cls.BASIC_POINT_MAP)
        trades["exp"] = trades.eval("sign * p_entry * lots / point")
        # Reset the index and group by the alias and currency.
        trades = trades.reset_index()[["alias", "base", "quote", "exp"]]
        # Calculate the sum of the exposure in the base and quote currencies.
        exp_base: Series = - trades.groupby(["alias", "base"])["exp"].sum()
        exp_quote: Series = trades.groupby(["alias", "quote"])["exp"].sum()
        # Concatenate the base and quote exposures and rename the index.
        exp = concat((exp_quote, exp_base)).rename_axis(cls.INDEX_EXPOSURES_USD)
        # Return the exposures grouped by the alias and currency.
        return exp.groupby(cls.INDEX_EXPOSURES_USD).sum().unstack("cur").round(2)
