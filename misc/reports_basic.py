# Import required libraries for data manipulation, visualization and system operations
import os, sys, numpy
from pandas import Series, DataFrame, concat, read_csv
from pandas import date_range, Timestamp, Timedelta, DatetimeIndex
from matplotlib.pyplot import subplots, style, Axes, rcParams
from matplotlib.ticker import MaxNLocator
style.use("dark_background")

# ███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████

# Configure matplotlib parameters for consistent plot styling
rcParams.update({"figure.titlesize": 14, "figure.titleweight": "bold",
    "legend.fontsize": 10, "xtick.labelsize": 12, "ytick.labelsize": 12,
    "ytick.right": True, "ytick.labelleft": False, "ytick.labelright": True,
    "font.family": "sans-serif"})

# ███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
# ███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
# ███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████

#▄▄▄▄▄▄▄▄▄▄▄▄▄
class TMReport:
    """
    Trading Monitor Report Generator Class
    
    Handles the generation of various trading performance reports including time series,
    lot distribution, and order grid visualizations. Reports are saved as image files
    suitable for distribution in Telegram or Slack messages.
    """

    # Class constants for file naming and report configuration
    DT_FILENAME = "%Y.%m.%d_%H.%M"
    REPORT_FOLDER = os.path.split(os.path.split(__file__)[0])[0] + "/logs/"
    REPORT_PATH_TEMP = REPORT_FOLDER + "tm_reporter_{subject}_{timestamp}.jpg"
    LEGEND_SERIES = "{0[target]} (\${0[profit]}, \${0[margin]}, {0[orders]:.0f})"
    ACCOUNT_LIMIT = 42
    DPI = 100

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod
    def compress_accounts(cls, df: DataFrame):
        """
        Selects the top performing accounts based on ACCOUNT_LIMIT
        Inputs: (DataFrame) containing account performance metrics
        Output: (Index) Selected account identifiers sorted by performance
        """
        return df.max().sort_values(ascending = False).index[: cls.ACCOUNT_LIMIT]

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod
    def report_series(cls, profit: DataFrame, margin: DataFrame, orders: DataFrame):
        """
        Generates a time series report showing profit, margin, and order counts
        Inputs: (DataFrames) containing account performance metrics
        Output: (str) path to saved report and (str) descriptive message
        """
        # Set description for the report. Will be the actual literal Telegram message.
        desc = "Timeseries and current account states."

        # Get time range from orders DataFrame
        since: Timestamp = orders.index[0]
        until: Timestamp = orders.index[-1]
        path = cls.REPORT_PATH_TEMP.format(subject = "series",
                timestamp = until.strftime(cls.DT_FILENAME))

        # Create legend data by combining latest values from all series
        legend = {"profit": profit.iloc[-1], "margin": margin.iloc[-1], "orders": orders.iloc[-1]}
        # Reshape and format legend data using pandas operations
        legend = concat(legend, names = ["series", "target"]).unstack("series").round(2)
        legend = legend.reset_index().agg(cls.LEGEND_SERIES.format, axis = "columns")

        # Initialize subplot axes for profit, margin, and orders
        axs_profit: Axes = None; axs_margin: Axes = None; axs_orders: Axes = None
        fig_args = {"nrows": 3, "ncols": 1, "figsize": (16, 10), "sharex": True}
        fig_series, (axs_profit, axs_margin, axs_orders) = subplots(**fig_args)

        # Define common plotting parameters for all series
        plot_args = {"marker": ".", "ms": 3, "lw": 0.5}

        # Plot each series with the same time index but different values
        axs_profit.plot(profit.index, profit.values, **plot_args)
        axs_margin.plot(margin.index, margin.values, **plot_args)
        axs_orders.plot(orders.index, orders.values, **plot_args)
        axs_orders.set_xlim(orders.index[0], orders.index[-1])

        # Configure legend position and appearance
        legend_sep = 0.15
        bbox_to_anchor = (1 + legend_sep, 0, legend_sep, 1)
        axs_profit.legend(legend, bbox_to_anchor = bbox_to_anchor)
        legend_header = "Account (& current profit, margin, #orders):"
        axs_profit.text(1 + legend_sep, 1.01, legend_header, fontsize = 12,
            transform = axs_profit.transAxes);

        # Add grid to all subplots with consistent styling
        axs_profit.grid(True, lw = 1.5, ls = "-.", color = "white", alpha = 1/3)
        axs_margin.grid(True, lw = 1.5, ls = "-.", color = "white", alpha = 1/3)
        axs_orders.grid(True, lw = 1.5, ls = "-.", color = "white", alpha = 1/3)

        # Create and format title with time range information
        until_str = until.strftime("%H:%M")
        since_str = since.strftime("%Y/%m/%d %H:%M")
        title = f"Account timeseries from {since_str} to {until_str}"
        title = title + "\n" + "‾" * int(len(title) * 1.2)
        axs_profit.set_title(title, fontsize = 14, fontweight = "bold")

        # Configure x-axis ticks and labels with 5-minute intervals
        xticks = date_range(since, until, freq = Timedelta(minutes = 5))
        xlabels = xticks.strftime("%H:%M")

        axs_orders.set_xticks(xticks)
        axs_orders.set_xticklabels(xlabels, rotation = 90)

        # Set y-axis tick locators for all subplots
        axs_profit.yaxis.set_major_locator(MaxNLocator(10))
        axs_margin.yaxis.set_major_locator(MaxNLocator(10))
        axs_orders.yaxis.set_major_locator(MaxNLocator(10))

        # Add labels to identify each subplot
        axs_profit.text(0.01, 0.95, "Profit", fontsize = 14, transform = axs_profit.transAxes, va = "top", fontweight = "bold")
        axs_margin.text(0.01, 0.95, "Margin", fontsize = 14, transform = axs_margin.transAxes, va = "top", fontweight = "bold")
        axs_orders.text(0.01, 0.95, "Orders", fontsize = 14, transform = axs_orders.transAxes, va = "top", fontweight = "bold")

        # Adjust subplot positions to make room for legend
        shrink_factor = 0.55

        # Calculate and set new positions for all subplots
        axs_orders_pos = axs_orders.get_position()
        width = axs_orders_pos.xmax - (x := axs_orders_pos.xmin)
        height = axs_orders_pos.ymax - (y := axs_orders_pos.ymin)
        axs_orders.set_position([x, y, width * shrink_factor, height])
        y = axs_margin.get_position().ymin
        axs_margin.set_position([x, y, width * shrink_factor, height])
        y = axs_profit.get_position().ymin
        axs_profit.set_position([x, y, width * shrink_factor, height])

        # Save figure to file with high DPI for quality
        axs_profit.figure.savefig(path, dpi = 250)

        # Reset y-axis tick label position to default
        rcParams.update({"ytick.labelleft": True, "ytick.labelright": False})

        return path, desc

# ███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod
    def report_lot_dist(cls, states: DataFrame, until: Timestamp = None):
        """
        Generates a lot distribution report showing RMS-related variables
        (exposure and active trades' counters)
        
        Inputs:
            states (DataFrame): Current account states and metrics
            until (Timestamp): Report timestamp. Defaults to current time.
            
        Output: (str) path to saved report and (str) descriptive message
        """
        # Set description for the report. Will be the actual literal Telegram message.
        desc = "Relative exposure (net lots) and order count."

        # Set default timestamp to current time if not provided
        if (until is None): until = Timestamp.utcnow()
        path = cls.REPORT_PATH_TEMP.format(subject = "lot_dist",
                timestamp = until.strftime(cls.DT_FILENAME))

        # Initialize subplot axes for order distribution and lot distribution
        axs_odist: Axes = None  ;  axs_ldist: Axes = None
        fig_args = {"ncols": 2, "figsize": (16, 10), "sharey": True}
        fig_ldist, (axs_ldist, axs_odist) = subplots(**fig_args)
        
        # Remove any rows with null indices and create a copy
        states: DataFrame = states.loc[states.index.notnull()].copy()

        # Calculate total copies and orders for each account
        states["n_copies"] = (states["n_cop_buy"] + states["n_cop_sell"]).astype(int)
        states["n_orders"] = (states["n_ord_buy"] + states["n_ord_sell"]).astype(int)

        # Create mapping for y-axis coordinates (reversed order)
        coord_mapper = Series(states.index[:: -1])
        axs_odist.set_yticks(coord_mapper.index)
        axs_odist.set_yticklabels(coord_mapper.values)
        states = states.loc[coord_mapper.values]

        # Plot horizontal bars for orders distribution
        axs_odist.barh(states.index, states["n_orders"], color = "royalblue")
        axs_odist.barh(states.index, states["n_copies"], color = "skyblue")

        # Plot horizontal bars for lot distribution and configure x-axis
        axs_ldist.barh(states.index, states["exp"], color = "pink")
        axs_ldist.set_xlim(-1, 1)
        xticks = Series(numpy.arange(-1, 1.01, 0.2))
        xlabels = (xticks * 100).map("{:.0f}".format) + "%"
        axs_ldist.set_xticks(xticks), axs_ldist.set_xticklabels(xlabels)

        # Add grids to both subplots
        axs_odist.grid(True, lw = 1.5, ls = "-.", color = "white", alpha = 1/3)
        axs_ldist.grid(True, lw = 1.5, ls = "-.", color = "white", alpha = 1/3)
        axs_odist.set_ylim(-0.5, coord_mapper.index.max() + 0.5)

        # Add lot information text for each account
        sign = numpy.sign(states["exp"].mean())
        ha = "right" if (sign > 0) else "left"
        for n, account in coord_mapper.items():
            lots_net = states.at[account, "lots_net"]
            lots_max = states.at[account, "lots_max"]
            text = f"{lots_net:.2f} / {lots_max:.0f}"
            axs_ldist.text(- sign * 0.05, n, text, ha = ha, va = "center", fontsize = 12, fontweight = "bold")

        # Add x-axis labels for both plots
        axs_ldist.set_xlabel("Current exposure = net lots / max net lot allowed",
            fontsize = 14, fontweight = "bold")

        axs_odist.set_xlabel("Copied count (light) over all orders (dark)",
            fontsize = 14, fontweight = "bold")

        # Create and format title with timestamp
        until_dt = until.strftime("%Y/%m/%d %H:%M")
        title = f"Exposure distribution and order count as of {until_dt}"
        title = title + "\n" + "‾" * int(len(title) * 1.2)
        axs_odist.figure.suptitle(title, fontsize = 16, fontweight = "bold")

        # Adjust layout and save figure
        axs_odist.figure.set_tight_layout(True)
        axs_odist.figure.savefig(path, dpi = 250)

        return path, desc

# ███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod
    def report_order_grid(cls, grid: DataFrame, until: Timestamp = None):
        """
        Generates a grid report showing order relationships between accounts
        
        Inputs:
            grid (DataFrame): Order relationship matrix
            until (Timestamp): Report timestamp. Defaults to current time.
            
        Output: (str) path to saved report and (str) descriptive message
        """
        # Set description for the report. Will be the actual literal Telegram message.
        desc = "Order grid with source-target relationships & origin."

        # Set default timestamp to current time if not provided
        if (until is None): until = Timestamp.utcnow()
        path = cls.REPORT_PATH_TEMP.format(subject = "order_grid",
                    timestamp = until.strftime(cls.DT_FILENAME))
        
        # Replace NaN values with empty strings for better display
        grid = grid.fillna("")
        grid_str = grid.to_string(
            max_cols = grid.shape[1],
            max_rows = grid.shape[0])

        # Split grid into pages for better readability
        grid_str = list()
        accounts, tickets = grid.shape
        for n in range(0, tickets, page_width := 10):
            # Process grid in chunks of page_width columns
            df = grid.iloc[:, n : n + page_width]
            df = df.to_string(max_rows = 99, max_cols = page_width)
            grid_str.append(df)

        # Join all pages with double line breaks
        grid_str = str.join(2 * "\n", grid_str)

        # Initialize plot for displaying the grid
        axs_grid: Axes = None
        box_height = 7.5 * (1 + tickets // page_width)
        fig_grid, axs_grid = subplots(figsize = (14, box_height))
        axs_grid.axis("off")

        # Create and format title with timestamp
        until_dt = until.strftime("%Y/%m/%d %H:%M")
        title = f"Grid of source-target relationships as of {until_dt}"
        title = title + "\n" + "‾" * int(len(title) * 1.2)
        axs_grid.set_title(title, fontsize = 16, fontweight = "bold")

        # Add the grid text to the plot with monospace font
        axs_grid.text(0, 1, grid_str, va = "top", ha = "left",
            fontdict = {"family": "Monospace"}, fontsize = 10, fontweight = "bold",
            transform = axs_grid.transAxes)

        # Adjust layout and save the figure
        axs_grid.figure.set_tight_layout(True)
        axs_grid.figure.savefig(path, dpi = 150)

        return path, desc

# ███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
# ███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
# ███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████

if (__name__ == "__main__"):
    
    profit = read_csv(TMReport.REPORT_FOLDER + "profit.csv", index_col = "timestamp")
    margin = read_csv(TMReport.REPORT_FOLDER + "margin.csv", index_col = "timestamp")
    orders = read_csv(TMReport.REPORT_FOLDER + "orders.csv", index_col = "timestamp")
    states = read_csv(TMReport.REPORT_FOLDER + "states.csv", index_col = "target")
    
    order_grid = read_csv(TMReport.REPORT_FOLDER + "order_grid.csv", header = [0, 1], dtype = str)
    order_grid = order_grid.drop(index = [0]).set_index(order_grid.columns[0])
    order_grid.columns = order_grid.columns.rename(order_grid.index.names[0])
    order_grid.index = order_grid.index.rename("target")
    
    profit.index = DatetimeIndex(profit.index)
    margin.index = DatetimeIndex(margin.index)
    orders.index = DatetimeIndex(orders.index)

    accounts = TMReport.compress_accounts(margin)
    until = orders.index[-1]
    profit, margin, orders = profit[accounts], margin[accounts], orders[accounts]
    path_report_series = TMReport.report_series(profit, margin, orders)
    path_report_lot_dist = TMReport.report_lot_dist(states, until)
    path_report_order_grid = TMReport.report_order_grid(order_grid, until)