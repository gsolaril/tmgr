#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀

# Import required system and async libraries
import os, sys, asyncio
from argparse import ArgumentParser

# Add current directory to Python path
sys.path.append("./")

# Import custom utilities and order management functions
from core.utils import *
from misc.order_multi_close import close_all_orders

# Set up command line argument parser with various options
arg_parser = ArgumentParser(description = "Trade Copier/Manager")
# Debug mode flag which adds more in-depth logging (e.g.: URL printings)
arg_parser.add_argument("-d", "--debug", default = True, action = "store_true")
# Session name parameter. Overrides the one provided by the global config table.
arg_parser.add_argument("-n", "--name", default = SESSION_NAME, type = str)
# Number of ZMQ workers parameter. Recommended:Not more than 30.
arg_parser.add_argument("-z", "--nzmq", default = N_WORKERS_ZMQ, type = int)
# Flag to close all existing orders on target accounts before starting.
arg_parser.add_argument("-c", "--close_all_orders",
            default = False, action = "store_true")

# Parse command line arguments
args = arg_parser.parse_args()
# Extract individual arguments
ARG__DEBUG_MODE = getattr(args, "debug")
ARG__SESSION_NAME = getattr(args, "name")
ARG__N_WORKERS_ZMQ = getattr(args, "nzmq")
CLOSE_ALL_ORDERS = getattr(args, "close_all_orders")

# Override default settings with command line arguments if provided
if (ARG__DEBUG_MODE is not None): DEBUG_MODE = ARG__DEBUG_MODE
if (ARG__SESSION_NAME is not None): SESSION_NAME = ARG__SESSION_NAME
if (ARG__N_WORKERS_ZMQ is not None): N_WORKERS_ZMQ = ARG__N_WORKERS_ZMQ

# Warning message for closing all orders
verbose = "\nClosing all old orders in 5 seconds, to start clean test..."
verbose += "\nNote: DON'T run order-closer on REAL-FUNDED ACCOUNTS!!!"
verbose += "\nIf that's the case: keyboard-interrupt this RIGHT NOW!!!"

# Close all existing orders if flag is set (currently disabled with 'and False')
if CLOSE_ALL_ORDERS and False:
    Log.critical(verbose), time.sleep(5)
    df = close_all_orders().to_dict(orient = "index")
    print("Close-all-orders responses:\n%s" % json.dumps(df, indent = 4))
    Log.success("Done with pre-closing. Starting TradeManager test...")

#██████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████

# Import trading system components
from core.executor import StreamExecutor
from core.receiver import StreamReceiver
from core.reporter import AccountReporter
from core.watcher import AccountWatcher

# Bind session name to logger
Log = Log.bind(ID = SESSION_NAME)

#██████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████

# Format strings for logging messages
VERBOSE_WAIT = f"Token-copying instances will wait %d seconds."
VERBOSE_WATCHERS = f"StreamExecutor launched %d watchers"

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄
async def main():
    # Initialize account watcher
    accw = AccountWatcher(name = SESSION_NAME)
    Log.warning(VERBOSE_WAIT % TOKEN_TIMEOUT)
    # Wait for token timeout period
    await asyncio.sleep(TOKEN_TIMEOUT)
    
    # Initialize stream receiver and executor
    #accr = AccountReporter(name = SESSION_NAME)  # Currently commented out
    recv = StreamReceiver(name = SESSION_NAME)
    exec = StreamExecutor(name = SESSION_NAME,
       nzmq = N_WORKERS_ZMQ)
    await asyncio.sleep(1)
    Log.info(VERBOSE_WATCHERS % exec.watchers)
    
    # Gather and run all coroutines from different components
    await asyncio.gather(
       *accw.coroutines.values(),
       #*accr.coroutines.values(),  # Currently commented out
       *recv.coroutines.values(),
       *exec.coroutines.values(),
    )

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
if (__name__ == "__main__"):
    try:
        # Run the main async function
        asyncio.run(main())
    except KeyboardInterrupt:
        # Handle graceful shutdown on Ctrl+C
        Log.warning("Ctrl+C, interrupted")
        Log.success("See you next time :)")
    