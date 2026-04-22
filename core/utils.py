#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀

import os, sys, time, json
sys.path.append("./")

from loguru import logger as Log
from aiohttp import ClientSession, ClientTimeout, FormData
from enum import IntEnum, StrEnum
from urllib.parse import urlencode
from configparser import ConfigParser
from pandas import Series, DataFrame, Timestamp, read_sql
from loki_logger_handler.loki_logger_handler import LokiLoggerHandler

PATH_MAIN = __file__.split(os.path.sep)[: -2]
PATH_MAIN = str.join(os.path.sep, PATH_MAIN)

from misc.loki_formatter import LokiFormatter

_DEFAULTS = ConfigParser()
file = PATH_MAIN + "/_defaults.ini"
_DEFAULTS.read(os.path.normpath(file))

#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
#███████████████████████████████████████████████████████████████████████████████████████████████████  Environment variables  ███
#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████

DEBUG_MODE = os.getenv("DEBUG_MODE", _DEFAULTS["GENERAL"]["DEBUG_MODE"])
SESSION_NAME = os.getenv("SESSION_NAME", _DEFAULTS["GENERAL"]["SESSION_NAME"])
N_WORKERS_ZMQ = os.getenv("N_WORKERS_ZMQ", _DEFAULTS["GENERAL"]["N_WORKERS_ZMQ"])
TOKEN_TIMEOUT = os.getenv("TOKEN_TIMEOUT", _DEFAULTS["GENERAL"]["TOKEN_TIMEOUT"])
TRADE_TIMEOUT = os.getenv("TRADE_TIMEOUT", _DEFAULTS["GENERAL"]["TRADE_TIMEOUT"])

DB_NAME = os.getenv("DB_NAME", _DEFAULTS["DB"]["NAME"]) # PostgreSQL database name.
DB_USER = os.getenv("DB_USER", _DEFAULTS["DB"]["USER"]) # PostgreSQL database user.
DB_PASS = os.getenv("DB_PASS", _DEFAULTS["DB"]["PASS"]) # PostgreSQL database password.
DB_HOST = os.getenv("DB_HOST", _DEFAULTS["DB"]["HOST"]) # PostgreSQL database host.
DB_PORT = os.getenv("DB_PORT", _DEFAULTS["DB"]["PORT"]) # PostgreSQL database port.

TABLE_ACCS = os.getenv("TABLE_ACCS", _DEFAULTS["DB"]["TABLE_ACCS"]) # Table for accounts.
TABLE_CONF = os.getenv("TABLE_CONF", _DEFAULTS["DB"]["TABLE_CONF"]) # ... global configurations
TABLE_DPST = os.getenv("TABLE_DPST", _DEFAULTS["DB"]["TABLE_DPST"]) # ... deposits (may fix PNL jumps)
TABLE_CACT = os.getenv("TABLE_CACT", _DEFAULTS["DB"]["TABLE_CACT"]) # ... currently active trades.
TABLE_CCLS = os.getenv("TABLE_CCLS", _DEFAULTS["DB"]["TABLE_CCLS"]) # ... closed trade history.
TABLE_MULT = os.getenv("TABLE_MULT", _DEFAULTS["DB"]["TABLE_MULT"]) # ... source-target lot multipliers.
TABLE_MISS = os.getenv("TABLE_MISS", _DEFAULTS["DB"]["TABLE_MISS"]) # ... missing or loose trades.
TABLE_RESP = os.getenv("TABLE_RESP", _DEFAULTS["DB"]["TABLE_RESP"]) # ... order response objects.
TABLE_SYMS = os.getenv("TABLE_SYMS", _DEFAULTS["DB"]["TABLE_SYMS"]) # ... symbol specs and mappings.
TABLE_TSRS = os.getenv("TABLE_TSRS", _DEFAULTS["DB"]["TABLE_TSRS"]) # ... state variable timeseries.

REDIS_HOST = os.getenv("REDIS_HOST", _DEFAULTS["REDIS"]["HOST"]) # Redis host.
REDIS_PORT = os.getenv("REDIS_PORT", _DEFAULTS["REDIS"]["PORT"]) # Redis port.
REDIS_KT = os.getenv("KEY_TOKENS", _DEFAULTS["REDIS"]["KEY_TOKENS"]) # Redis key pattern for token stashables.

ZMQ_HOST = os.getenv("ZMQ_HOST", _DEFAULTS["ZMQ"]["HOST"]) # ZMQ host.
ZMQ_PORT = os.getenv("ZMQ_PORT", _DEFAULTS["ZMQ"]["PORT"]) # ZMQ port.

LOKI_HOST = os.getenv("LOKI_HOST", _DEFAULTS["LOKI"]["HOST"]) # Loki host.
LOKI_PORT = os.getenv("LOKI_PORT", _DEFAULTS["LOKI"]["PORT"]) # Loki port.

MT_API_URL_4 = os.getenv("MT_API_URL_4", _DEFAULTS["MT"]["URL_4"]) # MT4 API URL.
MT_API_URL_5 = os.getenv("MT_API_URL_5", _DEFAULTS["MT"]["URL_5"]) # MT5 API URL.

TGRAM_TOKEN = os.getenv("TOKEN_TELEGRAM", _DEFAULTS["TELEGRAM"]["TOKEN"]) # Telegram token.
TGRAM_CHATS = os.getenv("CHATS_TELEGRAM", _DEFAULTS["TELEGRAM"]["CHATS"]) # Telegram chats.
SLACK_TOKEN = os.getenv("TOKEN_SLACK", _DEFAULTS["SLACK"]["TOKEN"]) # Slack token.
SLACK_CHATS = os.getenv("CHATS_SLACK", _DEFAULTS["SLACK"]["CHATS"]) # Slack chats.
TGRAM_CHATS = str.split(TGRAM_CHATS, ", ")[0]
SLACK_CHATS = str.split(SLACK_CHATS, ", ")[0]

#███████████████████████████████████████████████████████████████████████████████████████████████████████████ Derived variables 

DEBUG_MODE = DEBUG_MODE.lower().__eq__("true")
N_WORKERS_ZMQ: int = int(N_WORKERS_ZMQ)
# Max duration for token and trade validity.
TOKEN_TIMEOUT: int = int(TOKEN_TIMEOUT)
TRADE_TIMEOUT: int = int(TRADE_TIMEOUT)

# Turn port numbers to integers.
DB_PORT: int = int(DB_PORT) 
ZMQ_PORT: int = int(ZMQ_PORT)
LOKI_PORT: int = int(LOKI_PORT)
REDIS_PORT: int = int(REDIS_PORT)

REDIS_HASH_FORMAT = "{name}::{area}"

# PostgreSQL database URL.
DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
# ZMQ URL.
ZMQ_URL = f"tcp://{ZMQ_HOST}:{ZMQ_PORT}"
# Loki URL.
LOKI_URL = f"http://{LOKI_HOST}:{LOKI_PORT}/loki/api/v1/push"

#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
#██████████████████████████████████████████████████████████████████████████████████████████████████████████████████  Loguru  ███
#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████

TEMPL_LOG_FILENAME = os.path.normpath(PATH_MAIN + "/logs/{time:MM-DD HH.mm}.log") # Naming pattern for log files.

Log.remove(0) # Reset structure of Loguru's logging object.

args = {"backtrace": False, "level": "DEBUG" if DEBUG_MODE else "INFO", 
  "colorize": True, "serialize": False, "format": LokiFormatter.FORMAT}

# Create a custom handler for Loguru to be compatible with Grafana/Loki's API.
custom_handler = LokiLoggerHandler(url = LOKI_URL, timeout = 10, labelKeys = {},
        labels = {"application": SESSION_NAME}, defaultFormatter = LokiFormatter)

Log.add(**args, sink = sys.stdout) # Print to stdout.
Log.add(**args, sink = custom_handler) # Send to Loki.
Log = Log.bind(ID = SESSION_NAME) # Bind the TM's name to the Loguru object.

#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
#██████████████████████████████████████████████████████████████████████████████████████████████████████████  MT API wrapper  ███
#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████

#▄▄▄▄▄▄▄▄▄▄
class MT_API:
    """Wrapper for MetaTrader API. Can be extended or adjusted to perform
    inside any type of trading venue with HTTP requests to REST APIs."""
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    class VersionMT(IntEnum): MT4, MT5 = 4, 5 # Supported versions of MetaTrader.
    class ProtocolURL(StrEnum): HTTP, WS = "http", "ws" # Supported protocols.

    VER_DEFAULT = VersionMT.MT5 # Default version of MetaTrader.
    MT_VERSIONS = {4: VersionMT.MT4, 5: VersionMT.MT5}
    
    BASE_URL = {VersionMT.MT4: MT_API_URL_4, VersionMT.MT5: MT_API_URL_5}
    LATEST_BUILD = {VersionMT.MT4: "1400", VersionMT.MT5: "4153"}
    HEADER = dict(accept = "text/json") # Header for HTTP requests.

    # Test servers for each version (old, no longer used).
    SERVERS = {ver: dict() for ver in MT_VERSIONS.values()}
    # Timestamp-to-MT-string format.
    DT_FORMAT = "%Y-%m-%dT%H:%M:%S"

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod
    def get_base_url(cls, version: VersionMT, protocol: ProtocolURL = ProtocolURL.HTTP):
        """Return the base URL for the given version and protocol."""
        return cls.BASE_URL[version].format(protocol = protocol.value)
    
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod
    async def request(cls, url: str, verbose: str, data: dict = None,
                      ignore_errors: bool = False, timeout: int = 10):
        """
        Perform a request to the given URL with the given data.
        Includes the complete handling of the async request, the
        parsing of the response, the logging of the outcome and
        some debugging features (e.g. URL for manual testing).

        Inputs:
            url (str): The URL to request.
            verbose (str): The verbose string to use for logging.
            data (dict): The data to send with the request.
            ignore_errors (bool): Whether to ignore errors.
            timeout (int): The timeout for the request.

        Output: (str or dict) HTTP response, decoded. Type depends on endpoint.
        """
        args = {"headers": MT_API.HEADER, "timeout": ClientTimeout(total = timeout)}
        # If the verbose string ends with "$", show the raw response whatever it is.
        if (show_response := verbose.endswith("$")): verbose = verbose[: -1]
        success, failure = verbose + " successful. ", verbose + " failed. "

        try:
            url2 = url + "?" + urlencode(data) # Complete URL for printing/logging.
            # Won't be really used in the request (header specified above is text/json)
            Log.debug("Calling: \"%s\"" % url2) # ...but serves for manual clicking.
            
            async with ClientSession(**args) as session:
                async with session.get(url2) as request:
                    response: str = await request.text() # Get the response as text, later to be decoded.
                    error = f"#{request.status} > {request.reason}" # Error message if the request failed.
                    assert (request.status == 200), error # Assert that the HTTP request was successful.
                    if show_response: success += response # Show the raw response if applicable from input.

                    Log.success(success)
                    # Most of non-JSON string outputs are "OK"...
                    if (response in ["", "OK"]): return "OK" # Keep them.
                    # Otherwise, decode the response into a dictionary.
                    else: return json.loads(response) 

        except TimeoutError:
            Log.error(f"Timeout after {timeout} secs at \"{url2}\"")
            return "Timeout"
        
        except AssertionError:
            if isinstance(response, str):
                # If still JSON-decodable, print MT error label.
                error += "(%s)" % json.loads(response)["message"]
            if not ignore_errors: Log.error(failure + error)
            return error
        
        except Exception as EXC:
            if not ignore_errors:
                Log.exception(EXC)
            return repr(EXC)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod
    async def get_server_ip(cls, server: str, version: VersionMT):
        """
        Get the IP address of the given server. Currently not being needed,
        given that the database can just store the IP addresses beforehand.

        Inputs:
            server (str): The server to get the IP address of.
            version (VersionMT): The version of MetaTrader to use.

        Output: (str) The IP address of the given server.
        """

        IP: str = None
        IPs = list()

        if (server in cls.SERVERS[version]):
            IP = (IPs := cls.SERVERS[version][server])[0]
            # Log the IP address of the given server.
            Log.success(f"Search for \"{server}\" IP not needed.")
            # Log the other IP addresses for the given server.
            Log.debug(f"Chose IP: {IP := IPs[0]}\n(Others: {IPs})")
            return IP

        # Get the IP address of the given server through HTTP request to the MT API.
        url = cls.get_base_url(version, cls.ProtocolURL.HTTP) + (endpoint := "Search")
        verbose = f"{endpoint} for \"{server}\" IP found ({version.name}): $"
        response = await cls.request(url, data = {"company": server}, verbose = verbose)
        
        if (response is not None):
             
            try: # Cycle through the
                # shown search results:
                for broker in response:
                    for item in broker["results"]:
                        name, IPs = item.values()
                        cls.SERVERS[version][name] = IPs
                # Get the firstmost IP address that's from the
                # server labelled the most similar to the input.
                IP = (IPs := cls.SERVERS[version][server])[0]
            except Exception as EXC: Log.exception(EXC)
            Log.debug(f"Chose IP: {IP}\nFound: {IPs}")
        
        return IP
    
#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
#████████████████████████████████████████████████████████████████████████████████████████████████████████  Telegram wrapper  ███
#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████

class TGram_API:

    BASE_URL = "https://api.telegram.org/bot{token}/"
    VERBOSE_SENT = "Sending to Telegram #{chat}: \"{message}\""
    VERBOSE_READ = "Received from Telegram ({}):\n => {}"

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod
    async def send(cls, token: str, chat: str, message: str, filepath: str = None):
        """
        Send a message to the given chat.

        Inputs:
            token (str): The token for the Telegram bot.
            chat (str): The chat to send the message to.
            message (str): The message to send.
            filepath (str): The path to the file to send.

        Output: (bool, dict) Whether the message was sent successfully and the result.
        """
        # Get the URL for the given token.
        url = cls.BASE_URL.format(token = token) + "send"
        # Get the URL for the given token.
        url += "Message" if (filepath is None) else "Photo"
        # Get the argument for the text.
        arg_text = "text" if (filepath is None) else "caption"
        
        data = {"chat_id": str(chat), "parse_mode": "html", arg_text: message}
        # If the file path is a string, add the file to the data.
        if isinstance(filepath, str): data["photo"] = open(filepath, "rb")

        verbose = cls.VERBOSE_SENT.format(chat = chat, message = message)
        # If the file path is a string, add the file to the verbose.
        if isinstance(filepath, str): verbose += f"\n[File: \"{filepath}\"]"

        args = {"data": FormData(data)}
        async with ClientSession() as session:
            async with session.post(url, **args) as request:
                # Parse the response and check message sent.
                response: dict = await request.json()
                Log.debug(verbose)

                if response.__contains__("result"):
                    # Return the result if it's successful.
                    return response["ok"], response["result"]
                elif response.__contains__("error_code"):
                    # Error code 429 means rate limit.
                    if (response["error_code"] in [429]):
                        response["ok"] = True # Nothing to do.
                    return response.pop("ok"), response
                else: return response.pop("ok"), response
        
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod
    async def read(cls, token: str, verbose: bool = True):
        """
        Read the most recent messages from the given chat. Will serve as a way
        to manually request for reports through a message in Telegram groups.

        Inputs:
            token (str): The token for the Telegram bot.
            verbose (bool): Whether to print the verbose.

        Output: (dict) The last message from the given chat, unparsed.
        """
        url = cls.BASE_URL.format(token = token) + "getUpdates"
        data = FormData(dict(offset = "100", limit = "100"))

        async with ClientSession() as session:
            async with session.post(url, data = data) as request:
                try: response = dict.values(await request.json())
                except Exception as EXC: response = (False, EXC)
                if verbose: Log.debug(cls.VERBOSE_READ.format(*response))
                return response # Return a list of decoded messages.

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod
    async def last(cls, token: str, verbose: bool = True):
        """
        Process only the last message from the given group chat, any user involved.

        Inputs:
            token (str): The token for the Telegram bot.
            verbose (bool): Whether to print the verbose.

        Output: (dict) The last message from the given chat with user info.
        """

        status, result = await cls.read(token, False)
        if not status or (len(result) == 0): return dict()
        
        result: dict = result[-1]
        update_id: int = result.pop("update_id")
        message: dict = result.pop("message", None)
        if (message is None): return None
        ts: int = message.pop("date") * 1e9

        user: dict = None
        user_name, user_id = None, None
        # Check if the message has a real user involved.
        if (user := message.get("from", None)):
            user_name = user_id = user.get("id", None)
            # Telegram contact info. Useful for auth.
            if user.get("first_name", None):
                user_name = user["first_name"]
            if user.get("last_name", None):
                user_name += " " + user["last_name"]
            if user.get("username", None):
                user_name = user["username"]

        # Return the update info.
        update = {"id": update_id, "ts": Timestamp(ts, tz = "UTC"),
            "message": message.pop("text", None), "chat_name": message["chat"]["title"],
            "chat_id": message["chat"]["id"], "user_name": user_name, "user_id": user_id}

        if verbose: Log.debug("Last Telegram update received:\n => " + str(update))
        return update    

#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
#███████████████████████████████████████████████████████████████████████████████████████████████████████████  Slack wrapper  ███
#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████

class Slack_API:

    BASE_URL = "https://www.slack.com/api/"
    HEADERS = {
        "Content-type": "application/x-www-form-urlencoded",
        "Authorization": "Bearer {token}",
    }
    
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @classmethod
    def get_base_request(cls, token: str):

        headers = cls.HEADERS.copy()
        headers["Authorization"] = headers["Authorization"].format(token = token)
        return cls.BASE_URL, headers

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def send(cls, channel: str, message: str, filename: str = None):

        pass
    
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def read(cls, channel: str):

        pass

#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████

if (__name__ == "__main__"):

    TOKEN = TGRAM_TOKEN
    CHATS = TGRAM_CHATS

    raise KeyboardInterrupt

    import asyncio

    args = dict()
    if True:
        args["chat"] = CHATS
        args["message"] = "<b>hello</b>"
        # args["filepath"] = "./image.jpg"
    result = asyncio.run(TGram_API.send(token = TOKEN, **args))

    print("result:", result)

    

    
