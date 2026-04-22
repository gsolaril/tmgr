import os, sys, dash, sqlalchemy, zmq, time
from numpy import nan
from loguru import logger as Log
from configparser import ConfigParser
from pandas import Series, DataFrame, to_datetime, read_sql
from loki_logger_handler.loki_logger_handler import LokiLoggerHandler

sys.path.append("./")

from misc.loki_formatter import LokiFormatter

_DEFAULTS = ConfigParser()
path_core = os.path.split(__file__)[0]
path_main = os.path.split(path_core)[0]
path_core = path_core.replace("./", "")
file = path_main + "/_defaults.ini"
_DEFAULTS.read(os.path.normpath(file))

#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
#███████████████████████████████████████████████████████████████████████████████████████████████████  Environment variables  ███
#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████

DEBUG_MODE = os.getenv("DEBUG_MODE", _DEFAULTS["GENERAL"]["DEBUG_MODE"])
SESSION_NAME = os.getenv("SESSION_NAME", _DEFAULTS["GENERAL"]["SESSION_NAME"])

DB_NAME = os.getenv("DB_NAME", _DEFAULTS["DB"]["NAME"])
DB_USER = os.getenv("DB_USER", _DEFAULTS["DB"]["USER"])
DB_PASS = os.getenv("DB_PASS", _DEFAULTS["DB"]["PASS"])
DB_HOST = os.getenv("DB_HOST", _DEFAULTS["DB"]["HOST"])
DB_PORT = os.getenv("DB_PORT", _DEFAULTS["DB"]["PORT"])

TABLE_ACCS = os.getenv("TABLE_ACCS", _DEFAULTS["DB"]["TABLE_ACCS"])
TABLE_AUTH = os.getenv("TABLE_AUTH", _DEFAULTS["DB"]["TABLE_AUTH"])
TABLE_CHGE = os.getenv("TABLE_CHGE", _DEFAULTS["DB"]["TABLE_CHGE"])
TABLE_CONF = os.getenv("TABLE_CONF", _DEFAULTS["DB"]["TABLE_CONF"])
TABLE_MULT = os.getenv("TABLE_MULT", _DEFAULTS["DB"]["TABLE_MULT"])

ZMQ_HOST = os.getenv("ZMQ_HOST", _DEFAULTS["ZMQ"]["HOST"])
ZMQ_PORT = os.getenv("ZMQ_PORT", _DEFAULTS["ZMQ"]["PORT"])

GUI_HOST = os.getenv("GUI_HOST", _DEFAULTS["GUI"]["HOST"])
GUI_PORT = os.getenv("GUI_PORT", _DEFAULTS["GUI"]["PORT"])

LOKI_HOST = os.getenv("LOKI_HOST", _DEFAULTS["LOKI"]["HOST"])
LOKI_PORT = os.getenv("LOKI_PORT", _DEFAULTS["LOKI"]["PORT"])

MT_API_URL_4 = os.getenv("MT_API_URL_4", _DEFAULTS["MT"]["URL_4"])
MT_API_URL_5 = os.getenv("MT_API_URL_5", _DEFAULTS["MT"]["URL_5"])

#███████████████████████████████████████████████████████████████████████████████████████████████████████████ Derived variables 

app = dash.Dash(name = SESSION_NAME, prevent_initial_callbacks = False, suppress_callback_exceptions = False)

DEBUG_MODE = DEBUG_MODE.lower().__eq__("true")

DB_PORT = int(DB_PORT)
ZMQ_PORT = int(ZMQ_PORT)
LOKI_PORT = int(LOKI_PORT)

DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

ZMQ_URL = f"tcp://{ZMQ_HOST}:{ZMQ_PORT}"

LOKI_URL = f"http://{LOKI_HOST}:{LOKI_PORT}/loki/api/V1/push"

#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
#██████████████████████████████████████████████████████████████████████████████████████████████████████████████████  Loguru  ███
#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████

TEMPL_LOG_FILENAME = os.path.normpath(path_main + "/logs/{time:MM-DD HH.mm}.log")

args = {"backtrace": False, "level": "DEBUG" if DEBUG_MODE else "INFO", 
    "colorize": True, "serialize": False, "format": LokiFormatter.FORMAT}

custom_handler = LokiLoggerHandler(url = LOKI_URL, timeout = 10, labelKeys = {},
        labels = {"application": SESSION_NAME}, defaultFormatter = LokiFormatter)

Log = Log.bind(ID = SESSION_NAME + "/GUI")

#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
#██████████████████████████████████████████████████████████████████████████████████████████████████████████████  ZMQ sender  ███
#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████

try:
    connMQ = zmq.Context()
    ZMQ = connMQ.socket(zmq.PUB)
    ZMQ.connect(ZMQ_URL)
except Exception as EXC:
    Log.exception(EXC)
    ZMQ = None

#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
#█████████████████████████████████████████████████████████████████████████████████████████████████████████  Database helper  ███
#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████

#▄▄▄▄▄▄▄▄▄▄▄▄▄
class Database:

    CATEGORIES = {"source": 0, "target": 1}
    CHANGELOG_TEMPLATE = DataFrame(columns = (CHANGELOG_COLUMNS := [
        "timestamp", "username", "parameter", "value_before", "value_after"]))
    CONN = sqlalchemy.create_engine(url = DB_URL, isolation_level = "AUTOCOMMIT")

    QUERY_CHLOG = "SELECT * FROM " + TABLE_CHGE + " ORDER BY timestamp DESC LIMIT {n};"

    QUERY_DESC = """
        SELECT attname AS index, description AS title,
            format_type(atttypid, atttypmod) AS dtype,
            NOT attnotnull AS not_null
        FROM pg_attribute LEFT JOIN pg_description
            ON (pg_attribute.attrelid = pg_description.objoid)
            AND (pg_attribute.attnum = pg_description.objsubid)
        LEFT JOIN pg_type ON (pg_attribute.atttypid = pg_type.oid)
        LEFT JOIN pg_class ON (pg_attribute.attrelid = pg_class.oid)
        WHERE (pg_class.relname = '{table}')
            AND (pg_attribute.attnum > 0)
        ORDER BY pg_attribute.attnum;
    """

    QUERY_MULT_SET = f"""
        INSERT INTO {TABLE_MULT} (source, target, factor)
        VALUES ('{{source}}', '{{target}}', {{factor}})
        ON CONFLICT (source, target)
        DO UPDATE SET factor = EXCLUDED.factor;
    """

    #▄▄▄▄▄▄▄▄▄▄▄
    @classmethod
    def desc_fields(cls, table: str): return read_sql(
        sql = cls.QUERY_DESC.format(table = table),
        con = cls.CONN, index_col = "index")
    
    #▄▄▄▄▄▄▄▄▄▄▄
    @classmethod
    def _get(cls, table: str):
        try: return read_sql(table, con = cls.CONN)
        except Exception as EXC: Log.exception(EXC)

    #▄▄▄▄▄▄▄▄▄▄▄
    @classmethod
    def _add(cls, table: str, *data):
        head = "INSERT INTO {table} ({keys}) VALUES"
        query: list = list() ; entry: dict = None

        for entry in data:
            values = map(repr, entry.values())
            values = str.join(", ", values)
            values = values.replace("False", "false")
            values = values.replace("True", "true")
            values = values.replace("nan", "null")
            keys = str.join(", ", entry.keys())
            query.append("\t(%s)" % values)
        
        query = str.join(",\n", query)
        query = head + "\n" + query + ";"
        query = str.format(query,
            table = table, keys = keys)
        
        query = sqlalchemy.text(query)
        with cls.CONN.connect() as conn:
            try: conn.execute(query); return data
            except Exception as EXC: Log.exception(EXC)

    #▄▄▄▄▄▄▄▄▄▄▄
    @classmethod
    def _set(cls, table: str, *data):
        head = "UPDATE {table} SET {K1} = {V1} WHERE "
        query: list = list() ; entry: dict = None

        for entry in data:
            K1, *cond_keys = entry.keys()
            V1, *cond_vals = map(repr, entry.values())
            entry = dict(zip(cond_keys, cond_vals))
            query_i = ["%s = %s" % KV for KV in entry.items()]
            query_i = head + str.join(" AND ", query_i) + ";"
            query.append(query_i.format(table = table, K1 = K1, V1 = V1))
        
        query = str.join("\n", query)
        query = sqlalchemy.text(query)
        with cls.CONN.connect() as conn:
            try: conn.execute(query); return data
            except Exception as EXC: Log.exception(EXC)
        
    #▄▄▄▄▄▄▄▄▄▄▄
    @classmethod
    def _del(cls, table: str, **data):
        [key] = data.keys()
        values = str.join(", ", map(repr, data[key]))
        query = "DELETE FROM {table} WHERE {key} IN ({values});"
        query = query.format(table = table, key = key, values = values)

        query = sqlalchemy.text(query)
        with cls.CONN.connect() as conn:
            try: conn.execute(query); return data
            except Exception as EXC: Log.exception(EXC)

#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████

    #▄▄▄▄▄▄▄▄▄▄▄
    @classmethod
    def user_get(cls): return cls._get(TABLE_AUTH).set_index("username")[["password", "access_level"]]
    #▄▄▄▄▄▄▄▄▄▄▄
    @classmethod
    def user_set(cls, *data): return cls._set(TABLE_AUTH, *data)
    #▄▄▄▄▄▄▄▄▄▄▄
    @classmethod
    def accounts_get(cls): return cls._get(TABLE_ACCS).set_index("alias").sort_index()
    #▄▄▄▄▄▄▄▄▄▄▄
    @classmethod
    def accounts_set(cls, *data): return cls._set(TABLE_ACCS, *data)
    #▄▄▄▄▄▄▄▄▄▄▄
    @classmethod
    def accounts_add(cls, *data): return cls._add(TABLE_ACCS, *data)
    #▄▄▄▄▄▄▄▄▄▄▄
    @classmethod
    def accounts_del(cls, **data): return cls._del(TABLE_ACCS, **data)
    #▄▄▄▄▄▄▄▄▄▄▄
    @classmethod
    def config_get(cls): return cls._get(TABLE_CONF).set_index("instance").loc[SESSION_NAME]
    #▄▄▄▄▄▄▄▄▄▄▄
    @classmethod
    def config_set(cls, *data): return cls._set(TABLE_CONF, *data)
    #▄▄▄▄▄▄▄▄▄▄▄
    @classmethod
    def mult_get(cls): return cls._get(TABLE_MULT).set_index([*cls.CATEGORIES])["factor"]
    #▄▄▄▄▄▄▄▄▄▄▄
    @classmethod
    def mult_set(cls, *data):
        
        entry: dict = None
        query: list = list()
        for entry in data: query.append(
            cls.QUERY_MULT_SET.format(**entry))
        
        query = str.join("\n", query)
        query = sqlalchemy.text(query)
        with cls.CONN.connect() as conn:
            try: conn.execute(query); return data
            except Exception as EXC: Log.exception(EXC)
    #▄▄▄▄▄▄▄▄▄▄▄
    @classmethod
    def change_add(cls, *data): return cls._add(TABLE_CHGE, *data)

    #▄▄▄▄▄▄▄▄▄▄▄
    @classmethod
    def change_get(cls, n: int):
        df = read_sql(cls.QUERY_CHLOG.format(n = n), con = cls.CONN)
        df["timestamp"] = to_datetime(df["timestamp"], unit = "us")
        df["timestamp"] = df["timestamp"].dt.strftime("%Y/%m/%d %X")
        return df

    #▄▄▄▄▄▄▄▄▄▄▄
    @classmethod
    def mult_grid_render(cls, multipliers: Series, categories: Series):
        sources = categories.index[categories.eq(cls.CATEGORIES["source"])]
        targets = categories.index[categories.eq(cls.CATEGORIES["target"])]
        multipliers: DataFrame = multipliers.sort_index().unstack("source")
        sources_idle = sources.difference(multipliers.columns)
        targets_idle = targets.difference(multipliers.index)
        for target in targets_idle: multipliers.loc[target, :] = 0.0
        for source in sources_idle: multipliers.loc[:, source] = 0.0
        multipliers = multipliers.fillna(0.0).loc[targets, sources]
        return multipliers.rename_axis("Target\\Source")
    
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def detect_changes(prev: DataFrame, last: DataFrame, columns_not_null: list = None):
        
        for column, dtype in prev.dtypes.items():
            if str(dtype).startswith("int"): dtype = float
            last[column] = last[column].astype(dtype)

        index_name = last.index.name
        items_add = last.index.difference(prev.index)
        items_del = prev.index.difference(last.index)
        items_set = last.index.intersection(prev.index)

        nulls = Series()
        if (columns_not_null is not None) and (len(columns_not_null) > 0):
            columns_not_null = columns_not_null.index[columns_not_null]
            nulls = last.reset_index().set_index(index_name, drop = False)
            nulls = nulls[columns_not_null].stack("field", dropna = False)
            nulls = nulls.isnull() | nulls.eq("")
            nulls = nulls.index.to_frame().fillna("?").loc[nulls]
            nulls: DataFrame = nulls.agg(".".join, axis = "columns")

        assert nulls.empty, "Forbidden nulls ->" + str.join(", ", nulls)

        items_add = last.loc[items_add].reset_index()
        items_add = items_add.to_dict(orient = "records")
        items_del = {index_name: items_del.to_list()}

        prev: DataFrame = prev.loc[items_set]
        last: DataFrame = last.loc[items_set]
        df_comp = DataFrame.merge(prev.stack("field").rename("prev"), last.stack("field").rename("last"),
                  left_index = True, right_index = True, how = "outer").dropna(how = "all").sort_index()
        
        df_comp = df_comp.loc[df_comp["prev"] != df_comp["last"]]
        to_dict = lambda line: {line["field"]: line["last"], index_name: line[index_name]}
        items_set = df_comp["last"].reset_index().apply(to_dict, axis = "columns")
        items_set: Series = items_set.to_list()

        return (items_add, items_del, items_set, df_comp)

#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████

if (__name__ == "__main__"):

    multipliers: DataFrame = Database.mult_get()
    accounts: DataFrame = Database.accounts_get()
    categories = accounts.loc[accounts["active"], "category"]
    multipliers = Database.mult_grid_render(multipliers, categories)
    print([*multipliers.reset_index().to_dict("index").values()])

    raise KeyboardInterrupt
    TABLE_COLUMNS = Database.desc_fields(TABLE_CONF)
    print(TABLE_COLUMNS["dtype"])
    TABLE_COLUMNS[["align", "edit"]] = "right", True
    TABLE_COLUMNS = TABLE_COLUMNS.drop(index = "instance")

    config = Database.config_get().loc[TABLE_COLUMNS.index]
    config.index = TABLE_COLUMNS["title"].rename("Parameter")
    config: DataFrame = DataFrame(config.rename("Value"))
    config = config.to_dict(orient = "index")