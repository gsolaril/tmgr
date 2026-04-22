import os, sys, dash
from numpy import nan
sys.path.append("./")
from gui.utils import *

#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
#█████████████████████████████████████████████████████████████████████████████████████████████████████████████  Account tab  ███
#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████

class TabAccount(dash.dcc.Tab):

    ID_OBJ = "tab-account"
    ID_TABLE = ID_OBJ + "-table"
    ID_TABLE_CONT = ID_TABLE + "-container"
    ID_BUTTON_CONT = ID_OBJ + "-button-container"
    ID_BUTTON_ADD_ROW = ID_OBJ + "-button-add_row"
    ID_BUTTON_CONFIRM = ID_OBJ + "-button-confirm"
    ID_COLUMN_PREFIX = ID_OBJ + "-column"
    ID_STATE_ICON = ID_OBJ + "-state-icon"
    ID_STATE_TEXT = ID_OBJ + "-state-text"

    ID_ATTR_USER = ID_OBJ + "-user"
    ID_ATTR_ACC_INITIAL = ID_OBJ + "-accounts"
    ID_ATTR_ACC_ADD = ID_OBJ + "-accounts-add"
    ID_ATTR_ACC_SET = ID_OBJ + "-accounts-set"
    ID_ATTR_ACC_DEL = ID_OBJ + "-accounts-del"

    STYLE_BUTTON_MARGIN = "20px"

    STYLE_DIV_TABLE_OBJ = {"height": "700px", "max-height": "700px", "margin": "10px"}
    STYLE_DIV_TABLE_CELL = {"textAlign": "left", "padding": "2px", "minWidth": "75px", 'textOverflow': 'ellipsis'}
    STYLE_DIV_TABLE_HEAD = {"whiteSpace": "normal", "textAlign": "center", "overflow": "hidden"}
    STYLE_BUTTONS = {"height": "40px", "margin": STYLE_BUTTON_MARGIN, "flexDirection": "row", 
                    "justify-content": "center", "display": "flex", "verticalAlign": "middle"}
    STYLE_BUTTON = {"marginLeft": STYLE_BUTTON_MARGIN, "marginRight": STYLE_BUTTON_MARGIN,
            "fontFamily": "Arial, sans-serif", "padding": "10px", "verticalAlign": "middle",
            "justify-content": "center", "font-size": "18px"}
    STYLE_STATE_ICON = {"marginLeft": STYLE_BUTTON_MARGIN, "marginRight": STYLE_BUTTON_MARGIN,
            "fontFamily": "Arial, sans-serif", "font-size": "24px", "verticalAlign": "middle",
            "justify-content": "center"}
    STYLE_STATE_TEXT = {"marginLeft": STYLE_BUTTON_MARGIN, "marginRight": STYLE_BUTTON_MARGIN,
            "font-size": "14px", "verticalAlign": "middle", "fontFamily": "Arial, sans-serif",
            "color": "gray"}

    BUTTON_ARGS = {"n_clicks": 0, "style": STYLE_BUTTON}

    ADD_ROW_OUTPUT = [dash.dependencies.Output(ID_STATE_ICON, "children", allow_duplicate = True),
                      dash.dependencies.Output(ID_STATE_TEXT, "children", allow_duplicate = True),
                      dash.dependencies.Output(ID_TABLE, "data", allow_duplicate = True)]
    ADD_ROW_INPUTS = [dash.dependencies.Input(ID_BUTTON_ADD_ROW, "n_clicks")]
    ADD_ROW_STATES = [dash.dependencies.State(ID_TABLE, "data")]

    CONFIRM_OUTPUT = [dash.dependencies.Output(ID_STATE_ICON, "children"),
                      dash.dependencies.Output(ID_STATE_TEXT, "children"),
                      dash.dependencies.Output(ID_ATTR_ACC_INITIAL, "data"),
                      dash.dependencies.Output(ID_TABLE, "data")]
    CONFIRM_INPUTS = [dash.dependencies.Input(ID_BUTTON_CONFIRM, "n_clicks")]
    CONFIRM_STATES = [dash.dependencies.State(ID_ATTR_ACC_INITIAL, "data"),
                      dash.dependencies.State(ID_TABLE, "data"),
                      dash.dependencies.State(ID_ATTR_USER, "data")]
    
    DELETE_INPUTS = [dash.dependencies.Input(ID_TABLE, "active_cell")]

    ADD_ROW_LABEL, CONFIRM_LABEL = "New account", "Apply changes"

    PARAMETER_CHANGE_FORMAT = "{0[object]}.{0[field]}".format

    ICON_CHANGE_PENDING, TEXT_CHANGE_PENDING = "⏳", "No changes yet."
    ICON_CHANGE_SUCCESS, TEXT_CHANGE_SUCCESS = "✅", "Changes applied"
    ICON_CHANGE_FAILURE, TEXT_CHANGE_FAILURE = "❌", "Changes failed. "
    ICON_DELETE, TEXT_CHANGE_FAILURE_NULL = "🗑️", "Forbidden nulls -> "
    TEXT_CHANGE_ADDED_ROW = "Account added. Click on \"Apply changes\" when ready."
    TEXT_CHANGE_DELETED = "Account deleted. Click on \"Apply changes\" when ready."

    TABLE_COLUMNS = DataFrame.from_dict(
        orient = "index", columns = ["dtype", "title", "align", "edit", "not_null"],
        data = dict(
            alias           = (str,     "Account",          "left",    True,   True),
            owner           = (str,     "Owner",            "left",    True,   False),
            id              = (int,     "ID#",              "left",    True,   True),
            password        = (str,     "Password",         "left",    True,   True),
            server          = (str,     "Server",           "left",    True,   True),
            mtver           = (str,     "MT#",              "center",  True,   True),
            sym_suffix      = (str,     "Symbol suffix",    "center",  True,   False),
            category        = (int,     "Category",         "center",  True,   True),
            active          = (bool,    "Active",           "center",  True,   False),
            max_lots_sig    = (float,   "Max Lot/trade",    "right",   True,   False),
            max_lots_net    = (float,   "Max Lot/net",      "right",   True,   False),
            #max_marg_usd    = (float,   "Max Marg/$",      "right",   True,   False),
            #max_marg_prc    = (float,   "Max Marg/%",      "right",   True,   False),
            #max_loss_usd    = (float,   "Max Risk/$",      "right",   True,   False),
            #max_loss_prc    = (float,   "Max Risk/%",      "right",   True,   False),
            delete          = (str,     "Delete",           "center",  False,  False),
        )
    )

    TABLE_INDEX_NAME = TABLE_COLUMNS.index[0]

    #▄▄▄▄▄▄▄▄▄▄▄
    @classmethod
    def accounts_get_dict(cls, access_level: int):
        accounts: DataFrame = Database.accounts_get()

        if (access_level >= 2):
            accounts["password"] = "********"
            accounts["delete"] = cls.ICON_DELETE
        
        accounts = accounts.reset_index()[list(cls.TABLE_COLUMNS.index)]
        accounts = accounts.rename(columns = cls.TABLE_COLUMNS["title"])
        return accounts.to_dict(orient = "records")

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, username: str, access_level: int = 3):

        Log.info("Switching to \"Account\" tab...")
        user = {"username": username, "access_level": access_level}
        accounts = self.accounts_get_dict(access_level)
        
        style_data_if = list()
        style_cell = {"minWidth": "75px"}
        style_data = {"padding": "0 8px 0 8px", "textOverflow": "ellipsis",
              "height": "auto", "overflow": "hidden", "maxHeight": "14px"}
        style_head = {"whiteSpace": "normal", "backgroundColor": "#DDDDEE",
              "height": "auto", "overflow": "hidden", "fontWeight": "bold",
              "fontFamily": "Arial, sans-serif", "fontSize": "14px", "textAlign": "center"}
        style_data_if.append({"if": {"row_index": "even"}, "backgroundColor": "#F6F6F6"})
        style_data_if.append({"if": {"state": "selected"}, "backgroundColor": "#CCCCFF"})
        style_data_if.append({"if": {"state": "selected", "column_id": "Delete"},
            "backgroundColor": "#FFCCCC"})

        columns: list = list()
        for column, key in self.TABLE_COLUMNS.iterrows():
            dtype, title, align, edit, not_null = key
            columns.append({"name": title, "editable": edit, "id": title})
            style_data_if.append({"if": {"column_id": title}, "textAlign": align,
                    "fontWeight": "bold" if (title == "Account") else "normal"})

        super().__init__(id = self.ID_OBJ, style = {"alignItems": "center"},
            label = self.__class__.__name__.replace("Tab", ""), children = [
                dash.dcc.Store(id = self.ID_ATTR_ACC_INITIAL, data = accounts),
                dash.dcc.Store(id = self.ID_ATTR_USER, data = user),
                dash.dash_table.DataTable(id = self.ID_TABLE, style_cell = style_cell,
                    style_data = style_data, style_header = style_head, columns = columns,
                    style_data_conditional = style_data_if, fixed_rows = {"headers": True},
                    data = accounts.copy(),
                ),
                dash.html.Hr(), 
                dash.html.Div(id = self.ID_BUTTON_CONT, style = self.STYLE_BUTTONS, children = [
                    dash.html.Button(id = self.ID_BUTTON_ADD_ROW, children = self.ADD_ROW_LABEL, **self.BUTTON_ARGS),
                    dash.html.Button(id = self.ID_BUTTON_CONFIRM, children = self.CONFIRM_LABEL, **self.BUTTON_ARGS),
                    dash.html.Div(id = self.ID_STATE_ICON, children = self.ICON_CHANGE_PENDING, style = self.STYLE_STATE_ICON),
                ]), dash.html.H6(id = self.ID_STATE_TEXT, style = self.STYLE_STATE_TEXT, children = "Hello!"),
            ]
        )

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @app.callback(ADD_ROW_OUTPUT, ADD_ROW_INPUTS, ADD_ROW_STATES, prevent_initial_call = True)
    def add_row(n_clicks: int, data: list):

        if (n_clicks > 0): data.append(dict())
        icon = TabAccount.ICON_CHANGE_PENDING
        text = TabAccount.TEXT_CHANGE_ADDED_ROW
        return icon, text, data
    
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @app.callback(ADD_ROW_OUTPUT, DELETE_INPUTS, ADD_ROW_STATES, prevent_initial_call = True)
    def delete_row(clicked_cell: int, data: list):
        
        column: str = clicked_cell["column_id"]
        icon = TabAccount.ICON_CHANGE_PENDING
        text = TabAccount.TEXT_CHANGE_PENDING
        if (column.lower() == "delete"):
            acc = data.pop(clicked_cell["row"])
            text = TabAccount.TEXT_CHANGE_DELETED
        return icon, text, data

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @app.callback(CONFIRM_OUTPUT, CONFIRM_INPUTS, CONFIRM_STATES, prevent_initial_call = True)
    def confirm(n_clicks: int, prev: dict, last: dict, user: dict):

        if (n_clicks < 0): return
        INDEX_NAME = TabAccount.TABLE_INDEX_NAME

        try:

            df_prev = DataFrame.from_records(prev)
            df_last = DataFrame.from_records(last)
            df_last = df_last.drop_duplicates(subset = df_last.columns[0])
            df_prev.columns = TabAccount.TABLE_COLUMNS.index.rename("field")
            df_last.columns = TabAccount.TABLE_COLUMNS.index.rename("field")
            df_prev = df_prev.set_index(INDEX_NAME)
            df_last = df_last.set_index(INDEX_NAME)

            df_prev = df_prev.drop(columns = ["delete"])
            df_last = df_last.drop(columns = ["delete"])
            df_last["active"] = df_last["active"].astype(str).str.lower().eq("true")

            ######################################################## Future function starts here
                
            items = Database.detect_changes(prev = df_prev, last = df_last,
                    columns_not_null = TabAccount.TABLE_COLUMNS["not_null"])
            items_add, items_del, items_set, df_comp = items

            Log.debug("Saving account changes...")

            username = user["username"]
            now = int(time.time() * 1e6)

            changelog = Database.CHANGELOG_TEMPLATE.copy()

            if len(items_add):
                Database.accounts_add(*items_add)
                items_add = DataFrame(items_add)
                Log.debug("-> Added:\n%s" % items_add)
                changelog["value_after"] = items_add[INDEX_NAME]
                changelog["parameter"] = ".new_account"
                changelog["value_before"] = ""

            if len(items_del[INDEX_NAME]):
                Log.debug("-> Deleting:\n%s" % items_del)
                Database.accounts_del(**items_del)
                changelog["value_before"] = items_del[INDEX_NAME]
                changelog["parameter"] = ".del_account"
                changelog["value_after"] = ""

            if len(items_set):
                Database.accounts_set(*items_set)
                Log.debug("-> Changed:\n%s" % items_set)
                df_comp: DataFrame = df_comp.sort_index().reset_index()
                df_comp = df_comp.rename(columns = {INDEX_NAME: "object"})
                changelog["value_before"] = df_comp["prev"].astype(str)
                changelog["value_after"] = df_comp["last"].astype(str)
                changelog["parameter"] = df_comp.agg(axis = "columns",
                            func = TabAccount.PARAMETER_CHANGE_FORMAT)

            if (n_changes := changelog.shape[0]) > 0:
                changelog["timestamp"], changelog["username"] = now, username
                changelog["timestamp"] = changelog["timestamp"] + range(n_changes)
                Database.change_add(*changelog.to_dict(orient = "index").values())
                text = TabAccount.TEXT_CHANGE_SUCCESS
                icon = TabAccount.ICON_CHANGE_SUCCESS
            else:
                text = TabAccount.TEXT_CHANGE_PENDING
                icon = TabAccount.ICON_CHANGE_PENDING

        except Exception as EXC:

            text = TabAccount.TEXT_CHANGE_FAILURE
            icon = TabAccount.ICON_CHANGE_FAILURE
            text += repr(EXC); Log.exception(EXC)
            
        Log.debug("Reloading table content...")
        df_last = TabAccount.accounts_get_dict(user["access_level"])
        return (icon, text, df_last.copy(), df_last.copy())
    