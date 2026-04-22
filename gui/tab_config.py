import os, sys, dash
sys.path.append("./")
from gui.utils import *

#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
#█████████████████████████████████████████████████████████████████████████████████████████████████████████████  Account tab  ███
#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████

class TabConfig(dash.dcc.Tab):

    ID_OBJ = "tab-config"
    ID_TABLE = ID_OBJ + "-table"
    ID_TABLE_CONT = ID_TABLE + "-container"
    ID_BUTTON_CONT = ID_OBJ + "-button-container"
    ID_BUTTON_CONFIRM = ID_OBJ + "-button-confirm"
    ID_STATE_ICON = ID_OBJ + "-state-icon"
    ID_STATE_TEXT = ID_OBJ + "-state-text"

    ID_ATTR_USER = ID_OBJ + "-user"
    ID_ATTR_ACC_INITIAL = ID_OBJ + "-config"
    ID_ATTR_ACC_SET = ID_OBJ + "-config-set"

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

    CONFIRM_OUTPUT = [dash.dependencies.Output(ID_STATE_ICON, "children"),
                      dash.dependencies.Output(ID_STATE_TEXT, "children"),
                      dash.dependencies.Output(ID_ATTR_ACC_INITIAL, "data"),
                      dash.dependencies.Output(ID_TABLE, "data")]
    CONFIRM_INPUTS = [dash.dependencies.Input(ID_BUTTON_CONFIRM, "n_clicks")]
    CONFIRM_STATES = [dash.dependencies.State(ID_ATTR_ACC_INITIAL, "data"),
                      dash.dependencies.State(ID_TABLE, "data"),
                      dash.dependencies.State(ID_ATTR_USER, "data")]

    CONFIRM_LABEL = "Apply changes"

    PARAMETER_CHANGE_FORMAT = "config.{0[field]}".format

    ICON_CHANGE_PENDING, TEXT_CHANGE_PENDING = "⏳", "No changes yet."
    ICON_CHANGE_SUCCESS, TEXT_CHANGE_SUCCESS = "✅", "Changes applied"
    ICON_CHANGE_FAILURE, TEXT_CHANGE_FAILURE = "❌", "Changes failed. "
    TEXT_CHANGE_FAILURE_NULL = "No empty fields are allowed!"

    TABLE_COLUMNS = Database.desc_fields(TABLE_CONF)
    TABLE_COLUMNS[["align", "edit"]] = "right", True
    TABLE_COLUMNS.loc[TABLE_COLUMNS["dtype"].str.contains("int"), "dtype"] = "int"
    TABLE_COLUMNS.loc[TABLE_COLUMNS["dtype"].str.contains("bool"), "dtype"] = "bool"
    TABLE_COLUMNS.loc[TABLE_COLUMNS["dtype"].str.contains("float"), "dtype"] = "float"
    TABLE_COLUMNS.loc[TABLE_COLUMNS["dtype"].str.contains("str|text|char"), "dtype"] = "str"

    TABLE_INDEX_NAME = TABLE_COLUMNS.index[0]

    #▄▄▄▄▄▄▄▄▄▄▄
    @classmethod
    def config_get_dict(cls, access_level: int):
        columns = cls.TABLE_COLUMNS["title"].drop(index = cls.TABLE_INDEX_NAME)
        config = Database.config_get().loc[columns.index]; config.index = columns.values
        return [{"Parameter": title, "Value": value} for title, value in config.items()]

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, username: str, access_level: int = 3):

        Log.info("Switching to \"Config\" tab...")
        user = {"username": username, "access_level": access_level}
        config = self.config_get_dict(access_level)
        
        style_cell = {"padding": "0 8px 0 8px", "minWidth": "75px"}
        style_data = {"textOverflow": "ellipsis",
              "height": "auto", "overflow": "hidden", "maxHeight": "14px"}
        style_head = {"whiteSpace": "normal", "backgroundColor": "#DDDDEE",
              "height": "auto", "overflow": "hidden", "fontWeight": "bold",
              "fontFamily": "Arial, sans-serif", "fontSize": "14px"}
        style_head_if: list = [
            {"if": {"column_id": "Value"}, "textAlign": "right"},
            {"if": {"column_id": "Parameter"}, "textAlign": "left"},
        ]
        style_data_if: list = [
            {"if": {"row_index": "even"}, "backgroundColor": "#F6F6F6"},
            {"if": {"state": "selected"}, "backgroundColor": "#CCCCFF"},
            {"if": {"column_id": "Value"}, "textAlign": "right", "fontWeight": "normal"},
            {"if": {"column_id": "Parameter"}, "textAlign": "left", "fontWeight": "bold"},
        ]

        columns: list = [
            dict(editable = False, name = "Parameter", id = "Parameter"),
            dict(editable = True, name = "Value", id = "Value"),
        ]

        super().__init__(id = self.ID_OBJ, style = {"alignItems": "center"},
            label = self.__class__.__name__.replace("Tab", ""), children = [
                dash.dcc.Store(id = self.ID_ATTR_ACC_INITIAL, data = config),
                dash.dcc.Store(id = self.ID_ATTR_USER, data = user),
                dash.dash_table.DataTable(id = self.ID_TABLE, style_cell = style_cell,
                    style_header = style_head, style_header_conditional = style_head_if,
                    style_data = style_data, style_data_conditional = style_data_if,
                    columns = columns, data = config.copy(),
                ),
                dash.html.Hr(), 
                dash.html.Div(id = self.ID_BUTTON_CONT, style = self.STYLE_BUTTONS, children = [
                    dash.html.Button(id = self.ID_BUTTON_CONFIRM, children = self.CONFIRM_LABEL, **self.BUTTON_ARGS),
                    dash.html.Div(id = self.ID_STATE_ICON, children = self.ICON_CHANGE_PENDING, style = self.STYLE_STATE_ICON),
                ]), dash.html.H6(id = self.ID_STATE_TEXT, style = self.STYLE_STATE_TEXT, children = "Hello!"),
            ]
        )

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @app.callback(CONFIRM_OUTPUT, CONFIRM_INPUTS, CONFIRM_STATES, prevent_initial_call = True)
    def confirm(n_clicks: int, prev: dict, last: dict, user: dict):
        
        if (n_clicks < 0): return
        INDEX_NAME = TabConfig.TABLE_INDEX_NAME
        
        try:
            df_prev = {item["Parameter"]: item["Value"] for item in prev}
            df_last = {item["Parameter"]: item["Value"] for item in last}
            df_prev = DataFrame.from_dict({SESSION_NAME: df_prev}, orient = "index")
            df_last = DataFrame.from_dict({SESSION_NAME: df_last}, orient = "index")
            df_prev.columns = TabConfig.TABLE_COLUMNS.index.rename("field")[1 :]
            df_last.columns = TabConfig.TABLE_COLUMNS.index.rename("field")[1 :]
            df_prev.index = df_prev.index.rename(INDEX_NAME)
            df_last.index = df_last.index.rename(INDEX_NAME)

            items = Database.detect_changes(prev = df_prev, last = df_last,
                    columns_not_null = TabConfig.TABLE_COLUMNS["not_null"])
            items_add, items_del, items_set, df_comp = items

            Log.debug("Saving account changes...")

            username = user["username"]
            now = int(time.time() * 1e6)

            changelog = Database.CHANGELOG_TEMPLATE.copy()

            if len(items_set):
                Database.config_set(*items_set)
                Log.debug("-> Changed:\n%s" % items_set)
                df_comp: DataFrame = df_comp.sort_index().reset_index()
                df_comp = df_comp.rename(columns = {INDEX_NAME: "object"})
                changelog["value_before"] = df_comp["prev"].astype(str)
                changelog["value_after"] = df_comp["last"].astype(str)
                changelog["parameter"] = df_comp.agg(axis = "columns",
                            func = TabConfig.PARAMETER_CHANGE_FORMAT)
            
            if (n_changes := changelog.shape[0]) > 0:
                changelog["timestamp"], changelog["username"] = now, username
                changelog["timestamp"] = changelog["timestamp"] + range(n_changes)
                Database.change_add(*changelog.to_dict(orient = "index").values())
                text = TabConfig.TEXT_CHANGE_SUCCESS
                icon = TabConfig.ICON_CHANGE_SUCCESS
            else:
                text = TabConfig.TEXT_CHANGE_PENDING
                icon = TabConfig.ICON_CHANGE_PENDING

        except Exception as EXC:

            text = TabConfig.TEXT_CHANGE_FAILURE
            icon = TabConfig.ICON_CHANGE_FAILURE
            text += repr(EXC); Log.exception(EXC)
            
        Log.debug("Reloading table content...")
        df_last = TabConfig.config_get_dict(user["access_level"])
        return (icon, text, df_last.copy(), df_last.copy())
    

    