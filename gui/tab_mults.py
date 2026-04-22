import os, sys, dash
sys.path.append("./")
from gui.utils import *
from pandas import concat

#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
#█████████████████████████████████████████████████████████████████████████████████████████████████████████████  Account tab  ███
#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████

class TabMults(dash.dcc.Tab):

    ID_OBJ = "tab-mults"
    ID_TABLE = ID_OBJ + "-table"
    ID_TABLE_CONT = ID_TABLE + "-container"
    ID_BUTTON_CONT = ID_OBJ + "-button-container"
    ID_BUTTON_ADD_ROW = ID_OBJ + "-button-add_row"
    ID_BUTTON_CONFIRM = ID_OBJ + "-button-confirm"
    ID_COLUMN_PREFIX = ID_OBJ + "-column"
    ID_STATE_ICON = ID_OBJ + "-state-icon"
    ID_STATE_TEXT = ID_OBJ + "-state-text"

    ID_ATTR_USER = ID_OBJ + "-user"
    ID_ATTR_MULT_INITIAL = ID_OBJ + "-mults"
    ID_ATTR_MULT_ACC_MAP = ID_OBJ + "-acc-map"

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
                      dash.dependencies.Output(ID_ATTR_MULT_INITIAL, "data"),
                      dash.dependencies.Output(ID_TABLE, "data")]
    CONFIRM_INPUTS = [dash.dependencies.Input(ID_BUTTON_CONFIRM, "n_clicks")]
    CONFIRM_STATES = [dash.dependencies.State(ID_ATTR_MULT_INITIAL, "data"),
                      dash.dependencies.State(ID_TABLE, "data"),
                      dash.dependencies.State(ID_ATTR_USER, "data"),
                      dash.dependencies.State(ID_ATTR_MULT_ACC_MAP, "data")]

    CONFIRM_LABEL = "Apply changes"

    PARAMETER_CHANGE_FORMAT = "factor({0[source]}, {0[target]})".format

    ICON_CHANGE_PENDING, TEXT_CHANGE_PENDING = "⏳", "No changes yet."
    ICON_CHANGE_SUCCESS, TEXT_CHANGE_SUCCESS = "✅", "Changes applied"
    ICON_CHANGE_FAILURE, TEXT_CHANGE_FAILURE = "❌", "Changes failed. "
    ICON_DELETE, TEXT_CHANGE_FAILURE_NULL = "🗑️", "Forbidden nulls -> "

    TABLE_INDEX_NAME = "Target\\Source"

    ACCOUNT_FORMAT_INDEX = "{0[server]} :: {0[id]} (\"{0[owner]}\")".format
    ACCOUNT_FORMAT_COLUMNS = "{0[server]}\n{0[id]} (\"{0[owner]}\")".format

    #▄▄▄▄▄▄▄▄▄▄▄
    @classmethod
    def mults_get_dict(cls, access_level: int):
        mults: DataFrame = Database.mult_get()
        accounts: DataFrame = Database.accounts_get()
        categories = accounts.loc[accounts["active"], "category"]
        mults = Database.mult_grid_render(mults, categories)
        sources, targets = [*mults.columns], [*mults.index]
        sources = accounts.loc[sources].agg(cls.ACCOUNT_FORMAT_COLUMNS, axis = "columns")
        targets = accounts.loc[targets].agg(cls.ACCOUNT_FORMAT_INDEX, axis = "columns")
        targets = mults.index = targets.rename(cls.TABLE_INDEX_NAME)
        sources = mults.columns = sources.rename("n/a")
        return (sources.rename("source"), targets.rename("target"),
                [*mults.reset_index().to_dict("index").values()] )

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, username: str, access_level: int = 3):

        Log.info("Switching to \"Mults\" tab...")
        user = {"username": username, "access_level": access_level}
        sources, targets, mults = self.mults_get_dict(access_level)
        account_map = {
            "source": sources.reset_index().set_index("source")["alias"].to_dict(),
            "target": targets.reset_index().set_index("target")["alias"].to_dict()
        }

        style_data_if = list()
        style_cell = {"minWidth": "200px"}
        style_data = {"padding": "0 8px 0 8px", "textOverflow": "ellipsis",
              "height": "auto", "overflow": "hidden", "maxHeight": "14px"}
        style_head = {"whiteSpace": "normal", "backgroundColor": "#DDDDEE",
              "height": "auto", "overflow": "hidden", "fontWeight": "bold",
              "fontFamily": "Arial, sans-serif", "fontSize": "14px", "textAlign": "center"}
        style_data_if.append({"if": {"row_index": "even"}, "backgroundColor": "#F6F6F6"})
        style_data_if.append({"if": {"state": "selected"}, "backgroundColor": "#CCCCFF"})
        style_data_if.append({"if": {"state": "selected", "column_id": "Delete"},
            "backgroundColor": "#FFCCCC"})
        style_data_if.append({"if": {"column_id": self.TABLE_INDEX_NAME},
                              "textAlign": "left", "fontWeight": "bold"})
        
        columns = [{"name": self.TABLE_INDEX_NAME,
            "editable": False, "id": self.TABLE_INDEX_NAME}]
        
        for source in sources.values: columns.append({
            "name": source, "id": source, "editable": True})

        super().__init__(id = self.ID_OBJ, style = {"alignItems": "center"},
            label = self.__class__.__name__.replace("Tab", ""), children = [
                dash.dcc.Store(id = self.ID_ATTR_MULT_ACC_MAP, data = account_map),
                dash.dcc.Store(id = self.ID_ATTR_MULT_INITIAL, data = mults),
                dash.dcc.Store(id = self.ID_ATTR_USER, data = user),
                dash.dash_table.DataTable(id = self.ID_TABLE, style_cell = style_cell,
                    style_data = style_data, style_header = style_head, data = mults.copy(),
                    style_data_conditional = style_data_if, fixed_rows = {"headers": True},
                    columns = columns,
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
    def confirm(n_clicks: int, prev: dict, last: dict, user: dict, account_map: dict):

        if (n_clicks < 0): return
        INDEX_NAME = TabMults.TABLE_INDEX_NAME

        try:
            source_map, target_map = Series(account_map["source"]), Series(account_map["target"])
            df_prev = DataFrame.from_records(prev).set_index(TabMults.TABLE_INDEX_NAME).sort_index()
            df_last = DataFrame.from_records(last).set_index(TabMults.TABLE_INDEX_NAME).sort_index()
            df_prev.columns = Series(df_prev.columns).rename("field").map(source_map)
            df_last.columns = Series(df_last.columns).rename("field").map(source_map)
            df_prev.index = Series(df_prev.index).rename("alias").map(target_map)
            df_last.index = Series(df_last.index).rename("alias").map(target_map)

            ######################################################## Future function starts here
                
            items = Database.detect_changes(prev = df_prev, last = df_last)
            items_add, items_del, items_set, df_comp = items

            Log.debug("Saving account changes...")

            username = user["username"]
            now = int(time.time() * 1e6)
            df_comp = df_comp.rename_axis(["target", "source"])
            changelog = Database.CHANGELOG_TEMPLATE.copy()

            items_set = list()
            for (target, source), factor in df_comp["last"].items():
                items_set.append({"factor": factor, "source": source,
                    "target": target})

            if len(items_set):
                Database.mult_set(*items_set)
                Log.debug("-> Changed:\n%s" % items_set)
                df_comp: DataFrame = df_comp.sort_index().reset_index()
                df_comp = df_comp.rename(columns = {INDEX_NAME: "object"})
                changelog["value_before"] = df_comp["prev"].astype(str)
                changelog["value_after"] = df_comp["last"].astype(str)
                changelog["parameter"] = df_comp.agg(axis = "columns",
                            func = TabMults.PARAMETER_CHANGE_FORMAT)
                

            if (n_changes := changelog.shape[0]) > 0:
                changelog["timestamp"], changelog["username"] = now, username
                changelog["timestamp"] = changelog["timestamp"] + range(n_changes)
                Database.change_add(*changelog.to_dict(orient = "index").values())
                text = TabMults.TEXT_CHANGE_SUCCESS
                icon = TabMults.ICON_CHANGE_SUCCESS
            else:
                text = TabMults.TEXT_CHANGE_PENDING
                icon = TabMults.ICON_CHANGE_PENDING

        except Exception as EXC:

            text = TabMults.TEXT_CHANGE_FAILURE
            icon = TabMults.ICON_CHANGE_FAILURE
            text += repr(EXC); Log.exception(EXC)
            
        Log.debug("Reloading table content...")
        df_last = TabMults.mults_get_dict(user["access_level"])
        return (icon, text, df_last[-1].copy(), df_last[-1].copy())
    