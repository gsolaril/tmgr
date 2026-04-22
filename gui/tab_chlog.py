import os, sys, dash
from numpy import nan
sys.path.append("./")
from gui.utils import *

#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
#███████████████████████████████████████████████████████████████████████████████████████████████████████████  Changelog tab  ███
#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████

class TabChlog(dash.dcc.Tab):

    ID_OBJ = "tab-account"
    ID_TABLE = ID_OBJ + "-table"
    ID_TABLE_CONT = ID_TABLE + "-container"
    ID_COLUMN_PREFIX = ID_OBJ + "-column"

    ID_ATTR_USER = ID_OBJ + "-user"

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
    
    DELETE_INPUTS = [dash.dependencies.Input(ID_TABLE, "active_cell")]

    PARAMETER_CHANGE_FORMAT = "{0[object]}.{0[field]}".format

    TABLE_COLUMNS = DataFrame.from_dict(
        orient = "index", columns = ["dtype", "title", "align", "edit", "not_null"],
        data = dict(
            timestamp           = (str,     "Timestamp",        "left",    False,   False),
            username            = (str,     "Username",         "left",    False,   False),
            parameter           = (str,     "Parameter",        "left",    False,   False),
            value_before        = (str,     "Value before",     "left",    False,   False),
            value_after         = (str,     "Value after",      "left",    False,   False),
        )
    )

    TABLE_INDEX_NAME = TABLE_COLUMNS.index[0]

    #▄▄▄▄▄▄▄▄▄▄▄
    @classmethod
    def changelog_get_dict(cls, access_level: int):

        changelog: DataFrame = Database.change_get(50)
        changelog = changelog.reset_index()[list(cls.TABLE_COLUMNS.index)]
        changelog = changelog.rename(columns = cls.TABLE_COLUMNS["title"])
        return changelog.to_dict(orient = "records")

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, username: str, access_level: int = 3):

        Log.info("Switching to \"Changelog\" tab...")
        user = {"username": username, "access_level": access_level}
        changelog = self.changelog_get_dict(access_level)
        
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
                "fontWeight": "bold" if (title in ["Timestamp", "Username"]) else "normal"})

        super().__init__(id = self.ID_OBJ, style = {"alignItems": "center"},
            label = self.__class__.__name__.replace("Tab", ""), children = [
                dash.dcc.Store(id = self.ID_ATTR_USER, data = user),
                dash.dash_table.DataTable(id = self.ID_TABLE, style_cell = style_cell,
                    style_data = style_data, style_header = style_head, columns = columns,
                    style_data_conditional = style_data_if, fixed_rows = {"headers": True},
                    data = changelog.copy(),
                ),
                dash.html.Hr(),
            ]
        )
