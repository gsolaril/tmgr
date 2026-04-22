import os, sys
sys.path.append("./")
from gui.utils import *
from gui.tab_account import TabAccount
from gui.tab_config import TabConfig
from gui.tab_mtrade import TabMTrade
from gui.tab_mults import TabMults
from gui.tab_chlog import TabChlog

#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
#███████████████████████████████████████████████████████████████████████████████████████████████████████████  Control panel  ███
#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████

class Panel(dash.html.Div):

    ID_OBJ = "object-panel"
    ID_TAB_ACCOUNT = "tab-account"
    ID_TAB_CONFIG = "tab-config"
    ID_TAB_MTRADE = "tab-mtrade"
    ID_TAB_MULTS = "tab-mults"
    ID_TAB_CHLOG = "tab-chlog"
    ID_TABS = "panel-tabs"
    ID_CONTENT = "panel-content"
    ID_ATTR_USER = ID_OBJ + "-user"

    TABS_OUTPUT = dash.dependencies.Output(ID_CONTENT, "children")
    TABS_INPUTS = [dash.dependencies.Input(ID_ATTR_USER, "data"),
                   dash.dependencies.Input(ID_TABS, "value")]

    OBJ_STYLE = {"display": "flex", "flexDirection": "column", "width": "95%", "justify-content": "center", "alignItems": "center"}

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, username: str, access_level: int = 3):

        args = {"last_login": int(time.time() * 1e6), "username": username}
        current_user = {"access_level": access_level, "username": username}
        Database.user_set(args)

        user_legend = f"### User: \"{username}\" - Access Level: {access_level}"

        super().__init__(style = self.OBJ_STYLE, id = self.ID_OBJ, children = [
            dash.dcc.Store(id = self.ID_ATTR_USER, data = current_user),
            dash.dcc.Tabs(id = self.ID_TABS, value = self.ID_TAB_MTRADE, children = [
                dash.dcc.Tab(label = "Account", id = self.ID_TAB_ACCOUNT, value = self.ID_TAB_ACCOUNT), 
                dash.dcc.Tab(label = "Config", id = self.ID_TAB_CONFIG, value = self.ID_TAB_CONFIG), 
                dash.dcc.Tab(label = "Mults", id = self.ID_TAB_MULTS, value = self.ID_TAB_MULTS),
                dash.dcc.Tab(label = "Manual trade", id = self.ID_TAB_MTRADE, value = self.ID_TAB_MTRADE),
                dash.dcc.Tab(label = "Changelog", id = self.ID_TAB_CHLOG, value = self.ID_TAB_CHLOG), 
            ]),
            dash.html.Hr(), dash.dcc.Markdown(id = "text-username", children = user_legend),
            dash.html.Hr(), dash.html.Div(id = self.ID_CONTENT),
        ])

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @app.callback(TABS_OUTPUT, TABS_INPUTS)
    def render_content(user: dict, selected_tab: str):

        verbose = "Selected tab: " + selected_tab
        verbose += "\nUser: {username}\nAccess level: {access_level}"
        verbose = verbose.format(selected_tab = selected_tab, **user)
        if (selected_tab == Panel.ID_TAB_ACCOUNT): return TabAccount(**user)
        elif (selected_tab == Panel.ID_TAB_CONFIG): return TabConfig(**user)
        elif (selected_tab == Panel.ID_TAB_MTRADE): return TabMTrade(**user)
        elif (selected_tab == Panel.ID_TAB_MULTS): return TabMults(**user)
        elif (selected_tab == Panel.ID_TAB_CHLOG): return TabChlog(**user)