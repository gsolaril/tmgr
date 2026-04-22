import os, sys
sys.path.append("./")
from gui.utils import *
from gui.panel import Panel

#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
#████████████████████████████████████████████████████████████████████████████████████████████████████████████  Login screen  ███
#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████

class Login(dash.html.Div):

    ID_OBJ = "object-login"
    ID_LOGIN = "title-login"
    ID_BUTTON = "button-login"
    ID_INPUT_USER = "input-username"
    ID_INPUT_PASS = "input-password"
    ID_CONTENT_LOGIN = "content-login"
    ID_ATTR_USERS = ID_OBJ + "-users"

    AUTH_FAILED = dash.html.Div("Login Failed")
    AUTH_OUTPUT = dash.dependencies.Output("main", "children")
    AUTH_INPUTS = [dash.dependencies.Input(ID_ATTR_USERS, "data"),
                   dash.dependencies.Input(ID_BUTTON, "n_clicks")]
    AUTH_STATES = [dash.dependencies.State(ID_INPUT_USER, "value"),
                   dash.dependencies.State(ID_INPUT_PASS, "value")]

    VERBOSE_WRONG_USER = "User \"{username}\" does not exist!"
    VERBOSE_WRONG_PASS = "Wrong password for \"{username}\"!"

    STYLE_FONT = {"fontFamily": "Arial, sans-serif"}
    STYLE_OBJ = {"display": "flex", "flexDirection": "column", "width": "95%", "alignItems": "center"}
    STYLE_ALL = {"verticalAlign": "middle", "justify-content": "center", "width": "33%", "padding": "10px"}
    STYLE_TITLE = {**STYLE_ALL, **STYLE_FONT, "font-size": "32px", "font-weight": "bold", "textAlign": "center"}
    STYLE_TXTBOX = {**STYLE_ALL, **STYLE_FONT, "font-size": "18px", "fontFamily": "Arial, sans-serif"}
    STYLE_BUTTON = {**STYLE_ALL, **STYLE_FONT, "font-size": "24px", "font-weight": "bold"}    

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self):

        users = Database.user_get().to_dict("index")
        Log.info(f"Retrieved {len(users)} users from \"{TABLE_AUTH}\"...")

        super().__init__(style = self.STYLE_OBJ, id = self.ID_OBJ,
            children = [
                dash.dcc.Store(id = self.ID_ATTR_USERS, data = users),
                dash.html.Div(id = self.ID_LOGIN, style = self.STYLE_TITLE,
                    children = SESSION_NAME),
                dash.html.Br(),
                dash.dcc.Input(id = self.ID_INPUT_USER, placeholder = "Username",
                                type = "text", style = self.STYLE_TXTBOX),
                dash.html.Br(),
                dash.dcc.Input(id = self.ID_INPUT_PASS, placeholder = "Password",
                            type = "password", style = self.STYLE_TXTBOX),
                dash.html.Br(),
                dash.html.Button(id = self.ID_BUTTON, children = "Login",
                            n_clicks = 0, style = self.STYLE_BUTTON),
                dash.html.Div(id = self.ID_CONTENT_LOGIN),
        ])
        
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @app.callback(AUTH_OUTPUT, AUTH_INPUTS, AUTH_STATES)
    def auth(users: dict, n_clicks: int, username: str, password: str):
        users: DataFrame = DataFrame.from_dict(users, orient = "index")

        if (n_clicks < 1): return Login()
        
        if (username not in users.index):

            Log.error(error := Login.VERBOSE_WRONG_USER.format(username = username))
            DESC_AUTH_FAILED = dash.html.Div(error, style = {"color": "red"})
            return dash.html.Div([Login.AUTH_FAILED, DESC_AUTH_FAILED])

        password_target, access_level = users.loc[username]

        if (password_target != password):
            
            error = Login.VERBOSE_WRONG_PASS.format(username = username)
            Log.error(error + f" ({password} != {password_target})")
            DESC_AUTH_FAILED = dash.html.Div(error, style = {"color": "red"})
            return dash.html.Div([Login.AUTH_FAILED, DESC_AUTH_FAILED])
        
        Log.success(f"\"{username} / {password}\", login successful...")
        
        return Panel(username, access_level)
