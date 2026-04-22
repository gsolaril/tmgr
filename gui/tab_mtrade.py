import os, sys, dash, msgpack

sys.path.append("./")

from core.actions import *
from gui.utils import *

#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████
#█████████████████████████████████████████████████████████████████████████████████████████████████████████████  Account tab  ███
#███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████

class TabMTrade(dash.dcc.Tab):

    ID_OBJ = "tab-mtrade"
    ID_BUTTON_CONT = ID_OBJ + "-button-container"
    ID_BUTTON_CLEAR = ID_OBJ + "-button-clear"
    ID_BUTTON_SEND = ID_OBJ + "-button-send"
    ID_STATE_ICON = ID_OBJ + "-state-icon"
    ID_STATE_TEXT = ID_OBJ + "-state-text"
    ID_ATTR_USER = ID_OBJ + "-user"
    ID_ATTR_TOKEN = ID_OBJ + "-tokens"
    ID_ATTR_MTVER = ID_OBJ + "-mtvers"
    ID_ATTR_MULTS = ID_OBJ + "-mults"
    ID_TEXT = ID_OBJ + "-text-"
    ID_BOX = ID_OBJ + "-box-"
    ID_DIV = ID_OBJ + "-div-"

    MT_API_URL_4 = MT_API_URL_4.format(protocol = "http") + "{endpoint}?{args}&id="
    MT_API_URL_5 = MT_API_URL_5.format(protocol = "http") + "{endpoint}?{args}&id="

    CHOICES_ACTION = ["Buy", "Sell", "BuyLimit", "SellLimit", "BuyStop", "SellStop", "Modify", "Close"]
    ROW_ACTION = [CHOICES_ACTION[0], CHOICES_ACTION]

    TEXT_INPUTS = dict(
        action = ("Action / endpoint",          *ROW_ACTION),
        source = ("Source account (alias)",     "",     "text"),
        ticket = ("Source ticket (integer)",    "",     "number"),
        comment = ("Source comment (string)",   "",     "text"),
        symbol = ("Standard symbol (string)",   "",     "text"),
        lot = ("Lot size (float, 2 digits)",    0.0,    "number"),
        op = ("Execution price (float)",        0.0,    "number"),
        sl = ("Stop loss price (float)",        0.0,    "number"),
        tp = ("Take profit price (float)",      0.0,    "number"),
    )

    STYLE_BUTTON_MARGIN = "20px"

    STYLE_ELEM_DIV = {"display": "flex", "align-items": "center", "justify-content": "center", "flexDirection": "row"}
    STYLE_ELEM = {"fontSize": "20px", "margin": "10px", "verticalAlign": "middle", "fontFamily": "Arial, sans-serif"}
    STYLE_ELEM_TEXT, STYLE_ELEM_BOX = {**STYLE_ELEM,"justify-content": "left"}, {**STYLE_ELEM,"justify-content": "right"}
    
    STYLE_ELEM_BOX["width"] = "300px"

    STYLE_FORM = {"alignItems": "center", "justify-content": "center", "display": "flex",
                  "flexDirection": "column", "width": "60%", "padding": "10px"}

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

    SEND_OUTPUT = [dash.dependencies.Output(ID_STATE_ICON, "children"),
                   dash.dependencies.Output(ID_STATE_TEXT, "children")]
    SEND_INPUTS = [dash.dependencies.Input(ID_BUTTON_SEND, "n_clicks")]
    SEND_STATES = list()

    for key in TEXT_INPUTS.keys(): SEND_STATES.append(
        dash.dependencies.State(ID_BOX + key, "value"))
    
    SEND_STATES.append(dash.dependencies.State(ID_ATTR_USER, "data"))

    CLEAR_LABEL, SEND_LABEL = "Clear form", "Send order"

    ICON_SEND_PENDING, TEXT_SEND_PENDING = "⏳", "No changes yet."
    ICON_SEND_SUCCESS, TEXT_SEND_SUCCESS = "✅", "Order success."
    ICON_SEND_FAILURE, TEXT_SEND_FAILURE = "❌", "Order failed."
    TEXT_SEND_FAILURE_NULL = "No empty fields are allowed!"

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def accounts_get_dict(cls):

        accounts = Database.accounts_get()
        is_target = accounts["active"] & accounts["category"].eq(1)
        return accounts.loc[is_target, ["token", "mtver"]].to_dict()
    
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def mults_get_dict(cls):
        
        df: DataFrame = Database.mult_get().reset_index()
        mults = {source: dict() for source in df["source"]}
        for _, (source, target, factor) in df.iterrows():
            mults[source][target] = factor
        return mults

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, username: str, access_level: int = 3):

        Log.info("Switching to \"Manual trade\" tab...")
        user = {"username": username, "access_level": access_level}
        accs, mults = self.accounts_get_dict(), self.mults_get_dict()
        tokens, mtvers = accs.pop("token"), accs.pop("mtver")
        sources = list(mults.keys())

        form = list()
        for key, (label, default, dtype) in self.TEXT_INPUTS.items():
            id_text, id_box, id_div = self.ID_TEXT + key, self.ID_BOX + key, self.ID_DIV + key
            children = {"text": dash.html.H6(id = id_text, children = label, style = self.STYLE_STATE_TEXT)}
            if isinstance(dtype, list): children["box"] = dash.dcc.Dropdown(
                id = id_box, options = dtype, style = self.STYLE_ELEM_BOX)
            elif (key == "source"): children["box"] = dash.dcc.Dropdown(
                id = id_box, options = sources, style = self.STYLE_ELEM_BOX)
            else: children["box"] = dash.dcc.Input(id = id_box, type = dtype,
                                value = default, style = self.STYLE_ELEM_BOX)
            form.append(dash.html.Div(style = self.STYLE_ELEM_DIV,
                    children = [*children.values()], id = id_div))
        
        div = dash.html.Div(id = self.ID_DIV[: -1], children = form, style = self.STYLE_FORM)

        super().__init__(id = self.ID_OBJ, style = {"alignItems": "center"},
            label = self.__class__.__name__.replace("Tab", ""), children = [
                dash.dcc.Store(id = self.ID_ATTR_TOKEN, data = tokens),
                dash.dcc.Store(id = self.ID_ATTR_MTVER, data = mtvers),
                dash.dcc.Store(id = self.ID_ATTR_MULTS, data = mults),
                dash.dcc.Store(id = self.ID_ATTR_USER, data = user), *form, dash.html.Hr(),
                dash.html.Div(id = self.ID_BUTTON_CONT, style = self.STYLE_BUTTONS, children = [
                    dash.html.Button(id = self.ID_BUTTON_CLEAR, children = self.CLEAR_LABEL, **self.BUTTON_ARGS),
                    dash.html.Div(id = self.ID_STATE_ICON, style = self.STYLE_STATE_ICON, children = "  "),
                    dash.html.Button(id = self.ID_BUTTON_SEND, children = self.SEND_LABEL, **self.BUTTON_ARGS),
                    dash.html.Div(id = self.ID_STATE_ICON, style = self.STYLE_STATE_ICON, children = self.ICON_SEND_PENDING),
                ]), dash.html.H6(id = self.ID_STATE_TEXT, style = self.STYLE_STATE_TEXT, children = "Hello!"),
            ]
        )

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @app.callback(SEND_OUTPUT, SEND_INPUTS, SEND_STATES, prevent_initial_call = True)
    def send(n_clicks: int, action: str, source: str, ticket: str, comment: str,
                symbol: str, lot: str, op: str, sl: str, tp: str, user: dict):

        if (n_clicks < 1): return
        comment = "[manual]" + comment
        
        args = {
            "n_clicks": n_clicks, "source": source, "ticket": ticket, "comment": comment,
            "action": action, "symbol": symbol, "lot": lot, "op": op, "sl": sl, "tp": tp}
        
        Log.debug("Manual order arguments:\n%s" % args)
        
        if (ticket == 0) or (source == ""):
            icon = TabMTrade.ICON_SEND_PENDING
            text = "Message invalid. Ticket and source must be provided..."
            return icon, text

        try:
            ticket = int(ticket)
            lot = round(float(lot), 2)
            op = round(float(op), 5)
            sl = round(float(sl), 5)
            tp = round(float(tp), 5)

            is_modify = (action.lower() == "modify")
            is_close = (action.lower() == "close")
            is_send = not (is_modify or is_close)
            now = int(time.time() * 1e6)

            message_json = {"ticket": ticket, "source": source, "t_send": now, "t_sign": now}

            if is_send:
                action_enum = OrderRequest.Action.OPEN
                if (action.lower() not in ["buy", "sell"]):
                    action_enum = OrderRequest.Action.OPEN_PEND
                order_type = str.upper(action)
                order_enum = OrderRequest.TYPE_STR_TO_ENUM[order_type]
                order_bools = OrderRequest.enum_to_bools(order_enum)
                message_json.update({"symbol": symbol, "base_value": 1, "lots": lot,
                    **order_bools, "openPrice": op, "stopLoss": sl, "takeProfit": tp,
                    "comment": comment})

            elif is_modify:
                action_enum = OrderRequest.Action.MODIFY
                if (op != 0.0): action_enum = OrderRequest.Action.MODIFY_PEND
                message_json.update({"stopLoss": sl, "takeProfit": tp})
            elif is_close:
                action_enum = OrderRequest.Action.CLOSE
                message_json.update({"closePrice": op, "lots": lot})

            message_json["action"] = action_enum.value
            message_form = OrderRequest.ZMQ_FORMATS[action_enum]
            message_json = {key: message_json[key] for key in message_form}

            message_zmq = list(message_json.values())
            encoded: bytes = msgpack.packb(message_zmq)
            if (ZMQ is not None): ZMQ.send(encoded)

            icon = TabMTrade.ICON_SEND_SUCCESS
            text = f"Message for manual order sent to ZMQ (\"{ZMQ_URL}\"):\n"
            Log.success(text := text + str(message_json))
        
        except Exception as EXC:
            icon = TabMTrade.ICON_SEND_FAILURE
            text = "Message for manual order failed:\n%s" % EXC.__repr__()
            Log.exception(EXC)

        return icon, text