import sys, flet as ft
from pandas import read_sql_table
sys.path.append("./")

from core.utils import *

df = read_sql_table("tc_multipliers", con = DB_URL)
df = df.set_index(["source", "target"])["factor"]
df = df.sort_index().unstack("target")
df_dict = df.to_dict(orient = "index")

class CellModifier:

    def __init__(self, source: str, target: str, value: float = 1.0):
        self.source, self.target, self.value = source, target, value

    def change(self, value: float):
        print(f"\"{self.source}->{self.target}\" mult changed from {self.value} to {value}")
        self.value = value

    def on_change(self, event: ft.ControlEvent):
        self.change(float(event.control.value))
        

def main(page: ft.Page):
    page.title = "Flet counter example"
    page.vertical_alignment = ft.MainAxisAlignment.CENTER

    txt_number = ft.TextField(value="0", text_align=ft.TextAlign.RIGHT, width=100)

    # Define the initial data for the table
    initial_data = df_dict

    page.add(
        ft.GridView(controls = [
            ft.DataTable(
                columns = [ft.DataColumn(ft.Text("source/target")),
                    *[ft.DataColumn(ft.Text(target)) for target in df.columns]],
                rows = [
                ft.DataRow(cells = [ft.DataCell(ft.Text(source)),
                        *[
                            ft.DataCell(ft.TextField(
                                value = value, keyboard_type = ft.KeyboardType.NUMBER,
                                on_submit = CellModifier(source, target, value).on_change
                            
                            ),) for target, value in row.items()
                        ],
                    ]) for source, row in initial_data.items()
                ],
                #alignment=ft.MainAxisAlignment.CENTER,
            )
        ],),
        
    )

ft.app(target=main, view=ft.AppView.WEB_BROWSER)#, host = "http://127.0.0.1/", port = 50000)
