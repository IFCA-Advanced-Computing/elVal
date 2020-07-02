# widget
import ipywidgets as widgets
from ipywidgets import HBox, VBox, Layout
from IPython.display import display
from IPython.display import clear_output
import os

import mysql.connector
import pandas as pd
import pandas.io.sql as sql
import matplotlib.pyplot as plt

def database_connection(host, database, user, password):
    mydb = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )
    return mydb

def execute_query(mydb, query):
    if "date" in query:
        result = sql.read_sql_query(query, mydb, index_col='date')
    else:
        result = sql.read_sql_query(query, mydb)

    return result

def define_query(table, param_list, date_column, date_ini, date_end):
    params = ''
    if len(param_list) == 1:
        params = param_list[0]
    else:
        for e in param_list:
            params = params + e + ','
        params = params[0:-1]
    if date_ini == None or date_end == None:
        query = "SELECT %s, %s FROM %s"% (date_column, params, table)
    else:
        query = "SELECT %s, %s FROM %s WHERE %s BETWEEN '%s' AND '%s'"% (date_column, params, table, date_column, date_ini, date_end)
    print(query)
    return query

def menu():
    
    def table_on_change(v):

        global date, folders, parambox
        clear_output()

        mydb = database_connection(os.environ['DB_HOST'], os.environ['DB_NAME'], os.environ['DB_USER'], os.environ['DB_PASS'])
        myresult = execute_query(mydb, "SHOW COLUMNS FROM %s" % v['new'])
        row = [item for item in myresult['Field']]
        mydb.close()

        list_files = row
        list_files.sort()

        parambox = widgets.SelectMultiple(options=[list_files[n] for n in range(len(list_files))],
                                multiple=True,
                                description='',)



        databox = HBox([tables, parambox])
        tab.children=[region_buttons, ini_date, end_date, databox]
        vbox1.children=[tab, button, out]
        user_interface.children = [vbox1]
        display(user_interface)


    region_buttons = widgets.ToggleButtons(
        options=['ElVal'],
        description='Reservoirs/Lakes:',
    )
    ini_date = widgets.DatePicker(
       description='Initial Date',
        disabled=False
    )
    end_date = widgets.DatePicker(
        description='End Date',
        disabled=False
    )
    mydb = database_connection(os.environ['DB_HOST'], os.environ['DB_NAME'], os.environ['DB_USER'], os.environ['DB_PASS'])
    myresult = execute_query(mydb, "SHOW TABLES")
    row = [item for item in myresult['Tables_in_elval_raw']]
    mydb.close()
    tables = widgets.SelectMultiple(
        options=row,
        multiple=False,
        # rows=10,
        description='Tables',
        disabled=False
    )

    tables.observe(table_on_change, names='value')
    databox = HBox([tables])

    tab = VBox(
        children=[
            region_buttons,
            ini_date, end_date, databox])
    button = widgets.Button(
        description='Run',
    )


    out = widgets.Output()
    @button.on_click
    def plot_on_click(b):
        with out:
            clear_output()
            if tables.value[0] != None and parambox.value[0] != None:
                mydb = database_connection(os.environ['DB_HOST'], os.environ['DB_NAME'], os.environ['DB_USER'], os.environ['DB_PASS'])
                result = execute_query(mydb, define_query(tables.value[0], parambox.value, "date", ini_date.value, end_date.value))

                mydb.close()
                result.plot(figsize=(10,5))
            else:
                print("You need to select some param")


    vbox1 = VBox(children=[tab, button, out])
    user_interface = widgets.Tab()
    user_interface.children = [vbox1]
    user_interface.set_title(0,'Data Base')
    return user_interface
