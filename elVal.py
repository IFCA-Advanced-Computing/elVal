# widget
import ipywidgets as widgets
from ipywidgets import HBox, VBox, Layout
from IPython.display import display
from IPython.display import clear_output

import csv
import datetime as dt
import mysql.connector
import numpy as np
import os
import pandas as pd
import pandas.io.sql as sql
import matplotlib.pyplot as plt
import glidertools as gt
from cmocean import cm as cmo  # we use this for colormaps
import xarray as xr

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
    return query

def heat_map(table, param_list, date_column, y_param, date_ini, date_end):
    mydb = database_connection(os.environ['DB_HOST'], region_buttons.value, os.environ['DB_USER'], os.environ['DB_PASS'])
    #result = execute_query(mydb, define_query("aquadam", ["depth", "temperature"], "date", "2018-09-12", "2018-10-12"))
    
    date_bounds = ""
    if date_ini != None and date_end != None:
        date_bounds = "WHERE date BETWEEN '%s' AND '%s'" % (date_ini, date_end)

    depths = []
    query = "SELECT DISTINCT(FLOOR(%s)) as depth FROM %s %s ORDER by depth" % (y_param, table, date_bounds)
    cursor = mydb.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    for e in result:
        depths.append(e[0])

    dates = []

    query = "SELECT YEAR(%s) as year, MONTH(%s) as month, DAY(%s) as day, COUNT(*) FROM %s %s GROUP BY YEAR(%s), MONTH(%s), DAY(%s)" % (date_column, date_column, date_column, table, date_bounds, date_column, date_column, date_column)
    cursor = mydb.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    for e in result:
        dates.append(e)
    
    data = {}
    for dp in param_list:
        param_data = []
        for p in depths:
            param_profile = []
            for e in dates:
                query_2 = "SELECT AVG(%s) as %s FROM %s WHERE YEAR(date) = %s AND MONTH(date) = %s AND DAY(date) = %s AND FLOOR(depth) = %f GROUP BY YEAR(date), MONTH(date), DAY(date), FLOOR(depth)" % (dp, dp, table, e[0], e[1], e[2], p)
                cursor_2 = mydb.cursor()
                cursor_2.execute(query_2)
                mi = cursor_2.fetchall()
                if len(mi) == 0:
                    param_profile.append(np.nan)
                else:
                    for row in mi:
                        param_profile.append(row[0])
                        break
            param_data.append(param_profile)
        da = xr.DataArray(data=param_data, dims=["depth", "time"])
        data[dp] = da
    mydb.close()
    new_dates = []
    for e in dates:
        new_dates.append("%s-%s-%s" % (e[0],e[1],e[2]))
    
    lon = [[-99.83, -99.32], [-99.79, -99.23]]
    lat = [[42.25, 42.21], [42.63, 42.59]]
    ds = xr.Dataset(
        data,
        coords={
        "longitude": (["x", "y"], lon),
        "latitude": (["x", "y"], lat),
        "time": new_dates,
        "depth": depths,
        },
    )
    
    date_chart = []
    for date in ds.time:
        da = "" + str(date.data)
        date_chart.append(dt.datetime.strptime(da, '%Y-%m-%d').date())
    x = date_chart
    y = ds.depth
    for e in ds.data_vars:
        plt.figure()
        gt.plot(x, y, ds.get(e), cmap=cmo.thermal, robust=True)
        plt.title('Fecha/Profundidad | Param: %s' % e)
        plt.xticks(rotation=90)
        plt.gcf().autofmt_xdate()
        plt.show()

def menu():
    global tables, region_buttons
    def table_on_change(v):

        global date, folders, parambox, tables
        clear_output()
        mydb = database_connection(os.environ['DB_HOST'], region_buttons.value, os.environ['DB_USER'], os.environ['DB_PASS'])
        print("SHOW COLUMNS FROM %s" % v['new'])
        myresult = execute_query(mydb, "SHOW COLUMNS FROM %s" % v['new'])
        row = [item for item in myresult['Field']]
        mydb.close()

        list_files = row
        list_files.sort()

        parambox = widgets.SelectMultiple(options=[list_files[n] for n in range(len(list_files))],
                                multiple=True,
                                description='',)



        databox = HBox([tables, parambox])
        tab.children=[region_buttons, ini_date, end_date, plot_type, databox]
        vbox1.children=[tab, button, export_csv, out]
        user_interface.children = [vbox1]
        display(user_interface)
        
    def reservoir_on_change(v):

        global date, folders, tables
        clear_output()
        
        mydb = database_connection(os.environ['DB_HOST'], region_buttons.value, os.environ['DB_USER'], os.environ['DB_PASS'])
        myresult = execute_query(mydb, "SHOW TABLES")
        row = [item for item in myresult['Tables_in_%s' % region_buttons.value]]
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
        tab.children=[region_buttons, ini_date, end_date, plot_type, databox]
        vbox1.children=[tab, button, export_csv, out]
        user_interface.children = [vbox1]
        display(user_interface)

   
    region_buttons = widgets.ToggleButtons(
        options=[('ElVal', 'elval_raw'), ('Santillana', 'santillana_raw')],
        description='Reservoirs/Lakes:',
    )
    region_buttons.observe(reservoir_on_change, names='value')
    
    ini_date = widgets.DatePicker(
       description='Initial Date',
        disabled=False
    )
    end_date = widgets.DatePicker(
        description='End Date',
        disabled=False
    )

    mydb = database_connection(os.environ['DB_HOST'], region_buttons.value, os.environ['DB_USER'], os.environ['DB_PASS'])
    myresult = execute_query(mydb, "SHOW TABLES")
    row = [item for item in myresult['Tables_in_%s' % region_buttons.value]]
    mydb.close()
    tables = widgets.SelectMultiple(
        options=row,
        multiple=False,
        # rows=10,
        description='Tables',
        disabled=False
    )

    tables.observe(table_on_change, names='value')
    
    plot_type = widgets.Dropdown(
            options=[('Param/fecha',1), ('Param/Fecha/Profundidad',2)],
            value=1,
            description='Chart type:',
            disabled=False,
        )
    
    databox = HBox([tables])

    tab = VBox(
        children=[
            region_buttons,
            ini_date, end_date, plot_type, databox])
    button = widgets.Button(
        description='Run',
    )
    export_csv = widgets.Button(
        description='Export CSV',
    )


    out = widgets.Output()
    @button.on_click
    def plot_on_click(b):
        global tables, parambox
        with out:
            clear_output()
            if tables.value[0] != None and parambox.value[0] != None:
                mydb = database_connection(os.environ['DB_HOST'], region_buttons.value, os.environ['DB_USER'], os.environ['DB_PASS'])
                result = execute_query(mydb, define_query(tables.value[0], parambox.value, "date", ini_date.value, end_date.value))

                mydb.close()
                if plot_type.value == 2:
                    heat_map(tables.value[0], parambox.value, "date", "depth", ini_date.value, end_date.value)
                else:
                    result.plot(figsize=(10,5))
            else:
                print("You need to select some param")
    
    @export_csv.on_click
    def csv_on_click(b):
        global tables, parambox
        with out:
            clear_output()
            if tables.value[0] != None and parambox.value[0] != None:
                mydb = database_connection(os.environ['DB_HOST'], region_buttons.value, os.environ['DB_USER'], os.environ['DB_PASS'])
                cursor = mydb.cursor()
                cursor.execute(define_query(tables.value[0], parambox.value, "date", ini_date.value, end_date.value))
                mydb.close()
                rows = cursor.fetchall()
                fp = open('./export_%s_%s_%s_%s-%s_.csv' % (region_buttons.value, tables.value[0], parambox.value, ini_date.value, end_date.value), 'w')
                myFile = csv.writer(fp)
                myFile.writerows(rows)
                print('Exported: ./export_%s_%s_%s_%s-%s_.csv' % (region_buttons.value, tables.value[0], parambox.value, ini_date.value, end_date.value))
                fp.close()
            else:
                print("You need to select some param")


    vbox1 = VBox(children=[tab, button, export_csv, out])
    user_interface = widgets.Tab()
    user_interface.children = [vbox1]
    user_interface.set_title(0,'Data Base')
    return user_interface