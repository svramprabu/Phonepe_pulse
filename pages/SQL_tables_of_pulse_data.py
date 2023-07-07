import os
import re
import streamlit as st
import geopandas as gpd
import pandas as pd
import plotly.express as px
import mysql.connector
from mysql.connector import Error
import plotly.graph_objects as go
import numpy as np
import sqlite3


mydb = sqlite3.connect('phonepe_pulse.db')
cursor = mydb.cursor()


sql_query="SELECT name FROM sqlite_master WHERE type='table';"
cursor.execute(sql_query)
tables=[]
for table in (cursor.fetchall()):
        tables.append(table[0])
    
table_option = st.selectbox("list of tables in db",tables)
# st.write(tables)
query = f"SELECT * FROM {table_option};"
display_df = pd.read_sql(query,mydb)
st.write(display_df)