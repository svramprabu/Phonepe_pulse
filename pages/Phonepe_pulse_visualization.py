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

user_option = st.selectbox('choose the type ',['transactions','users'])
user_year = st.sidebar.slider(':red[Choose the year of data you wish to extract]',min_value=2018,max_value=2022)
user_quarter = st.sidebar.slider(':red[Enter the quarter of the year to extract]',value=1,min_value=1,max_value=4)

query = f"SELECT distinct state_name FROM aggregated_state_tx_df;"
state_names_df = pd.read_sql(query,mydb)
states_list = list(state_names_df['state_name'])
user_state = st.sidebar.selectbox('choose the state',list(state_names_df['state_name']))

sql_query="SELECT name FROM sqlite_master WHERE type='table';"
cursor.execute(sql_query)
tables=[]
for table in (cursor.fetchall()):
        tables.append(table[0])
# if(st.button("view df")):
    
#     table_option = st.selectbox("list of tables in db",tables)
#     # st.write(tables)
#     query = f"SELECT * FROM {table_option};"
#     display_df = pd.read_sql(query,mydb)
    # st.write(display_df)

if user_option == 'transactions':
    user_choice = st.selectbox('choose the type',['aggregated','top'])  #'map', removed this as it is not required
    


    if user_choice == 'aggregated':
        
        regex_pattern=r"(aggregated).*(tx)"
        matches = [item for item in tables if re.match(regex_pattern, item)]
        # user_table = st.selectbox("choose",matches)

        Q=f"SELECT name as Type_of_Transaction,count as Number_of_transactions,amount as Rs_in_Crores FROM aggregated_tx_df WHERE year = '{user_year}' and quarter = '{user_quarter}';"
        df = pd.read_sql(Q,mydb)
        # st.write(f"Query: {Q1}")
        # st.write(df)
        fig = px.pie(df, values='Number_of_transactions', names='Type_of_Transaction', title=f'Transactions aggregated based on type in quarter {user_quarter} of {user_year}',hover_data=['Rs_in_Crores'])
        fig.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig)
        


        
        Q = f"SELECT state_name,sum(count) as Number_of_transactions,sum(amount) as Rs_in_Crores FROM aggregated_state_tx_df WHERE year = '{user_year}' and quarter = '{user_quarter}' group by state_name"#and state_name = '{user_state}';"
        df = pd.read_sql(Q,mydb)
        # st.write(f"Query: {Q2}")
        # st.write(df)
        # fig = px.pie(df, values='count', names='name', title='Population of European continent',hover_data=['amount'],hover_name='state_name')
        fig = px.choropleth(
                df,
                geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                featureidkey='properties.ST_NM',
                locations='state_name',
                color='Number_of_transactions',
                hover_data='Rs_in_Crores',
                title=f'Aggregated transactions in different states in quarter {user_quarter} of {user_year}',
                color_continuous_scale='Reds'
            )
        fig.update_geos(fitbounds="locations", visible=False)
        st.plotly_chart(fig)
        
        Q = f"SELECT state_name,sum(count) as Number_of_transactions,sum(amount) as Rs_in_Crores FROM aggregated_state_tx_df WHERE year = '{user_year}' and quarter = '{user_quarter}' and state_name = '{user_state}' group by state_name;"
        df = pd.read_sql(Q,mydb)
        # st.write(f"Query: {Q2}")
        # st.write(df)
        # fig = px.pie(df, values='count', names='name', title='Population of European continent',hover_data=['amount'],hover_name='state_name')
        fig = px.choropleth(
                df,
                geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                featureidkey='properties.ST_NM',
                locations='state_name',
                color='Number_of_transactions',
                hover_data='Rs_in_Crores',
                title=f'Aggregated transactions in {user_state} in quarter {user_quarter} of {user_year}',

                color_continuous_scale='Reds'
            )

        fig.update_geos(fitbounds="locations", visible=False)
        st.plotly_chart(fig)
        # st.write("ok")
    
    elif user_choice == 'top':
        Q = f"SELECT state_name,count as Number_of_transactions,amount as Rs_in_Crores FROM top_tx_df WHERE year = '{user_year}' and quarter = '{user_quarter}' "#and state_name = '{user_state}';"
        df = pd.read_sql(Q,mydb)
        # st.write(f"Query: {Q2}")
        # st.write(df)
        # fig = px.pie(df, values='count', names='name', title='Population of European continent',hover_data=['amount'],hover_name='state_name')
        fig = px.choropleth(
                df,
                geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                featureidkey='properties.ST_NM',
                locations='state_name',
                color='Number_of_transactions',
                hover_data='Rs_in_Crores',
                title='Top 10 states with highest number of transactions in India',
                color_continuous_scale='Reds'
            )

        fig.update_geos(fitbounds="locations", visible=False)
        st.plotly_chart(fig)

        Q=f"SELECT state_name,count as Number_of_transactions,amount as Rs_in_Crores FROM top_tx_df WHERE year = '{user_year}' and quarter = '{user_quarter}' order by amount desc;"
        df = pd.read_sql(Q,mydb)
        # st.write(f"Query: {Q1}")
        # st.write(df)
        fig = px.pie(df, values='Number_of_transactions', names='state_name', title='Top 10 states with highest number of transactions in India',hover_data=['Rs_in_Crores'])
        fig.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig)

        Q = f"SELECT state_name,name as district_name,count,amount as Rs_in_Crores FROM top_district_tx_df WHERE year = '{user_year}' and quarter = '{user_quarter}' and state_name in {tuple(states_list)[:]} order by amount desc;"
        df = pd.read_sql(Q,mydb)
        # st.write(f"Query: {Q2}")
        # st.write(df)
        
        # fig = px.pie(df, values='count', names='name', title='Population of European continent',hover_data=['amount'],hover_name='state_name')
        # fig = go.Figure(data=[go.Pie(labels=df['name'], values=df['count'], hole=.3)])
        fig = px.sunburst(df, path=['state_name', 'district_name'],values='Rs_in_Crores',title='Top districts in each state with highest number of transactions')
        # fig = px.sunburst(df, names=df['district_name'], parents=df['state_name'],values=df['Rs_in_Crores'],title='Top 10 states with highest number of transactions in India')
            #   color='amount', hover_data=['amount'],
            #   color_continuous_scale='RdBu',
            #   color_continuous_midpoint=np.average(df['amount'], weights=df['count']))
        # fig = px.pie(df, values='count', names='name', title='Population of European continent',hover_data=['amount'])
        # fig.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig)
        
        fig = px.bar(df, x="state_name", y="Rs_in_Crores", color="state_name", text="district_name",title='Top districts in each state with highest number of transactions')
        st.plotly_chart(fig)

        # Q = f"SELECT state_name,name,count,amount FROM top_district_tx_df WHERE year = '{user_year}' and quarter = '{user_quarter}';" and state_name in '{user_state}' ;"
        # df = pd.read_sql(Q,mydb)

        # fig = px.sunburst(df.loc[152:], path=['state_name', 'name'], values='amount',)
        # st.plotly_chart(fig)

        Q = f"SELECT state_name,name as pincode,count,amount as Rs_in_Crores FROM top_pincode_tx_df WHERE year = '{user_year}' and quarter = '{user_quarter}' and state_name in {tuple(states_list)[:]} order by amount desc;"
        df = pd.read_sql(Q,mydb)
        # st.write(df)
        fig = px.sunburst(df, path=['state_name', 'pincode'], values='Rs_in_Crores',title='Top pincodes in each state with highest number of transactions')
        st.plotly_chart(fig)

        fig = px.bar(df, x="state_name", y="Rs_in_Crores", color="state_name", text="pincode",title='Top pincodes in each state with highest number of transactions')
        st.plotly_chart(fig)
    
    elif user_choice == 'map':
        Q=f"SELECT state_name,count as Number_of_transactions,amount as Rs_in_Crores FROM map_tx_df WHERE year = '{user_year}' and quarter = '{user_quarter}';"
        df = pd.read_sql(Q,mydb)
        # st.write(f"Query: {Q1}")
        # st.write(df)
        fig = px.pie(df, values='Number_of_transactions', names='state_name', title='Aggregated transactions in different states',hover_data=['Rs_in_Crores'])
        fig.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig)

        Q = f"SELECT state_name,sum(count) as count,sum(amount) as amount FROM map_state_tx_df WHERE year = '{user_year}' and quarter = '{user_quarter}' group by state_name"#and state_name = '{user_state}';"
        df = pd.read_sql(Q,mydb)
        # st.write(f"Query: {Q2}")
        # st.write(df)
        # fig = px.pie(df, values='count', names='name', title='Population of European continent',hover_data=['amount'],hover_name='state_name')
        fig = px.choropleth(
                df,
                geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                featureidkey='properties.ST_NM',
                locations='state_name',
                color='count',
                title='Top 10 states with highest number of transactions in India',
                hover_data='amount',
                color_continuous_scale='Reds'
            )

        fig.update_geos(fitbounds="locations", visible=False)
        st.plotly_chart(fig)

        Q = f"SELECT state_name,sum(count) as count,sum(amount) as amount FROM aggregated_state_tx_df WHERE year = '{user_year}' and quarter = '{user_quarter}' and state_name = '{user_state}' group by state_name;"
        df = pd.read_sql(Q,mydb)
        # st.write(f"Query: {Q2}")
        # st.write(df)
        # fig = px.pie(df, values='count', names='name', title='Population of European continent',hover_data=['amount'],hover_name='state_name')
        fig = px.choropleth(
                df,
                geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                featureidkey='properties.ST_NM',
                locations='state_name',
                color='count',
                hover_data='amount',
                color_continuous_scale='Reds'
            )

        fig.update_geos(fitbounds="locations", visible=False)
        st.plotly_chart(fig)
        
elif user_option == 'users':
    user_choice = st.selectbox('choose the state',['aggregated','top']) #'map', removed as it is redundant
    
    if user_choice == 'aggregated':
        Q=f"SELECT brand,count as Number_of_users,percentage FROM aggregated_user_df WHERE year = '{user_year}' and quarter = '{user_quarter}';"
        df = pd.read_sql(Q,mydb)
        # st.write(f"Query: {Q1}")
        # st.write(df)
        fig = px.pie(df, values='Number_of_users', names='brand', title='Aggregated number of users based on different brands',hover_data=['percentage'])
        fig.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig)
        

        Q = f"SELECT state_name,sum(count) as Number_of_users FROM aggregated_state_user_df WHERE year = '{user_year}' and quarter = '{user_quarter}' group by state_name"#and state_name = '{user_state}';"
        # Q = f"SELECT * FROM aggregated_state_user_df WHERE year = '{user_year}' and quarter = '{user_quarter}' group by state_name"#and state_name = '{user_state}';"
        df = pd.read_sql(Q,mydb)
        # st.write(f"Query: {Q2}")
        # st.write(df)
        # fig = px.pie(df, values='count', names='name', title='Population of European continent',hover_data=['amount'],hover_name='state_name')
        fig = px.choropleth(
                df,
                geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                featureidkey='properties.ST_NM',
                locations='state_name',
                color='Number_of_users',
                # hover_data='percentage',
                title='Aggregated number of users including all brands in each state',
                color_continuous_scale='Reds'
            )
        fig.update_geos(fitbounds="locations", visible=False)
        st.plotly_chart(fig)
        
        Q = f"SELECT state_name,sum(count) as Number_of_users FROM aggregated_state_user_df WHERE year = '{user_year}' and quarter = '{user_quarter}' and state_name = '{user_state}' group by state_name"#and state_name = '{user_state}';"
        
        # Q = f"SELECT state_name,sum(count) as Number_of_transactions,sum(amount) as Rs_in_Crores FROM aggregated_state_tx_df WHERE year = '{user_year}' and quarter = '{user_quarter}' and state_name = '{user_state}' group by state_name;"
        df = pd.read_sql(Q,mydb)
        # st.write(f"Query: {Q2}")
        # st.write(df)
        # fig = px.pie(df, values='count', names='name', title='Population of European continent',hover_data=['amount'],hover_name='state_name')
        fig = px.choropleth(
                df,
                geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                featureidkey='properties.ST_NM',
                locations='state_name',
                color='Number_of_users',
                # hover_data='Rs_in_Crores',
                title=f'Aggregated transactions of all brands in {user_state}',
                color_continuous_scale='Reds'
            )

        fig.update_geos(fitbounds="locations", visible=False)
        st.plotly_chart(fig)



        Q = f"SELECT brand,count as Number_of_users,percentage FROM aggregated_state_user_df WHERE year = '{user_year}' and quarter = '{user_quarter}' and state_name = '{user_state}' ;"
        df = pd.read_sql(Q,mydb)
        # st.write(f"Query: {Q2}")
        # st.write(df)
        fig = px.pie(df, values='Number_of_users', names='brand', title=f'Aggregated transactions of different brands in {user_state}',hover_data=['percentage'],hover_name='brand')
        fig.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig)

        Q = f"SELECT state_name,brand,count as Number_of_users,percentage FROM aggregated_state_user_df WHERE year = '{user_year}' and quarter = '{user_quarter}' ;"
        df = pd.read_sql(Q,mydb)
        # st.write(f"Query: {Q2}")
        # st.write(df)
        fig = px.sunburst(df, path=['state_name', 'brand'], values='Number_of_users',title=" Aggregated transactions of different brands in each state")
        st.plotly_chart(fig)


        query = f"SELECT distinct brand FROM aggregated_state_user_df;"
        brand_names_df = pd.read_sql(query,mydb)
        brand_list = list(brand_names_df['brand'])
        user_brand = st.selectbox('choose the brand',brand_list)


        Q = f"SELECT brand,count as Number_of_users,percentage,year FROM aggregated_state_user_df WHERE brand = '{user_brand}' and quarter = '{user_quarter}' and state_name ='{user_state}'"#in {tuple(states_list)[:]} order by amount desc;"
        df = pd.read_sql(Q,mydb)
        # st.write(df)
        # fig = px.sunburst(df, path=['state_name', 'name'], values='amount',)
        # st.plotly_chart(fig)

        fig = px.bar(df, x="year", y="Number_of_users", color="Number_of_users", text="year",title=f"{user_brand}'s growth in number of users over the years")
        st.plotly_chart(fig)
    
    elif user_choice == 'top':
        Q = f"SELECT state_name,registeredUsers as Number_of_users FROM top_user_state WHERE year = '{user_year}' and quarter = '{user_quarter}' "#and state_name = '{user_state}';"
        df = pd.read_sql(Q,mydb)
        # st.write(f"Query: {Q2}")
        # st.write(df)
        fig = px.pie(df, values='Number_of_users', names='state_name', title=f'Top 10 states with highest number of users in the {user_quarter} quarter of {user_year}',hole=0.3)
        fig.update_traces(textposition='inside', textinfo='percent+label')

        # fig = px.choropleth(
        #         df,
        #         geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
        #         featureidkey='properties.ST_NM',
        #         locations='state_name',
        #         color='registeredUsers',
        #         # hover_data='percentage',
        #         color_continuous_scale='Reds'
        #     )
        # fig.update_geos(fitbounds="locations", visible=False)
        st.plotly_chart(fig)


        Q = f"SELECT district_name,registeredUsers as Number_of_users FROM top_user_district WHERE year = '{user_year}' and quarter = '{user_quarter}' "#and state_name = '{user_state}';"
        df = pd.read_sql(Q,mydb)
        # st.write(f"Query: {Q2}")
        # st.write(df)
        fig = px.pie(df, values='Number_of_users', names='district_name', title=f'Top 10 districts with highest number of users in the {user_quarter} quarter of {user_year}',hole=0.3)
        fig.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig)

        Q = f"SELECT pincode,registeredUsers as Number_of_users FROM top_user_pincode WHERE year = '{user_year}' and quarter = '{user_quarter}' "#and state_name = '{user_state}';"
        df = pd.read_sql(Q,mydb)
        # st.write(f"Query: {Q2}")
        # st.write(df)
        fig = px.pie(df, values='Number_of_users', names='pincode', title=f'Top 10 pincodes with highest number of users in the {user_quarter} quarter of {user_year}',hole=0.3)
        fig.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig)

        Q = f"SELECT state_name,district_name,registeredUsers as Number_of_users FROM top_user_district_by_state WHERE year = '{user_year}' and quarter = '{user_quarter}' order by registeredUsers desc"#and state_name = '{user_state}';"
        df = pd.read_sql(Q,mydb)
        # st.write(f"Query: {Q2}")
        # st.write(df)
        fig = px.bar(df, x="state_name", y="Number_of_users", color="state_name", text="district_name",title=f'Top 10 district in each state with highest number of users in the {user_quarter} quarter of {user_year}',)
        # fig.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig)

        Q = f"SELECT state_name,district_name,registeredUsers as Number_of_users FROM top_user_district_by_state WHERE year = '{user_year}' and quarter = '{user_quarter}' and state_name = '{user_state}' order by registeredUsers desc"#and state_name = '{user_state}';"
        df = pd.read_sql(Q,mydb)
        # st.write(f"Query: {Q2}")
        # st.write(df)
        # fig = px.bar(df, x="state_name", y="Number_of_users", color="state_name", text="district_name",title=f'Top 10 district in each state with highest number of users in the {user_quarter} quarter of {user_year}',)
        fig = px.pie(df, values='Number_of_users', names='district_name', title=f'Top 10 districts with highest number of users in the {user_quarter} quarter of {user_year} in {user_state}',hole=0.3)
        fig.update_traces(textposition='inside', textinfo='percent+label')

        # fig.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig)

        Q = f"SELECT state_name,pincode,registeredUsers as Number_of_users FROM top_user_pincode_by_state WHERE year = '{user_year}' and quarter = '{user_quarter}' order by registeredUsers desc"#and state_name = '{user_state}';"
        df = pd.read_sql(Q,mydb)
        # st.write(f"Query: {Q2}")
        # st.write(df)
        fig = px.bar(df, x="state_name", y="Number_of_users", color="state_name", text="pincode",title=f'Top 10 pincode in each state with highest number of users in the {user_quarter} quarter of {user_year}',)
        # fig.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig)

        Q = f"SELECT state_name,pincode,registeredUsers as Number_of_users FROM top_user_pincode_by_state WHERE year = '{user_year}' and quarter = '{user_quarter}' and state_name='{user_state}' order by registeredUsers desc"#and state_name = '{user_state}';"
        # Q = f"SELECT * FROM top_user_pincode_by_state where year = '{user_year}' and quarter = '{user_quarter}' and state_name = '{user_state}'"
        df = pd.read_sql(Q,mydb)
        # st.write(f"Query: {Q2}")
        # st.write(df)
        fig = px.pie(df, values='Number_of_users', names='pincode', title=f'Top 10 pincodes with highest number of users in the {user_quarter} quarter of {user_year} in {user_state}',hole=0.3)
        fig.update_traces(textposition='inside', textinfo='percent+label')

        # fig = px.bar(df, x="pincode", y="Number_of_users", color="state_name", text="pincode",title=f'Top 10 pincodes in each state with highest number of users in the {user_quarter} quarter of {user_year}')
        # fig.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig)
    
    elif user_choice == 'map':
        Q = f"SELECT state_name,registeredUsers FROM map_user_df WHERE year = '{user_year}' and quarter = '{user_quarter}' "#and state_name = '{user_state}';"
        df = pd.read_sql(Q,mydb)
        # st.write(f"Query: {Q2}")
        # st.write(df)
        # fig = px.pie(df, values='count', names='name', title='Population of European continent',hover_data=['amount'],hover_name='state_name')
        fig = px.choropleth(
                df,
                geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                featureidkey='properties.ST_NM',
                locations='state_name',
                color='registeredUsers',
                hover_data='registeredUsers',
                color_continuous_scale='Reds'
            )

        fig.update_geos(fitbounds="locations", visible=False)
        st.plotly_chart(fig)


        Q = f"SELECT state_name,pincode,registeredUsers FROM top_user_pincode_by_state WHERE year = '{user_year}' and quarter = '{user_quarter}' order by registeredUsers desc"#and state_name = '{user_state}';"
        df = pd.read_sql(Q,mydb)
        # st.write(f"Query: {Q2}")
        # st.write(df)
        fig = px.bar(df, x="state_name", y="registeredUsers", color="state_name", text="pincode")
        # fig.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig)











