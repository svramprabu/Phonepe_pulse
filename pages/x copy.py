import os
import streamlit as st
import geopandas as gpd
import pandas as pd
import plotly.express as px
import mysql.connector
from mysql.connector import Error
import plotly.graph_objects as go
import numpy as np
import sqlite3

# mydb = mysql.connector.connect(
#                                             host="aws.connect.psdb.cloud",
#                                             user="l7nhgfdaho498lxfjriu",
#                                             password="pscale_pw_MfDBUnlbjnrz22gSpBeBBIr3ilFJqG3RVw03IalBXmx",
#                                             database="yt_details")
mydb = sqlite3.connect('phonepe_pulse.db')
cursor = mydb.cursor()

database="phonepe_pulse.db"

# mydb = mysql.connector.connect(host='localhost',
#                                 # database='sql12618369',
#                                 user='root',
#                                 password='12345',
#                                 port=3306)




def next():
    user_option = st.selectbox('choose the type ',['transactions','users'])
    user_year = st.sidebar.slider(':red[Choose the year of data you wish to extract]',min_value=2018,max_value=2022)
    user_quarter = st.sidebar.slider(':red[Enter the quarter of the year to extract]',value=1,min_value=1,max_value=4)
    
    query = f"SELECT distinct state_name FROM {database}.aggregated_state_tx_df;"
    state_names_df = pd.read_sql(query,mydb)
    states_list = list(state_names_df['state_name'])
    user_state = st.sidebar.selectbox('choose the state',list(state_names_df['state_name']))

    if user_option == 'transactions':
        user_choice = st.selectbox('choose the state',['aggregated','map','top'])
        if user_choice == 'aggregated':
            Q=f"SELECT name,count,amount FROM {database}.aggregated_tx_df WHERE year = '{user_year}' and quarter = '{user_quarter}';"
            df = pd.read_sql(Q,mydb)
            # st.write(f"Query: {Q1}")
            # st.write(df)
            fig = px.pie(df, values='count', names='name', title='Population of European continent',hover_data=['amount'])
            fig.update_traces(textposition='inside', textinfo='percent+label')

            st.plotly_chart(fig)
            
            Q = f"SELECT state_name,sum(count) as count,sum(amount) as amount FROM {database}.aggregated_state_tx_df WHERE year = '{user_year}' and quarter = '{user_quarter}' group by state_name"#and state_name = '{user_state}';"
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
            
            Q = f"SELECT state_name,sum(count) as count,sum(amount) as amount FROM {database}.aggregated_state_tx_df WHERE year = '{user_year}' and quarter = '{user_quarter}' and state_name = '{user_state}' group by state_name;"
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
            st.write("ok")
        
        elif user_choice == 'top':
            Q = f"SELECT state_name,count,amount FROM {database}.top_tx_df WHERE year = '{user_year}' and quarter = '{user_quarter}' "#and state_name = '{user_state}';"
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

            Q=f"SELECT state_name,count,amount FROM {database}.top_tx_df WHERE year = '{user_year}' and quarter = '{user_quarter}' order by amount desc;"
            df = pd.read_sql(Q,mydb)
            # st.write(f"Query: {Q1}")
            # st.write(df)
            fig = px.pie(df, values='count', names='state_name', title='Population of European continent',hover_data=['amount'])
            fig.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig)

            Q = f"SELECT state_name,name,count,amount FROM {database}.top_district_tx_df WHERE year = '{user_year}' and quarter = '{user_quarter}' and state_name in {tuple(states_list)[:]} order by amount desc;"
            df = pd.read_sql(Q,mydb)
            # st.write(f"Query: {Q2}")
            # st.write(df)
            
            # fig = px.pie(df, values='count', names='name', title='Population of European continent',hover_data=['amount'],hover_name='state_name')
            # fig = go.Figure(data=[go.Pie(labels=df['name'], values=df['count'], hole=.3)])
            fig = px.sunburst(df, path=['state_name', 'name'], values='amount',)
                #   color='amount', hover_data=['amount'],
                #   color_continuous_scale='RdBu',
                #   color_continuous_midpoint=np.average(df['amount'], weights=df['count']))
            # fig = px.pie(df, values='count', names='name', title='Population of European continent',hover_data=['amount'])
            # fig.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig)
            
            fig = px.bar(df, x="state_name", y="amount", color="state_name", text="name")
            st.plotly_chart(fig)

            # Q = f"SELECT state_name,name,count,amount FROM {database}.top_district_tx_df WHERE year = '{user_year}' and quarter = '{user_quarter}';" and state_name in '{user_state}' ;"
            # df = pd.read_sql(Q,mydb)

            # fig = px.sunburst(df.loc[152:], path=['state_name', 'name'], values='amount',)
            # st.plotly_chart(fig)

            Q = f"SELECT state_name,name,count,amount FROM {database}.top_pincode_tx_df WHERE year = '{user_year}' and quarter = '{user_quarter}' and state_name in {tuple(states_list)[:]} order by amount desc;"
            df = pd.read_sql(Q,mydb)
            # st.write(df)
            fig = px.sunburst(df, path=['state_name', 'name'], values='amount',)
            st.plotly_chart(fig)

            fig = px.bar(df, x="state_name", y="amount", color="state_name", text="name")
            st.plotly_chart(fig)
        
        elif user_choice == 'map':
            Q=f"SELECT state_name,count,amount FROM {database}.map_tx_df WHERE year = '{user_year}' and quarter = '{user_quarter}';"
            df = pd.read_sql(Q,mydb)
            # st.write(f"Query: {Q1}")
            # st.write(df)
            fig = px.pie(df, values='count', names='state_name', title='Population of European continent',hover_data=['amount'])
            fig.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig)

            Q = f"SELECT state_name,sum(count) as count,sum(amount) as amount FROM {database}.map_state_tx_df WHERE year = '{user_year}' and quarter = '{user_quarter}' group by state_name"#and state_name = '{user_state}';"
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

            Q = f"SELECT state_name,sum(count) as count,sum(amount) as amount FROM {database}.aggregated_state_tx_df WHERE year = '{user_year}' and quarter = '{user_quarter}' and state_name = '{user_state}' group by state_name;"
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
        user_choice = st.selectbox('choose the state',['aggregated','map','top'])
        if user_choice == 'aggregated':
            Q=f"SELECT brand,count,percentage FROM {database}.aggregated_user_df WHERE year = '{user_year}' and quarter = '{user_quarter}';"
            df = pd.read_sql(Q,mydb)
            # st.write(f"Query: {Q1}")
            # st.write(df)
            fig = px.pie(df, values='count', names='brand', title='Population of European continent',hover_data=['percentage'])
            fig.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig)
            

            Q = f"SELECT state_name,sum(count) as count FROM {database}.aggregated_state_user_df WHERE year = '{user_year}' and quarter = '{user_quarter}' group by state_name"#and state_name = '{user_state}';"
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
                    # hover_data='percentage',
                    color_continuous_scale='Reds'
                )
            fig.update_geos(fitbounds="locations", visible=False)
            st.plotly_chart(fig)
            

            Q = f"SELECT brand,count,percentage FROM {database}.aggregated_state_user_df WHERE year = '{user_year}' and quarter = '{user_quarter}' and state_name = '{user_state}' ;"
            df = pd.read_sql(Q,mydb)
            # st.write(f"Query: {Q2}")
            # st.write(df)
            fig = px.pie(df, values='count', names='brand', title='Population of European continent',hover_data=['percentage'],hover_name='brand')
            fig.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig)

            Q = f"SELECT state_name,brand,count,percentage FROM {database}.aggregated_state_user_df WHERE year = '{user_year}' and quarter = '{user_quarter}' ;"
            df = pd.read_sql(Q,mydb)
            # st.write(f"Query: {Q2}")
            # st.write(df)
            fig = px.sunburst(df, path=['state_name', 'brand'], values='count',title=" Default: various text sizes, positions and angles")
            st.plotly_chart(fig)


            query = f"SELECT distinct brand FROM {database}.aggregated_state_user_df;"
            brand_names_df = pd.read_sql(query,mydb)
            brand_list = list(brand_names_df['brand'])
            user_brand = st.selectbox('choose the state',brand_list)


            Q = f"SELECT brand,count,percentage,year FROM {database}.aggregated_state_user_df WHERE brand = '{user_brand}' and quarter = '{user_quarter}' and state_name ='{user_state}'"#in {tuple(states_list)[:]} order by amount desc;"
            df = pd.read_sql(Q,mydb)
            # st.write(df)
            # fig = px.sunburst(df, path=['state_name', 'name'], values='amount',)
            # st.plotly_chart(fig)

            fig = px.bar(df, x="year", y="count", color="count", text="year",title=f"{user_brand} Default: various text sizes, positions and angles")
            st.plotly_chart(fig)
        elif user_choice == 'top':
            Q = f"SELECT state_name,registeredUsers FROM {database}.top_user_state WHERE year = '{user_year}' and quarter = '{user_quarter}' "#and state_name = '{user_state}';"
            df = pd.read_sql(Q,mydb)
            # st.write(f"Query: {Q2}")
            # st.write(df)
            fig = px.pie(df, values='registeredUsers', names='state_name', title='Population of European continent',hole=0.3)
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


            Q = f"SELECT district_name,registeredUsers FROM {database}.top_user_district WHERE year = '{user_year}' and quarter = '{user_quarter}' "#and state_name = '{user_state}';"
            df = pd.read_sql(Q,mydb)
            # st.write(f"Query: {Q2}")
            # st.write(df)
            fig = px.pie(df, values='registeredUsers', names='district_name', title='Population of European continent',hole=0.3)
            fig.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig)

            Q = f"SELECT pincode,registeredUsers FROM {database}.top_user_pincode WHERE year = '{user_year}' and quarter = '{user_quarter}' "#and state_name = '{user_state}';"
            df = pd.read_sql(Q,mydb)
            # st.write(f"Query: {Q2}")
            # st.write(df)
            fig = px.pie(df, values='registeredUsers', names='pincode', title='Population of European continent',hole=0.3)
            fig.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig)
    
            Q = f"SELECT state_name,district_name,registeredUsers FROM {database}.top_user_district_by_state WHERE year = '{user_year}' and quarter = '{user_quarter}' order by registeredUsers desc"#and state_name = '{user_state}';"
            df = pd.read_sql(Q,mydb)
            # st.write(f"Query: {Q2}")
            # st.write(df)
            fig = px.bar(df, x="state_name", y="registeredUsers", color="state_name", text="district_name")
            # fig.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig)

            Q = f"SELECT state_name,pincode,registeredUsers FROM {database}.top_user_pincode_by_state WHERE year = '{user_year}' and quarter = '{user_quarter}' order by registeredUsers desc"#and state_name = '{user_state}';"
            df = pd.read_sql(Q,mydb)
            # st.write(f"Query: {Q2}")
            # st.write(df)
            fig = px.bar(df, x="state_name", y="registeredUsers", color="state_name", text="pincode")
            # fig.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig)
        elif user_choice == 'map':
            Q = f"SELECT state_name,registeredUsers FROM {database}.map_user_df WHERE year = '{user_year}' and quarter = '{user_quarter}' "#and state_name = '{user_state}';"
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


            Q = f"SELECT state_name,pincode,registeredUsers FROM {database}.top_user_pincode_by_state WHERE year = '{user_year}' and quarter = '{user_quarter}' order by registeredUsers desc"#and state_name = '{user_state}';"
            df = pd.read_sql(Q,mydb)
            # st.write(f"Query: {Q2}")
            # st.write(df)
            fig = px.bar(df, x="state_name", y="registeredUsers", color="state_name", text="pincode")
            # fig.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig)


    

    






