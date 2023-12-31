import streamlit as st
import re
import sqlite3
from types import NoneType
import os
import json
import pandas as pd
import mysql.connector
from mysql.connector import Error


import subprocess

def clone_repository(url, destination):
    # Execute the Git command to clone the repository
    subprocess.run(['git', 'clone', url, destination])


data_path = r"C:\Users\SVR\Documents\GitHub\Phonepe_pulse\pulse"
#to find the list of state names as mentioned below
# dir_path = r'C:\\Users\\SVR\\Python vs code\\Guvi_Projects\\phonepe pulse\\pulse\\data\\aggregated\\user\\country\\india\\state\\'

states_list = [
  "andaman-&-nicobar-islands",
  "andhra-pradesh",
  "arunachal-pradesh",
  "assam",
  "bihar",
  "chandigarh",
  "chhattisgarh",
  "dadra-&-nagar-haveli-&-daman-&-diu",
  "delhi",
  "goa",
  "gujarat",
  "haryana",
  "himachal-pradesh",
  "jammu-&-kashmir",
  "jharkhand",
  "karnataka",
  "kerala",
  "ladakh",
  "lakshadweep",
  "madhya-pradesh",
  "maharashtra",
  "manipur",
  "meghalaya",
  "mizoram",
  "nagaland",
  "odisha",
  "puducherry",
  "punjab",
  "rajasthan",
  "sikkim",
  "tamil-nadu",
  "telangana",
  "tripura",
  "uttar-pradesh",
  "uttarakhand",
  "west-bengal"
]

def to_create_aggregated_transaction_dataframe():
    aggregated_tx_df = pd.DataFrame()
    for year in range(2018,2023):
        for i in range(1,5):
            x = open(rf"{data_path}\data\aggregated\transaction\country\india\{year}\{i}.json")
            data = json.load(x)
            
            for each_type in data['data']['transactionData']:
                new_row={
                    'name': each_type['name'],
                    # 'type': each_type['paymentInstruments'][0]['type'],
                    'count': each_type['paymentInstruments'][0]['count'],
                    'amount': each_type['paymentInstruments'][0]['amount'],
                    'year':str(year),
                    'quarter':i,
                    # 'country':'India'

                }
                
                aggregated_tx_df = pd.concat([aggregated_tx_df,pd.Series(new_row).to_frame().T], ignore_index=True)

    cursor.execute("drop table if exists aggregated_tx_df")
    cursor.execute(f"create table if not exists aggregated_tx_df({list(aggregated_tx_df.columns)[0]} TEXT,{list(aggregated_tx_df.columns)[1]} INTEGER ,{list(aggregated_tx_df.columns)[2]} INTEGER,{list(aggregated_tx_df.columns)[3]} TEXT,{list(aggregated_tx_df.columns)[4]} INTEGER)")
    for each_row in range(len(aggregated_tx_df)):
        val = tuple(aggregated_tx_df.loc[each_row])
        sql = "insert into aggregated_tx_df values (?,?,?,?,?)"
        cursor.execute(sql,val)
        mydb.commit()
    
    st.write("loaded to table aggregated_tx_df")

def to_create_aggregated_transaction_dataframe_by_state():
    aggregated_state_tx_df = pd.DataFrame()
    
    for each_state in states_list:
        for year in range(2018,2023):
            for i in range(1,5):
                x = open(rf"{data_path}\data\aggregated\transaction\country\india\state\{each_state}\{year}\{i}.json")
                data = json.load(x)
                for each in (data['data']['transactionData']):
                    new_row={
                        'state_name': re.sub(r"\-", " ", each_state).title(),
                        'name':each['name'],
                        # "type": each['paymentInstruments'][0]['type'],
                        "count": each['paymentInstruments'][0]['count'],
                        "amount": each['paymentInstruments'][0]['amount'],
                        'year':str(year),
                        'quarter':i
                    }
                    aggregated_state_tx_df = pd.concat([aggregated_state_tx_df,pd.Series(new_row).to_frame().T], ignore_index=True)
    
    df = aggregated_state_tx_df
    for i in df['state_name']:
        df['state_name'].loc[df['state_name']==i]=i.replace('Andaman & Nicobar Islands', 'Andaman & Nicobar')
        df['state_name'].loc[df['state_name']==i]=i.replace('Dadra & Nagar Haveli & Daman & Diu', 'Dadra and Nagar Haveli and Daman and Diu')
    aggregated_state_tx_df = df
                   
    cursor.execute("drop table if exists aggregated_state_tx_df")
    cursor.execute(f"create table if not exists aggregated_state_tx_df({list(aggregated_state_tx_df.columns)[0]} TEXT,{list(aggregated_state_tx_df.columns)[1]} TEXT,{list(aggregated_state_tx_df.columns)[2]} INTEGER ,{list(aggregated_state_tx_df.columns)[3]} INTEGER,{list(aggregated_state_tx_df.columns)[4]} TEXT,{list(aggregated_state_tx_df.columns)[5]} INTEGER)")
    for each_row in range(len(aggregated_state_tx_df)):
        val = tuple(aggregated_state_tx_df.loc[each_row])
        sql = "insert into aggregated_state_tx_df values (?,?,?,?,?,?)"
        cursor.execute(sql,val)
        mydb.commit()
    st.write("loaded to table aggregated_state_tx_df")
    
def to_create_aggregated_user_dataframe():

    aggregated_user_df = pd.DataFrame()
    for year in range(2018,2023):
        for i in range(1,5):
            x = open(rf"{data_path}\data\aggregated\user\country\india\{year}\{i}.json")
            data = json.load(x)
            if type(data['data']['usersByDevice']) is not NoneType:
                for each_device in data['data']['usersByDevice']:
                    new_row={
                    "brand": each_device['brand'],
                    "count": each_device['count'],
                    "percentage": each_device['percentage'],
                    "year": str(year),
                    "quarter":i,
                    # 'country':'India'
                        }
                    aggregated_user_df = pd.concat([aggregated_user_df,pd.Series(new_row).to_frame().T], ignore_index=True)


    cursor.execute("drop table if exists aggregated_user_df")
    cursor.execute(f"create table if not exists aggregated_user_df({list(aggregated_user_df.columns)[0]} TEXT,{list(aggregated_user_df.columns)[1]} INTEGER,{list(aggregated_user_df.columns)[2]} REAL ,{list(aggregated_user_df.columns)[3]} TEXT,{list(aggregated_user_df.columns)[4]} INTEGER)")
    for each_row in range(len(aggregated_user_df)):
        val = tuple(aggregated_user_df.loc[each_row])
        sql = "insert into aggregated_user_df values (?,?,?,?,?)"
        cursor.execute(sql,val)
        mydb.commit()
    st.write("loaded to table aggregated_user_df")
    
def to_create_aggregated_user_dataframe_by_state():

    aggregated_state_user_df = pd.DataFrame()
    for each_state in states_list:
        for year in range(2018,2023):
            for i in range(1,5):
                x = open(rf"{data_path}\data\aggregated\user\country\india\state\{each_state}\{year}\{i}.json")
                data = json.load(x)
                if type(data['data']['usersByDevice']) is not NoneType:
                    for each_device in data['data']['usersByDevice']:
                        new_row={
                        'state_name':re.sub(r"\-", " ", each_state).title(),
                        "brand": each_device['brand'],
                        "count": each_device['count'],
                        "percentage": each_device['percentage'],
                        "year": str(year),
                        "quarter":i,
                        # 'country':'India'
                            }
                        aggregated_state_user_df = pd.concat([aggregated_state_user_df,pd.Series(new_row).to_frame().T], ignore_index=True)
    
    df = aggregated_state_user_df
    for i in df['state_name']:
        df['state_name'].loc[df['state_name']==i]=i.replace('Andaman & Nicobar Islands', 'Andaman & Nicobar')
        df['state_name'].loc[df['state_name']==i]=i.replace('Dadra & Nagar Haveli & Daman & Diu', 'Dadra and Nagar Haveli and Daman and Diu')
    aggregated_state_user_df = df

    cursor.execute("drop table if exists aggregated_state_user_df")
    cursor.execute(f"create table if not exists aggregated_state_user_df({list(aggregated_state_user_df.columns)[0]} TEXT,{list(aggregated_state_user_df.columns)[1]} TEXT,{list(aggregated_state_user_df.columns)[2]} INTEGER ,{list(aggregated_state_user_df.columns)[3]} REAL,{list(aggregated_state_user_df.columns)[4]} TEXT,{list(aggregated_state_user_df.columns)[5]} INTEGER)")
    for each_row in range(len(aggregated_state_user_df)):
        val = tuple(aggregated_state_user_df.loc[each_row])
        sql = "insert into aggregated_state_user_df values (?,?,?,?,?,?)"
        cursor.execute(sql,val)
        mydb.commit()
    st.write("loaded to table aggregated_state_user_df")
    
def to_create_map_of_transactions_dataframe():

    map_tx_df = pd.DataFrame()
    for year in range(2018,2023):
        for i in range(1,5):
            x = open(rf"{data_path}\data\map\transaction\hover\country\india\{year}\{i}.json")
            data = json.load(x)
            if type(data['data']['hoverDataList']) is not NoneType:

                for each_state in (data['data']['hoverDataList']):
                    new_row={'state_name': re.sub(r"\-", " ", each_state['name']).title(),
                        # 'type':each_state['metric'][0]['type'],
                        'count':each_state['metric'][0]['count'],
                        'amount':each_state['metric'][0]['amount'],
                        'year':str(year),
                        'quarter':i}
                    map_tx_df = pd.concat([map_tx_df,pd.Series(new_row).to_frame().T], ignore_index=True)
    df = map_tx_df
    for i in df['state_name']:
        df['state_name'].loc[df['state_name']==i]=i.replace('Andaman & Nicobar Islands', 'Andaman & Nicobar')
        df['state_name'].loc[df['state_name']==i]=i.replace('Dadra & Nagar Haveli & Daman & Diu', 'Dadra and Nagar Haveli and Daman and Diu')
    map_tx_df = df

    cursor.execute("drop table if exists map_tx_df")
    cursor.execute(f"create table if not exists map_tx_df({list(map_tx_df.columns)[0]} TEXT,{list(map_tx_df.columns)[1]} INTEGER,{list(map_tx_df.columns)[2]} INTEGER ,{list(map_tx_df.columns)[3]} TEXT,{list(map_tx_df.columns)[4]} INTEGER)")
    for each_row in range(len(map_tx_df)):
        val = tuple(map_tx_df.loc[each_row])
        sql = "insert into map_tx_df values (?,?,?,?,?)"
        cursor.execute(sql,val)
        mydb.commit()
    st.write("loaded to table map_tx_df")

def to_create_map_of_transactions_dataframe_by_state():

    map_state_tx_df = pd.DataFrame()
    for state in states_list:
        for year in range(2018,2023):
            for i in range(1,5):
                x = open(rf"{data_path}\data\map\transaction\hover\country\india\state\{state}\{year}\{i}.json")
                data = json.load(x)
                # st.json(data['data']['hoverDataList'])
                if type(data['data']['hoverDataList']) is not NoneType:

                    for each_state in (data['data']['hoverDataList']):
                        new_row={'state_name':re.sub(r"\-", " ", state).title(),
                            'name': each_state['name'],
                            # 'type':each_state['metric'][0]['type'],
                            'count':each_state['metric'][0]['count'],
                            'amount':each_state['metric'][0]['amount'],
                            'year':str(year),
                            'quarter':i}
                        # map_state_tx_df=map_state_tx_df.append(new_row,ignore_index=True)
                        map_state_tx_df = pd.concat([map_state_tx_df,pd.Series(new_row).to_frame().T], ignore_index=True)
    
    df = map_state_tx_df
    for i in df['state_name']:
        df['state_name'].loc[df['state_name']==i]=i.replace('Andaman & Nicobar Islands', 'Andaman & Nicobar')
        df['state_name'].loc[df['state_name']==i]=i.replace('Dadra & Nagar Haveli & Daman & Diu', 'Dadra and Nagar Haveli and Daman and Diu')
    map_state_tx_df = df
                        

    # st.write("Map of transactions of each state by year in 4 quarters from year 2018 to 2022")
    cursor.execute("drop table if exists map_state_tx_df")
    cursor.execute(f"create table if not exists map_state_tx_df({list(map_state_tx_df.columns)[0]} TEXT,{list(map_state_tx_df.columns)[1]} TEXT,{list(map_state_tx_df.columns)[2]} INTEGER ,{list(map_state_tx_df.columns)[3]} INTEGER,{list(map_state_tx_df.columns)[4]} TEXT,{list(map_state_tx_df.columns)[5]} INTEGER)")
    for each_row in range(len(map_state_tx_df)):
        val = tuple(map_state_tx_df.loc[each_row])
        sql = "insert into map_state_tx_df values (?,?,?,?,?,?)"
        cursor.execute(sql,val)
        mydb.commit()
    st.write("loaded to table map_state_tx_df")

def to_create_map_of_users_dataframe():

    map_user_df = pd.DataFrame()
    for year in range(2018,2023):
        for i in range(1,5):
            x = open(rf"{data_path}\data\map\user\hover\country\india\{year}\{i}.json")
            data = json.load(x)
            # st.write(data['data']['hoverData'].values())
            for each_key in (data['data']['hoverData'].keys()):
                # st.write(data['data']['hoverData'][f'{each_key}']['registeredUsers'])

                new_row ={
                    'state_name':re.sub(r"\-", " ", each_key).title(),
                    'registeredUsers': data['data']['hoverData'][f'{each_key}']['registeredUsers'],
                    'year':str(year),
                    'quarter':i
                }
                # map_user_df = map_user_df.append(new_row,ignore_index=True)
                map_user_df = pd.concat([map_user_df,pd.Series(new_row).to_frame().T], ignore_index=True)

    df = map_user_df
    for i in df['state_name']:
        df['state_name'].loc[df['state_name']==i]=i.replace('Andaman & Nicobar Islands', 'Andaman & Nicobar')
        df['state_name'].loc[df['state_name']==i]=i.replace('Dadra & Nagar Haveli & Daman & Diu', 'Dadra and Nagar Haveli and Daman and Diu')
    map_user_df = df

    # st.write("Map of users by year in 4 quarters from year 2018 to 2022")
    cursor.execute("drop table if exists map_user_df")
    cursor.execute(f"create table if not exists map_user_df({list(map_user_df.columns)[0]} TEXT,{list(map_user_df.columns)[1]} INTEGER,{list(map_user_df.columns)[2]} TEXT,{list(map_user_df.columns)[3]} INTEGER)")
    for each_row in range(len(map_user_df)):
        val = tuple(map_user_df.loc[each_row])
        sql = "insert into map_user_df values (?,?,?,?)"
        cursor.execute(sql,val)
        mydb.commit()
    st.write("loaded to table map_user_df")

def to_create_map_of_users_dataframe_by_state():
    map_state_user_df = pd.DataFrame()
    for state in states_list:
        for year in range(2018,2023):
            for i in range(1,5):
                x = open(rf"{data_path}\data\map\transaction\hover\country\india\state\{state}\{year}\{i}.json")
                data = json.load(x)
                # st.write(data['data']['hoverDataList'])
                for each_key in (data['data']['hoverDataList']):
                    # st.write(each_key)

                    new_row ={
                        'state_name':re.sub(r"\-", " ", state).title(),
                        'district_name':each_key['name'],
                        # 'registeredUsers': data['data']['hoverDataList'][f'{each_key}']['registeredUsers'],
                        'count':each_key['metric'][0]['count'],
                        'amount':each_key['metric'][0]['amount'],
                        # 'type':each_key['metric'][0]['type'],
                        'year':str(year),
                        'quarter':i
                    }
                    # map_state_user_df = map_state_user_df.append(new_row,ignore_index=True)
                    map_state_user_df = pd.concat([map_state_user_df,pd.Series(new_row).to_frame().T], ignore_index=True)
    
    df = map_state_user_df
    for i in df['state_name']:
        df['state_name'].loc[df['state_name']==i]=i.replace('Andaman & Nicobar Islands', 'Andaman & Nicobar')
        df['state_name'].loc[df['state_name']==i]=i.replace('Dadra & Nagar Haveli & Daman & Diu', 'Dadra and Nagar Haveli and Daman and Diu')
    map_state_user_df = df
                    

    # st.write("Map of users of each state by year in 4 quarters from year 2018 to 2022")
    cursor.execute("drop table if exists map_state_user_df")
    cursor.execute(f"create table if not exists map_state_user_df({list(map_state_user_df.columns)[0]} TEXT,{list(map_state_user_df.columns)[1]} TEXT,{list(map_state_user_df.columns)[2]} INTEGER,{list(map_state_user_df.columns)[3]} INTEGER,{list(map_state_user_df.columns)[4]} TEXT,{list(map_state_user_df.columns)[5]} INTEGER)")
    for each_row in range(len(map_state_user_df)):
        val = tuple(map_state_user_df.loc[each_row])
        sql = "insert into map_state_user_df values (?,?,?,?,?,?)"
        cursor.execute(sql,val)
        mydb.commit()
    st.write("loaded to table map_state_user_df")

def to_create_top_transactions_dataframe():
    top_tx_df = pd.DataFrame()
    for year in range(2018,2023):
        for i in range(1,5):
            x = open(rf"{data_path}\data\top\transaction\country\india\{year}\{i}.json")
            data = json.load(x)
            for each_state in (data['data']['states']):
                new_row={
                    'state_name':re.sub(r"\-", " ", each_state['entityName']).title(),
                    # 'type':each_state['metric']['type'],
                    'count':each_state['metric']['count'],
                    'amount':each_state['metric']['amount'],
                    'year':str(year),
                    'quarter':i
                }
                # top_tx_df=top_tx_df.append(new_row,ignore_index=True)
                top_tx_df = pd.concat([top_tx_df,pd.Series(new_row).to_frame().T], ignore_index=True)
    df = top_tx_df
    for i in df['state_name']:
        df['state_name'].loc[df['state_name']==i]=i.replace('Andaman & Nicobar Islands', 'Andaman & Nicobar')
        df['state_name'].loc[df['state_name']==i]=i.replace('Dadra & Nagar Haveli & Daman & Diu', 'Dadra and Nagar Haveli and Daman and Diu')
    top_tx_df = df

    # st.write("Top transactions by year in 4 quarters from year 2018 to 2022")
    cursor.execute("drop table if exists top_tx_df")
    cursor.execute(f"create table if not exists top_tx_df({list(top_tx_df.columns)[0]} TEXT,{list(top_tx_df.columns)[1]} INTEGER,{list(top_tx_df.columns)[2]} INTEGER ,{list(top_tx_df.columns)[3]} TEXT,{list(top_tx_df.columns)[4]} INTEGER)")
    for each_row in range(len(top_tx_df)):
        val = tuple(top_tx_df.loc[each_row])
        sql = "insert into top_tx_df values (?,?,?,?,?)"
        cursor.execute(sql,val)
        mydb.commit()
    st.write("loaded to table top_tx_df")

def to_create_top_transactions_dataframe_by_state():
    top_district_tx_df = pd.DataFrame()
    top_pincode_tx_df = pd.DataFrame()


    for state in states_list:
        for year in range(2018,2023):
            for i in range(1,5):
                x = open(rf"{data_path}\data\top\transaction\country\india\state\{state}\{year}\{i}.json")
                data = json.load(x)
                # if type(data['data']['districts']) is not NoneType:
                for each_district in (data['data']['districts']):
                    new_row={
                    'state_name':re.sub(r"\-", " ", state).title(),
                    'name':each_district['entityName'],
                    # 'type':each_district['metric']['type'],
                    'count':each_district['metric']['count'],
                    'amount':each_district['metric']['amount'],
                    'year':str(year),
                    'quarter':i
                    }
                    # top_state_tx_df=top_state_tx_df.append(new_row,ignore_index=True)
                    top_district_tx_df = pd.concat([top_district_tx_df,pd.Series(new_row).to_frame().T], ignore_index=True)
                    

                for each_pin in (data['data']['pincodes']):
                    new_row={
                    'state_name':re.sub(r"\-", " ", state).title(),
                    'name':each_pin['entityName'],
                    # 'type':each_pin['metric']['type'],
                    'count':each_pin['metric']['count'],
                    'amount':each_pin['metric']['amount'],
                    'year':str(year),
                    'quarter':i
                    }
                    # top_state_tx_df=top_state_tx_df.append(new_row,ignore_index=True)
                    top_pincode_tx_df = pd.concat([top_pincode_tx_df,pd.Series(new_row).to_frame().T], ignore_index=True)
    df = top_pincode_tx_df
    for i in df['state_name']:
        df['state_name'].loc[df['state_name']==i]=i.replace('Andaman & Nicobar Islands', 'Andaman & Nicobar')
        df['state_name'].loc[df['state_name']==i]=i.replace('Dadra & Nagar Haveli & Daman & Diu', 'Dadra and Nagar Haveli and Daman and Diu')
    top_pincode_tx_df = df

    df = top_district_tx_df
    for i in df['state_name']:
        df['state_name'].loc[df['state_name']==i]=i.replace('Andaman & Nicobar Islands', 'Andaman & Nicobar')
        df['state_name'].loc[df['state_name']==i]=i.replace('Dadra & Nagar Haveli & Daman & Diu', 'Dadra and Nagar Haveli and Daman and Diu')
    top_district_tx_df = df

    cursor.execute("drop table if exists top_district_tx_df")
    cursor.execute(f"create table if not exists top_district_tx_df({list(top_district_tx_df.columns)[0]} TEXT,{list(top_district_tx_df.columns)[1]} TEXT,{list(top_district_tx_df.columns)[2]} INTEGER ,{list(top_district_tx_df.columns)[3]} INTEGER,{list(top_district_tx_df.columns)[4]} TEXT,{list(top_district_tx_df.columns)[5]} INTEGER)")
    for each_row in range(len(top_district_tx_df)):
        val = tuple(top_district_tx_df.loc[each_row])
        sql = "insert into top_district_tx_df values (?,?,?,?,?,?)"
        cursor.execute(sql,val)
        mydb.commit()

    cursor.execute("drop table if exists top_pincode_tx_df")
    cursor.execute(f"create table if not exists top_pincode_tx_df({list(top_pincode_tx_df.columns)[0]} TEXT,{list(top_pincode_tx_df.columns)[1]} TEXT,{list(top_pincode_tx_df.columns)[2]} INTEGER ,{list(top_pincode_tx_df.columns)[3]} INTEGER,{list(top_pincode_tx_df.columns)[4]} TEXT,{list(top_pincode_tx_df.columns)[5]} INTEGER)")
    for each_row in range(len(top_pincode_tx_df)):
        val = tuple(top_pincode_tx_df.loc[each_row])
        sql = "insert into top_pincode_tx_df values (?,?,?,?,?,?)"
        cursor.execute(sql,val)
        mydb.commit()
    
    st.write("loaded to table top_pincode_tx_df,top_district_tx_df")


def to_create_top_users_dataframe():

    top_user_state=pd.DataFrame()
    top_user_district = pd.DataFrame()
    top_user_pincode = pd.DataFrame()
    for year in range(2018,2023):
        for i in range(1,5):
            x = open(rf"{data_path}\data\top\user\country\india\{year}\{i}.json")
            data = json.load(x)
            # st.json(data['data']['states'])
            for n in (data['data']['states']):
                new_row={
                    'state_name':re.sub(r"\-", " ", n['name']).title(),
                    'registeredUsers':n['registeredUsers'],
                    'year':str(year),
                    'quarter':i
                }
                # top_user_state=top_user_state.append(new_row,ignore_index=True)
                top_user_state = pd.concat([top_user_state,pd.Series(new_row).to_frame().T], ignore_index=True)
                

            for n in (data['data']['districts']):
                # st.write(n)
                new_row={
                    'district_name':n['name'],
                    'registeredUsers':n['registeredUsers'],
                    'year':str(year),
                    'quarter':i
                }
                # top_user_district = top_user_district.append(new_row,ignore_index=True)
                top_user_district = pd.concat([top_user_district,pd.Series(new_row).to_frame().T], ignore_index=True)

            for n in (data['data']['pincodes']):
                # st.write(n)
                new_row={
                    'pincode':n['name'],
                    'registeredUsers':n['registeredUsers'],
                    'year':str(year),
                    'quarter':i
                }
                # top_user_pincode = top_user_pincode.append(new_row,ignore_index=True)
                top_user_pincode = pd.concat([top_user_pincode,pd.Series(new_row).to_frame().T], ignore_index=True)
    df = top_user_state
    for i in df['state_name']:
        df['state_name'].loc[df['state_name']==i]=i.replace('Andaman & Nicobar Islands', 'Andaman & Nicobar')
        df['state_name'].loc[df['state_name']==i]=i.replace('Dadra & Nagar Haveli & Daman & Diu', 'Dadra and Nagar Haveli and Daman and Diu')
    top_user_state = df
                
    cursor.execute("drop table if exists top_user_state")
    cursor.execute(f"create table if not exists top_user_state({list(top_user_state.columns)[0]} TEXT,{list(top_user_state.columns)[1]} INTEGER,{list(top_user_state.columns)[2]} TEXT,{list(top_user_state.columns)[3]} INTEGER)")
    for each_row in range(len(top_user_state)):
        val = tuple(top_user_state.loc[each_row])
        sql = "insert into top_user_state values (?,?,?,?)"
        cursor.execute(sql,val)
        mydb.commit()

    cursor.execute("drop table if exists top_user_district")
    cursor.execute(f"create table if not exists top_user_district({list(top_user_district.columns)[0]} TEXT,{list(top_user_district.columns)[1]} INTEGER,{list(top_user_district.columns)[2]} TEXT,{list(top_user_district.columns)[3]} INTEGER)")
    for each_row in range(len(top_user_district)):
        val = tuple(top_user_district.loc[each_row])
        sql = "insert into top_user_district values (?,?,?,?)"
        cursor.execute(sql,val)
        mydb.commit()

    cursor.execute("drop table if exists top_user_pincode")
    cursor.execute(f"create table if not exists top_user_pincode({list(top_user_pincode.columns)[0]} TEXT,{list(top_user_pincode.columns)[1]} INTEGER,{list(top_user_pincode.columns)[2]} TEXT,{list(top_user_pincode.columns)[3]} INTEGER)")
    for each_row in range(len(top_user_pincode)):
        val = tuple(top_user_pincode.loc[each_row])
        sql = "insert into top_user_pincode values (?,?,?,?)"
        cursor.execute(sql,val)
        mydb.commit()
    
    st.write("loaded to table top_user_state,top_user_district,top_user_pincode")

def to_create_top_users_dataframe_by_state():
    # top_user_state=pd.DataFrame()
    top_user_district_by_state = pd.DataFrame()
    top_user_pincode_by_state = pd.DataFrame()
    for state in states_list:
        for year in range(2018,2023):
            for i in range(1,5):
                x = open(rf"{data_path}\data\top\user\country\india\state\{state}\{year}\{i}.json")
                data = json.load(x)
                for n in (data['data']['districts']):
                # st.write(n)
                    new_row={
                    'state_name':re.sub(r"\-", " ", state).title(),
                    'district_name':n['name'],
                    'registeredUsers':n['registeredUsers'],
                    'year':str(year),
                    'quarter':i
                        }
                    # top_user_district_by_state = top_user_district_by_state.append(new_row,ignore_index=True)
                    top_user_district_by_state = pd.concat([top_user_district_by_state,pd.Series(new_row).to_frame().T], ignore_index=True)
                    

            for n in (data['data']['pincodes']):
                # st.write(n)
                new_row={
                    'state_name':re.sub(r"\-", " ", state).title(),
                    'pincode':n['name'],
                    'registeredUsers':n['registeredUsers'],
                    'year':str(year),
                    'quarter':i
                }
                # top_user_pincode_by_state = top_user_pincode_by_state.append(new_row,ignore_index=True)
                top_user_pincode_by_state = pd.concat([top_user_pincode_by_state,pd.Series(new_row).to_frame().T], ignore_index=True)
    
    df = top_user_pincode_by_state
    for i in df['state_name']:
        df['state_name'].loc[df['state_name']==i]=i.replace('Andaman & Nicobar Islands', 'Andaman & Nicobar')
        df['state_name'].loc[df['state_name']==i]=i.replace('Dadra & Nagar Haveli & Daman & Diu', 'Dadra and Nagar Haveli and Daman and Diu')
    top_user_pincode_by_state = df
    
    df = top_user_district_by_state
    for i in df['state_name']:
        df['state_name'].loc[df['state_name']==i]=i.replace('Andaman & Nicobar Islands', 'Andaman & Nicobar')
        df['state_name'].loc[df['state_name']==i]=i.replace('Dadra & Nagar Haveli & Daman & Diu', 'Dadra and Nagar Haveli and Daman and Diu')
    top_user_district_by_state = df

    cursor.execute("drop table if exists top_user_district_by_state")
    cursor.execute(f"create table if not exists top_user_district_by_state({list(top_user_district_by_state.columns)[0]} TEXT,{list(top_user_district_by_state.columns)[1]} TEXT,{list(top_user_district_by_state.columns)[2]} INTEGER ,{list(top_user_district_by_state.columns)[3]} TEXT,{list(top_user_district_by_state.columns)[4]} INTEGER)")
    for each_row in range(len(top_user_district_by_state)):
        val = tuple(top_user_district_by_state.loc[each_row])
        sql = "insert into top_user_district_by_state values (?,?,?,?,?)"
        cursor.execute(sql,val)
        mydb.commit()

    cursor.execute("drop table if exists top_user_pincode_by_state")
    cursor.execute(f"create table if not exists top_user_pincode_by_state({list(top_user_pincode_by_state.columns)[0]} TEXT,{list(top_user_pincode_by_state.columns)[1]} TEXT,{list(top_user_pincode_by_state.columns)[2]} INTEGER ,{list(top_user_pincode_by_state.columns)[3]} TEXT,{list(top_user_pincode_by_state.columns)[4]} INTEGER)")
    for each_row in range(len(top_user_pincode_by_state)):
        val = tuple(top_user_pincode_by_state.loc[each_row])
        sql = "insert into top_user_pincode_by_state values (?,?,?,?,?)"
        cursor.execute(sql,val)
        mydb.commit()
   
    st.write("loaded to table top_user_district_by_state,top_user_pincode_by_state")


if __name__ == "__main__":

    repository_url = 'https://github.com/PhonePe/pulse'
    destination_path = os.getcwd() #r'C:\Users\SVR\Documents\GitHub\Phonepe_pulse\pulse' 

    if(st.sidebar.button("click to clone phonepe pulse data github repo")):
        clone_repository(repository_url, destination_path)
    #enable this to clone the pulse data to your destination folder
    
    mydb = sqlite3.connect(rf'{destination_path}\pages\phonepe_pulse.db')
    cursor = mydb.cursor()
    # st.write(destination_path)

    if st.button('aggregated_tx_df'):
        to_create_aggregated_transaction_dataframe()
        
    if st.button('aggregated_state_tx_df'):
        #     # aggregated_state_tx_df =  
        to_create_aggregated_transaction_dataframe_by_state()

    if st.button('aggregated_user_df'):
            #     # aggregated_user_df = 
        to_create_aggregated_user_dataframe()
            
    if st.button('aggregated_state_user_df'):
            # #     # aggregated_state_user_df = 
        to_create_aggregated_user_dataframe_by_state()
            
    if st.button('map_tx_df'):
            # #     # map_tx_df = 
        to_create_map_of_transactions_dataframe()
            
    if st.button('map_state_tx_df'):
            # #     # map_state_tx_df = 
        to_create_map_of_transactions_dataframe_by_state()
            
    if st.button('map_user_df'):
            # #     # map_user_df = 
        to_create_map_of_users_dataframe()
            
    if st.button('map_state_user_df'):
            # #     # map_state_user_df = 
        to_create_map_of_users_dataframe_by_state()
            
    if st.button('top_tx_df'):
            # #     # top_tx_df = 
        to_create_top_transactions_dataframe()
            
    if st.button('top_district_tx_df,top_pincode_tx_df'):
            # #     # top_district_tx_df,top_pincode_tx_df = 
        to_create_top_transactions_dataframe_by_state()
            
    if st.button('top_user_state,top_user_district,top_user_pincode'):
            # #     # top_user_state,top_user_district,top_user_pincode = 
        to_create_top_users_dataframe()
            
    if st.button('top_user_district_by_state,top_user_pincode_by_state'):
            # #     # top_user_district_by_state,top_user_pincode_by_state = 
        to_create_top_users_dataframe_by_state()
            
    

    st.write('done')
    
    

        


        





    


    
            
    







