import math
from snowflake.snowpark import Session
import snowflake.connector as sf
from snowflake.snowpark.functions import avg,sum,col,lit
from snowflake.snowpark.types import DecimalType
import pandas as pd
from datetime import date, datetime, timedelta
import streamlit as st
import altair as alt
import numpy as np
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import plotly.offline as py

st.set_page_config(
     page_title="Snowflake Cost Dashboard",
     page_icon="🧊",
     layout="wide",
     initial_sidebar_state="expanded",
     menu_items={
         'Get Help': 'https://developers.snowflake.com',
         'About': "This is an *extremely* cool app powered by Snowpark for Python, Streamlit, and Snowflake Data Marketplace"
     }
)

# DATA_CT=('C:\Streamlit\StreamlitApp\ComputePage\cmpcredittrend.csv')
# DATA_CO=('C:\Streamlit\StreamlitApp\ComputePage\cmpcostcover.csv')

# DATA_ATT=('C:\Streamlit\StreamlitApp\ComputePage\cmpcraccnttopten.csv')
# DATA_WTT=('C:\Streamlit\StreamlitApp\ComputePage\cmpcrwhtopten.csv')
# DATA_CAT=('C:\Streamlit\StreamlitApp\ComputePage\cmpcostbyaccnt.csv')
DATA_CT='https://raw.githubusercontent.com/anideepak/addemoapp/main/Demo/Demodata/ComputePage/Cmpcredittrend.csv'
DATA_CO='https://raw.githubusercontent.com/anideepak/addemoapp/main/Demo/Demodata/ComputePage/Cmpcostcover.csv'

DATA_ATT='https://raw.githubusercontent.com/anideepak/addemoapp/main/Demo/Demodata/ComputePage/Cmpcraccnttopten.csv'
DATA_WTT='https://raw.githubusercontent.com/anideepak/addemoapp/main/Demo/Demodata/ComputePage/Cmpcrwhtopten.csv'
DATA_CAT='https://raw.githubusercontent.com/anideepak/addemoapp/main/Demo/Demodata/ComputePage/cmpcostbyaccnt.csv'
     
sidebar = st.sidebar

# Create Session object

# Create Snowpark DataFrames that loads data from coe_metrics
def load_ct_data():
    #Compute Credit Trend
    #****************** 
     datact2=pd.read_csv(DATA_CT)
     datact = datact2[datact2['years'] == cmpyear]

     #create columns
     datact['total_cr'] = datact['total_cr'].fillna(0)
     #prev_row = datact['earliest_date'][0]
    #  for i, row in enumerate(datact['earliest_date'][:][1:]):
    #      if type(row) is float and math.isnan(row):
    #         tmp = datetime.strptime(prev_row, "%m/%d/%Y %H:%M")
    #         row = (tmp + timedelta(days=1)).strftime("%m/%d/%Y %H:%M")
    #         datact['earliest_date'].loc[i+1] = row
    #      prev_row = row
        
     # datact['earliest_date']
     # datact['earliest_date'] = pd.to_datetime(datact['earliest_date'])
     datact['x_date'] = pd.to_datetime(datact['earliest_date'])
     # print(datact)
     datact['monyr'] = datact['month'].astype(str) + ' ' + datact['years'].astype(str)
     b=datact['monyr']
     c=datact['earliest_date']
     a=datact['total_cr'].round(0)
     #st.write(datact)
     #Try with go
     fig1=go.Figure([go.Scatter(x=c, y=datact['total_cr'] ,mode = "lines")])
     fig1.update_layout(
         xaxis = dict(
             tickmode = 'linear',
            #  tick0=datact['monyr'].loc[0],
             dtick = 60,
             ticktext = datact['monyr'],
         )
     )
     #fig1.up .update_layout(xaxis_tickformat ={"x_date": "%B %d, %Y"})
     
     return fig1     
def load_co_data():
    #Compute Cost Overage
    #********************
    # *************TO DO************Have aggregated (sum)  usage_amount per month and oc_usage_type
     codata1=pd.read_csv(DATA_CO)
     
     #yrslist = codata1['years'].unique()
     #cmpyear = st.sidebar.selectbox("Select Year:",yrslist)
     codata = codata1[codata1['years'] == cmpyear]

    #  st.write(codata)
     #return data
     #create columns
     codata['usage_amount'] = codata['usage_amount']
     a=codata['month']
     b=codata['usage_amount'].round(0)
     codata['oc_usage_type'] = codata['oc_usage_type'].fillna('(Blank)')
     c=codata['oc_usage_type']
     #b=btdata['usage_amount'].to_string()
     #st.write(btdata)
     cochart=px.bar(codata, x=a,y=b,color=c,labels={'a':'Month','usage_amount':'USD Spend Amount','oc_usage_type':'Expense Category'},
                    hover_data=['oc_usage_type'],
                    text_auto=True
                    #title="Monthly Billing Trend on Usage for the Year",
                    )
     cochart.update_xaxes(title_text='Month')
     cochart.update_yaxes(title_text='In Thousand USD')
     cochart.update_traces(textfont_size=12,textangle=0,textposition="auto")  #,cliponaxis=False
     #btchart.add_traces(texttemplat="%{y:.0f}")
     return cochart     

def load_craccnttopten():
    #Top 10 Total Credit  Accounts
    #****************** 
     attdata=pd.read_csv(DATA_ATT)
     attdata1 = attdata[attdata['years'] == cmpyear]
     attdata = attdata.sort_values(by=["total_credits"])
     #st.write(attdata)
     #return data
     #create columns
     attdata['total_credits'] = attdata['total_credits'].fillna(0)
     #attdata['usage_amount'] = attdata['usage_amount'] # .map('{:,.0f}'.format)
     a=attdata['account']
     b=attdata['total_credits']
     x_label = attdata['total_credits'] = attdata['total_credits'].map('{:,.0f}'.format)
     #b=btdata['usage_amount'].to_string()
     attchart=px.bar(attdata, y=a,x=b)
     #,labels={'a':'Month','usage_amount':'USD Spend Aamount'},
     #               title="Top Ten Spending Account for the Year",
     attchart.update_xaxes(title_text='Account')
     attchart.update_yaxes(title_text='In Thousand USD')
     attchart.update_traces(textfont_size=12,textangle=0,textposition="outside",cliponaxis=False)
     
     return attchart
 
def load_crwhtopten():
    #Top 10 Total Credit Warehouses
    #****************************** 
     wttdata=pd.read_csv(DATA_WTT)
     wttdata1 = wttdata[wttdata['years'] == cmpyear]
     wttdata = wttdata.sort_values(by=["total_credits"])
     #st.write(attdata)
     #return data
     #create columns
     wttdata['total_credits'] = wttdata['total_credits'].fillna(0)
     #attdata['usage_amount'] = attdata['usage_amount'] # .map('{:,.0f}'.format)
     a=wttdata['warehouse_name']
     b=wttdata['total_credits']
     x_label = wttdata['total_credits'] = wttdata['total_credits'].map('{:,.0f}'.format)
     #b=btdata['usage_amount'].to_string()
     wttchart=px.bar(wttdata, y=a,x=b)
     #,labels={'a':'Month','usage_amount':'USD Spend Aamount'},
     #               title="Top Ten Spending Account for the Year",
     wttchart.update_xaxes(title_text='Warehouses')
     wttchart.update_yaxes(title_text='In Thousand USD')
     wttchart.update_traces(textfont_size=12,textangle=0,textposition="outside",cliponaxis=False)
     
     return wttchart
 
def row_in_bold(row):
    if row.ACCOUNT_NAME == 'Total': 
        return ['background-color: yellow'] * len(row)
    return [''] * len(row)
 
def load_costbyaccnt():
    #Cost by Account
    #***************
    #**************** TO DO ***************************Select Top 10 from the records 
     catdata=pd.read_csv(DATA_CAT)
     sum_values = ["Total", catdata["USAGE_CREDITS"].sum().round(0), catdata["USAGE_AMOUNT"].sum()]
     catdata = catdata.append({'ACCOUNT_NAME':sum_values[0],"USAGE_CREDITS":sum_values[1],"USAGE_AMOUNT":sum_values[2] },ignore_index=True)
     catdata = catdata.style.apply(row_in_bold,axis=1)
     #st.write(catdata)
     return catdata
 

def main():
    st.title("Snowflake Cost and Spend Dashboard")
    st.subheader("Powered by Streamlit")
    # st.sidebar.title("This is the sidebar")
    # st.sidebar.markdown("Select any page")
    # #st.container()

     # TO use account list for selection if required
    #  DATA_ATT=('C:\Streamlit\StreamlitApp\HomePage\hometopten.csv')
    #  totdata=pd.read_csv(DATA_ATT)
    #  aclist = totdata['account'].unique()
    #  accntcmp = st.sidebar.selectbox("Select Account:",aclist)
    #  # END OF  use account list for selection if required
                 
    # if st.sidebar.button("Display Chart!"):
    ch1 = load_ct_data()
    ch2 = load_co_data()
    ch3 = load_craccnttopten()
    ch4 = load_crwhtopten()
    ch5 = load_costbyaccnt()
    #ch2 = load_accnttopten()
    col1, col2 = st.columns(2)
    with col1:
        st.header("Compute Credits Trend")
        st.plotly_chart(ch1)
    with col2:
        st.header("Compute Cost Overage")
        st.plotly_chart(ch2)
    col1, col2, col3 = st.columns(3)

    with col1:
        st.header("Credits - Top 10 Accounts")
        st.plotly_chart(ch3)
    with col2:
        st.header("Credits - Top 10 Warehouses")
        st.plotly_chart(ch4)
    with col3:
        st.header("Cost by Account")
        st.write(ch5)

if __name__ == '__main__':
    global accntcmp
    global cmpyear
    datact1=pd.read_csv(DATA_CT)
    yrlist = datact1['years'].unique()
    cmpyear = st.sidebar.selectbox("Select Year:",yrlist)
    main()     
