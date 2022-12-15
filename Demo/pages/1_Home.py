from snowflake.snowpark import Session
import snowflake.connector as sf
from snowflake.snowpark.functions import avg,sum,col,lit
from snowflake.snowpark.types import DecimalType
import pandas as pd
from datetime import date
import streamlit as st
import altair as alt
import numpy as np
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go

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

# DATA_URL=('C:\Streamlit\StreamlitApp\HomePage\homevalues.csv')
# DATA_BT=('C:\Streamlit\StreamlitApp\HomePage\homebillingtrend.csv')
# DATA_ATT=('C:\Streamlit\StreamlitApp\HomePage\hometopten.csv')
DATA_URL=('Demodata/HomePage/homevalues.csv')
DATA_BT=('Demodata/HomePage/homebillingtrend.csv')
DATA_ATT=('Demodata/HomePage/hometopten.csv')

sidebar = st.sidebar

# In[7]:

# Create Session object

# Create Snowpark DataFrames that loads data from coe_metrics
def load_data():
    #Display Totals
    #****************** 
     totdata=pd.read_csv(DATA_URL)
     #return data
     st.container()
     #stdata=st.dataframe(totdata)
     #create columns
     #stg, cmp, accnt, dbscol, whscol = st.columns(6)
     #ytdbl.write("YTD Billing US DOllars")
     accntname = 'Greater Than'
     rptyear = 2022
     totdata.round(0)
     a=totdata['ytdbill'].to_string().strip("0")
     b=totdata['storage'].to_string().strip("0")
     c=totdata['compute'].to_string().strip("0")
     d=totdata['accounts'].to_string().strip("0")
     e=totdata['dbs'].to_string().strip("0")
     fy=totdata['whs'].to_string().strip("0")
     lastonetodel = accntname + str(rptyear)
     col1, col2, col3, col4, col5, col6 = st.columns(6)
     #col1.metric("YTD Bill is here",a)
     #col1.metric("Storage",b)
     
     with col1:
        st.header("YTD Bill in USD")
        st.write(a)
     with col2:
        st.header("Storage")
        st.write(b)
     with col3:
        st.header("Compute")
        st.write(c)
     with col4:
        st.header("Accounts")
        st.write(d)
     with col5:
        st.header("Databases")
        st.write(e)
     with col6:
        st.header("Warehouses")
        st.write(fy)

def load_billingtrend():
    #Billing Trend
    #****************** 
     btdata=pd.read_csv(DATA_BT)
     # Select account and Year for Reporting
     

     #create columns
     btdata['usage_amount'] = btdata['usage_amount'].fillna(0)
     btdata['usage_amount'] = btdata['usage_amount']  #.map('{:,.0f}'.format)
     #btdata['usage_amount'] = pd.to_numeric(btdata['usage_amount'],downcast="integer")  #.map('{:,.0f}'.format)

     a=btdata['month']
     b=btdata['usage_amount'].round(0)
     btdata['oc_usage_type'] = btdata['oc_usage_type'].fillna('(Blank)')
     c=btdata['oc_usage_type']
     #b=btdata['usage_amount'].to_string()
     #st.write(btdata)
     btchart=px.bar(btdata, x=a,y=b,color=c,labels={'a':'Month','usage_amount':'USD Spend Amount','oc_usage_type':'Expense Category'},
                    hover_data=['oc_usage_type'],
                    text_auto=True
                    #title="Monthly Billing Trend on Usage for the Year",
                    )
     btchart.update_xaxes(title_text='Month')
     btchart.update_yaxes(title_text='In Thousand USD')
     btchart.update_traces(textfont_size=12,textangle=0,textposition="auto")  #,cliponaxis=False
     #btchart.add_traces(texttemplat="%{y:.0f}")
     return btchart     

def load_accnttopten():
    #Top 10 Accounts
    #****************** 
     attdata=pd.read_csv(DATA_ATT)
     #create columns
     attdata['usage_amount'] = attdata['usage_amount'].fillna(0)
     a=attdata['account']
     b=attdata['usage_amount']
     x_label = attdata['usage_amount'] = attdata['usage_amount'].map('{:,.0f}'.format)
     ##***Working****
    #  attchart=px.bar(attdata, y=a,x=b)
    #  attchart.update_xaxes(title_text='Account')
    #  attchart.update_yaxes(title_text='In Thousand USD')
    #  attchart.update_traces(textfont_size=12,textangle=0,textposition="outside",cliponaxis=False)
     ##*** End Working****
     
     ##***Animated****
     attchart=px.bar(attdata,x=b,y=a,color=b,animation_frame="years")
     attchart.update_xaxes(title_text='Account')
     attchart.update_yaxes(title_text='In Thousand USD')
     attchart.update_traces(textfont_size=12,textangle=0,textposition="outside",cliponaxis=False)
     
     return attchart
     

def main():
    st.title("Snowflake Cost and Spend Dashboard")
    st.subheader("Powered by Streamlit")
    # st.sidebar.title("This is the sidebar")
    # st.sidebar.markdown("Select any page")
    # st.container()
    global accntname
    global rptyear
                 
    #if st.sidebar.button("Display Chart!"):
    load_data()
    ch1 = load_billingtrend()
    ch2 = load_accnttopten()
    #with st.container():
    #st.balloons()    
    col1, col2 = st.columns(2)
    with col1:
        st.header("Monthly Billing Trend on Usage for the Year")
        st.plotly_chart(ch1)

    with col2:
        st.header("Top Ten Spending Account for the Year")
        st.plotly_chart(ch2)

if __name__ == '__main__':
    main()     
