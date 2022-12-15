import streamlit as st

st.set_page_config(
    page_title="Snowflake Cost and Spend Dashboard",
    page_icon="🧊",
)

st.write("# Powered by Streamlit!")

st.sidebar.success("Select a Dashboard Page.")

st.markdown(
    """
    The Dashboard reports Yeraly metrics of Snowflake Credits.
    Measures with respect to Compute, Storage and Queries are reported for the Current Year.
    The Dashboard also has metrics for SNowflake Accounts owned by Accenture
    **👈 Select a Dashboard Page from the sidebar** 
    
"""
)
