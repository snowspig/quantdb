import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
import urllib.parse
from pymongo import MongoClient
import seaborn as sns
from datetime import datetime
import re

# Set page config
st.set_page_config(
    page_title="Stock Name Changes Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Add custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1E88E5;
        text-align: center;
        margin-bottom: 1rem;
    }
    .sub-header {
        font-size: 1.5rem;
        color: #0D47A1;
        margin-top: 1.5rem;
        margin-bottom: 0.5rem;
    }
    .card {
        background-color: #f0f2f6;
        border-radius: 10px;
        padding: 1rem;
        margin-bottom: 1rem;
        box-shadow: 0 2px 5px rgba(0, 0, 0, 0.1);
    }
    .metric-value {
        font-size: 2rem;
        font-weight: bold;
        color: #1E88E5;
    }
    .metric-label {
        font-size: 0.9rem;
        color: #555;
    }
    .highlight {
        background-color: #e3f2fd;
        padding: 0.2rem 0.5rem;
        border-radius: 4px;
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)

# Sidebar for database connection
st.sidebar.header("Database Connection")

# Connection parameters
default_username = "adminUser"
default_password = "MongoAdmin@2025!"
default_host = "106.14.185.239"
default_port = "27017"
default_auth_source = "admin"
default_db_name = "tushare_data"
default_collection = "previous_name"

with st.sidebar.expander("MongoDB Connection Settings", expanded=False):
    username = st.text_input("Username", value=default_username)
    password = st.text_input("Password", value=default_password, type="password")
    host = st.text_input("Host", value=default_host)
    port = st.text_input("Port", value=default_port)
    auth_source = st.text_input("Auth Source", value=default_auth_source)
    db_name = st.text_input("Database Name", value=default_db_name)
    collection_name = st.text_input("Collection", value=default_collection)

# Function to connect to MongoDB and get data
@st.cache_data(ttl=3600)
def get_data_from_mongodb(username, password, host, port, auth_source, db_name, collection_name):
    try:
        # Create MongoDB connection string
        password_encoded = urllib.parse.quote_plus(password)
        conn_str = f"mongodb://{username}:{password_encoded}@{host}:{port}/{auth_source}?authSource={auth_source}"
        
        # Connect to MongoDB
        client = MongoClient(conn_str)
        db = client[db_name]
        collection = db[collection_name]
        
        # Get data from MongoDB
        cursor = collection.find({}, {'_id': 0})
        df = pd.DataFrame(list(cursor))
        
        # Process data
        if 'begindate' in df.columns:
            # Convert date fields to datetime
            df['begindate'] = pd.to_datetime(df['begindate'], format='%Y%m%d', errors='coerce')
            if 'enddate' in df.columns:
                df['enddate'] = pd.to_datetime(df['enddate'], format='%Y%m%d', errors='coerce')
            if 'ann_dt' in df.columns:
                df['ann_dt'] = pd.to_datetime(df['ann_dt'], format='%Y%m%d', errors='coerce')
            
            # Extract year for analysis
            df['year'] = df['begindate'].dt.year
            df['month'] = df['begindate'].dt.month
            
            # Create full date column for time series
            df['date'] = df['begindate']
        
        return df
    except Exception as e:
        st.error(f"Error connecting to MongoDB: {str(e)}")
        return None

# Load data
try:
    with st.sidebar.spinner("Loading data..."):
        df = get_data_from_mongodb(username, password, host, port, auth_source, db_name, collection_name)
    
    if df is not None and not df.empty:
        st.sidebar.success(f"✅ Connected to MongoDB: {len(df)} records loaded")
        
        # Sidebar filters
        st.sidebar.header("Filters")
        
        # Date range filter
        min_date = df['begindate'].min().date()
        max_date = df['begindate'].max().date()
        date_range = st.sidebar.date_input(
            "Date Range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date
        )
        
        if len(date_range) == 2:
            start_date, end_date = date_range
            df_filtered = df[(df['begindate'].dt.date >= start_date) & 
                            (df['begindate'].dt.date <= end_date)]
        else:
            df_filtered = df
            
        # Market code filter
        market_codes = sorted(df['market_code'].unique())
        selected_market_codes = st.sidebar.multiselect(
            "Market Code",
            options=market_codes,
            default=market_codes
        )
        
        if selected_market_codes:
            df_filtered = df_filtered[df_filtered['market_code'].isin(selected_market_codes)]
            
        # Change reason filter (if available)
        if 'changereason' in df.columns:
            # Get valid change reasons (excluding NaN)
            valid_reasons = sorted(df['changereason'].dropna().unique())
            selected_reasons = st.sidebar.multiselect(
                "Change Reason",
                options=valid_reasons,
                default=[]
            )
            
            if selected_reasons:
                df_filtered = df_filtered[df_filtered['changereason'].isin(selected_reasons)]
        
        # Main dashboard content
        st.markdown("<div class='main-header'>Stock Name Changes Dashboard</div>", unsafe_allow_html=True)
        
        # Key metrics
        st.markdown("<div class='sub-header'>Key Metrics</div>", unsafe_allow_html=True)
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.markdown("<div class='card'>", unsafe_allow_html=True)
            st.markdown(f"<div class='metric-value'>{len(df_filtered):,}</div>", unsafe_allow_html=True)
            st.markdown("<div class='metric-label'>Total Name Changes</div>", unsafe_allow_html=True)
            st.markdown("</div>", unsafe_allow_html=True)
            
        with col2:
            st.markdown("<div class='card'>", unsafe_allow_html=True)
            unique_stocks = df_filtered['ts_code'].nunique()
            st.markdown(f"<div class='metric-value'>{unique_stocks:,}</div>", unsafe_allow_html=True)
            st.markdown("<div class='metric-label'>Unique Stocks</div>", unsafe_allow_html=True)
            st.markdown("</div>", unsafe_allow_html=True)
            
        with col3:
            st.markdown("<div class='card'>", unsafe_allow_html=True)
            years_span = df_filtered['year'].max() - df_filtered['year'].min()
            st.markdown(f"<div class='metric-value'>{years_span}</div>", unsafe_allow_html=True)
            st.markdown("<div class='metric-label'>Years Covered</div>", unsafe_allow_html=True)
            st.markdown("</div>", unsafe_allow_html=True)
            
        with col4:
            st.markdown("<div class='card'>", unsafe_allow_html=True)
            avg_changes = round(len(df_filtered) / years_span if years_span > 0 else 0)
            st.markdown(f"<div class='metric-value'>{avg_changes}</div>", unsafe_allow_html=True)
            st.markdown("<div class='metric-label'>Avg. Changes/Year</div>", unsafe_allow_html=True)
            st.markdown("</div>", unsafe_allow_html=True)
        
        # Visualizations
        tab1, tab2, tab3, tab4 = st.tabs(["Time Analysis", "Market Distribution", "Change Reasons", "Data Explorer"])
        
        with tab1:
            st.markdown("<div class='sub-header'>Name Changes Over Time</div>", unsafe_allow_html=True)
            
            # Show yearly trend
            yearly_counts = df_filtered.groupby('year').size().reset_index(name='count')
            
            # Plotly chart
            fig_yearly = px.bar(
                yearly_counts, 
                x='year', 
                y='count',
                title='Stock Name Changes by Year',
                labels={'year': 'Year', 'count': 'Number of Name Changes'},
                color='count',
                color_continuous_scale='Blues'
            )
            fig_yearly.update_layout(height=500)
            st.plotly_chart(fig_yearly, use_container_width=True)
            
            # Monthly pattern
            st.markdown("<div class='sub-header'>Monthly Pattern</div>", unsafe_allow_html=True)
            monthly_counts = df_filtered.groupby('month').size().reset_index(name='count')
            monthly_counts = monthly_counts.sort_values('month')
            
            fig_monthly = px.bar(
                monthly_counts,
                x='month',
                y='count',
                title='Stock Name Changes by Month',
                labels={'month': 'Month', 'count': 'Number of Name Changes'},
                color='count',
                color_continuous_scale='Greens'
            )
            fig_monthly.update_layout(height=400)
            fig_monthly.update_xaxes(tickvals=list(range(1, 13)), 
                                    ticktext=['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                                              'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'])
            st.plotly_chart(fig_monthly, use_container_width=True)
            
        with tab2:
            st.markdown("<div class='sub-header'>Market Distribution</div>", unsafe_allow_html=True)
            
            # Market code distribution
            market_counts = df_filtered['market_code'].value_counts().reset_index()
            market_counts.columns = ['Market Code', 'Count']
            
            col1, col2 = st.columns([2, 3])
            
            with col1:
                st.dataframe(market_counts, use_container_width=True, hide_index=True)
                
            with col2:
                fig_market = px.pie(
                    market_counts,
                    values='Count',
                    names='Market Code',
                    title='Distribution by Market Code',
                    color_discrete_sequence=px.colors.qualitative.Pastel
                )
                fig_market.update_traces(textposition='inside', textinfo='percent+label')
                st.plotly_chart(fig_market, use_container_width=True)
            
            # Market code trends over time
            st.markdown("<div class='sub-header'>Market Code Trends Over Time</div>", unsafe_allow_html=True)
            
            market_year_counts = df_filtered.groupby(['year', 'market_code']).size().reset_index(name='count')
            
            fig_market_trend = px.line(
                market_year_counts,
                x='year',
                y='count',
                color='market_code',
                title='Name Changes by Market Code Over Time',
                labels={'year': 'Year', 'count': 'Number of Name Changes', 'market_code': 'Market Code'}
            )
            fig_market_trend.update_layout(height=500)
            st.plotly_chart(fig_market_trend, use_container_width=True)
        
        with tab3:
            if 'changereason' in df_filtered.columns:
                st.markdown("<div class='sub-header'>Change Reasons Analysis</div>", unsafe_allow_html=True)
                
                # Filter out NaN values for analysis
                df_with_reasons = df_filtered.dropna(subset=['changereason'])
                
                # Change reason counts
                reason_counts = df_with_reasons['changereason'].value_counts().reset_index()
                reason_counts.columns = ['Change Reason', 'Count']
                reason_counts = reason_counts.sort_values('Count', ascending=False)
                
                # Display top reasons
                st.markdown("#### Top Change Reasons")
                st.dataframe(reason_counts.head(10), use_container_width=True, hide_index=True)
                
                # Visualize reason distribution
                fig_reason = px.bar(
                    reason_counts.head(10),
                    x='Change Reason',
                    y='Count',
                    title='Top 10 Change Reasons',
                    color='Count',
                    color_continuous_scale='Viridis'
                )
                fig_reason.update_layout(height=500)
                st.plotly_chart(fig_reason, use_container_width=True)
                
                # Reasons over time
                st.markdown("#### Change Reasons Over Time")
                reason_year = df_with_reasons.groupby(['year', 'changereason']).size().reset_index(name='count')
                
                # Get top 5 reasons for visualization
                top_reasons = reason_counts['Change Reason'].head(5).tolist()
                reason_year_filtered = reason_year[reason_year['changereason'].isin(top_reasons)]
                
                fig_reason_trend = px.line(
                    reason_year_filtered,
                    x='year',
                    y='count',
                    color='changereason',
                    title='Top 5 Change Reasons Over Time',
                    labels={'year': 'Year', 'count': 'Number of Name Changes', 'changereason': 'Change Reason'}
                )
                fig_reason_trend.update_layout(height=500)
                st.plotly_chart(fig_reason_trend, use_container_width=True)
            else:
                st.info("Change reason data is not available in the dataset.")
        
        with tab4:
            st.markdown("<div class='sub-header'>Data Explorer</div>", unsafe_allow_html=True)
            
            # Search functionality
            search_term = st.text_input("Search Stock Code or Name", "")
            
            if search_term:
                search_results = df_filtered[
                    (df_filtered['ts_code'].str.contains(search_term, case=False, na=False)) | 
                    (df_filtered['s_info_name'].str.contains(search_term, case=False, na=False))
                ]
                st.dataframe(search_results, use_container_width=True)
            else:
                # Show recent changes
                st.markdown("#### Recent Stock Name Changes")
                recent_changes = df_filtered.sort_values('begindate', ascending=False).head(100)
                st.dataframe(recent_changes, use_container_width=True)
            
            # Download button for filtered data
            csv = df_filtered.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="Download Filtered Data as CSV",
                data=csv,
                file_name=f"stock_name_changes_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv",
            )
            
            # Show stocks with multiple name changes
            st.markdown("#### Stocks with Multiple Name Changes")
            
            stock_change_counts = df_filtered['ts_code'].value_counts().reset_index()
            stock_change_counts.columns = ['Stock Code', 'Name Change Count']
            multiple_changes = stock_change_counts[stock_change_counts['Name Change Count'] > 1]
            multiple_changes = multiple_changes.sort_values('Name Change Count', ascending=False)
            
            st.dataframe(multiple_changes.head(20), use_container_width=True, hide_index=True)
            
            # Visualize multiple name changes
            if not multiple_changes.empty:
                fig_multiple = px.bar(
                    multiple_changes.head(20),
                    x='Stock Code',
                    y='Name Change Count',
                    title='Top 20 Stocks by Number of Name Changes',
                    color='Name Change Count',
                    color_continuous_scale='Reds'
                )
                fig_multiple.update_layout(height=500)
                st.plotly_chart(fig_multiple, use_container_width=True)
    else:
        st.error("Failed to load data from MongoDB. Please check your connection settings.")
        
except Exception as e:
    st.error(f"An error occurred: {str(e)}")
    st.code(f"Error details: {str(e)}")