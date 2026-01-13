import streamlit as st
import pandas as pd
import duckdb
from google.cloud import storage
import plotly.graph_objects as go
import os

# Setup config
GOLD_BUCKET_NAME = "crypto-gold-crypto-platform-carlo-2026"  
LOCAL_DATA_PATH = "/tmp/market_summary.parquet"

# Page setup
st.set_page_config(page_title="Cryptop Strategy Dashboard", layout="wide")
st.title("Crypto Data Platform: Strategy Command Center")

# Data loader
@st.cache_data(ttl=600)
def load_data():
    # Downloads the latest data from GCS
    if not os.path.exists(LOCAL_DATA_PATH):
        try:
            storage_client = storage.Client()
            bucket = storage_client.bucket(GOLD_BUCKET_NAME)
            blob = bucket.blob("analytics/market_summary.parquet")
            blob.download_to_filename(LOCAL_DATA_PATH)
            st.toast("Data downloaded from Gold Layer!", icon="☁️")
        except Exception as error:
            st.error(f"Failed to download data: {error}")
            return pd.DataFrame()
    
    # Query with DuckDB
    con = duckdb.connect()
    df = con.execute(f"SELECT * FROM read_parquet('{LOCAL_DATA_PATH}')").df()
    return df

# Main
df = load_data()

if not df.empty:
    # Get unique coins
    coins = df['coin_id'].unique()
    
    # Create tabs for each coin
    tabs = st.tabs([c.upper() for c in coins])

    for coin_index, coin in enumerate(coins):
        with tabs[coin_index]:
            # Filter data for this coin
            coin_data = df[df['coin_id'] == coin].sort_values('recorded_at')
            latest = coin_data.iloc[-1]

            # 1. Headlines (Metrics)
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Current Price", f"${latest['price_usd']:,.2f}")
            with col2:
                st.metric("7-Day SMA", f"${latest['sma_7d']:,.2f}")
            with col3:
                # Signal Logic Display
                signal = latest['signal']
                color = "green" if signal == "BUY" else "red"
                st.markdown(f"### Signal: :{color}[{signal}]")

            # 2. Chart (Price vs SMA)
            fig = go.Figure()
            
            # Price Line
            fig.add_trace(go.Scatter(
                x=coin_data['recorded_at'], 
                y=coin_data['price_usd'],
                mode='lines',
                name='Price (USD)',
                line=dict(color='cyan')
            ))

            # SMA Line
            fig.add_trace(go.Scatter(
                x=coin_data['recorded_at'], 
                y=coin_data['sma_7d'],
                mode='lines',
                name='7-Day SMA',
                line=dict(color='orange', dash='dash')
            ))

            fig.update_layout(title=f"{coin.upper()} Strategy Analysis", template="plotly_dark")
            st.plotly_chart(fig, use_container_width=True)

            # 3. Raw Data (Expandable)
            with st.expander("See Raw Data"):
                st.dataframe(coin_data.sort_values('recorded_at', ascending=False))
else:
    st.warning("No data found. Make sure the Gold Pipeline has run successfully!")

# Sidebar Info
st.sidebar.markdown("### 🏗 Pipeline Status")
st.sidebar.success("Gold Layer: Connected")
st.sidebar.info("Updates every 10 minutes")
