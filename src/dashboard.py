import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from pathlib import Path
from google.cloud import storage
import io
import os
from dotenv import load_dotenv, find_dotenv

# --- SETUP ---
# Automatically find and load .env file
load_dotenv(find_dotenv())

# --- CONFIGURATION ---
ST_PAGE_TITLE = "🪙 Crypto Strategy Command Center"
CLOUD_BUCKET_NAME = os.getenv("GOLD_BUCKET_NAME")
PARQUET_FILENAME = "analyzed_market_summary.parquet"

# Define Paths
BASE_DIR = Path(__file__).resolve().parent.parent
LOCAL_GOLD_PATH = BASE_DIR / "data" / "gold" / PARQUET_FILENAME

# Setup page config
st.set_page_config(page_title=ST_PAGE_TITLE, layout="wide", page_icon="🪙")
st.title(f"📊 {ST_PAGE_TITLE}")

# --- DATA LOADER ---
@st.cache_data(ttl=600)
def load_data(source_mode):
    """
    Loads the 'Gold Layer' analytics data from the selected source.

    This function uses Streamlit's caching mechanism (@st.cache_data) to store 
    the result for 10 minutes (600s), preventing redundant network requests or 
    disk reads during user interaction.

    Args:
        source_mode (str): The data origin.
            - "LOCAL": Reads 'analyzed_market_summary.parquet' from the local 'data/gold/' directory.
                       Useful for development and backtesting results.
            - "CLOUD": Downloads the same file directly from the configured Google Cloud Storage bucket.
                       Useful for monitoring the live production pipeline.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the historical market data, 
                      enriched with financial metrics (RSI, SMA, FDV, Volume).
                      Returns an empty DataFrame if the file is missing or connection fails.
    """
    # 1. LOCAL MODE
    if source_mode == "LOCAL":
        if not LOCAL_GOLD_PATH.exists():
            st.error(f"❌ Local file not found: {LOCAL_GOLD_PATH}")
            st.warning("Tip: Run 'make local' to generate it.")
            return pd.DataFrame()
        try:
            return pd.read_parquet(LOCAL_GOLD_PATH, engine='fastparquet')
        except Exception as error:
            st.error(f"❌ Error reading local file: {error}")
            return pd.DataFrame()

    # 2. CLOUD MODE
    elif source_mode == "CLOUD":
        if not CLOUD_BUCKET_NAME:
            st.error("❌ GOLD_BUCKET_NAME not found in .env file.")
            return pd.DataFrame()
        try:
            storage_client = storage.Client()
            bucket = storage_client.bucket(CLOUD_BUCKET_NAME)
            blob = bucket.blob(PARQUET_FILENAME)

            if not blob.exists():
                st.error(f"❌ Cloud file not found: gs://{CLOUD_BUCKET_NAME}/{PARQUET_FILENAME}")
                return pd.DataFrame()

            data_bytes = blob.download_as_bytes()
            return pd.read_parquet(io.BytesIO(data_bytes), engine='fastparquet')
        except Exception as error:
            st.error(f"❌ Cloud Connection Failed: {error}")
            return pd.DataFrame()

    return pd.DataFrame()

# --- HELPER: METRIC FORMATTER ---
def format_large_number(num):
    """
    Formats large financial figures into human-readable strings (e.g., 1.5B, 200M).

    Used specifically for high-value metrics like Market Cap, Fully Diluted Valuation (FDV),
    and Trading Volume, where raw integers are difficult to scan quickly.

    Args:
        num (float): The raw number to format.

    Returns:
        str: A formatted string (e.g., '$1.25B', '$500.00M', or '$10,500').
             Returns 'N/A' if the input is null/NaN.
    """
    if pd.isna(num): return "N/A"
    if num >= 1_000_000_000: return f"${num/1_000_000_000:.2f}B"
    if num >= 1_000_000: return f"${num/1_000_000:.2f}M"
    return f"${num:,.0f}"

# --- MAIN APP ---
def main():
    """
    The Main Driver Function for the Streamlit Strategy Command Center.

    Orchestrates the entire UI workflow:
    1. **Configuration**: Toggles between LOCAL (Dev) and CLOUD (Live) data sources.
    2. **Data Loading**: Fetches the latest 'Rich Schema' dataset (including FDV, Rank, Supply).
    3. **Asset Profile**: Displays high-level stats (Market Rank, ATH Distance, Current Price).
    4. **Financial Analysis**: 
       - Renders Trading Signals (BUY/SELL/WAIT).
       - Visualizes Momentum (RSI) and Trends (SMA).
       - Compares Market Cap vs. FDV to assess token dilution.
    5. **Correlation**: Generates a heatmap to show how assets move relative to each other.
    """
    # Sidebar: Mode Selection
    st.sidebar.header("⚙️ Settings")
    data_source = st.sidebar.radio("Data Source:", ("LOCAL", "CLOUD"), index=0)

    # Status Indicators
    if data_source == "CLOUD":
        st.sidebar.success(f"☁️ Connected: {CLOUD_BUCKET_NAME}")
    else:
        st.sidebar.info(f"🏠 Mode: Local Disk")

    # Load Data
    df = load_data(data_source)

    if df.empty:
        st.warning("⚠️ No data loaded. Run the pipeline first.")
        return

    # Ensure Timestamp Column Exists
    time_col = 'source_updated_at'
    if time_col not in df.columns:
        st.error(f"❌ Critical: Missing '{time_col}' in dataset.")
        return

    # Convert & Sort
    df[time_col] = pd.to_datetime(df[time_col])
    df = df.sort_values(time_col)

    # Sidebar: Asset Selection
    coin_col = 'symbol' if 'symbol' in df.columns else 'coin_id'
    all_coins = df[coin_col].unique()
    selected_coin = st.sidebar.selectbox("Select Asset", all_coins, index=0)

    # Filter Data
    coin_df = df[df[coin_col] == selected_coin].copy()
    latest = coin_df.iloc[-1]

# --- 1. ASSET HEADER (RICH METRICS) ---
    st.markdown("---")
    col_h1, col_h2, col_h3, col_h4 = st.columns(4)

    with col_h1:
        st.metric("Asset Name", latest.get('name', selected_coin))

    with col_h2:
        # Get rank without default 'N/A' string to avoid type errors
        rank = latest.get('market_cap_rank') 

        # Check if rank exists and is a valid number
        if pd.notna(rank):
            try:
                st.metric("Market Rank", f"#{int(float(rank))}")
            except (ValueError, TypeError):
                st.metric("Market Rank", "N/A")
        else:
            st.metric("Market Rank", "N/A")

    with col_h3:
        st.metric("Current Price", f"${latest['current_price']:,.4f}")

    with col_h4:
        # Calculate Distance from ATH
        ath = latest.get('ath', 0)
        current = latest['current_price']
        if ath > 0:
            pct_down = ((current - ath) / ath) * 100
            st.metric("All-Time High", f"${ath:,.2f}", f"{pct_down:.1f}%", delta_color="inverse")
        else:
            st.metric("All-Time High", "N/A")

    # --- 2. TRADING SIGNALS & DEPTH ---
    st.markdown("### 🚦 Trading Signals & Market Depth")
    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        signal = latest.get('signal', 'WAIT')
        if signal == "BUY": st.success(f"🟢 BUY SIGNAL")
        elif signal == "SELL": st.error(f"🔴 SELL SIGNAL")
        else: st.info(f"⚪ WAIT")

    with col2:
        rsi = latest.get('rsi_14d', 50)
        st.metric("RSI (14d)", f"{rsi:.1f}", "Overbought > 70" if rsi > 70 else "Oversold < 30" if rsi < 30 else "Neutral", delta_color="off")

    with col3:
        volatility = latest.get('volatility_7d', 0)
        st.metric("Volatility (7d)", f"{volatility:.2f}")

    with col4:
        # Market Cap vs FDV
        mc = latest.get('market_cap', 0)
        fdv = latest.get('fully_diluted_valuation', 0)
        st.metric("Market Cap", format_large_number(mc))

    with col5:
        st.metric("FDV (Theoretical)", format_large_number(fdv))

    # --- 3. CHARTS ---
    st.markdown("---")
    tab1, tab2 = st.tabs(["📈 Price Action", "📊 Volume & Supply"])

    with tab1:
        # Price & SMA Chart
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=coin_df[time_col], y=coin_df['current_price'], mode='lines+markers', name='Price', line=dict(color='#00CC96')))

        if 'sma_7d' in coin_df.columns:
            fig.add_trace(go.Scatter(x=coin_df[time_col], y=coin_df['sma_7d'], mode='lines', name='SMA 7D', line=dict(color='#EF553B', dash='dash')))

        fig.update_layout(template="plotly_dark", height=500, title=f"{selected_coin.upper()} Price Trend")
        st.plotly_chart(fig, use_container_width=True)

        # RSI Chart
        fig_rsi = go.Figure()
        fig_rsi.add_trace(go.Scatter(x=coin_df[time_col], y=coin_df['rsi_14d'], mode='lines', name='RSI', line=dict(color='#AB63FA')))
        fig_rsi.add_hline(y=70, line_dash="dot", line_color="red")
        fig_rsi.add_hline(y=30, line_dash="dot", line_color="#00CC96")
        fig_rsi.update_layout(template="plotly_dark", height=250, yaxis=dict(range=[0, 100]), title="Momentum (RSI)")
        st.plotly_chart(fig_rsi, use_container_width=True)

    with tab2:
        # Volume Chart
        if 'total_volume' in coin_df.columns:
            fig_vol = go.Figure()
            fig_vol.add_trace(go.Bar(x=coin_df[time_col], y=coin_df['total_volume'], name='Volume', marker_color='#636EFA'))
            fig_vol.update_layout(template="plotly_dark", height=400, title="24h Trading Volume")
            st.plotly_chart(fig_vol, use_container_width=True)
        else:
            st.info("Volume data not available.")

    # --- 4. CORRELATION MATRIX ---
    st.markdown("### 🔥 Market Correlation")
    try:
        # Pivot Data for Correlation
        pivot_df = df.pivot_table(index=time_col, columns=coin_col, values='current_price')
        # Resample to Hourly to align timestamps and handle gaps
        pivot_df = pivot_df.resample('h').mean().ffill().bfill()

        if len(pivot_df.columns) > 1:
            corr = pivot_df.corr()
            st.dataframe(corr.style.background_gradient(cmap="RdYlGn", vmin=-1, vmax=1).format("{:.2f}"), use_container_width=True)
        else:
            st.info("Need more than 1 asset to calculate correlation.")
    except Exception as error:
        st.warning(f"Could not calculate correlation: {error}")

    # --- RAW DATA ---
    with st.expander("📂 View Raw Data"):
        st.dataframe(coin_df.sort_values(time_col, ascending=False))

if __name__ == "__main__":
    main()
