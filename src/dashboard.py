import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from pathlib import Path
from google.cloud import storage
import io
import fastparquet
import os
from dotenv import load_dotenv

# --- SETUP ---
# Load environment variables (e.g. Bucket Names) from .env file
load_dotenv()

# --- CONFIGURATION ---
ST_PAGE_TITLE = "🪙 Crypto Strategy Command Center"

# 🔑 Load Config
# I use os.getenv to keep sensitive bucket names out of the source code
CLOUD_BUCKET_NAME = os.getenv("GOLD_BUCKET_NAME")
PARQUET_FILENAME = "analyzed_market_summary.parquet"

# Define Paths
BASE_DIR = Path(__file__).resolve().parent.parent
LOCAL_GOLD_PATH = BASE_DIR / "data" / "gold" / PARQUET_FILENAME

# Setup page config (Must be the first Streamlit command)
st.set_page_config(page_title=ST_PAGE_TITLE, layout="wide")
st.title(f"📊 {ST_PAGE_TITLE}")

# --- DATA LOADER ---
@st.cache_data(ttl=600)
def load_data(source_mode):
    """
    Loads the analyzed 'Gold Layer' data based on the selected mode.

    This function uses Streamlit's caching mechanism (@st.cache_data) to store 
    the result for 10 minutes (600s). This prevents re-downloading the large 
    Parquet file on every user interaction (clicking buttons, changing filters).

    Args:
        source_mode (str): The data source to read from. 
                           Options: "LOCAL" (Disk) or "CLOUD" (GCS Bucket).

    Returns:
        pd.DataFrame: A pandas DataFrame containing the historical market data.
                      Returns an empty DataFrame if the file is missing or corrupt.
    """
    # 1. LOCAL MODE: Read from the local 'data/gold' folder
    if source_mode == "LOCAL":
        if not LOCAL_GOLD_PATH.exists():
            st.error(f"❌ Local file not found: {LOCAL_GOLD_PATH}")
            st.warning("Tip: Run 'python run_pipeline.py --mode local' to generate it.")
            return pd.DataFrame()

        try:
            return pd.read_parquet(LOCAL_GOLD_PATH, engine='fastparquet')
        except Exception as error:
            st.error(f"❌ Error reading local file: {error}")
            return pd.DataFrame()

    # 2. CLOUD MODE: Read directly from Google Cloud Storage.
    elif source_mode == "CLOUD":
        try:
            # Initialize Client
            storage_client = storage.Client()
            bucket = storage_client.bucket(CLOUD_BUCKET_NAME)
            blob = bucket.blob(PARQUET_FILENAME)

            if not blob.exists():
                st.error(f"❌ Cloud file not found: gs://{CLOUD_BUCKET_NAME}/{PARQUET_FILENAME}")
                return pd.DataFrame()

            # Download as bytes in memory
            data_bytes = blob.download_as_bytes()
            return pd.read_parquet(io.BytesIO(data_bytes), engine='fastparquet')

        except Exception as error:
            st.error(f"❌ Cloud Connection Failed: {error}")
            st.warning("Tip: Did you run 'gcloud auth application-default login'?")
            return pd.DataFrame()

# --- MAIN APP ---
def main():
    """
    The Main Driver Function for the Streamlit Dashboard.

    Orchestrates the UI layout:
    1. Sidebar: Controls for Data Source (Local/Cloud) and Asset Selection.
    2. Data Loading: Fetches data via load_data().
    3. Transformation: Filters data by the selected Coin ID.
    4. Visualization: Renders KPI Metrics and Plotly Interactive Charts.
    """
    # Sidebar configuration
    st.sidebar.header("⚙️ Settings")

    # The Mode Toggle
    data_source = st.sidebar.radio(
        "Data Source:",
        ("LOCAL", "CLOUD"),
        index=0, 
        help="Switch between live Cloud Storage data and local disk data."
    )

    # I check this after the user selects a mode
    if data_source == "CLOUD" and not CLOUD_BUCKET_NAME:
        st.error("❌ GOLD_BUCKET_NAME not found in environment variables.")
        st.info("Please create a .env file with your bucket name.")
        st.stop()

    # Show status badge
    if data_source == "CLOUD":
        st.success(f"☁️ Live Mode: Connected to {CLOUD_BUCKET_NAME}")
    else:
        st.info(f"🏠 Dev Mode: Reading from local disk")

    # Load data
    df = load_data(data_source)

    # --- SIDEBAR FOOTER ---
    st.sidebar.markdown("---")
    st.sidebar.caption(f"Engine: fastparquet v{fastparquet.__version__}")

    if df.empty:
        return

    # Sidebar Filters
    st.sidebar.header("🔍 Filters")

    # 1. Identify Coin Column (Handles schema variations)
    coin_col = 'symbol' if 'symbol' in df.columns else 'coin_id'

    if coin_col in df.columns:
        all_coins = df[coin_col].unique()
        selected_coin = st.sidebar.selectbox("Select Asset", all_coins, index=0)

        # Filter Data for the specific coin
        coin_df = df[df[coin_col] == selected_coin].copy()

    # 2. Identify Time Column
        time_col = None
        # PRIORITY: Use the actual source time if available!
        if 'source_updated_at' in coin_df.columns:
            time_col = 'source_updated_at'
        elif 'ingested_timestamp' in coin_df.columns:
            time_col = 'ingested_timestamp'
        elif 'analyzed_at' in coin_df.columns:
            time_col = 'analyzed_at'
        elif 'timestamp' in coin_df.columns:
            time_col = 'timestamp'

        if not time_col:
            st.error("❌ Data missing timestamp column (checked: ingested_timestamp, analyzed_at).")
            st.write("Available columns:", list(coin_df.columns))
            return

        # Sort by time for correct plotting
        coin_df[time_col] = pd.to_datetime(coin_df[time_col])
        coin_df = coin_df.sort_values(time_col)

        # --- KPI METRICS ---
        if not coin_df.empty:
            latest = coin_df.iloc[-1]

            # Safe Getters to prevent KeyErrors
            price = latest.get('current_price', 0)
            sma = latest.get('sma_7d', 0)
            vol = latest.get('volatility_7d', 0.0)
            signal = latest.get('signal', 'N/A')

            vol_display = f"{vol:,.2f}" if pd.notna(vol) else "0.00"

            # RETRIEVE RSI (Safe get, default to 50 if missing)
            rsi = latest.get('rsi_14d', 50.0)

            # UPDATE: Change columns from 4 to 5
            col1, col2, col3, col4, col5 = st.columns(5)

            with col1: st.metric("Current Price", f"${price:,.2f}")
            with col2: st.metric("7-Day SMA", f"${sma:,.2f}")
            with col3: st.metric("Volatility Index", vol_display) 
            with col4: 
                if signal == "BUY":
                    st.success(f"🟢 {signal}")
                elif signal == "SELL":
                    st.error(f"🔴 {signal}")
                else:
                    st.metric("Signal", signal)

            # NEW: Add the RSI Metric
            with col5:
                rsi_val = f"{rsi:.1f}"
                if rsi > 70:
                    st.metric("14-Day RSI", rsi_val, "Overbought", delta_color="inverse")
                elif rsi < 30:
                    st.metric("14-Day RSI", rsi_val, "Oversold", delta_color="normal")
                else:
                    st.metric("14-Day RSI", rsi_val, "Neutral", delta_color="off")

        # --- CHART ---
        st.subheader(f"📈 Price Trend: {selected_coin.upper()}")

        fig = go.Figure()

        # Trace 1: Actual Price
        fig.add_trace(go.Scatter(
            x=coin_df[time_col], 
            y=coin_df['current_price'], 
            mode='lines+markers', 
            name='Price', 
            line=dict(color='#00CC96', width=2)
        ))

        # Trace 2: 7-Day SMA
        if sma > 0:
            fig.add_trace(go.Scatter(
                x=coin_df[time_col], 
                y=coin_df['sma_7d'], 
                mode='lines', 
                name='7-Day SMA', 
                line=dict(color='#EF553B', dash='dash', width=2)
            ))

        fig.update_layout(
            template="plotly_dark", 
            height=500, 
            xaxis_title="Time (Ingested)", 
            yaxis_title="Price (USD)",
            legend=dict(orientation="h", y=1.1)
        )

        st.plotly_chart(fig, use_container_width=True)

        # New RSI Chart
        st.subheader("📊 Momentum (RSI)")

        fig_rsi = go.Figure()

        # Trace 1: RSI Line
        fig_rsi.add_trace(go.Scatter(
            x=coin_df[time_col],
            y=coin_df['rsi_14d'],
            mode='lines',
            name='RSI',
            line=dict(color='#AB63FA', width=2)
        ))

        # Add Reference Lines (70 and 30)
        fig_rsi.add_hline(y=70, line_dash="dot", line_color="red", annotation_text="Overbought (70)")
        fig_rsi.add_hline(y=30, line_dash="dot", line_color="#00CC96", annotation_text="Oversold (30)")

        # Shade the "Normal" area
        fig_rsi.add_hrect(y0=30, y1=70, line_width=0, fillcolor="rgba(255, 255, 255, 0.1)", layer="below")

        fig_rsi.update_layout(
            template="plotly_dark",
            height=250, # Shorter chart for Oscillator
            yaxis=dict(range=[0, 100]), # Fixed RSI Range
            xaxis_title="Time (Ingested)",
            margin=dict(t=20) # Tight margins
        )

        st.plotly_chart(fig_rsi, use_container_width=True)

        # Start of Heatmap Section
        st.markdown("---")
        st.subheader("🔥 Risk Heatmap (Correlation Matrix)")
        st.caption("Do assets move together? (1.0 = Identical Movement, 0.0 = No Relation)")

        try:
            # 1. Preparation
            heatmap_data = df[[time_col, coin_col, 'current_price']].copy()

            # Ensure proper types
            heatmap_data[time_col] = pd.to_datetime(heatmap_data[time_col])
            heatmap_data['current_price'] = pd.to_numeric(heatmap_data['current_price'], errors='coerce')

            # This makes BTC at 10:00:05 and ETH at 10:00:12 align on the same row.
            heatmap_data['aligned_time'] = heatmap_data[time_col].dt.floor('h') 

            # 2. Pivot
            pivot_df = heatmap_data.pivot_table(
                index='aligned_time', 
                columns=coin_col, 
                values='current_price'
            )

            # 3. Fill Gaps
            # ffill() ensures that if a coin misses one hour, I assume price stayed stable.
            pivot_df = pivot_df.ffill().bfill()

            # 4. Calculate Correlation
            # I need at least 2 periods of overlapping data to calculate a trend
            corr_matrix = pivot_df.corr(min_periods=2)

            # 5. Render
            st.dataframe(
                corr_matrix.style.format("{:.2f}").background_gradient(cmap="RdYlGn", axis=None, vmin=-1, vmax=1),
                use_container_width=True
            )

        except Exception as error:
            st.warning(f"⚠️ Not enough data to generate heatmap yet. Need multiple assets. ({error})")

        # Raw Data Expander (Existing Code)
        with st.expander("See Raw Data"):
            st.dataframe(coin_df.sort_values(time_col, ascending=False))
    else:
        st.error(f"Data missing '{coin_col}' column.")

if __name__ == "__main__":
    main()
