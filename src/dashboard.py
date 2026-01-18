import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from pathlib import Path
from google.cloud import storage
import io

# --- CONFIGURATION ---
ST_PAGE_TITLE = "🪙 Crypto Strategy Command Center"
CLOUD_BUCKET_NAME = "cdp-gold-analyze-bucket" 
CLOUD_BLOB_NAME = "analytics/market_summary.parquet"

BASE_DIR = Path(__file__).resolve().parent.parent
LOCAL_GOLD_PATH = BASE_DIR / "data" / "gold" / "analyzed_market_summary.parquet"

# Setup page config
st.set_page_config(page_title=ST_PAGE_TITLE, layout="wide")
st.title(f"📊 {ST_PAGE_TITLE}")

# --- DATA LOADER ---
@st.cache_data(ttl=600)  # Cache for 10 mins
def load_data(source_mode):
    """
    Loads Gold Layer data based on the selected mode (LOCAL or CLOUD).

    This function serves as the primary data ingestion point for the dashboard.
    It handles switching between local disk storage (for development) and 
    Google Cloud Storage (for production), including error handling for missing files
    or connection issues.

    Args:
        source_mode (str): The data source to read from. 
                           Expected values: "LOCAL" or "CLOUD".

    Returns:
        pd.DataFrame: A pandas DataFrame containing the market summary data.
                      Returns an empty DataFrame if the file is missing or an error occurs.
    """
    if source_mode == "LOCAL":
        # st.info(f"🏠 Mode: LOCAL (Reading from {LOCAL_GOLD_PATH})")

        if not LOCAL_GOLD_PATH.exists():
            st.error(f"❌ Local file not found: {LOCAL_GOLD_PATH}")
            st.warning("Tip: Run 'python src/pipeline/run_pipeline.py' to generate local data.")
            return pd.DataFrame()

        try:
            return pd.read_parquet(LOCAL_GOLD_PATH)
        except Exception as error:
            st.error(f"❌ Error reading local file: {error}")
            return pd.DataFrame()

    elif source_mode == "CLOUD":
        # st.info(f"☁️ Mode: CLOUD (Reading from gs://{CLOUD_BUCKET_NAME})")

        try:
            # Download from GCS into memory (no local file needed)
            storage_client = storage.Client()
            bucket = storage_client.bucket(CLOUD_BUCKET_NAME)
            blob = bucket.blob(CLOUD_BLOB_NAME)

            if not blob.exists():
                st.error(f"❌ Cloud file not found: gs://{CLOUD_BUCKET_NAME}/{CLOUD_BLOB_NAME}")
                return pd.DataFrame()

            data_bytes = blob.download_as_bytes()
            return pd.read_parquet(io.BytesIO(data_bytes))

        except Exception as error:
            st.error(f"❌ Cloud Connection Failed: {error}")
            st.warning("Tip: Did you run 'gcloud auth application-default login'?")
            return pd.DataFrame()

# --- MAIN APP ---
def main():
    """
    The main execution entry point for the Streamlit dashboard.

    Orchestrates the UI layout, including:
    1. Sidebar configuration (Data Source toggle, Asset filters).
    2. Data loading based on user selection.
    3. KPI Metric display (Price, SMA, Volatility, Signal).
    4. Interactive Plotly chart rendering.
    """
    # Sidebar Configuration
    st.sidebar.header("⚙️ Settings")
    
    # 1. The Mode Toggle (The Refactor Feature!)
    data_source = st.sidebar.radio(
        "Data Source:",
        ("CLOUD", "LOCAL"),
        index=0, # Default to Cloud
        help="Switch between live Cloud Storage data and local disk data."
    )
    
    # Show status badge
    if data_source == "CLOUD":
        st.success(f"☁️ Live Mode: Connected to {CLOUD_BUCKET_NAME}")
    else:
        st.info(f"🏠 Dev Mode: Reading from local disk")

    # Load Data dynamically based on toggle
    df = load_data(data_source)
    
    if df.empty:
        st.warning("⚠️ No data available. Please check your source.")
        return

    # Sidebar Filters
    st.sidebar.header("🔍 Filters")
    all_coins = df['coin_id'].unique()
    selected_coin = st.sidebar.selectbox("Select Asset", all_coins, index=0)

    # Filter & Sort
    coin_df = df[df['coin_id'] == selected_coin].copy()
    coin_df = coin_df.sort_values("extraction_timestamp")

    # --- KPI METRICS ---
    if not coin_df.empty:
        latest = coin_df.iloc[-1]
        
        # Format Volatility safely
        vol = latest.get('volatility_7d', 0.0)
        vol_display = f"{vol:,.2f}" if pd.notna(vol) else "0.00"

        # Create 4 columns for metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1: st.metric("Current Price", f"${latest['price_usd']:,.2f}")
        with col2: st.metric("7-Day SMA", f"${latest['sma_7d']:,.2f}")
        with col3: st.metric("Volatility Index", vol_display) 
        with col4: st.metric("Trading Signal", latest['signal'])

    # --- CHART ---
    st.subheader(f"📈 Price Trend: {selected_coin.upper()}")

    fig = go.Figure()
    # Line: Actual Price
    fig.add_trace(go.Scatter(
        x=coin_df['extraction_timestamp'], 
        y=coin_df['price_usd'], 
        mode='lines', 
        name='Price', 
        line=dict(color='#00CC96', width=2)
    ))
    # Line: SMA (Dashed)
    fig.add_trace(go.Scatter(
        x=coin_df['extraction_timestamp'], 
        y=coin_df['sma_7d'], 
        mode='lines', 
        name='7-Day SMA', 
        line=dict(color='#EF553B', dash='dash', width=2)
    ))

    fig.update_layout(
        template="plotly_dark", 
        height=500, 
        xaxis_title="Timestamp", 
        yaxis_title="Price (USD)",
        legend=dict(orientation="h", y=1.1)
    )

    st.plotly_chart(fig, use_container_width=True)

    # Raw Data Expander
    with st.expander("See Raw Data"):
        st.dataframe(coin_df.sort_values("extraction_timestamp", ascending=False))

if __name__ == "__main__":
    main()
