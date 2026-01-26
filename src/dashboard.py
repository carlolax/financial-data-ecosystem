import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from pathlib import Path
from google.cloud import storage
import io
import fastparquet

# --- CONFIGURATION ---
ST_PAGE_TITLE = "🪙 Crypto Strategy Command Center"
CLOUD_BUCKET_NAME = "cdp-gold-analyze-bucket"
CLOUD_BLOB_NAME = "analytics/market_summary.parquet"

BASE_DIR = Path(__file__).resolve().parent.parent
LOCAL_GOLD_PATH = BASE_DIR / "data" / "gold" / "analyzed_market_data.parquet"

# Setup page config
st.set_page_config(page_title=ST_PAGE_TITLE, layout="wide")
st.title(f"📊 {ST_PAGE_TITLE}")

# --- DATA LOADER ---
@st.cache_data(ttl=600)
def load_data(source_mode):
    # Loads Gold Layer data based on the selected mode (LOCAL or CLOUD).
    if source_mode == "LOCAL":
        if not LOCAL_GOLD_PATH.exists():
            st.error(f"❌ Local file not found: {LOCAL_GOLD_PATH}.")
            st.warning("Tip: Run 'python src/pipeline/run_pipeline.py' to generate local data.")
            return pd.DataFrame()

        try:
            return pd.read_parquet(LOCAL_GOLD_PATH, engine='fastparquet')
        except Exception as error:
            st.error(f"❌ Error reading local file: {error}.")
            return pd.DataFrame()

    elif source_mode == "CLOUD":
        try:
            storage_client = storage.Client()
            bucket = storage_client.bucket(CLOUD_BUCKET_NAME)
            blob = bucket.blob(CLOUD_BLOB_NAME)

            if not blob.exists():
                st.error(f"❌ Cloud file not found: gs://{CLOUD_BUCKET_NAME}/{CLOUD_BLOB_NAME}")
                return pd.DataFrame()

            data_bytes = blob.download_as_bytes()
            return pd.read_parquet(io.BytesIO(data_bytes), engine='fastparquet')

        except Exception as error:
            st.error(f"❌ Cloud Connection Failed: {error}")
            st.warning("Tip: Did you run 'gcloud auth application-default login'?")
            return pd.DataFrame()

# --- MAIN APP ---
def main():
    # Sidebar configuration
    st.sidebar.header("⚙️ Settings")
    
    # The Mode Toggle
    data_source = st.sidebar.radio(
        "Data Source:",
        ("CLOUD", "LOCAL"),
        index=0, 
        help="Switch between live Cloud Storage data and local disk data."
    )
    
    # Show status badge
    if data_source == "CLOUD":
        st.success(f"☁️ Live Mode: Connected to {CLOUD_BUCKET_NAME}.")
    else:
        st.info(f"🏠 Dev Mode: Reading from local disk.")

    # Load data
    df = load_data(data_source)
    
    # --- SIDEBAR FOOTER ---
    st.sidebar.markdown("---")
    st.sidebar.caption(f"Engine: fastparquet v{fastparquet.__version__}")
    
    if df.empty:
        st.warning("⚠️ No data available. Please check your source.")
        return

    # Sidebar Filters
    st.sidebar.header("🔍 Filters")
    if 'coin_id' in df.columns:
        all_coins = df['coin_id'].unique()
        selected_coin = st.sidebar.selectbox("Select Asset", all_coins, index=0)

        # Filter and Sort
        coin_df = df[df['coin_id'] == selected_coin].copy()
        
        if 'extraction_timestamp' in coin_df.columns:
            coin_df = coin_df.sort_values("extraction_timestamp")
            
            # --- KPI METRICS ---
            if not coin_df.empty:
                latest = coin_df.iloc[-1]
                
                vol = latest.get('volatility_7d', 0.0)
                vol_display = f"{vol:,.2f}" if pd.notna(vol) else "0.00"

                col1, col2, col3, col4 = st.columns(4)
                with col1: st.metric("Current Price", f"${latest.get('price_usd', 0):,.2f}")
                with col2: st.metric("7-Day SMA", f"${latest.get('sma_7d', 0):,.2f}")
                with col3: st.metric("Volatility Index", vol_display) 
                with col4: st.metric("Trading Signal", latest.get('signal', 'N/A'))

            # --- CHART ---
            st.subheader(f"📈 Price Trend: {selected_coin.upper()}")

            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=coin_df['extraction_timestamp'], 
                y=coin_df['price_usd'], 
                mode='lines', 
                name='Price', 
                line=dict(color='#00CC96', width=2)
            ))
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

            with st.expander("See Raw Data"):
                st.dataframe(coin_df.sort_values("extraction_timestamp", ascending=False))
        else:
            st.error("Data missing 'extraction_timestamp' column.")
    else:
        st.error("Data missing 'coin_id' column. Check your parquet file structure.")

# Entry point for running the pipeline's dashboard.
if __name__ == "__main__":
    main()
