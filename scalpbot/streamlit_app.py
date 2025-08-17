# streamlit_app.py
import streamlit as st
import pandas as pd
from pathlib import Path

st.set_page_config(layout="wide", page_title="ScalpBot Dashboard")

st.title("ScalpBot — Dashboard")

signals_file = Path("signals_log.csv")
trades_file = Path("trades_log.csv")

if signals_file.exists():
    signals = pd.read_csv(signals_file)
    st.subheader("Signals (latest 200)")
    st.dataframe(signals.tail(200))
else:
    st.info("signals_log.csv not found yet.")

if trades_file.exists():
    trades = pd.read_csv(trades_file)
    st.subheader("Trades / Positions (all)")
    st.dataframe(trades.tail(200))
    # quick metrics
    total_pnl = trades["pnl_usd"].dropna().sum()
    st.metric("Realised PnL (USD)", f"{total_pnl:.2f}")
else:
    st.info("trades_log.csv not found yet.")

st.sidebar.header("Controls")
st.sidebar.write("Start main.py separately. Streamlit only reads logs.")
