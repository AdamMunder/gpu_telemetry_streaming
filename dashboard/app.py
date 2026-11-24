"""Live dashboard: reads ONLY the gold table and refreshes every few seconds.

Run on Windows (not in Docker), from the project folder:
    pip install streamlit deltalake pandas
    streamlit run dashboard/app.py
"""
import time
from pathlib import Path

import pandas as pd
import streamlit as st
from deltalake import DeltaTable

GOLD_PATH = Path(__file__).resolve().parent.parent / "lakehouse" / "gold" / "model_minute"
REFRESH_SECONDS = 5

st.set_page_config(page_title="GPU telemetry", layout="wide")
st.title("GPU telemetry: live view (simulated data)")
st.caption(f"Reads the gold table only. Refreshes every {REFRESH_SECONDS} seconds.")

try:
    gold = DeltaTable(str(GOLD_PATH)).to_pandas()
except Exception as err:  # the table may not exist yet, or a write may be in progress
    st.warning(f"Gold table not readable yet: {err}")
    time.sleep(REFRESH_SECONDS)
    st.rerun()

gold = gold.sort_values("minute")

# ---------- headline numbers, one column per GPU model ----------
totals = gold.groupby("model_name").agg(
    energy_kwh=("energy_kwh", "sum"),
    max_temp_c=("max_temp_c", "max"),
    danger_events=("danger_events", "sum"),
    readings=("readings", "sum"),
    price_usd=("model_price_usd", "max"),
)
columns = st.columns(len(totals))
for col, (model, row) in zip(columns, totals.iterrows()):
    with col:
        st.subheader(model)
        st.metric("Energy used", f"{row.energy_kwh:.4f} kWh")
        st.metric("Max temperature", f"{row.max_temp_c:.1f} C")
        st.metric("DANGER events (>90 C)", int(row.danger_events))
        st.caption(f"{int(row.readings)} readings, price per GPU ${row.price_usd:,.0f}")

# ---------- trends per minute ----------
left, right = st.columns(2)
with left:
    st.markdown("**Average temperature per minute (C)**")
    st.line_chart(gold.pivot_table(index="minute", columns="model_name", values="avg_temp_c"))
with right:
    st.markdown("**Energy per minute (kWh)**")
    st.bar_chart(gold.pivot_table(index="minute", columns="model_name", values="energy_kwh"))

with st.expander("Gold table (latest minutes)"):
    st.dataframe(gold.sort_values("minute", ascending=False), use_container_width=True)

# ---------- auto refresh ----------
time.sleep(REFRESH_SECONDS)
st.rerun()