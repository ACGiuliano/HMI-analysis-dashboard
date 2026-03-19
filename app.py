import pandas as pd
import streamlit as st
import plotly.express as px
from pathlib import Path

# =========================
# CONFIG
# =========================
OUTPUT_FOLDER = Path("output")
SUMMARY_FILE = OUTPUT_FOLDER / "summary_results.csv"
COMBINED_FILE = OUTPUT_FOLDER / "combined_weekly_analysis_no_outliers.csv"

st.set_page_config(
    page_title="HMI Dashboard",
    layout="wide"
)

st.title("HMI Trend Dashboard")

# =========================
# LOAD DATA
# =========================
@st.cache_data
def load_data():
    summary = pd.read_csv(SUMMARY_FILE)
    combined = pd.read_csv(COMBINED_FILE)

    # Convert dates
    for col in ["week_start", "week_end"]:
        if col in combined.columns:
            combined[col] = pd.to_datetime(combined[col])

    for col in ["start_week", "end_week"]:
        if col in summary.columns:
            summary[col] = pd.to_datetime(summary[col])

    return summary, combined


try:
    summary_df, combined_df = load_data()
except:
    st.error("❌ Could not load output files. Run your analysis script first.")
    st.stop()

# =========================
# SIDEBAR
# =========================
st.sidebar.header("Filters")

hide_outliers = st.sidebar.checkbox("Hide Outliers", value=True)

filtered_df = summary_df.copy()

if hide_outliers and "is_outlier" in filtered_df.columns:
    filtered_df = filtered_df[~filtered_df["is_outlier"]]

# =========================
# KPI SECTION
# =========================
st.subheader("Overview")

total_mills = filtered_df["file_name"].nunique()
total_events = filtered_df["num_high_severity_events"].sum()

overall_pct = combined_df["total_pct_change"].dropna().iloc[0]

col1, col2, col3 = st.columns(3)

col1.metric("Mills Included", f"{total_mills}")
col2.metric("Total HMI Events", f"{int(total_events):,}")
col3.metric("Overall % Change", f"{overall_pct:.2f}%")

# =========================
# COMBINED TREND
# =========================
st.subheader("Combined Weekly Trend")

fig = px.line(
    combined_df,
    x="week_end",
    y="count",
    markers=True,
    title="Weekly HMI Count"
)

fig.update_layout(
    xaxis_title="Week Ending",
    yaxis_title="Count"
)

st.plotly_chart(fig, width="stretch")

# =========================
# SUMMARY TABLE
# =========================
st.subheader("Per-Mill Summary")

display_cols = [
    "file_name",
    "start_week_count",
    "end_week_count",
    "overall_pct_change",
    "trend_direction",
    "num_high_severity_events",
    "is_outlier"
]

display_cols = [c for c in display_cols if c in filtered_df.columns]

st.dataframe(
    filtered_df[display_cols].sort_values("overall_pct_change"),
    width="stretch"
)

# =========================
# MILL DETAIL
# =========================
st.subheader("Mill Detail")

selected_mill = st.selectbox(
    "Select a mill",
    filtered_df["file_name"].unique()
)

mill_file = OUTPUT_FOLDER / f"{selected_mill}_weekly_analysis.csv"

if mill_file.exists():
    mill_df = pd.read_csv(mill_file)

    for col in ["week_start", "week_end"]:
        if col in mill_df.columns:
            mill_df[col] = pd.to_datetime(mill_df[col])

    col1, col2, col3 = st.columns(3)

    start_val = mill_df["count"].iloc[0]
    end_val = mill_df["count"].iloc[-1]
    total_pct = mill_df["total_pct_change"].dropna().iloc[0]

    col1.metric("Start Week", f"{start_val}")
    col2.metric("End Week", f"{end_val}")
    col3.metric("Total % Change", f"{total_pct:.2f}%")

    # Chart
    fig2 = px.line(
        mill_df,
        x="week_end",
        y="count",
        markers=True,
        title=f"{selected_mill} Weekly Trend"
    )

    st.plotly_chart(fig2, width="stretch")

    # Weekly table
    st.dataframe(mill_df, width="stretch")

else:
    st.warning("No weekly file found for this mill.")

# =========================
# DOWNLOADS
# =========================
st.subheader("⬇️ Downloads")

col1, col2 = st.columns(2)

with col1:
    with open(SUMMARY_FILE, "rb") as f:
        st.download_button(
            "Download Summary",
            data=f,
            file_name="summary_results.csv"
        )

with col2:
    with open(COMBINED_FILE, "rb") as f:
        st.download_button(
            "Download Combined Data",
            data=f,
            file_name="combined_weekly_analysis.csv"
        )