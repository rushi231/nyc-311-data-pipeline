"""
dashboard/app.py

Streamlit analytics dashboard for NYC 311 data.
Connects directly to PostgreSQL marts schema.

Run:  streamlit run dashboard/app.py
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import text
from datetime import datetime, timedelta

from config import get_engine

# ─────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────
st.set_page_config(
    page_title="NYC 311 Analytics",
    page_icon="🗽",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    .metric-card {
        background: #f8f9fa;
        border-radius: 8px;
        padding: 16px 20px;
        border-left: 4px solid #0066cc;
    }
    .stMetric label { font-size: 13px !important; color: #666 !important; }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────
# DATA LOADING  (cached so queries don't re-run on every interaction)
# ─────────────────────────────────────────

@st.cache_data(ttl=300)  # cache for 5 minutes
def load_summary_kpis(start_date, end_date):
    engine = get_engine()
    sql = text("""
        SELECT
            COUNT(*)                                                    AS total_requests,
            COUNT(*) FILTER (WHERE status = 'Open')                    AS open_requests,
            COUNT(*) FILTER (WHERE status = 'Closed')                  AS closed_requests,
            ROUND(AVG(resolution_hours) FILTER (WHERE resolution_hours IS NOT NULL)::numeric, 1)
                                                                        AS avg_resolution_hours,
            COUNT(*) FILTER (WHERE is_overdue = TRUE)                  AS overdue_requests,
            ROUND(
                100.0 * COUNT(*) FILTER (WHERE is_overdue = TRUE)
                / NULLIF(COUNT(*) FILTER (WHERE status = 'Closed'), 0),
                1
            )                                                           AS overdue_pct
        FROM marts.fct_requests
        WHERE created_date BETWEEN :start AND :end
    """)
    with engine.connect() as conn:
        row = conn.execute(sql, {"start": start_date, "end": end_date}).fetchone()
    return row._asdict() if row else {}


@st.cache_data(ttl=300)
def load_requests_by_day(start_date, end_date):
    engine = get_engine()
    sql = text("""
        SELECT
            DATE_TRUNC('day', created_date)::date AS day,
            COUNT(*) AS requests
        FROM marts.fct_requests
        WHERE created_date BETWEEN :start AND :end
        GROUP BY 1
        ORDER BY 1
    """)
    with engine.connect() as conn:
        return pd.read_sql(sql, conn, params={"start": start_date, "end": end_date})


@st.cache_data(ttl=300)
def load_top_complaint_types(start_date, end_date, top_n=15):
    engine = get_engine()
    sql = text("""
        SELECT
            ct.complaint_type,
            ct.category,
            COUNT(*) AS requests,
            ROUND(AVG(f.resolution_hours)::numeric, 1) AS avg_hours
        FROM marts.fct_requests f
        JOIN marts.dim_complaint_type ct ON f.complaint_type_id = ct.complaint_type_id
        WHERE f.created_date BETWEEN :start AND :end
        GROUP BY ct.complaint_type, ct.category
        ORDER BY requests DESC
        LIMIT :top_n
    """)
    with engine.connect() as conn:
        return pd.read_sql(sql, conn, params={"start": start_date, "end": end_date, "top_n": top_n})


@st.cache_data(ttl=300)
def load_borough_breakdown(start_date, end_date):
    engine = get_engine()
    sql = text("""
        SELECT
            b.borough,
            COUNT(*)                                                AS total,
            COUNT(*) FILTER (WHERE f.status = 'Open')              AS open_count,
            ROUND(AVG(f.resolution_hours)::numeric, 1)             AS avg_resolution_hours,
            ROUND(
                100.0 * COUNT(*) FILTER (WHERE f.is_overdue = TRUE)
                / NULLIF(COUNT(*) FILTER (WHERE f.status = 'Closed'), 0),
                1
            )                                                       AS overdue_pct
        FROM marts.fct_requests f
        JOIN marts.dim_borough b ON f.borough_id = b.borough_id
        WHERE f.created_date BETWEEN :start AND :end
          AND b.borough != 'Unspecified'
        GROUP BY b.borough
        ORDER BY total DESC
    """)
    with engine.connect() as conn:
        return pd.read_sql(sql, conn, params={"start": start_date, "end": end_date})


@st.cache_data(ttl=300)
def load_resolution_distribution(start_date, end_date):
    engine = get_engine()
    sql = text("""
        SELECT resolution_hours
        FROM marts.fct_requests
        WHERE created_date BETWEEN :start AND :end
          AND resolution_hours IS NOT NULL
          AND resolution_hours > 0
          AND resolution_hours < 500
    """)
    with engine.connect() as conn:
        return pd.read_sql(sql, conn, params={"start": start_date, "end": end_date})


@st.cache_data(ttl=300)
def load_pipeline_runs():
    engine = get_engine()
    sql = text("""
        SELECT
            run_started_at, run_finished_at, status,
            rows_fetched, rows_loaded, rows_failed_validation
        FROM raw.pipeline_runs
        ORDER BY run_started_at DESC
        LIMIT 20
    """)
    with engine.connect() as conn:
        return pd.read_sql(sql, conn)


# ─────────────────────────────────────────
# SIDEBAR FILTERS
# ─────────────────────────────────────────

st.sidebar.title("🗽 NYC 311 Analytics")
st.sidebar.markdown("---")

date_range = st.sidebar.selectbox(
    "Date range",
    ["Last 7 days", "Last 30 days", "Last 90 days", "Last 365 days"],
    index=1,
)

days_map = {
    "Last 7 days": 7,
    "Last 30 days": 30,
    "Last 90 days": 90,
    "Last 365 days": 365,
}
end_date = datetime.utcnow()
start_date = end_date - timedelta(days=days_map[date_range])

st.sidebar.markdown("---")
st.sidebar.markdown("**About**")
st.sidebar.caption(
    "Live analytics on NYC 311 service requests. "
    "Data refreshed daily via automated ETL pipeline."
)
st.sidebar.markdown("[View pipeline code on GitHub](https://github.com)")


# ─────────────────────────────────────────
# MAIN CONTENT
# ─────────────────────────────────────────

st.title("NYC 311 Service Request Analytics")
st.caption(f"Showing data from {start_date.strftime('%b %d, %Y')} to {end_date.strftime('%b %d, %Y')}")

# ── KPI CARDS ──────────────────────────────
kpis = load_summary_kpis(start_date, end_date)

if kpis:
    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        st.metric("Total Requests", f"{kpis.get('total_requests', 0):,}")
    with col2:
        st.metric("Open Requests", f"{kpis.get('open_requests', 0):,}")
    with col3:
        st.metric("Closed Requests", f"{kpis.get('closed_requests', 0):,}")
    with col4:
        avg_h = kpis.get("avg_resolution_hours")
        st.metric(
            "Avg Resolution",
            f"{avg_h:.0f} hrs" if avg_h else "N/A",
            help="Average hours from open to close"
        )
    with col5:
        pct = kpis.get("overdue_pct")
        st.metric(
            "Overdue Rate",
            f"{pct:.1f}%" if pct else "0%",
            delta=None,
            help="% of closed requests that exceeded their due date"
        )
else:
    st.warning("No data found for selected date range. Run the pipeline first.")
    st.stop()

st.markdown("---")

# ── ROW 1: Time series + Borough bar ───────
col_left, col_right = st.columns([2, 1])

with col_left:
    st.subheader("Requests over time")
    daily_df = load_requests_by_day(start_date, end_date)
    if not daily_df.empty:
        fig = px.area(
            daily_df, x="day", y="requests",
            color_discrete_sequence=["#0066cc"],
            labels={"day": "Date", "requests": "Requests"},
        )
        fig.update_layout(
            margin=dict(l=0, r=0, t=10, b=0),
            height=280,
            showlegend=False,
            plot_bgcolor="white",
            paper_bgcolor="white",
        )
        fig.update_xaxes(showgrid=False)
        fig.update_yaxes(gridcolor="#f0f0f0")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No data available")

with col_right:
    st.subheader("Requests by borough")
    borough_df = load_borough_breakdown(start_date, end_date)
    if not borough_df.empty:
        fig2 = px.bar(
            borough_df.sort_values("total"),
            x="total", y="borough",
            orientation="h",
            color_discrete_sequence=["#0066cc"],
            labels={"total": "Requests", "borough": ""},
        )
        fig2.update_layout(
            margin=dict(l=0, r=0, t=10, b=0),
            height=280,
            showlegend=False,
            plot_bgcolor="white",
            paper_bgcolor="white",
        )
        fig2.update_xaxes(gridcolor="#f0f0f0")
        fig2.update_yaxes(showgrid=False)
        st.plotly_chart(fig2, use_container_width=True)

st.markdown("---")

# ── ROW 2: Top complaints + Resolution histogram ───
col_a, col_b = st.columns([3, 2])

with col_a:
    st.subheader("Top complaint types")
    complaints_df = load_top_complaint_types(start_date, end_date)
    if not complaints_df.empty:
        fig3 = px.bar(
            complaints_df.sort_values("requests"),
            x="requests", y="complaint_type",
            orientation="h",
            color="avg_hours",
            color_continuous_scale="Blues",
            labels={
                "requests": "Total requests",
                "complaint_type": "",
                "avg_hours": "Avg hours to close",
            },
        )
        fig3.update_layout(
            margin=dict(l=0, r=0, t=10, b=0),
            height=400,
            plot_bgcolor="white",
            paper_bgcolor="white",
        )
        st.plotly_chart(fig3, use_container_width=True)

with col_b:
    st.subheader("Resolution time distribution")
    dist_df = load_resolution_distribution(start_date, end_date)
    if not dist_df.empty:
        fig4 = px.histogram(
            dist_df, x="resolution_hours",
            nbins=40,
            color_discrete_sequence=["#0066cc"],
            labels={"resolution_hours": "Hours to resolution"},
        )
        fig4.update_layout(
            margin=dict(l=0, r=0, t=10, b=0),
            height=400,
            showlegend=False,
            plot_bgcolor="white",
            paper_bgcolor="white",
        )
        fig4.update_yaxes(title="Requests", gridcolor="#f0f0f0")
        fig4.update_xaxes(showgrid=False)
        st.plotly_chart(fig4, use_container_width=True)

st.markdown("---")

# ── ROW 3: Borough detail table ────────────
st.subheader("Borough performance table")
if not borough_df.empty:
    display_df = borough_df.rename(columns={
        "borough": "Borough",
        "total": "Total Requests",
        "open_count": "Open",
        "avg_resolution_hours": "Avg Resolution (hrs)",
        "overdue_pct": "Overdue %",
    })
    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
    )

st.markdown("---")

# ── PIPELINE HEALTH TAB ────────────────────
with st.expander("🔧 Pipeline run history"):
    runs_df = load_pipeline_runs()
    if not runs_df.empty:
        runs_df["Status"] = runs_df["status"].map(
            {"success": "✅ Success", "failed": "❌ Failed", "running": "🔄 Running"}
        )
        st.dataframe(
            runs_df[["run_started_at", "Status", "rows_fetched", "rows_loaded", "rows_failed_validation"]].rename(columns={
                "run_started_at": "Started",
                "rows_fetched": "Fetched",
                "rows_loaded": "Loaded",
                "rows_failed_validation": "Failed Validation",
            }),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No pipeline runs recorded yet.")
