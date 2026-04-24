import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "../data/banking.duckdb")

st.set_page_config(page_title="Banking Analytics", page_icon="🏦", layout="wide")

@st.cache_data
def query(sql):
    con = duckdb.connect(DB_PATH, read_only=True)
    df = con.execute(sql).df()
    con.close()
    return df

st.sidebar.title("🏦 Banking Analytics")
st.sidebar.markdown("---")

plan_filter = st.sidebar.multiselect("Plan Type", ["free","smart","metal"], default=["free","smart","metal"])
country_filter = st.sidebar.multiselect("Country", ["de","at","es","fr","it","nl","be","pt"], default=["de","at","es","fr"])

plans = "('" + "','".join(plan_filter) + "')"
countries = "('" + "','".join(country_filter) + "')"

st.title("📊 Product Analytics Dashboard")
st.caption("Powered by dbt models on DuckDB · Simulated N26-style data")

col1, col2, col3, col4 = st.columns(4)

total_users = query(f"SELECT count(*) as n FROM main_product.dim_users WHERE plan_type IN {plans} AND country IN {countries}")["n"][0]
verified = query(f"SELECT count(*) as n FROM main_product.dim_users WHERE kyc_status='verified' AND plan_type IN {plans} AND country IN {countries}")["n"][0]
revenue = query(f"SELECT coalesce(round(sum(amount),0),0) as r FROM main_finance.fct_transactions WHERE status='completed' AND plan_type IN {plans} AND country IN {countries}")["r"][0]
ltv = query(f"SELECT coalesce(round(avg(lifetime_value),2),0) as l FROM main_product.dim_users WHERE plan_type IN {plans} AND country IN {countries}")["l"][0]

col1.metric("Total Users", f"{total_users:,}")
col2.metric("KYC Verified", f"{verified:,}", f"{round(100*verified/max(total_users,1),1)}%")
col3.metric("Total Volume", f"€{revenue:,.0f}")
col4.metric("Avg LTV / User", f"€{ltv:,.2f}")

st.markdown("---")

st.subheader("🔁 Cohort Retention (D1 / D7 / D30)")
retention = query(f"""
    SELECT signup_month::date as month,
           avg(retention_rate_d1) as D1,
           avg(retention_rate_d7) as D7,
           avg(retention_rate_d30) as D30
    FROM main_product.fct_retention
    WHERE plan_type IN {plans} AND country IN {countries}
    GROUP BY 1 ORDER BY 1
""")
if not retention.empty:
    fig = go.Figure()
    for col, color in [("D1","#636EFA"),("D7","#EF553B"),("D30","#00CC96")]:
        fig.add_trace(go.Scatter(x=retention["month"], y=retention[col], name=col,
                                  line=dict(color=color, width=2), mode="lines+markers"))
    fig.update_layout(yaxis_title="Retention %", height=350, margin=dict(t=20))
    st.plotly_chart(fig, use_container_width=True)

st.subheader("🎯 Activation Funnel")
funnel = query(f"""
    SELECT sum(total_registered) as Registered,
           sum(total_kyc_verified) as KYC_Verified,
           sum(total_card_activated) as Card_Activated,
           sum(total_fully_activated) as First_Transfer
    FROM main_marketing.fct_activation_funnel
    WHERE plan_type IN {plans} AND country IN {countries}
""")
if not funnel.empty:
    vals = [funnel["Registered"][0], funnel["KYC_Verified"][0], funnel["Card_Activated"][0], funnel["First_Transfer"][0]]
    fig2 = go.Figure(go.Funnel(
        y=["Registered","KYC Verified","Card Activated","First Transfer"],
        x=vals, textinfo="value+percent initial",
        marker=dict(color=["#636EFA","#EF553B","#00CC96","#AB63FA"])
    ))
    fig2.update_layout(height=320, margin=dict(t=20))
    st.plotly_chart(fig2, use_container_width=True)

col_a, col_b = st.columns(2)

with col_a:
    st.subheader("📉 Churn Rate by Plan")
    churn = query(f"""
        SELECT plan_type, round(avg(churn_rate),2) as churn_rate
        FROM main_product.fct_churn
        WHERE plan_type IN {plans} AND country IN {countries}
        GROUP BY 1 ORDER BY 2 desc
    """)
    if not churn.empty:
        fig3 = px.bar(churn, x="plan_type", y="churn_rate", color="plan_type", text="churn_rate")
        fig3.update_layout(height=300, showlegend=False, margin=dict(t=20))
        st.plotly_chart(fig3, use_container_width=True)

with col_b:
    st.subheader("💳 Engagement Segments")
    segments = query(f"""
        SELECT engagement_segment, count(*) as users
        FROM main_product.dim_users
        WHERE plan_type IN {plans} AND country IN {countries}
        GROUP BY 1
    """)
    if not segments.empty:
        fig4 = px.pie(segments, names="engagement_segment", values="users")
        fig4.update_layout(height=300, margin=dict(t=20))
        st.plotly_chart(fig4, use_container_width=True)

st.subheader("💰 Monthly Transaction Volume")
tx = query(f"""
    SELECT transaction_month::date as month,
           count(*) as n_transactions,
           sum(amount) as volume
    FROM main_finance.fct_transactions
    WHERE status='completed' AND plan_type IN {plans} AND country IN {countries}
    GROUP BY 1 ORDER BY 1
""")
if not tx.empty:
    fig5 = px.bar(tx, x="month", y="volume", color_discrete_sequence=["#636EFA"])
    fig5.update_layout(height=300, margin=dict(t=20), yaxis_title="Volume (€)")
    st.plotly_chart(fig5, use_container_width=True)
