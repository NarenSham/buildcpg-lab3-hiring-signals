"""Hiring Signals Dashboard - Streamlit App"""

import streamlit as st
import polars as pl
import plotly.express as px
import plotly.graph_objects as go
import duckdb
from datetime import datetime

# Page config
st.set_page_config(
    page_title="Hiring Signals Dashboard",
    page_icon="🎯",
    layout="wide",
)

# Load data directly from DuckDB
@st.cache_data(ttl=3600)  # Cache for 1 hour
def load_data():
    """Load data directly from DuckDB database."""
    try:
        # Connect to DuckDB
        db_path = "/app/warehouse/hiring_signals.duckdb"
        conn = duckdb.connect(db_path, read_only=True)
        
        # Load lead scores
        lead_scores = pl.read_database(
            """
            SELECT 
                company,
                composite_score,
                velocity_score,
                tech_match_score,
                volume_score,
                jobs_this_week,
                jobs_last_week,
                score_metadata as tech_stack,
                week_start
            FROM lead_scores
            ORDER BY composite_score DESC
            """,
            connection=conn
        )
        
        # Load company trends
        company_trends = pl.read_database(
            """
            SELECT 
                company_normalized,
                company,
                week_start,
                jobs_posted,
                tech_stack
            FROM company_stats
            ORDER BY company_normalized, week_start
            """,
            connection=conn
        )
        
        # Calculate summary statistics
        stats = conn.execute("""
            SELECT 
                COUNT(*) as total_companies,
                SUM(CASE WHEN composite_score >= 80 THEN 1 ELSE 0 END) as hot_leads,
                SUM(CASE WHEN composite_score >= 50 AND composite_score < 80 THEN 1 ELSE 0 END) as warm_leads,
                SUM(CASE WHEN composite_score < 50 THEN 1 ELSE 0 END) as cold_leads,
                AVG(composite_score) as avg_score,
                MAX(week_start) as latest_week
            FROM lead_scores
        """).fetchone()
        
        # Get tech distribution
        tech_dist_raw = conn.execute("""
            SELECT 
                tech,
                COUNT(*) as count
            FROM company_stats,
                UNNEST(string_split(tech_stack, ',')) as t(tech)
            WHERE tech_stack != ''
            GROUP BY tech
            ORDER BY count DESC
        """).fetchall()
        
        tech_distribution = {tech.strip(): count for tech, count in tech_dist_raw}
        
        summary = {
            "generated_at": datetime.now().isoformat(),
            "latest_week": str(stats[5]) if stats[5] else "N/A",
            "total_companies": stats[0] or 0,
            "hot_leads": stats[1] or 0,
            "warm_leads": stats[2] or 0,
            "cold_leads": stats[3] or 0,
            "avg_composite_score": round(stats[4], 2) if stats[4] else 0,
            "tech_distribution": tech_distribution,
        }
        
        conn.close()
        
        return lead_scores, company_trends, summary
        
    except Exception as e:
        st.error(f"❌ Error loading data from database: {e}")
        st.info(f"Database path: /app/warehouse/hiring_signals.duckdb")
        st.info("💡 Make sure Dagster has materialized all assets first")
        st.stop()

# Load data
lead_scores, company_trends, summary = load_data()

# Header
st.title("🎯 Hiring Signals Dashboard")
st.markdown(f"**Latest Update:** {summary['generated_at'][:19]} | **Week:** {summary['latest_week']}")

# Metrics Row
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Total Companies", summary['total_companies'])

with col2:
    st.metric("🔥 Hot Leads", summary['hot_leads'], delta="≥80 score")

with col3:
    st.metric("⚡ Warm Leads", summary['warm_leads'], delta="50-79 score")

with col4:
    st.metric("Avg Score", f"{summary['avg_composite_score']:.1f}")

st.divider()

# ============================================================================
# Section 1: Top Leads
# ============================================================================

st.header("🏆 Top 20 Leads")

# Convert to pandas for display
top_20 = lead_scores.head(20).to_pandas()

# Color code by score
def color_score(val):
    if val >= 80:
        return 'background-color: #90EE90'  # Light green
    elif val >= 50:
        return 'background-color: #FFD700'  # Gold
    else:
        return 'background-color: #FFB6C1'  # Light red

styled_table = top_20[['company', 'composite_score', 'velocity_score', 'tech_match_score', 
                       'volume_score', 'jobs_this_week', 'tech_stack']].style.applymap(
    color_score, subset=['composite_score']
)

st.dataframe(styled_table, use_container_width=True, height=600)

st.divider()

# ============================================================================
# Section 2: Score Distribution
# ============================================================================

st.header("📊 Score Distribution")

col1, col2 = st.columns(2)

with col1:
    # Histogram of scores
    fig_hist = px.histogram(
        lead_scores.to_pandas(),
        x="composite_score",
        nbins=20,
        title="Distribution of Composite Scores",
        labels={"composite_score": "Composite Score"},
        color_discrete_sequence=["#3b82f6"]
    )
    fig_hist.update_layout(showlegend=False)
    st.plotly_chart(fig_hist, use_container_width=True)

with col2:
    # Score components breakdown (top 10)
    top_10 = lead_scores.head(10).to_pandas()
    
    fig_components = go.Figure()
    fig_components.add_trace(go.Bar(
        name='Velocity',
        x=top_10['company'],
        y=top_10['velocity_score'],
    ))
    fig_components.add_trace(go.Bar(
        name='Tech Match',
        x=top_10['company'],
        y=top_10['tech_match_score'],
    ))
    fig_components.add_trace(go.Bar(
        name='Volume',
        x=top_10['company'],
        y=top_10['volume_score'],
    ))
    
    fig_components.update_layout(
        title="Score Components (Top 10 Companies)",
        barmode='group',
        xaxis_tickangle=-45
    )
    st.plotly_chart(fig_components, use_container_width=True)

st.divider()

# ============================================================================
# Section 3: Technology Trends
# ============================================================================

st.header("💻 Technology Distribution")

col1, col2 = st.columns([2, 1])

with col1:
    # Tech distribution chart
    tech_dist = summary['tech_distribution']
    
    if tech_dist:
        tech_df = pl.DataFrame({
            "technology": list(tech_dist.keys()),
            "company_count": list(tech_dist.values())
        }).sort("company_count", descending=True).to_pandas()
        
        fig_tech = px.bar(
            tech_df,
            x="company_count",
            y="technology",
            orientation='h',
            title="Companies by Technology Stack",
            labels={"company_count": "Number of Companies"},
            color="company_count",
            color_continuous_scale="Blues"
        )
        st.plotly_chart(fig_tech, use_container_width=True)
    else:
        st.info("No technology data available yet")

with col2:
    st.subheader("Top Technologies")
    if tech_dist:
        for tech, count in list(tech_dist.items())[:10]:
            st.metric(tech, count, delta=f"{count/summary['total_companies']*100:.0f}%")
    else:
        st.info("No data available")

st.divider()

# ============================================================================
# Section 4: Hiring Velocity Trends
# ============================================================================

st.header("📈 Hiring Velocity Trends")

# Select companies to plot
companies = company_trends["company"].unique().to_list()
default_companies = companies[:5] if len(companies) >= 5 else companies

selected_companies = st.multiselect(
    "Select companies to compare:",
    options=companies,
    default=default_companies
)

if selected_companies:
    filtered_trends = company_trends.filter(
        pl.col("company").is_in(selected_companies)
    ).to_pandas()
    
    fig_trends = px.line(
        filtered_trends,
        x="week_start",
        y="jobs_posted",
        color="company",
        title="Weekly Job Postings Over Time",
        labels={"jobs_posted": "Jobs Posted", "week_start": "Week"},
        markers=True
    )
    st.plotly_chart(fig_trends, use_container_width=True)
else:
    st.info("👆 Select companies above to view trends")

st.divider()

# ============================================================================
# Section 5: Search & Filter
# ============================================================================

st.header("🔍 Search Leads")

col1, col2, col3 = st.columns(3)

with col1:
    min_score = st.slider("Minimum Score", 0, 100, 50)

with col2:
    tech_options = list(summary['tech_distribution'].keys()) if summary['tech_distribution'] else []
    selected_techs = st.multiselect(
        "Filter by Technology",
        options=tech_options,
        default=[]
    )

with col3:
    min_jobs = st.number_input("Minimum Jobs This Week", min_value=0, value=0)

# Apply filters
filtered = lead_scores.filter(pl.col("composite_score") >= min_score)
filtered = filtered.filter(pl.col("jobs_this_week") >= min_jobs)

if selected_techs:
    # Filter by tech stack
    for tech in selected_techs:
        filtered = filtered.filter(pl.col("tech_stack").str.contains(tech))

st.write(f"**Found {len(filtered)} companies matching criteria**")
st.dataframe(filtered.to_pandas(), use_container_width=True)

# ============================================================================
# Footer
# ============================================================================

st.divider()
st.caption(f"Data last updated: {summary['generated_at']} | Total companies tracked: {summary['total_companies']}")