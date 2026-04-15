import streamlit as st
import pandas as pd
from datetime import date, timedelta, datetime
import altair as alt
import html
import logging
import requests
import os
import time
from io import BytesIO
from supabase import create_client
from dotenv import load_dotenv
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
GITHUB_TOKEN    = os.getenv("GITHUB_TOKEN")
GITHUB_REPO     = os.getenv("GITHUB_REPO")       # e.g. "Shreshth1006/Dashboard-"
GITHUB_WORKFLOW = os.getenv("GITHUB_WORKFLOW")   # e.g. "scraper.yml"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# -----------------------
# Configuration
# -----------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

APP_TITLE    = "Instagram Analytics"
APP_SUBTITLE = "Channel Performance Dashboard"
LOGO_DIR     = "images"

LOGO_MAP = {
    "ani_trending":   "ANI TRNDING.png",
    "brut.india":     "Brut.png",
    "hindustantimes": "Hindustantime.jpeg",
    "indiatoday":     "iNDIATODAY.png",
    "ndtvindia":      "NDTV INDIA.jpg",
    "ndtv":           "ndtv.png",
    "news9live":      "news9live.png",
    "news24official": "NEWS24.jpg",
    "the_hindu":      "The hindu .jpeg",
    "timesnow":       "TIMESNOW.png",
    "timesofindia":   "TOI.webp",
}

# -----------------------
# Page Config
# -----------------------
st.set_page_config(page_title=APP_TITLE, layout="wide", initial_sidebar_state="expanded")

# -----------------------
# CSS
# -----------------------
def apply_styles():
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    * { font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif; }
    .stApp { background-color: #000000; }
    [data-testid="stSidebar"] { background-color: #0a0a0a; border-right: 1px solid #1a1a1a; }
    [data-testid="stSidebar"] .stMarkdown { color: #ffffff; }
    footer { visibility: hidden; }
    .main-title { color: #5b7bfc; font-size: 36px; font-weight: 700; margin-bottom: 4px; letter-spacing: -0.5px; }
    .main-subtitle { color: #6b7280; font-size: 16px; font-weight: 400; margin-bottom: 40px; }
    .section-title { color: #ffffff; font-size: 24px; font-weight: 600; margin-bottom: 24px; margin-top: 40px; }
    .last-updated { color: #6b7280; font-size: 12px; margin-top: 6px; margin-bottom: 16px; }
    .account-card { background-color: #0f0f0f; border: 1px solid #1f1f1f; border-radius: 12px; padding: 28px 24px; margin-bottom: 20px; transition: all 0.2s ease; cursor: pointer; }
    .account-card:hover { border-color: #5b7bfc; background-color: #121212; transform: translateY(-2px); box-shadow: 0 8px 16px rgba(91, 123, 252, 0.15); }
    .date-range-section { background-color: #0f0f0f; border: 1px solid #1f1f1f; border-radius: 12px; padding: 20px 16px; margin-top: 24px; }
    .date-range-title { color: #ffffff; font-size: 14px; font-weight: 600; margin-bottom: 16px; }
    .stRadio > label { display: none; }
    div[data-testid="metric-container"] { background-color: #0f0f0f; border: 1px solid #1f1f1f; border-radius: 12px; padding: 20px; }
    div[data-testid="metric-container"] label { color: #6b7280; font-size: 14px; }
    div[data-testid="metric-container"] [data-testid="stMetricValue"] { color: #ffffff; font-size: 28px; font-weight: 700; }
    .stButton > button { background-color: #0f0f0f; border: 1px solid #1f1f1f; border-radius: 8px; color: #ffffff; padding: 10px 20px; font-weight: 500; transition: all 0.2s; }
    .stButton > button:hover { background-color: #1a1a1a; border-color: #2a2a2a; }
    .stSelectbox > div > div { background-color: #0f0f0f; border: 1px solid #1f1f1f; border-radius: 8px; }
    .stSelectbox label { color: #ffffff; font-weight: 500; }
    .block-container { padding-top: 3rem; padding-bottom: 1rem; }
    .ch-toggle button { font-size: 11px !important; padding: 3px 8px !important; min-height: 26px !important; height: 26px !important; border-radius: 6px !important; background-color: #1a1a1a !important; border: 1px solid #2a2a2a !important; color: #9ca3af !important; font-weight: 400 !important; }
    .ch-toggle button:hover { background-color: #2a2a2a !important; color: #ffffff !important; border-color: #3a3a3a !important; }
    </style>
    """, unsafe_allow_html=True)

# -----------------------
# GitHub Actions Trigger
# -----------------------
def trigger_scraper():
    """Trigger the GitHub Actions scraper workflow via API."""
    if not GITHUB_TOKEN or not GITHUB_REPO or not GITHUB_WORKFLOW:
        return False, "GitHub secrets not configured."

    url = f"https://api.github.com/repos/{GITHUB_REPO}/actions/workflows/{GITHUB_WORKFLOW}/dispatches"
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    payload = {"ref": "main"}

    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=10)
        if resp.status_code == 204:
            return True, "✅ Scraper triggered! Data will be updated in approximately 10–15 minutes. Please avoid clicking the button repeatedly and allow the scraping process to complete."
        else:
            return False, f"❌ Failed: {resp.status_code} — {resp.text}"
    except Exception as e:
        return False, f"❌ Error: {e}"

def get_last_scrape_time():
    """Get the most recent scraped_time from Supabase."""
    try:
        resp = supabase.table("posts").select("scraped_time").order("scraped_time", desc=True).limit(1).execute()
        if resp.data and resp.data[0].get("scraped_time"):
            dt = datetime.fromisoformat(resp.data[0]["scraped_time"])
            return dt.strftime("%-I:%M %p, %d %b %Y")
    except:
        pass
    return "Unknown"

# -----------------------
# Data Loading
# -----------------------
@st.cache_data(ttl=300)
def load_data(cache_key):
    try:
        response = supabase.table("posts") \
            .select("*") \
            .order("post_time", desc=True) \
            .limit(1000) \
            .execute()

        data = response.data

        if not data:
            st.warning("⚠️ No data found in database")
            return pd.DataFrame()

        df = pd.DataFrame(data)
        logger.info(f"Loaded {len(df)} rows from Supabase")
        return df

    except Exception as e:
        st.error(f"❌ Supabase error: {e}")
        st.stop()

def preprocess(df):
    df = df.copy()
    df["username"] = df["username"].astype(str)
    df["likes"]    = pd.to_numeric(df["likes"],    errors="coerce").fillna(0).astype(int)
    df["comments"] = pd.to_numeric(df["comments"], errors="coerce").fillna(0).astype(int)
    if "media_url" in df.columns:
        df["media_url"] = df["media_url"].apply(
            lambda x: html.unescape(str(x).strip())
            if pd.notna(x) and str(x).strip() not in ("", "0", "nan", "None") else None
        )
    df["posted_at_dt"] = pd.to_datetime(df["post_time"], errors="coerce")
    df = df.dropna(subset=["posted_at_dt"])
    df["post_date"] = df["posted_at_dt"].dt.date
    if "post_link" in df.columns:
        df["post_id"] = df["post_link"].apply(
            lambda x: x.split("/")[-2] if pd.notna(x) and "/" in str(x) else ""
        )
    df = df.dropna(subset=["username"])
    return df

# -----------------------
# Utilities
# -----------------------
def format_number(num):
    if num >= 1_000_000: return f"{num/1_000_000:.1f}M"
    elif num >= 1_000:   return f"{num/1_000:.1f}K"
    return str(int(num))

def filter_by_date(df, start, end):
    return df[(df["post_date"] >= start) & (df["post_date"] <= end)]

@st.cache_data(ttl=3600)
def get_logo_path(username):
    filename = LOGO_MAP.get(username)
    if not filename: return None
    path = os.path.join(LOGO_DIR, filename)
    return path if os.path.exists(path) else None

# -----------------------
# Views
# -----------------------
def render_accounts(df):
    st.markdown('<div class="section-title">Select Account</div>', unsafe_allow_html=True)
    stats = df.groupby("username").agg({'post_id': 'count', 'likes': 'sum'}).reset_index()
    stats.columns = ['username', 'posts', 'total_likes']
    stats = stats.sort_values('total_likes', ascending=False).reset_index(drop=True)

    st.markdown("""
    <style>
    div[data-testid="stHorizontalBlock"] .stButton > button {
        background-color: #0f0f0f !important; border: 1px solid #1f1f1f !important;
        border-radius: 12px !important; padding: 20px !important; text-align: left !important;
        height: auto !important; min-height: 80px !important; white-space: normal !important;
        line-height: 1.5 !important; color: #ffffff !important; font-size: 15px !important;
        font-weight: 500 !important; transition: all 0.2s ease !important;
    }
    div[data-testid="stHorizontalBlock"] .stButton > button:hover {
        border-color: #5b7bfc !important; background-color: #121212 !important;
        transform: translateY(-2px) !important; box-shadow: 0 8px 16px rgba(91,123,252,0.15) !important;
    }
    </style>""", unsafe_allow_html=True)

    cols = st.columns(3)
    for idx, row in stats.iterrows():
        with cols[idx % 3]:
            logo_path = get_logo_path(row['username'])
            label = f"@{row['username']}\n\n{row['posts']} posts  ·  {format_number(row['total_likes'])} likes"
            btn_col, logo_col = st.columns([4, 1])
            with btn_col:
                if st.button(label, key=f"btn_{row['username']}", use_container_width=True):
                    st.session_state['selected_account'] = row['username']
                    st.session_state['page'] = 'account_detail'
                    st.rerun()
            with logo_col:
                st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)
                if logo_path:
                    st.image(logo_path, width=56)
                else:
                    st.markdown(f"""
                        <div style="width:52px;height:52px;border-radius:8px;background:#1a1a2e;
                            display:flex;align-items:center;justify-content:center;
                            border:1px solid #2a2a2a;color:#5b7bfc;font-size:22px;font-weight:700;">
                            {row['username'][0].upper()}
                        </div>""", unsafe_allow_html=True)

def render_account_detail(df, username):
    account_df = df[df["username"] == username].copy()
    if account_df.empty:
        st.warning(f"No data for @{username}")
        return

    col1, col2 = st.columns([8, 1])
    with col1:
        st.markdown(f'<div class="section-title">@{username}</div>', unsafe_allow_html=True)
    with col2:
        if st.button("← Back", key="back"):
            st.session_state['page'] = 'accounts'
            st.rerun()

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Posts",          f"{len(account_df):,}")
    col2.metric("Total Likes",    format_number(account_df['likes'].sum()))
    col3.metric("Total Comments", format_number(account_df['comments'].sum()))
    col4.metric("Avg Likes",      format_number(int(account_df['likes'].mean())))

    st.markdown("---")
    st.markdown("### Recent Posts")

    # ✅ Show ALL posts, no .head(20) limit
    for _, row in account_df.sort_values('posted_at_dt', ascending=False).iterrows():
        with st.container():
            col1, col2, col3 = st.columns([1, 4, 2])
            with col1:
                if pd.notna(row.get('media_url')):
                    st.image(row['media_url'], use_container_width=True)
            with col2:
                caption = str(row.get('caption', ''))[:200]
                st.markdown(f"**{caption}...**" if len(str(row.get('caption', ''))) > 200 else f"**{caption}**")
                if pd.notna(row.get('posted_at_dt')):
                    st.caption(f"🕒 {row['posted_at_dt'].strftime('%d %b, %I:%M %p')}")
                if pd.notna(row.get('post_link')) and str(row.get('post_link', '')).startswith('http'):
                    st.markdown(f"[🔗 View Original Post]({row['post_link']})")
            with col3:
                st.metric("❤️", f"{row['likes']:,}")
                st.metric("💬", f"{row['comments']:,}")
        st.markdown("---")

def render_analytics(df):
    st.markdown('<div class="section-title">Analytics Overview</div>', unsafe_allow_html=True)
    if df.empty:
        st.warning("No data available for the selected date range.")
        return

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Posts",    f"{len(df):,}")
    col2.metric("Accounts",       f"{df['username'].nunique()}")
    col3.metric("Total Likes",    format_number(df['likes'].sum()))
    col4.metric("Total Comments", format_number(df['comments'].sum()))
    st.markdown("---")

    account_stats = df.groupby("username").agg(
        posts          = ("post_id",   "count"),
        total_likes    = ("likes",     "sum"),
        avg_likes      = ("likes",     "mean"),
        total_comments = ("comments",  "sum"),
        followers      = ("followers", "first"),
    ).reset_index()
    account_stats["total_engagement"]  = account_stats["total_likes"] + 10 * account_stats["total_comments"]
    account_stats["engagement_rate_%"] = (account_stats["total_engagement"] / account_stats["followers"] * 100).round(2)

    st.markdown("### Performance by Account")
    st.dataframe(
        account_stats[["username","posts","total_likes","avg_likes","total_comments","followers","engagement_rate_%"]]
        .sort_values("total_likes", ascending=False)
        .style.format({"posts":"{:,}","total_likes":"{:,}","avg_likes":"{:,.0f}",
                       "total_comments":"{:,}","followers":"{:,}","engagement_rate_%":"{:.2f}%"}),
        use_container_width=True, hide_index=True,
    )
    st.markdown("---")

    chart_left, chart_right = st.columns(2)
    with chart_left:
        st.markdown("### 🥧 Likes Share by Channel")
        pie = alt.Chart(account_stats).mark_arc(innerRadius=50).encode(
            theta=alt.Theta("total_likes:Q", stack=True),
            color=alt.Color("username:N", legend=alt.Legend(orient="bottom", columns=2), scale=alt.Scale(scheme="tableau10")),
            tooltip=[alt.Tooltip("username:N", title="Channel"), alt.Tooltip("total_likes:Q", title="Total Likes", format=",")],
        ).properties(height=340)
        st.altair_chart(pie, use_container_width=True)

    with chart_right:
        st.markdown("### 📈 Engagement Rate by Channel")
        bar = alt.Chart(account_stats).mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4).encode(
            x=alt.X("username:N", sort="-y", title="Channel", axis=alt.Axis(labelAngle=-35)),
            y=alt.Y("engagement_rate_%:Q", title="Engagement Rate (%)"),
            color=alt.Color("username:N", legend=None, scale=alt.Scale(scheme="tableau10")),
            tooltip=[alt.Tooltip("username:N", title="Channel"),
                     alt.Tooltip("engagement_rate_%:Q", title="Engagement Rate (%)", format=".2f"),
                     alt.Tooltip("followers:Q", title="Followers", format=",")],
        ).properties(height=340)
        st.altair_chart(bar, use_container_width=True)

def render_top_posts(df):
    st.markdown('<div class="section-title">Top Posts</div>', unsafe_allow_html=True)
    col1, col2 = st.columns(2)
    with col1: n = st.selectbox("Show top", [10, 20, 30, 50], key="top_n")
    with col2: metric = st.selectbox("Ranked by", ["Latest", "Likes", "Comments"], key="metric")

    if metric == "Latest":
        top = df.sort_values('posted_at_dt', ascending=False).head(n)  # ✅ newest first
    elif metric == "Likes":
        top = df.nlargest(n, "likes")
    else:
        top = df.nlargest(n, "comments")

    st.markdown(f"### Top {n} by {metric}")

    for _, row in top.iterrows():
        with st.container():
            col1, col2, col3 = st.columns([1, 4, 2])
            with col1:
                if pd.notna(row.get('media_url')):
                    st.image(row['media_url'], use_container_width=True)
            with col2:
                st.markdown(f"**@{row['username']}**")
                caption = str(row.get('caption', ''))[:200]
                st.markdown(f"{caption}..." if len(str(row.get('caption', ''))) > 200 else caption)
                if pd.notna(row.get('posted_at_dt')):
                    st.caption(f"🕒 {row['posted_at_dt'].strftime('%d %b, %I:%M %p')} IST")
                if pd.notna(row.get('post_link')) and str(row.get('post_link', '')).startswith('http'):
                    st.markdown(f"[🔗 View Original Post]({row['post_link']})")
            with col3:
                st.metric("❤️", f"{row['likes']:,}")
                st.metric("💬", f"{row['comments']:,}")
        st.markdown("---")
# -----------------------
# Auth
# -----------------------
APP_PASSWORD = "TOI@1234"

def check_auth():
    if st.session_state.get('authenticated'):
        return True

    st.markdown("""
    <style>
    .login-wrap { max-width:380px; margin:120px auto 0 auto; background:#0f0f0f;
        border:1px solid #1f1f1f; border-radius:16px; padding:40px 36px; text-align:center; }
    .login-title { color:#5b7bfc; font-size:26px; font-weight:700; margin-bottom:6px; }
    .login-sub { color:#6b7280; font-size:14px; margin-bottom:28px; }
    </style>""", unsafe_allow_html=True)

    st.markdown('<div class="login-wrap">', unsafe_allow_html=True)
    st.markdown('<div class="login-title">🔐 Instagram Analytics</div>', unsafe_allow_html=True)
    st.markdown('<div class="login-sub">Enter password to continue</div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

    _, center, _ = st.columns([1, 2, 1])
    with center:
        pwd = st.text_input("Password", type="password", placeholder="Enter password...", label_visibility="collapsed")
        if st.button("Login", use_container_width=True):
            if pwd == APP_PASSWORD:
                st.session_state['authenticated'] = True
                st.rerun()
            else:
                st.error("❌ Incorrect password. Try again.")
    return False

# -----------------------
# Main
# -----------------------
def main():
    apply_styles()

    if not check_auth():
        st.stop()

    if 'page' not in st.session_state:
        st.session_state['page'] = 'top_posts'
    if 'selected_account' not in st.session_state:
        st.session_state['selected_account'] = None

    cache_key = int(time.time() // 300)

    with st.spinner("Loading..."):
        df = load_data(cache_key)
        df = preprocess(df)

    with st.sidebar:
        st.markdown(f'<div style="color:#5b7bfc;font-size:20px;font-weight:700;margin-bottom:2px;">{APP_TITLE}</div>', unsafe_allow_html=True)
        st.markdown(f'<div style="color:#6b7280;font-size:12px;margin-bottom:16px;">{APP_SUBTITLE}</div>', unsafe_allow_html=True)

        # ✅ Last updated time
        last_updated = get_last_scrape_time()
        st.markdown(f'<div class="last-updated">🕒 Last scraped: {last_updated}</div>', unsafe_allow_html=True)

        # ✅ Trigger scraper button
        if st.button("🚀 Run Scraper Now", use_container_width=True):
            with st.spinner("Triggering scraper..."):
                success, msg = trigger_scraper()
            if success:
                st.success(msg)
            else:
                st.error(msg)

        st.markdown("---")

        pages = {
            'Top Posts': ('🏆', 'top_posts'),
            'Accounts':  ('🏠', 'accounts'),
            'Analytics': ('📊', 'analytics'),
        }
        for label, (icon, page_id) in pages.items():
            if st.button(f"{icon}  {label}", key=f"nav_{page_id}", use_container_width=True):
                st.session_state['page'] = page_id
                st.rerun()

        # ✅ Manual cache refresh
        st.markdown("---")
        if st.button("🔄 Refresh Data", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

        st.markdown('<div class="date-range-section">', unsafe_allow_html=True)
        st.markdown('<div class="date-range-title">Date Range</div>', unsafe_allow_html=True)

        min_date     = df["post_date"].min()
        max_date     = df["post_date"].max()
        today        = date.today()
        default_date = min(max(today, min_date), max_date)

        if 'start' not in st.session_state:
            st.session_state['start'] = default_date
        if 'end' not in st.session_state:
            st.session_state['end'] = default_date

        start = st.date_input("Start", min_value=min_date, max_value=max_date, key="start")
        end   = st.date_input("End",   min_value=min_date, max_value=max_date, key="end")
        st.markdown('</div>', unsafe_allow_html=True)

        st.markdown("---")
        st.markdown('<div class="date-range-title">Filter Channels</div>', unsafe_allow_html=True)
        all_channels = sorted(df["username"].unique().tolist())

        if 'channel_filter' not in st.session_state:
            st.session_state['channel_filter'] = all_channels

        col_a, col_b = st.columns(2)
        with col_a:
            st.markdown('<div class="ch-toggle">', unsafe_allow_html=True)
            if st.button("Select All", key="select_all", use_container_width=True):
                st.session_state['channel_filter'] = all_channels
                st.rerun()
            st.markdown('</div>', unsafe_allow_html=True)
        with col_b:
            st.markdown('<div class="ch-toggle">', unsafe_allow_html=True)
            if st.button("Clear All", key="deselect_all", use_container_width=True):
                st.session_state['channel_filter'] = []
                st.rerun()
            st.markdown('</div>', unsafe_allow_html=True)

        selected_channels = st.multiselect(
            label="Channels", options=all_channels,
            default=st.session_state['channel_filter'],
            key="channel_filter", label_visibility="collapsed",
        )
        if not selected_channels:
            selected_channels = all_channels

    filtered_df = filter_by_date(df, start, end)
    filtered_df = filtered_df[filtered_df["username"].isin(selected_channels)]

    st.markdown(f'<div class="main-title">{APP_TITLE}</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="main-subtitle">{APP_SUBTITLE}</div>', unsafe_allow_html=True)

    if st.session_state['page'] == 'accounts':
        render_accounts(filtered_df)
    elif st.session_state['page'] == 'account_detail':
        render_account_detail(filtered_df, st.session_state['selected_account'])
    elif st.session_state['page'] == 'analytics':
        render_analytics(filtered_df)
    elif st.session_state['page'] == 'top_posts':
        render_top_posts(filtered_df)

if __name__ == "__main__":
    main()

#git add . && git commit -m "update" && git pull origin main --rebase && git push origin main