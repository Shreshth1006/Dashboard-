import streamlit as st
import pandas as pd
from datetime import date, timedelta, datetime
import altair as alt
import html
import logging
import requests
import os
import time
import json
import re
from supabase import create_client
from dotenv import load_dotenv
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
GITHUB_TOKEN    = os.getenv("GITHUB_TOKEN")
GITHUB_REPO     = os.getenv("GITHUB_REPO")
GITHUB_WORKFLOW = os.getenv("GITHUB_WORKFLOW")
GEMINI_API_KEY  = os.getenv("GEMINI_API_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

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

st.set_page_config(page_title=APP_TITLE, layout="wide", initial_sidebar_state="expanded")

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
    .main-subtitle { color: #6b7280; font-size: 16px; font-weight: 400; margin-bottom: 24px; }
    .section-title { color: #ffffff; font-size: 24px; font-weight: 600; margin-bottom: 24px; margin-top: 40px; }
    .last-updated { color: #6b7280; font-size: 12px; margin-top: 6px; margin-bottom: 16px; }
    .account-card { background-color: #0f0f0f; border: 1px solid #1f1f1f; border-radius: 12px; padding: 28px 24px; margin-bottom: 20px; }
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
    .date-filter-bar { background-color: #0f0f0f; border: 1px solid #1f1f1f; border-radius: 12px; padding: 14px 20px; margin-bottom: 28px; }
    .ai-box { background: linear-gradient(135deg, #0f0f1a 0%, #0a0a0f 100%); border: 1px solid #2a2a4a; border-radius: 16px; padding: 24px 28px; margin-bottom: 20px; position: relative; overflow: hidden; }
    .ai-box::before { content: ''; position: absolute; top: 0; left: 0; right: 0; height: 2px; background: linear-gradient(90deg, #5b7bfc, #a78bfa, #5b7bfc); }
    .ai-box-title { color: #a78bfa; font-size: 12px; font-weight: 700; text-transform: uppercase; letter-spacing: 1.2px; margin-bottom: 14px; }
    .ai-box-body { color: #e2e8f0; font-size: 14px; line-height: 1.85; }
    .ai-badge { display: inline-flex; align-items: center; background: linear-gradient(135deg, #1a1a3a, #0f0f2a); border: 1px solid #3a3a6a; border-radius: 20px; padding: 4px 14px; font-size: 12px; color: #a78bfa; font-weight: 600; margin-bottom: 20px; }
    .stat-card { background: #0f0f1a; border: 1px solid #1f1f3a; border-radius: 12px; padding: 16px 20px; text-align: center; }
    .stat-label { color: #6b7280; font-size: 12px; font-weight: 500; margin-bottom: 4px; }
    .stat-value { color: #ffffff; font-size: 22px; font-weight: 700; }
    .cat-pill { display: inline-block; padding: 3px 10px; border-radius: 10px; font-size: 11px; font-weight: 600; margin: 3px; }
    .missed-table { width:100%; border-collapse:collapse; font-size:13px; }
    .missed-table th { background:#1a1a2e; color:#a78bfa; padding:10px 12px; text-align:left; font-size:11px; font-weight:700; text-transform:uppercase; border-bottom:1px solid #2a2a4a; }
    .missed-table td { padding:10px 12px; border-bottom:1px solid #1a1a2a; vertical-align:top; line-height:1.6; }
    .heatmap-tbl { width:100%; border-collapse:collapse; font-size:12px; }
    .heatmap-tbl th { background:#1a1a2e; color:#a78bfa; padding:9px 6px; text-align:center; font-size:10px; font-weight:700; border-bottom:1px solid #2a2a4a; white-space:nowrap; }
    .heatmap-tbl th:first-child { text-align:left; min-width:110px; }
    .heatmap-tbl td { padding:7px 6px; border-bottom:1px solid #111; text-align:center; font-size:12px; }
    .heatmap-tbl td:first-child { text-align:left; }
    </style>
    """, unsafe_allow_html=True)

def trigger_scraper():
    if not GITHUB_TOKEN or not GITHUB_REPO or not GITHUB_WORKFLOW:
        return False, "GitHub secrets not configured."
    url = f"https://api.github.com/repos/{GITHUB_REPO}/actions/workflows/{GITHUB_WORKFLOW}/dispatches"
    headers = {"Authorization": f"Bearer {GITHUB_TOKEN}", "Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"}
    try:
        resp = requests.post(url, headers=headers, json={"ref": "main"}, timeout=10)
        if resp.status_code == 204:
            return True, "✅ Scraper triggered! Data will be updated in approximately 10-15 minutes."
        return False, f"❌ Failed: {resp.status_code} — {resp.text}"
    except Exception as e:
        return False, f"❌ Error: {e}"

def get_last_scrape_time():
    try:
        resp = supabase.table("posts").select("scraped_time").order("scraped_time", desc=True).limit(1).execute()
        if resp.data and resp.data[0].get("scraped_time"):
            dt = datetime.fromisoformat(resp.data[0]["scraped_time"])
            return dt.strftime("%-I:%M %p, %d %b %Y")
    except:
        pass
    return "Unknown"

@st.cache_data(ttl=300)
def load_data(cache_key):
    try:
        response = supabase.table("posts").select("*").order("post_time", desc=True).limit(1000).execute()
        data = response.data
        if not data:
            st.warning("⚠️ No data found in database")
            return pd.DataFrame()
        return pd.DataFrame(data)
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
            if pd.notna(x) and str(x).strip() not in ("", "0", "nan", "None") else None)
    df["posted_at_dt"] = pd.to_datetime(df["post_time"], errors="coerce")
    df = df.dropna(subset=["posted_at_dt"])
    df["post_date"] = df["posted_at_dt"].dt.date
    if "post_link" in df.columns:
        df["post_id"] = df["post_link"].apply(
            lambda x: x.split("/")[-2] if pd.notna(x) and "/" in str(x) else "")
    return df.dropna(subset=["username"])

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

def render_date_filter(df, page_key):
    min_date    = df["post_date"].min()
    max_date    = df["post_date"].max()
    today       = date.today()
    default_day = min(max(today, min_date), max_date)
    sk, ek = f"date_start_{page_key}", f"date_end_{page_key}"
    if sk not in st.session_state: st.session_state[sk] = default_day
    if ek not in st.session_state: st.session_state[ek] = default_day
    st.markdown('<div class="date-filter-bar">', unsafe_allow_html=True)
    c1, c2, c3, c4, c5 = st.columns([2, 3, 0.3, 3, 1.5])
    with c1: st.markdown('<div style="color:#6b7280;font-size:13px;font-weight:600;padding-top:6px;">📅 Date Range</div>', unsafe_allow_html=True)
    with c2: start = st.date_input("From", min_value=min_date, max_value=max_date, key=sk, label_visibility="collapsed")
    with c3: st.markdown('<div style="text-align:center;color:#6b7280;padding-top:6px;">→</div>', unsafe_allow_html=True)
    with c4: end = st.date_input("To", min_value=min_date, max_value=max_date, key=ek, label_visibility="collapsed")
    with c5:
        if st.button("Reset", key=f"reset_{page_key}", use_container_width=True):
            st.session_state[sk] = default_day
            st.session_state[ek] = default_day
            st.rerun()
    st.markdown('</div>', unsafe_allow_html=True)
    return start, end

# ===========================================================================
# AI INSIGHTS — plain text approach, no JSON parsing
# ===========================================================================
TOI_USERNAME = "timesofindia"

CATEGORIES  = ["Politics","Crime","Entertainment","Sports","Business",
                "International","Technology","Health","Lifestyle","Viral/Human Interest"]
CAT_COLORS  = {
    "Politics":"#ef4444","Crime":"#f97316","Entertainment":"#a855f7",
    "Sports":"#3b82f6","Business":"#22c55e","International":"#06b6d4",
    "Technology":"#8b5cf6","Health":"#ec4899","Lifestyle":"#f59e0b",
    "Viral/Human Interest":"#10b981",
}

# Section markers — Gemini must use these exactly
SECTION_KEYS = [
    "TOI_VS_COMPETITION",
    "BIGGEST_THREAT",
    "MISSED_OPPORTUNITIES",
    "CAPTION_IDEAS",
    "HASHTAGS",
    "ACTION_PLAN",
    "CATEGORY_BREAKDOWN",
]

def build_payload(df):
    from collections import Counter
    today = date.today()
    tdf = df[df["post_date"] == today].copy()
    if tdf.empty:
        ld = df["post_date"].max()
        tdf = df[df["post_date"] == ld].copy()
        data_date = str(ld)
    else:
        data_date = str(today)

    platforms = {}
    toi_caps  = set()
    for uname, grp in tdf.groupby("username"):
        caps = grp["caption"].dropna().tolist()
        tags = []
        for c in caps:
            tags.extend([w.lower() for w in str(c).split() if w.startswith("#")])
        top_tags = [h for h, _ in Counter(tags).most_common(10)]
        n = 15 if uname == TOI_USERNAME else 8
        sample = [str(c)[:250] for c in caps[:n]]
        if uname == TOI_USERNAME:
            toi_caps = set([str(c)[:120].lower().strip() for c in caps])
        platforms[uname] = {
            "total_posts":    len(grp),
            "total_likes":    int(grp["likes"].sum()),
            "total_comments": int(grp["comments"].sum()),
            "avg_likes":      round(float(grp["likes"].mean()), 1),
            "avg_comments":   round(float(grp["comments"].mean()), 1),
            "top_hashtags":   top_tags,
            "sample_captions":sample,
            "post_times":     grp["posted_at_dt"].dt.strftime("%H:%M").tolist()[:10],
        }

    rivals = tdf[tdf["username"] != TOI_USERNAME].sort_values("likes", ascending=False)
    missed, seen = [], set()
    for _, row in rivals.iterrows():
        cap = str(row.get("caption",""))[:200]
        key = cap[:80].lower().strip()
        if key in seen: continue
        fw = " ".join(cap.split()[:6]).lower()
        if not any(fw[:30] in t for t in toi_caps) and len(missed) < 8:
            missed.append({"rival": row["username"], "likes": int(row["likes"]),
                           "caption_snippet": cap})
            seen.add(key)

    viral = [{"rival": r["username"], "likes": int(r["likes"]),
              "caption_snippet": str(r.get("caption",""))[:150]}
             for _, r in rivals[rivals["likes"] >= 50000].head(3).iterrows()]

    toi_df = tdf[tdf["username"] == TOI_USERNAME]
    best_hrs = toi_df.nlargest(max(1, len(toi_df)//5), "likes")["posted_at_dt"].dt.strftime("%H:%M").tolist() if not toi_df.empty else []

    return {"data_date": data_date, "total_posts": len(tdf),
            "platforms": platforms, "missed": missed,
            "viral": viral, "toi_best_hours": best_hrs,
            "toi_df_raw": tdf}


def call_gemini(payload):
    if not GEMINI_API_KEY:
        return "ERROR: GEMINI_API_KEY not set."

    toi  = payload["platforms"].get(TOI_USERNAME, {})
    rivals = {k: v for k, v in payload["platforms"].items() if k != TOI_USERNAME}
    cats = ", ".join(CATEGORIES)
    channels = list(payload["platforms"].keys())

    prompt = f"""You are a senior social media strategist for Times of India (TOI) Instagram team.
Analyse today's data and return your response using EXACTLY the section markers below.
Each section starts with ===SECTION_NAME=== on its own line.
Do NOT use markdown, asterisks, bullet dashes, or JSON. Write in plain readable sentences and numbered lists only.

Today: {payload['data_date']}
TOI data: {json.dumps(toi)}
Rivals: {json.dumps(rivals)}
Missed (rival posts TOI didn't cover, sorted by likes): {json.dumps(payload['missed'])}
Viral rival posts (>50K likes): {json.dumps(payload['viral'])}
TOI best performing post times today: {payload['toi_best_hours']}
All channels in data: {channels}

===TOI_VS_COMPETITION===
Write 3-4 sentences comparing TOI's total likes, avg likes per post, and comments vs the top 3 rivals. Use exact numbers.

===BIGGEST_THREAT===
Name one channel. Then 3-4 sentences explaining exactly why they beat TOI today with specific numbers and what content worked.

===MISSED_OPPORTUNITIES===
List 4-6 specific stories/topics rivals covered that TOI missed. For each write:
TOPIC: [topic name] | RIVAL: @[channel] | THEIR LIKES: [number]
Story summary in one sentence.

===CAPTION_IDEAS===
For each topic in MISSED_OPPORTUNITIES, write a ready-to-post TOI Instagram caption.
Format each as:
CAPTION FOR [TOPIC NAME]:
[2-3 sentence punchy English caption TOI can post right now]

===HASHTAGS===
List exactly 10 hashtags TOI should use today based on trending topics. One per line, starting with #.

===ACTION_PLAN===
List 5 numbered concrete actions TOI team should take right now — specific post timings, content gaps, quick wins.

===CATEGORY_BREAKDOWN===
For each channel estimate how many of their posts today fall into each category.
Use this exact format for every channel:
CHANNEL: @[channelname]
Politics:[n] Crime:[n] Entertainment:[n] Sports:[n] Business:[n] International:[n] Technology:[n] Health:[n] Lifestyle:[n] Viral:[n]

Categories to use: {cats}
"""

    try:
        resp = requests.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/gemini-flash-latest:generateContent?key={GEMINI_API_KEY}",
            headers={"Content-Type": "application/json"},
            json={"contents": [{"parts": [{"text": prompt}]}],
                  "generationConfig": {"temperature": 0.4, "maxOutputTokens": 3500}},
            timeout=90,
        )
        resp.raise_for_status()
        return resp.json()["candidates"][0]["content"]["parts"][0]["text"]
    except requests.exceptions.Timeout:
        return "ERROR: Request timed out."
    except requests.exceptions.HTTPError as e:
        return f"ERROR: API {e.response.status_code}"
    except Exception as e:
        return f"ERROR: {e}"


def extract_sections(text):
    """Split raw text into dict of section_key -> content using ===KEY=== markers."""
    result = {}
    pattern = r'===([A-Z_]+)==='
    parts   = re.split(pattern, text)
    # parts = [before_first, KEY1, content1, KEY2, content2, ...]
    for i in range(1, len(parts) - 1, 2):
        key     = parts[i].strip()
        content = parts[i + 1].strip() if i + 1 < len(parts) else ""
        result[key] = content
    return result


def parse_category_breakdown(text, channels):
    """Parse CATEGORY_BREAKDOWN section into dict of channel -> {cat: count}."""
    result = {}
    short_map = {"Viral": "Viral/Human Interest"}
    for line_block in re.split(r'CHANNEL:\s*', text):
        if not line_block.strip():
            continue
        lines = line_block.strip().splitlines()
        ch_line = lines[0].strip().lstrip("@").strip()
        # Match to known channels (fuzzy)
        matched = None
        for ch in channels:
            if ch.lower() in ch_line.lower() or ch_line.lower() in ch.lower():
                matched = ch
                break
        if not matched:
            continue
        counts = {c: 0 for c in CATEGORIES}
        for line in lines[1:]:
            for token in line.split():
                if ":" in token:
                    k, _, v = token.partition(":")
                    k = short_map.get(k.strip(), k.strip())
                    if k in counts:
                        try:
                            counts[k] = int(v.strip())
                        except ValueError:
                            pass
        result[matched] = counts
    return result


def render_ai_insights(df):
    st.markdown('<div class="section-title">🤖 AI Insights</div>', unsafe_allow_html=True)
    st.markdown('<div class="ai-badge">✨ Powered by Gemini</div>', unsafe_allow_html=True)

    today = date.today()
    tdf   = df[df["post_date"] == today]
    if tdf.empty:
        ld  = df["post_date"].max()
        tdf = df[df["post_date"] == ld]
        dlabel = f"Latest available: {ld.strftime('%d %b %Y')}"
    else:
        dlabel = f"Today · {today.strftime('%d %b %Y')}"

    st.markdown(f'<div style="color:#6b7280;font-size:13px;margin-bottom:20px;">📅 Analysing: <span style="color:#a78bfa;font-weight:600;">{dlabel}</span></div>', unsafe_allow_html=True)

    c1, c2, c3, c4 = st.columns(4)
    for col, lbl, val in [(c1,"Posts Today",f"{len(tdf):,}"),
                           (c2,"Channels Active",f"{tdf['username'].nunique()}"),
                           (c3,"Total Likes",format_number(int(tdf['likes'].sum()))),
                           (c4,"Total Comments",format_number(int(tdf['comments'].sum())))]:
        with col:
            st.markdown(f'<div class="stat-card"><div class="stat-label">{lbl}</div><div class="stat-value">{val}</div></div>', unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    ck = f"ai_v5_{str(tdf['post_date'].max()) if not tdf.empty else 'nd'}_{len(tdf)}"

    col_btn, col_note = st.columns([2, 8])
    with col_btn:
        if st.button("🔄 Re-run Analysis", key="rerun_ai"):
            if ck in st.session_state: del st.session_state[ck]
            st.rerun()
    with col_note:
        st.markdown('<div style="color:#6b7280;font-size:12px;padding-top:12px;">Cached per snapshot. Click Re-run to refresh.</div>', unsafe_allow_html=True)

    if ck not in st.session_state:
        with st.spinner("🤖 Analysing... 20-40 seconds"):
            payload = build_payload(df)
            if payload["total_posts"] == 0:
                st.session_state[ck] = {"_error": "No posts found for analysis."}
            else:
                raw = call_gemini(payload)
                if raw.startswith("ERROR"):
                    st.session_state[ck] = {"_error": raw}
                else:
                    secs = extract_sections(raw)
                    secs["_channels"] = list(payload["platforms"].keys())
                    secs["_raw"] = raw
                    st.session_state[ck] = secs

    S = st.session_state.get(ck, {})

    if "_error" in S:
        st.error(S["_error"])
        return

    if not S or "_raw" not in S:
        st.warning("No analysis available. Click Re-run.")
        return

    channels = S.get("_channels", [])

    # ── ROW 1: TOI vs Competition | Biggest Threat ───────────────────────
    col_l, col_r = st.columns(2)
    with col_l:
        body = S.get("TOI_VS_COMPETITION", "No data.")
        st.markdown(f'<div class="ai-box"><div class="ai-box-title">📊 TOI vs Competition</div><div class="ai-box-body">{body}</div></div>', unsafe_allow_html=True)
    with col_r:
        body = S.get("BIGGEST_THREAT", "No data.")
        st.markdown(f'<div class="ai-box"><div class="ai-box-title">🎯 Biggest Threat Today</div><div class="ai-box-body">{body}</div></div>', unsafe_allow_html=True)

    # ── MISSED OPPORTUNITIES TABLE ────────────────────────────────────────
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown("### ⚡ Missed Opportunities")
    missed_raw = S.get("MISSED_OPPORTUNITIES", "")
    caption_raw = S.get("CAPTION_IDEAS", "")

    # Parse missed lines: TOPIC: x | RIVAL: @y | THEIR LIKES: z
    missed_rows = []
    for line in missed_raw.splitlines():
        m = re.match(r'TOPIC:\s*(.+?)\s*\|\s*RIVAL:\s*@?(\S+)\s*\|\s*THEIR LIKES:\s*([\d,KkMm]+)', line)
        if m:
            topic, rival, likes_str = m.group(1).strip(), m.group(2).strip(), m.group(3).strip()
            # Try to find matching caption
            cap_match = re.search(rf'CAPTION FOR {re.escape(topic.upper())}[:\s]*\n(.*?)(?=\nCAPTION FOR|\Z)', caption_raw, re.DOTALL | re.IGNORECASE)
            caption = cap_match.group(1).strip() if cap_match else ""
            missed_rows.append((topic, rival, likes_str, caption))

    if missed_rows:
        rows_html = "".join([f"""<tr>
            <td style="color:#f97316;font-weight:600;">@{r}</td>
            <td style="color:#ef4444;font-weight:700;">{lk}</td>
            <td style="color:#fbbf24;font-weight:600;">{t}</td>
            <td style="color:#a78bfa;font-style:italic;">{c}</td>
        </tr>""" for t, r, lk, c in missed_rows])
        st.markdown(f'<div style="background:#0f0f1a;border:1px solid #2a2a4a;border-radius:12px;padding:4px;overflow-x:auto;"><table class="missed-table"><thead><tr><th>Rival</th><th>Likes</th><th>Topic Missed</th><th>✍️ Caption for TOI</th></tr></thead><tbody>{rows_html}</tbody></table></div>', unsafe_allow_html=True)
    else:
        # Fallback: show raw text in boxes
        col_l2, col_r2 = st.columns(2)
        with col_l2:
            st.markdown(f'<div class="ai-box"><div class="ai-box-title">⚡ Missed Opportunities</div><div class="ai-box-body">{missed_raw}</div></div>', unsafe_allow_html=True)
        with col_r2:
            st.markdown(f'<div class="ai-box"><div class="ai-box-title">✍️ Caption Ideas</div><div class="ai-box-body">{caption_raw}</div></div>', unsafe_allow_html=True)

    # ── CATEGORY HEATMAP ─────────────────────────────────────────────────
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown("### 📊 Content Category Heatmap")
    cat_raw  = S.get("CATEGORY_BREAKDOWN", "")
    cat_data = parse_category_breakdown(cat_raw, channels)

    if cat_data:
        header = "".join([f'<th title="{c}">{c[:5]}.</th>' for c in CATEGORIES])
        rows   = ""
        for ch in channels:
            cats = cat_data.get(ch, {c: 0 for c in CATEGORIES})
            total = sum(cats.values()) or 1
            cells = ""
            for cat in CATEGORIES:
                n   = int(cats.get(cat, 0))
                pct = n / total
                if pct == 0:     bg, fg, fw = "#080810","#222","400"
                elif pct < 0.15: bg, fg, fw = CAT_COLORS[cat]+"33","#9ca3af","400"
                elif pct < 0.30: bg, fg, fw = CAT_COLORS[cat]+"66","#e2e8f0","600"
                else:            bg, fg, fw = CAT_COLORS[cat]+"cc","#ffffff","700"
                cells += f'<td style="background:{bg};color:{fg};font-weight:{fw};">{n if n else ""}</td>'
            is_toi = ch == TOI_USERNAME
            nc = "#5b7bfc" if is_toi else "#e2e8f0"
            fw2 = "700" if is_toi else "400"
            rows += f'<tr><td style="color:{nc};font-weight:{fw2};white-space:nowrap;">{"⭐ " if is_toi else ""}@{ch}</td>{cells}</tr>'

        legend = "".join([f'<span class="cat-pill" style="background:{CAT_COLORS[c]}33;color:{CAT_COLORS[c]};border:1px solid {CAT_COLORS[c]}55;">{c}</span>' for c in CATEGORIES])
        st.markdown(f'<div style="background:#0f0f1a;border:1px solid #2a2a4a;border-radius:12px;padding:4px;overflow-x:auto;"><table class="heatmap-tbl"><thead><tr><th>Channel</th>{header}</tr></thead><tbody>{rows}</tbody></table></div><div style="margin-top:10px;">{legend}</div><div style="color:#6b7280;font-size:11px;margin-top:6px;">Numbers = estimated posts per category. Darker = higher share. ⭐ = TOI</div>', unsafe_allow_html=True)
    else:
        st.markdown(f'<div class="ai-box"><div class="ai-box-title">📊 Category Breakdown</div><div class="ai-box-body">{cat_raw}</div></div>', unsafe_allow_html=True)

    # ── HASHTAGS + ACTION PLAN ────────────────────────────────────────────
    st.markdown("<br>", unsafe_allow_html=True)
    col_h, col_a = st.columns(2)
    with col_h:
        tags = [t.strip() for t in S.get("HASHTAGS","").splitlines() if t.strip().startswith("#")]
        if tags:
            pills = "".join([f'<span class="cat-pill" style="background:#1a1a3a;color:#a78bfa;border:1px solid #3a3a6a;">{t}</span>' for t in tags])
            st.markdown(f'<div class="ai-box"><div class="ai-box-title">#️⃣ Hashtags for TOI Today</div><div style="margin-top:8px;line-height:2.4;">{pills}</div></div>', unsafe_allow_html=True)
        else:
            st.markdown(f'<div class="ai-box"><div class="ai-box-title">#️⃣ Hashtags for TOI Today</div><div class="ai-box-body">{S.get("HASHTAGS","")}</div></div>', unsafe_allow_html=True)
    with col_a:
        actions = [l.strip() for l in S.get("ACTION_PLAN","").splitlines() if l.strip()]
        if actions:
            items = "".join([f'<div style="color:#e2e8f0;font-size:13px;padding:6px 0;border-bottom:1px solid #1a1a2a;">{a}</div>' for a in actions])
            st.markdown(f'<div class="ai-box"><div class="ai-box-title">💡 Action Plan for TOI</div>{items}</div>', unsafe_allow_html=True)
        else:
            st.markdown(f'<div class="ai-box"><div class="ai-box-title">💡 Action Plan for TOI</div><div class="ai-box-body">{S.get("ACTION_PLAN","")}</div></div>', unsafe_allow_html=True)

    # ── CHANNEL STATS ─────────────────────────────────────────────────────
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown("### 📋 Channel Stats (Today)")
    cs = tdf.groupby("username").agg(posts=("likes","count"), likes=("likes","sum"), comments=("comments","sum")).reset_index()
    cs["avg_likes"] = (cs["likes"] / cs["posts"]).round(0).astype(int)
    cs = cs.sort_values("likes", ascending=False).reset_index(drop=True)
    cols = st.columns(3)
    for i, row in cs.iterrows():
        is_toi = row["username"] == TOI_USERNAME
        with cols[i % 3]:
            st.markdown(f"""<div class="stat-card" style="text-align:left;margin-bottom:16px;border-color:{'#5b7bfc' if is_toi else '#1f1f3a'};">
                <div style="color:{'#5b7bfc' if is_toi else '#a78bfa'};font-weight:700;font-size:14px;margin-bottom:10px;">{'⭐ ' if is_toi else ''}@{row['username']}</div>
                <div style="display:flex;gap:16px;flex-wrap:wrap;">
                    <div><div class="stat-label">Posts</div><div style="color:#fff;font-weight:600;">{row['posts']}</div></div>
                    <div><div class="stat-label">Likes</div><div style="color:#fff;font-weight:600;">{format_number(row['likes'])}</div></div>
                    <div><div class="stat-label">Comments</div><div style="color:#fff;font-weight:600;">{format_number(row['comments'])}</div></div>
                    <div><div class="stat-label">Avg Likes</div><div style="color:#fff;font-weight:600;">{format_number(row['avg_likes'])}</div></div>
                </div></div>""", unsafe_allow_html=True)

# ===========================================================================
# VIEWS — unchanged
# ===========================================================================
def render_accounts(df):
    start, end = render_date_filter(df, page_key="accounts")
    df = filter_by_date(df, start, end)
    st.markdown('<div class="section-title">Select Account</div>', unsafe_allow_html=True)
    stats = df.groupby("username").agg({'post_id': 'count', 'likes': 'sum'}).reset_index()
    stats.columns = ['username', 'posts', 'total_likes']
    stats = stats.sort_values('total_likes', ascending=False).reset_index(drop=True)
    st.markdown("""<style>
    div[data-testid="stHorizontalBlock"] .stButton > button {
        background-color: #0f0f0f !important; border: 1px solid #1f1f1f !important;
        border-radius: 12px !important; padding: 20px !important; text-align: left !important;
        height: auto !important; min-height: 80px !important; white-space: normal !important;
        line-height: 1.5 !important; color: #ffffff !important; font-size: 15px !important;
        font-weight: 500 !important; transition: all 0.2s ease !important; }
    div[data-testid="stHorizontalBlock"] .stButton > button:hover {
        border-color: #5b7bfc !important; background-color: #121212 !important;
        transform: translateY(-2px) !important; box-shadow: 0 8px 16px rgba(91,123,252,0.15) !important; }
    </style>""", unsafe_allow_html=True)
    cols = st.columns(3)
    for idx, row in stats.iterrows():
        with cols[idx % 3]:
            logo_path = get_logo_path(row['username'])
            label = f"@{row['username']}\n\n{row['posts']} posts  ·  {format_number(row['total_likes'])} likes"
            bc, lc = st.columns([4, 1])
            with bc:
                if st.button(label, key=f"btn_{row['username']}", use_container_width=True):
                    st.session_state['selected_account'] = row['username']
                    st.session_state['page'] = 'account_detail'
                    st.rerun()
            with lc:
                st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)
                if logo_path: st.image(logo_path, width=56)
                else: st.markdown(f'<div style="width:52px;height:52px;border-radius:8px;background:#1a1a2e;display:flex;align-items:center;justify-content:center;border:1px solid #2a2a2a;color:#5b7bfc;font-size:22px;font-weight:700;">{row["username"][0].upper()}</div>', unsafe_allow_html=True)

def render_account_detail(df, username):
    start, end = render_date_filter(df, page_key="account_detail")
    df = filter_by_date(df, start, end)
    adf = df[df["username"] == username].copy()
    if adf.empty:
        st.warning(f"No data for @{username} in the selected date range.")
        return
    c1, c2 = st.columns([8, 1])
    with c1: st.markdown(f'<div class="section-title">@{username}</div>', unsafe_allow_html=True)
    with c2:
        if st.button("← Back", key="back"):
            st.session_state['page'] = 'accounts'
            st.rerun()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Posts",          f"{len(adf):,}")
    c2.metric("Total Likes",    format_number(adf['likes'].sum()))
    c3.metric("Total Comments", format_number(adf['comments'].sum()))
    c4.metric("Avg Likes",      format_number(int(adf['likes'].mean())))
    st.markdown("---")
    st.markdown("### Recent Posts")
    for _, row in adf.sort_values('posted_at_dt', ascending=False).iterrows():
        with st.container():
            c1, c2, c3 = st.columns([1, 4, 2])
            with c1:
                if pd.notna(row.get('media_url')): st.image(row['media_url'], use_container_width=True)
            with c2:
                cap = str(row.get('caption',''))[:200]
                st.markdown(f"**{cap}...**" if len(str(row.get('caption',''))) > 200 else f"**{cap}**")
                if pd.notna(row.get('posted_at_dt')): st.caption(f"🕒 {row['posted_at_dt'].strftime('%d %b, %I:%M %p')}")
                if pd.notna(row.get('post_link')) and str(row.get('post_link','')).startswith('http'):
                    st.markdown(f"[🔗 View Original Post]({row['post_link']})")
            with c3:
                st.metric("❤️", f"{row['likes']:,}")
                st.metric("💬", f"{row['comments']:,}")
        st.markdown("---")

def render_analytics(df):
    start, end = render_date_filter(df, page_key="analytics")
    df = filter_by_date(df, start, end)
    st.markdown('<div class="section-title">Analytics Overview</div>', unsafe_allow_html=True)
    if df.empty:
        st.warning("No data available for the selected date range.")
        return
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Posts",    f"{len(df):,}")
    c2.metric("Accounts",       f"{df['username'].nunique()}")
    c3.metric("Total Likes",    format_number(df['likes'].sum()))
    c4.metric("Total Comments", format_number(df['comments'].sum()))
    st.markdown("---")
    account_stats = df.groupby("username").agg(
        posts=("post_id","count"), total_likes=("likes","sum"), avg_likes=("likes","mean"),
        total_comments=("comments","sum"), followers=("followers","first"),
    ).reset_index()
    account_stats["total_engagement"]  = account_stats["total_likes"] + 10 * account_stats["total_comments"]
    account_stats["engagement_rate_%"] = (account_stats["total_engagement"] / account_stats["followers"] * 100).round(2)
    st.markdown("### Performance by Account")
    st.dataframe(
        account_stats[["username","posts","total_likes","avg_likes","total_comments","followers","engagement_rate_%"]]
        .sort_values("total_likes", ascending=False)
        .style.format({"posts":"{:,}","total_likes":"{:,}","avg_likes":"{:,.0f}","total_comments":"{:,}","followers":"{:,}","engagement_rate_%":"{:.2f}%"}),
        use_container_width=True, hide_index=True)
    st.markdown("---")
    cl, cr = st.columns(2)
    with cl:
        st.markdown("### 🥧 Likes Share by Channel")
        pie = alt.Chart(account_stats).mark_arc(innerRadius=50).encode(
            theta=alt.Theta("total_likes:Q", stack=True),
            color=alt.Color("username:N", legend=alt.Legend(orient="bottom", columns=2), scale=alt.Scale(scheme="tableau10")),
            tooltip=[alt.Tooltip("username:N", title="Channel"), alt.Tooltip("total_likes:Q", title="Total Likes", format=",")],
        ).properties(height=340)
        st.altair_chart(pie, use_container_width=True)
    with cr:
        st.markdown("### 📈 Engagement Rate by Channel")
        bar = alt.Chart(account_stats).mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4).encode(
            x=alt.X("username:N", sort="-y", title="Channel", axis=alt.Axis(labelAngle=-35)),
            y=alt.Y("engagement_rate_%:Q", title="Engagement Rate (%)"),
            color=alt.Color("username:N", legend=None, scale=alt.Scale(scheme="tableau10")),
            tooltip=[alt.Tooltip("username:N", title="Channel"), alt.Tooltip("engagement_rate_%:Q", format=".2f"), alt.Tooltip("followers:Q", format=",")],
        ).properties(height=340)
        st.altair_chart(bar, use_container_width=True)

def render_top_posts(df):
    start, end = render_date_filter(df, page_key="top_posts")
    df = filter_by_date(df, start, end)
    st.markdown('<div class="section-title">Top Posts</div>', unsafe_allow_html=True)
    c1, c2 = st.columns(2)
    with c1: n = st.selectbox("Show top", [10, 20, 30, 50], key="top_n")
    with c2: metric = st.selectbox("Ranked by", ["Latest", "Likes", "Comments"], key="metric")
    if df.empty:
        st.warning("No data available for the selected date range.")
        return
    if metric == "Latest":   top = df.sort_values('posted_at_dt', ascending=False).head(n)
    elif metric == "Likes":  top = df.nlargest(n, "likes")
    else:                    top = df.nlargest(n, "comments")
    st.markdown(f"### Top {n} by {metric}")
    for _, row in top.iterrows():
        with st.container():
            c1, c2, c3 = st.columns([1, 4, 2])
            with c1:
                if pd.notna(row.get('media_url')): st.image(row['media_url'], use_container_width=True)
            with c2:
                st.markdown(f"**@{row['username']}**")
                cap = str(row.get('caption',''))[:200]
                st.markdown(f"{cap}..." if len(str(row.get('caption',''))) > 200 else cap)
                if pd.notna(row.get('posted_at_dt')): st.caption(f"🕒 {row['posted_at_dt'].strftime('%d %b, %I:%M %p')} IST")
                if pd.notna(row.get('post_link')) and str(row.get('post_link','')).startswith('http'):
                    st.markdown(f"[🔗 View Original Post]({row['post_link']})")
            with c3:
                st.metric("❤️", f"{row['likes']:,}")
                st.metric("💬", f"{row['comments']:,}")
        st.markdown("---")

APP_PASSWORD = "TOI@1234"

def check_auth():
    if st.session_state.get('authenticated'): return True
    st.markdown("""<style>
    .login-wrap { max-width:380px; margin:120px auto 0 auto; background:#0f0f0f; border:1px solid #1f1f1f; border-radius:16px; padding:40px 36px; text-align:center; }
    .login-title { color:#5b7bfc; font-size:26px; font-weight:700; margin-bottom:6px; }
    .login-sub { color:#6b7280; font-size:14px; margin-bottom:28px; }
    </style>""", unsafe_allow_html=True)
    st.markdown('<div class="login-wrap"><div class="login-title">🔐 Instagram Analytics</div><div class="login-sub">Enter password to continue</div></div>', unsafe_allow_html=True)
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

def main():
    apply_styles()
    if not check_auth(): st.stop()
    if 'page' not in st.session_state: st.session_state['page'] = 'top_posts'
    if 'selected_account' not in st.session_state: st.session_state['selected_account'] = None

    cache_key = int(time.time() // 300)
    with st.spinner("Loading..."):
        df = load_data(cache_key)
        df = preprocess(df)

    with st.sidebar:
        st.markdown(f'<div style="color:#5b7bfc;font-size:20px;font-weight:700;margin-bottom:2px;">{APP_TITLE}</div>', unsafe_allow_html=True)
        st.markdown(f'<div style="color:#6b7280;font-size:12px;margin-bottom:16px;">{APP_SUBTITLE}</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="last-updated">🕒 Last scraped: {get_last_scrape_time()}</div>', unsafe_allow_html=True)
        if st.button("🚀 Run Scraper Now", use_container_width=True):
            with st.spinner("Triggering scraper..."):
                ok, msg = trigger_scraper()
            if ok: st.success(msg)
            else:  st.error(msg)
        st.markdown("---")
        for label, (icon, pid) in {'Top Posts':('🏆','top_posts'),'Accounts':('🏠','accounts'),'Analytics':('📊','analytics'),'AI Insights':('🤖','ai_insights')}.items():
            if st.button(f"{icon}  {label}", key=f"nav_{pid}", use_container_width=True):
                st.session_state['page'] = pid
                st.rerun()
        st.markdown("---")
        if st.button("🔄 Refresh Data", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
        st.markdown("---")
        st.markdown('<div class="date-range-title">Filter Channels</div>', unsafe_allow_html=True)
        all_channels = sorted(df["username"].unique().tolist())
        if 'channel_filter' not in st.session_state: st.session_state['channel_filter'] = all_channels
        ca, cb = st.columns(2)
        with ca:
            st.markdown('<div class="ch-toggle">', unsafe_allow_html=True)
            if st.button("Select All", key="select_all", use_container_width=True):
                st.session_state['channel_filter'] = all_channels; st.rerun()
            st.markdown('</div>', unsafe_allow_html=True)
        with cb:
            st.markdown('<div class="ch-toggle">', unsafe_allow_html=True)
            if st.button("Clear All", key="deselect_all", use_container_width=True):
                st.session_state['channel_filter'] = []; st.rerun()
            st.markdown('</div>', unsafe_allow_html=True)
        selected_channels = st.multiselect("Channels", options=all_channels, default=st.session_state['channel_filter'], key="channel_filter", label_visibility="collapsed")
        if not selected_channels: selected_channels = all_channels

    fdf = df[df["username"].isin(selected_channels)]
    st.markdown(f'<div class="main-title">{APP_TITLE}</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="main-subtitle">{APP_SUBTITLE}</div>', unsafe_allow_html=True)

    p = st.session_state['page']
    if p == 'accounts':        render_accounts(fdf)
    elif p == 'account_detail': render_account_detail(fdf, st.session_state['selected_account'])
    elif p == 'analytics':     render_analytics(fdf)
    elif p == 'top_posts':     render_top_posts(fdf)
    elif p == 'ai_insights':   render_ai_insights(fdf)

if __name__ == "__main__":
    main()

#git add . && git commit -m "update" && git pull origin main --rebase && git push origin main