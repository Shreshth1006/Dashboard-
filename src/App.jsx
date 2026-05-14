import { useState, useEffect, useCallback, useRef } from "react";
import { createClient } from "@supabase/supabase-js";

// ─── Supabase ────────────────────────────────────────────────────────────────
const supabase = createClient(
  import.meta.env.VITE_SUPABASE_URL,
  import.meta.env.VITE_SUPABASE_ANON_KEY
);

// ─── Constants ───────────────────────────────────────────────────────────────
const APP_TITLE = "Instagram Analytics";
const APP_SUBTITLE = "Channel Performance Dashboard";
const TOI_USERNAME = "timesofindia";

const CATEGORIES = [
  "Politics","Crime","Entertainment","Sports","Business",
  "International","Technology","Health","Lifestyle","Viral/Human Interest",
];

const CAT_COLORS = {
  Politics: "#ef4444", Crime: "#f97316", Entertainment: "#a855f7",
  Sports: "#3b82f6", Business: "#22c55e", International: "#06b6d4",
  Technology: "#8b5cf6", Health: "#ec4899", Lifestyle: "#f59e0b",
  "Viral/Human Interest": "#10b981",
};

// ─── Helpers ─────────────────────────────────────────────────────────────────
function formatNumber(num) {
  if (num >= 1_000_000) return `${(num / 1_000_000).toFixed(1)}M`;
  if (num >= 1_000) return `${(num / 1_000).toFixed(1)}K`;
  return String(Math.floor(num));
}

function toDateStr(d) {
  return d instanceof Date ? d.toISOString().slice(0, 10) : String(d);
}

function parseDate(s) {
  if (!s) return null;
  const d = new Date(s);
  return isNaN(d) ? null : d;
}

function postDate(row) {
  const d = parseDate(row.post_time);
  return d ? d.toISOString().slice(0, 10) : null;
}

function fmtDateTime(isoStr) {
  const d = parseDate(isoStr);
  if (!d) return "";
  return d.toLocaleString("en-IN", {
    day: "2-digit", month: "short",
    hour: "2-digit", minute: "2-digit", hour12: true,
  });
}

function todayStr() {
  return new Date().toISOString().slice(0, 10);
}

function getFirstName(email) {
  return (email || "").split(".")[0].split("@")[0];
}

// ─── Supabase helpers ─────────────────────────────────────────────────────────
async function fetchPosts() {
  const { data, error } = await supabase
    .from("posts")
    .select("*")
    .order("post_time", { ascending: false })
    .limit(1000);
  if (error) throw error;
  return data || [];
}

async function fetchLastScrapeTime() {
  try {
    const { data } = await supabase
      .table("posts")
      .select("scraped_time")
      .order("scraped_time", { ascending: false })
      .limit(1);
    if (data?.[0]?.scraped_time) {
      return fmtDateTime(data[0].scraped_time);
    }
  } catch {}
  return "Unknown";
}

async function loginUser(email, password) {
  const { data, error } = await supabase
    .from("dashboard_users")
    .select("*")
    .eq("user_email", email.trim().toLowerCase())
    .eq("user_password", password)
    .single();
  if (error || !data) return null;
  return data;
}

// ─── Session / presence ───────────────────────────────────────────────────────
async function upsertSession(user) {
  // Update last_seen if row exists, insert if not — email is unique key
  const { error } = await supabase.from("active_sessions")
    .upsert({
      user_email: user.user_email,
      user_name:  user.user_name,
      last_seen:  new Date().toISOString(),
    }, { onConflict: "user_email", ignoreDuplicates: false });
  if (error) console.warn("Session upsert error:", error.message);
}

async function removeSession(email) {
  await supabase.from("active_sessions").delete().eq("user_email", email);
}

async function fetchActiveSessions() {
  // Active = last_seen within last 3 minutes
  const cutoff = new Date(Date.now() - 3 * 60 * 1000).toISOString();
  const { data, error } = await supabase
    .from("active_sessions")
    .select("user_name, user_email, last_seen")
    .gte("last_seen", cutoff);
  if (error) console.warn("Fetch sessions error:", error.message);
  return data || [];
}

// ─── GitHub scraper trigger ───────────────────────────────────────────────────
async function triggerScraper() {
  const token    = import.meta.env.VITE_GITHUB_TOKEN;
  const repo     = import.meta.env.VITE_GITHUB_REPO;
  const workflow = import.meta.env.VITE_GITHUB_WORKFLOW;
  if (!token || !repo || !workflow)
    return { ok: false, msg: "GitHub secrets not configured." };
  try {
    const res = await fetch(
      `https://api.github.com/repos/${repo}/actions/workflows/${workflow}/dispatches`,
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${token}`,
          Accept: "application/vnd.github+json",
          "X-GitHub-Api-Version": "2022-11-28",
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ ref: "main" }),
      }
    );
    if (res.status === 204)
      return { ok: true, msg: "✅ Scraper triggered! Data updates in ~10-15 minutes." };
    return { ok: false, msg: `❌ Failed: ${res.status}` };
  } catch (e) {
    return { ok: false, msg: `❌ Error: ${e.message}` };
  }
}

// ─── Gemini AI ────────────────────────────────────────────────────────────────
function buildPayload(rows) {
  const today = todayStr();
  let trows = rows.filter(r => postDate(r) === today);
  let dataDate = today;
  if (!trows.length) {
    const dates = rows.map(r => postDate(r)).filter(Boolean).sort();
    const last = dates[dates.length - 1];
    trows = rows.filter(r => postDate(r) === last);
    dataDate = last || today;
  }

  const platforms = {};
  const toiCaps = new Set();

  const grouped = {};
  for (const row of trows) {
    const u = row.username;
    if (!grouped[u]) grouped[u] = [];
    grouped[u].push(row);
  }

  for (const [uname, grp] of Object.entries(grouped)) {
    const caps = grp.map(r => r.caption).filter(Boolean);
    const tagCounts = {};
    for (const c of caps) {
      for (const w of String(c).split(" ")) {
        if (w.startsWith("#")) {
          const t = w.toLowerCase();
          tagCounts[t] = (tagCounts[t] || 0) + 1;
        }
      }
    }
    const topTags = Object.entries(tagCounts).sort((a,b)=>b[1]-a[1]).slice(0,10).map(([t])=>t);
    const n = uname === TOI_USERNAME ? 15 : 8;
    const sample = caps.slice(0, n).map(c => String(c).slice(0, 250));
    if (uname === TOI_USERNAME) {
      for (const c of caps) toiCaps.add(String(c).slice(0, 120).toLowerCase().trim());
    }
    const likes = grp.map(r => Number(r.likes) || 0);
    const comments = grp.map(r => Number(r.comments) || 0);
    platforms[uname] = {
      total_posts: grp.length,
      total_likes: likes.reduce((a,b)=>a+b,0),
      total_comments: comments.reduce((a,b)=>a+b,0),
      avg_likes: +(likes.reduce((a,b)=>a+b,0)/Math.max(likes.length,1)).toFixed(1),
      avg_comments: +(comments.reduce((a,b)=>a+b,0)/Math.max(comments.length,1)).toFixed(1),
      top_hashtags: topTags,
      sample_captions: sample,
    };
  }

  const rivals = trows
    .filter(r => r.username !== TOI_USERNAME)
    .sort((a,b) => (Number(b.likes)||0) - (Number(a.likes)||0));

  const missed = [];
  const seen = new Set();
  for (const row of rivals) {
    const cap = String(row.caption || "").slice(0, 200);
    const key = cap.slice(0, 80).toLowerCase().trim();
    if (seen.has(key)) continue;
    const fw = cap.split(" ").slice(0,6).join(" ").toLowerCase();
    const inToi = [...toiCaps].some(t => t.includes(fw.slice(0,30)));
    if (!inToi && missed.length < 8) {
      missed.push({ rival: row.username, likes: Number(row.likes)||0, caption_snippet: cap });
      seen.add(key);
    }
  }

  const viral = rivals
    .filter(r => (Number(r.likes)||0) >= 50000)
    .slice(0,3)
    .map(r => ({ rival: r.username, likes: Number(r.likes)||0, caption_snippet: String(r.caption||"").slice(0,150) }));

  return {
    data_date: dataDate,
    total_posts: trows.length,
    platforms,
    missed,
    viral,
    channels: Object.keys(platforms),
  };
}

async function callGemini(payload) {
  const key = import.meta.env.VITE_GEMINI_API_KEY;
  if (!key) return "ERROR: GEMINI_API_KEY not set.";

  const toi = payload.platforms[TOI_USERNAME] || {};
  const rivals = Object.fromEntries(Object.entries(payload.platforms).filter(([k])=>k!==TOI_USERNAME));
  const cats = CATEGORIES.join(", ");

  const prompt = `You are a senior social media strategist for Times of India (TOI) Instagram team.
Analyse today's data and return your response using EXACTLY the section markers below.
Each section starts with ===SECTION_NAME=== on its own line.
Do NOT use markdown, asterisks, bullet dashes, or JSON. Write in plain readable sentences and numbered lists only.

Today: ${payload.data_date}
TOI data: ${JSON.stringify(toi)}
Rivals: ${JSON.stringify(rivals)}
Missed (rival posts TOI didn't cover, sorted by likes): ${JSON.stringify(payload.missed)}
Viral rival posts (>50K likes): ${JSON.stringify(payload.viral)}

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
Categories to use: ${cats}`;

  const url = `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key=${key}`;
  const body = {
    contents: [{ parts: [{ text: prompt }] }],
    generationConfig: { temperature: 0.4, maxOutputTokens: 8000 },
  };

  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const res = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      if (res.status === 503) {
        await new Promise(r => setTimeout(r, 10000 * (attempt + 1)));
        continue;
      }
      if (!res.ok) return `ERROR: API ${res.status}`;
      const data = await res.json();
      return data.candidates?.[0]?.content?.parts?.[0]?.text || "ERROR: Empty response";
    } catch (e) {
      if (attempt < 2) { await new Promise(r => setTimeout(r, 5000)); continue; }
      return `ERROR: ${e.message}`;
    }
  }
  return "ERROR: Gemini unavailable after 3 retries.";
}

function extractSections(text) {
  const result = {};
  const parts = text.split(/===([A-Z_]+)===/);
  for (let i = 1; i < parts.length - 1; i += 2) {
    result[parts[i].trim()] = (parts[i+1] || "").trim();
  }
  return result;
}

function parseCategoryBreakdown(text, channels) {
  const result = {};
  const shortMap = { Viral: "Viral/Human Interest" };
  for (const block of text.split(/CHANNEL:\s*/)) {
    if (!block.trim()) continue;
    const lines = block.trim().split("\n");
    const chLine = lines[0].trim().replace(/^@/, "");
    const matched = channels.find(ch =>
      ch.toLowerCase().includes(chLine.toLowerCase()) ||
      chLine.toLowerCase().includes(ch.toLowerCase())
    );
    if (!matched) continue;
    const counts = Object.fromEntries(CATEGORIES.map(c => [c, 0]));
    for (const line of lines.slice(1)) {
      for (const token of line.split(/\s+/)) {
        if (token.includes(":")) {
          const [k, v] = token.split(":");
          const key = shortMap[k.trim()] || k.trim();
          if (key in counts) counts[key] = parseInt(v) || 0;
        }
      }
    }
    result[matched] = counts;
  }
  return result;
}

// ─── CSS ──────────────────────────────────────────────────────────────────────
const GLOBAL_CSS = `
  @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600;700&family=Syne:wght@600;700;800&display=swap');

  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

  :root {
    --bg:           #f5f6fa;
    --bg2:          #ffffff;
    --bg3:          #f0f1f7;
    --bg4:          #e8eaf6;
    --border:       #e2e4ef;
    --border2:      #c9ccdf;
    --text:         #111827;
    --text-muted:   #6b7280;
    --text-subtle:  #9ca3af;
    --accent:       #4f6ef7;
    --accent2:      #7c5cfc;
    --accent-glow:  rgba(79,110,247,0.10);
    --danger:       #ef4444;
    --success:      #16a34a;
    --warning:      #d97706;
    --sidebar-w:    240px;
    --radius:       12px;
    --font-display: 'Syne', sans-serif;
    --font-body:    'DM Sans', sans-serif;
  }

  html, body, #root {
    height: 100%;
    background: var(--bg);
    color: var(--text);
    font-family: var(--font-body);
    font-size: 14px;
    line-height: 1.6;
  }

  ::-webkit-scrollbar { width: 4px; height: 4px; }
  ::-webkit-scrollbar-track { background: var(--bg3); }
  ::-webkit-scrollbar-thumb { background: var(--border2); border-radius: 4px; }

  .online-section {
    background: var(--bg3); border: 1px solid var(--border);
    border-radius: 10px; padding: 10px 12px; margin-bottom: 4px;
  }
  .online-title {
    font-size: 11px; font-weight: 700; text-transform: uppercase;
    letter-spacing: 0.7px; color: var(--text-muted); margin-bottom: 8px;
    display: flex; align-items: center; gap: 6px;
  }
  .online-dot { width: 7px; height: 7px; border-radius: 50%; background: var(--success); display: inline-block; animation: pulse 2s infinite; }
  .online-user {
    display: flex; align-items: center; gap: 7px;
    font-size: 12px; color: var(--text); padding: 3px 0;
  }
  .online-avatar {
    width: 20px; height: 20px; border-radius: 5px;
    background: var(--accent-glow); border: 1px solid var(--border2);
    display: flex; align-items: center; justify-content: center;
    font-size: 10px; font-weight: 700; color: var(--accent); flex-shrink: 0;
  }
  .online-you { font-size: 10px; color: var(--accent); font-weight: 600; margin-left: auto; }
  .scraper-busy-warn {
    font-size: 11px; color: var(--warning);
    background: #fffbeb; border: 1px solid #fde68a;
    border-radius: 8px; padding: 7px 10px; margin-bottom: 4px; line-height: 1.5;
  }
  @keyframes pulse {
    0%, 100% { opacity: 1; } 50% { opacity: 0.4; }
  }

  a { color: var(--accent); text-decoration: none; }
  a:hover { text-decoration: underline; }

  /* Layout */
  .app-shell { display: flex; height: 100vh; overflow: hidden; }

  .sidebar {
    width: var(--sidebar-w);
    min-width: var(--sidebar-w);
    background: var(--bg2);
    border-right: 1px solid var(--border);
    display: flex;
    flex-direction: column;
    padding: 24px 16px;
    gap: 4px;
    overflow-y: auto;
  }

  .sidebar-brand { margin-bottom: 20px; padding: 0 8px; }
  .sidebar-brand h1 {
    font-family: var(--font-display);
    font-size: 18px;
    font-weight: 800;
    color: var(--accent);
    letter-spacing: -0.3px;
  }
  .sidebar-brand p { font-size: 11px; color: var(--text-muted); margin-top: 2px; }

  .last-scraped {
    font-size: 11px; color: var(--text-muted);
    padding: 6px 8px;
    background: var(--bg3);
    border-radius: 8px;
    margin-bottom: 8px;
  }

  .nav-btn {
    display: flex; align-items: center; gap: 10px;
    width: 100%; padding: 10px 12px;
    background: transparent; border: none; border-radius: 10px;
    color: var(--text-muted); font-family: var(--font-body);
    font-size: 13.5px; font-weight: 500;
    cursor: pointer; transition: all 0.15s;
    text-align: left;
  }
  .nav-btn:hover { background: var(--bg3); color: var(--text); }
  .nav-btn.active { background: var(--accent-glow); color: var(--accent); }
  .nav-btn .icon { font-size: 16px; width: 20px; text-align: center; }

  .sidebar-divider { height: 1px; background: var(--border); margin: 8px 0; }

  .action-btn {
    width: 100%; padding: 9px 12px;
    background: var(--bg3); border: 1px solid var(--border);
    border-radius: 10px; color: var(--text);
    font-family: var(--font-body); font-size: 13px; font-weight: 500;
    cursor: pointer; transition: all 0.15s;
    display: flex; align-items: center; justify-content: center; gap: 6px;
  }
  .action-btn:hover { border-color: var(--accent); color: var(--accent); }
  .action-btn:disabled { opacity: 0.5; cursor: not-allowed; }

  .channel-filter-label {
    font-size: 11px; font-weight: 600; text-transform: uppercase;
    letter-spacing: 0.8px; color: var(--text-muted);
    padding: 0 8px; margin: 4px 0;
  }

  .filter-row { display: flex; gap: 6px; margin-bottom: 6px; }
  .filter-row button {
    flex: 1; padding: 5px 0;
    background: var(--bg3); border: 1px solid var(--border);
    border-radius: 7px; color: var(--text-muted);
    font-size: 11px; cursor: pointer; transition: all 0.15s;
    font-family: var(--font-body);
  }
  .filter-row button:hover { border-color: var(--accent); color: var(--accent); }

  .channel-list { display: flex; flex-direction: column; gap: 3px; }
  .channel-chip {
    display: flex; align-items: center; gap: 8px;
    padding: 5px 8px; border-radius: 7px;
    cursor: pointer; transition: background 0.12s;
    font-size: 12px; color: var(--text-muted);
  }
  .channel-chip:hover { background: var(--bg3); }
  .channel-chip.selected { color: var(--text); }
  .channel-chip .ch-dot {
    width: 22px; height: 22px; border-radius: 6px;
    background: var(--accent-glow); border: 1px solid var(--border2);
    display: flex; align-items: center; justify-content: center;
    font-size: 10px; font-weight: 700; color: var(--accent);
    flex-shrink: 0;
  }
  .channel-chip input[type=checkbox] { accent-color: var(--accent); }

  /* Main content */
  .main-content {
    flex: 1; overflow-y: auto; padding: 32px 36px;
    background: var(--bg);
  }

  .page-header { margin-bottom: 28px; }
  .page-title {
    font-family: var(--font-display);
    font-size: 28px; font-weight: 800;
    color: var(--text); letter-spacing: -0.5px;
  }
  .page-subtitle { font-size: 13px; color: var(--text-muted); margin-top: 4px; }

  /* Metrics row */
  .metrics-row { display: grid; gap: 16px; margin-bottom: 28px; }
  .metrics-row.cols-4 { grid-template-columns: repeat(4, 1fr); }
  .metrics-row.cols-3 { grid-template-columns: repeat(3, 1fr); }

  .metric-card {
    background: var(--bg2); border: 1px solid var(--border);
    border-radius: var(--radius); padding: 18px 20px;
  }
  .metric-label { font-size: 12px; color: var(--text-muted); font-weight: 500; margin-bottom: 6px; }
  .metric-value { font-size: 26px; font-weight: 700; color: var(--text); font-family: var(--font-display); }

  /* Date filter bar */
  .date-bar {
    display: flex; align-items: center; gap: 12px;
    background: var(--bg2); border: 1px solid var(--border);
    border-radius: var(--radius); padding: 12px 18px;
    margin-bottom: 24px; flex-wrap: wrap;
  }
  .date-bar-label { font-size: 12px; font-weight: 600; color: var(--text-muted); white-space: nowrap; }
  .date-bar input[type=date] {
    background: var(--bg3); border: 1px solid var(--border);
    border-radius: 8px; color: var(--text); padding: 6px 10px;
    font-family: var(--font-body); font-size: 13px;
    color-scheme: dark;
  }
  .date-bar-arrow { color: var(--text-muted); }
  .date-bar .reset-btn {
    margin-left: auto; padding: 6px 14px;
    background: var(--bg3); border: 1px solid var(--border);
    border-radius: 8px; color: var(--text-muted);
    font-size: 12px; cursor: pointer; font-family: var(--font-body);
    transition: all 0.15s;
  }
  .date-bar .reset-btn:hover { border-color: var(--accent); color: var(--accent); }

  /* Stat card */
  .stat-card {
    background: var(--bg2); border: 1px solid var(--border);
    border-radius: var(--radius); padding: 16px 18px;
    text-align: center;
  }
  .stat-label { font-size: 11px; color: var(--text-muted); font-weight: 500; margin-bottom: 4px; }
  .stat-value { font-size: 20px; font-weight: 700; color: var(--text); }

  /* Section title */
  .section-title {
    font-family: var(--font-display);
    font-size: 20px; font-weight: 700;
    color: var(--text); margin: 32px 0 20px;
    letter-spacing: -0.3px;
  }

  /* Table */
  .data-table { width: 100%; border-collapse: collapse; font-size: 13px; }
  .data-table th {
    background: var(--bg3); color: var(--text-muted);
    padding: 10px 14px; text-align: left;
    font-size: 11px; font-weight: 700; text-transform: uppercase;
    letter-spacing: 0.6px; border-bottom: 1px solid var(--border2);
    white-space: nowrap;
  }
  .data-table td {
    padding: 10px 14px; border-bottom: 1px solid var(--border);
    color: var(--text); vertical-align: top;
  }
  .data-table tr:hover td { background: var(--bg2); }
  .table-wrap {
    background: var(--bg2); border: 1px solid var(--border);
    border-radius: var(--radius); overflow: hidden; overflow-x: auto;
    margin-bottom: 24px;
  }

  /* Post card */
  .post-card {
    display: grid; grid-template-columns: 80px 1fr 120px;
    gap: 16px; align-items: start;
    background: var(--bg2); border: 1px solid var(--border);
    border-radius: var(--radius); padding: 16px;
    margin-bottom: 12px; transition: border-color 0.15s;
  }
  .post-card:hover { border-color: var(--border2); }
  .post-thumb {
    width: 80px; height: 80px; object-fit: cover;
    border-radius: 8px; border: 1px solid var(--border);
  }
  .post-thumb-placeholder {
    width: 80px; height: 80px;
    background: var(--bg3); border-radius: 8px;
    border: 1px solid var(--border);
    display: flex; align-items: center; justify-content: center;
    color: var(--text-subtle); font-size: 24px;
  }
  .post-username { font-size: 12px; font-weight: 700; color: var(--accent); margin-bottom: 4px; }
  .post-caption { font-size: 13px; color: var(--text); line-height: 1.5; margin-bottom: 6px; }
  .post-time { font-size: 11px; color: var(--text-muted); margin-bottom: 4px; }
  .post-link { font-size: 11px; }
  .post-stats { display: flex; flex-direction: column; gap: 8px; }
  .post-stat { background: var(--bg3); border-radius: 8px; padding: 8px 12px; text-align: center; }
  .post-stat-icon { font-size: 14px; }
  .post-stat-val { font-size: 14px; font-weight: 700; color: var(--text); }

  /* Account cards */
  .account-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 16px; }
  .account-card {
    background: var(--bg2); border: 1px solid var(--border);
    border-radius: var(--radius); padding: 20px;
    cursor: pointer; transition: all 0.15s;
    display: flex; align-items: center; gap: 14px;
  }
  .account-card:hover {
    border-color: var(--accent); transform: translateY(-2px);
    box-shadow: 0 8px 20px rgba(108,140,255,0.1);
  }
  .account-avatar {
    width: 44px; height: 44px; border-radius: 10px;
    background: var(--accent-glow); border: 1px solid var(--border2);
    display: flex; align-items: center; justify-content: center;
    font-size: 18px; font-weight: 800; color: var(--accent);
    flex-shrink: 0; font-family: var(--font-display);
  }
  .account-name { font-size: 13px; font-weight: 700; color: var(--text); }
  .account-meta { font-size: 11px; color: var(--text-muted); margin-top: 2px; }

  /* AI section */
  .ai-badge {
    display: inline-flex; align-items: center; gap: 6px;
    background: var(--accent-glow); border: 1px solid var(--accent);
    border-radius: 20px; padding: 4px 14px;
    font-size: 12px; color: var(--accent); font-weight: 600; margin-bottom: 20px;
  }

  .ai-box {
    background: var(--bg2); border: 1px solid var(--border2);
    border-radius: 14px; padding: 20px 22px; margin-bottom: 16px;
    position: relative; overflow: hidden;
  }
  .ai-box::before {
    content: ''; position: absolute; top: 0; left: 0; right: 0; height: 2px;
    background: linear-gradient(90deg, var(--accent), var(--accent2), var(--accent));
  }
  .ai-box-title {
    font-size: 11px; font-weight: 700; text-transform: uppercase;
    letter-spacing: 1.2px; color: var(--accent2); margin-bottom: 12px;
  }
  .ai-box-body { font-size: 13px; color: var(--text); line-height: 1.8; }

  .two-col { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }

  /* Heatmap */
  .heatmap-tbl { width: 100%; border-collapse: collapse; font-size: 12px; }
  .heatmap-tbl th {
    background: var(--bg3); color: var(--text-muted);
    padding: 9px 6px; text-align: center;
    font-size: 10px; font-weight: 700; text-transform: uppercase;
    border-bottom: 1px solid var(--border2); white-space: nowrap;
  }
  .heatmap-tbl th:first-child { text-align: left; min-width: 130px; padding-left: 12px; }
  .heatmap-tbl td { padding: 7px 6px; border-bottom: 1px solid var(--border); text-align: center; }
  .heatmap-tbl td:first-child { text-align: left; padding-left: 12px; }
  .heatmap-tbl tr:hover td { background: var(--bg2); }

  /* Cat pills */
  .cat-pill {
    display: inline-block; padding: 3px 10px;
    border-radius: 10px; font-size: 11px; font-weight: 600; margin: 3px;
  }

  /* Hashtag pills */
  .tag-pill {
    display: inline-block; padding: 4px 12px;
    background: var(--accent-glow); border: 1px solid var(--accent);
    border-radius: 20px; font-size: 12px; color: var(--accent);
    font-weight: 600; margin: 3px;
  }

  /* Action list */
  .action-item {
    padding: 8px 0; border-bottom: 1px solid var(--border);
    font-size: 13px; color: var(--text);
  }
  .action-item:last-child { border-bottom: none; }

  /* Channel stat cards */
  .ch-stat-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 14px; margin-bottom: 24px; }
  .ch-stat-card {
    background: var(--bg2); border: 1px solid var(--border);
    border-radius: var(--radius); padding: 14px 16px;
  }
  .ch-stat-card.toi { border-color: var(--accent); }
  .ch-stat-name { font-size: 13px; font-weight: 700; color: var(--accent2); margin-bottom: 8px; }
  .ch-stat-name.toi { color: var(--accent); }
  .ch-stat-row { display: flex; gap: 16px; flex-wrap: wrap; }
  .ch-stat-item .stat-label { font-size: 11px; color: var(--text-muted); }
  .ch-stat-item .stat-num { font-size: 14px; font-weight: 600; color: var(--text); }

  /* Login */
  .login-page {
    min-height: 100vh; display: flex; align-items: center;
    justify-content: center; background: var(--bg);
  }
  .login-card {
    width: 380px; background: var(--bg2);
    border: 1px solid var(--border); border-radius: 20px;
    padding: 40px 36px; text-align: center;
    box-shadow: 0 8px 40px rgba(108,140,255,0.08);
  }
  .login-title {
    font-family: var(--font-display); font-size: 26px;
    font-weight: 800; color: var(--accent); margin-bottom: 6px;
  }
  .login-sub { font-size: 13px; color: var(--text-muted); margin-bottom: 28px; }
  .login-field {
    width: 100%; padding: 11px 14px;
    background: var(--bg3); border: 1px solid var(--border);
    border-radius: 10px; color: var(--text);
    font-family: var(--font-body); font-size: 14px;
    margin-bottom: 12px; outline: none; transition: border-color 0.15s;
  }
  .login-field:focus { border-color: var(--accent); }
  .login-btn {
    width: 100%; padding: 12px;
    background: var(--accent); border: none;
    border-radius: 10px; color: #fff;
    font-family: var(--font-display); font-size: 15px; font-weight: 700;
    cursor: pointer; transition: opacity 0.15s; margin-top: 4px;
  }
  .login-btn:hover { opacity: 0.9; }
  .login-btn:disabled { opacity: 0.5; cursor: not-allowed; }
  .login-error { font-size: 13px; color: var(--danger); margin-top: 10px; }

  /* Toast */
  .toast {
    position: fixed; bottom: 24px; right: 24px;
    padding: 12px 18px; border-radius: 10px;
    font-size: 13px; font-weight: 500;
    z-index: 1000; animation: fadeInUp 0.2s ease;
    max-width: 320px;
  }
  .toast.success { background: #1a3a1a; border: 1px solid var(--success); color: var(--success); }
  .toast.error { background: #3a1a1a; border: 1px solid var(--danger); color: var(--danger); }
  @keyframes fadeInUp {
    from { opacity: 0; transform: translateY(10px); }
    to   { opacity: 1; transform: translateY(0); }
  }

  /* Guest lock */
  .guest-lock {
    display: flex; flex-direction: column; align-items: center;
    justify-content: center; gap: 12px;
    padding: 60px 40px; text-align: center;
    background: var(--bg2); border: 1px solid var(--border);
    border-radius: 16px; margin-top: 20px;
  }
  .guest-lock-icon { font-size: 40px; }
  .guest-lock-title { font-family: var(--font-display); font-size: 20px; font-weight: 700; color: var(--text); }
  .guest-lock-sub { font-size: 13px; color: var(--text-muted); }

  /* Select */
  select {
    background: var(--bg3); border: 1px solid var(--border);
    border-radius: 8px; color: var(--text); padding: 7px 10px;
    font-family: var(--font-body); font-size: 13px; outline: none;
    cursor: pointer;
  }

  /* Spinner */
  .spinner-wrap { display: flex; align-items: center; justify-content: center; padding: 60px; gap: 12px; }
  .spinner {
    width: 24px; height: 24px;
    border: 3px solid var(--border2);
    border-top-color: var(--accent);
    border-radius: 50%;
    animation: spin 0.7s linear infinite;
  }
  @keyframes spin { to { transform: rotate(360deg); } }

  /* Bar chart */
  .bar-chart-wrap { overflow-x: auto; }
  .bar-chart { display: flex; flex-direction: column; gap: 8px; }
  .bar-row { display: flex; align-items: center; gap: 10px; }
  .bar-label { font-size: 12px; color: var(--text-muted); width: 130px; flex-shrink: 0; text-align: right; }
  .bar-track { flex: 1; height: 20px; background: var(--bg3); border-radius: 4px; overflow: hidden; }
  .bar-fill { height: 100%; background: var(--accent); border-radius: 4px; transition: width 0.5s ease; }
  .bar-val { font-size: 12px; color: var(--text-muted); width: 60px; }

  /* Pie chart substitute */
  .pie-legend { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 12px; }
  .pie-legend-item { display: flex; align-items: center; gap: 6px; font-size: 12px; color: var(--text-muted); }
  .pie-dot { width: 10px; height: 10px; border-radius: 50%; flex-shrink: 0; }

  /* Missed table */
  .missed-table { width: 100%; border-collapse: collapse; font-size: 12px; }
  .missed-table th {
    background: var(--bg3); color: var(--text-muted);
    padding: 9px 12px; text-align: left;
    font-size: 11px; font-weight: 700; text-transform: uppercase;
    border-bottom: 1px solid var(--border2);
  }
  .missed-table td {
    padding: 10px 12px; border-bottom: 1px solid var(--border);
    color: var(--text); vertical-align: top; line-height: 1.6;
  }
  .missed-table tr:hover td { background: var(--bg2); }

  /* Back btn */
  .back-btn {
    display: inline-flex; align-items: center; gap: 6px;
    padding: 7px 14px; background: var(--bg3);
    border: 1px solid var(--border); border-radius: 8px;
    color: var(--text-muted); font-size: 13px;
    cursor: pointer; transition: all 0.15s; font-family: var(--font-body);
    margin-bottom: 20px;
  }
  .back-btn:hover { border-color: var(--accent); color: var(--accent); }

  /* Rerun btn */
  .rerun-btn {
    padding: 8px 16px; background: var(--bg3);
    border: 1px solid var(--border); border-radius: 8px;
    color: var(--text); font-size: 13px; cursor: pointer;
    transition: all 0.15s; font-family: var(--font-body);
  }
  .rerun-btn:hover { border-color: var(--accent); color: var(--accent); }

  @media (max-width: 900px) {
    .metrics-row.cols-4 { grid-template-columns: repeat(2, 1fr); }
    .account-grid { grid-template-columns: repeat(2, 1fr); }
    .two-col { grid-template-columns: 1fr; }
    .ch-stat-grid { grid-template-columns: repeat(2, 1fr); }
    .main-content { padding: 20px 16px; }
    .sidebar { width: 200px; min-width: 200px; }
  }
`;

// ─── Components ───────────────────────────────────────────────────────────────

function Toast({ message, type, onClose }) {
  useEffect(() => {
    const t = setTimeout(onClose, 4000);
    return () => clearTimeout(t);
  }, [onClose]);
  return <div className={`toast ${type}`}>{message}</div>;
}

function Spinner({ label }) {
  return (
    <div className="spinner-wrap">
      <div className="spinner" />
      <span style={{ color: "var(--text-muted)", fontSize: 13 }}>{label || "Loading..."}</span>
    </div>
  );
}

function GuestLock() {
  return (
    <div className="guest-lock">
      <div className="guest-lock-icon">🔒</div>
      <div className="guest-lock-title">Members Only</div>
      <div className="guest-lock-sub">
        Please login with your TOI account to access this feature.<br />
        Contact your admin to become a member.
      </div>
    </div>
  );
}

function DateBar({ minDate, maxDate, start, end, onStart, onEnd, onReset }) {
  return (
    <div className="date-bar">
      <span className="date-bar-label">📅 Date Range</span>
      <input type="date" value={start} min={minDate} max={maxDate}
        onChange={e => onStart(e.target.value)} />
      <span className="date-bar-arrow">→</span>
      <input type="date" value={end} min={minDate} max={maxDate}
        onChange={e => onEnd(e.target.value)} />
      <button className="reset-btn" onClick={onReset}>Reset</button>
    </div>
  );
}

function useDateFilter(rows) {
  const dates = rows.map(r => postDate(r)).filter(Boolean).sort();
  const minDate = dates[0] || todayStr();
  const maxDate = dates[dates.length - 1] || todayStr();
  const def = maxDate;
  const [start, setStart] = useState(def);
  const [end, setEnd] = useState(def);

  useEffect(() => {
    setStart(def);
    setEnd(def);
  }, [minDate, maxDate]);

  const filtered = rows.filter(r => {
    const d = postDate(r);
    return d && d >= start && d <= end;
  });

  const reset = () => { setStart(def); setEnd(def); };

  return { start, end, setStart, setEnd, filtered, minDate, maxDate, reset };
}

// ─── Pages ────────────────────────────────────────────────────────────────────

function TopPostsPage({ rows }) {
  const { start, end, setStart, setEnd, filtered, minDate, maxDate, reset } = useDateFilter(rows);
  const [topN, setTopN] = useState(10);
  const [metric, setMetric] = useState("Latest");

  let sorted = [...filtered];
  if (metric === "Latest") sorted.sort((a, b) => new Date(b.post_time) - new Date(a.post_time));
  else if (metric === "Likes") sorted.sort((a, b) => (Number(b.likes)||0) - (Number(a.likes)||0));
  else sorted.sort((a, b) => (Number(b.comments)||0) - (Number(a.comments)||0));
  const top = sorted.slice(0, topN);

  return (
    <div>
      <div className="page-header">
        <div className="page-title">Top Posts</div>
      </div>
      <DateBar minDate={minDate} maxDate={maxDate}
        start={start} end={end}
        onStart={setStart} onEnd={setEnd} onReset={reset} />
      <div style={{ display: "flex", gap: 12, marginBottom: 20 }}>
        <div>
          <label style={{ fontSize: 12, color: "var(--text-muted)", marginRight: 8 }}>Show top</label>
          <select value={topN} onChange={e => setTopN(Number(e.target.value))}>
            {[10,20,30,50].map(n => <option key={n}>{n}</option>)}
          </select>
        </div>
        <div>
          <label style={{ fontSize: 12, color: "var(--text-muted)", marginRight: 8 }}>Ranked by</label>
          <select value={metric} onChange={e => setMetric(e.target.value)}>
            {["Latest","Likes","Comments"].map(m => <option key={m}>{m}</option>)}
          </select>
        </div>
      </div>

      {filtered.length === 0
        ? <p style={{ color: "var(--text-muted)" }}>No data for selected range.</p>
        : top.map((row, i) => <PostCard key={i} row={row} showUsername />)
      }
    </div>
  );
}

function PostCard({ row, showUsername }) {
  return (
    <div className="post-card">
      <div>
        {row.media_url && !row.media_url.match(/^(0|nan|None|)$/)
          ? <img className="post-thumb" src={row.media_url} alt="" onError={e => { e.target.style.display="none"; }} />
          : <div className="post-thumb-placeholder">📷</div>
        }
      </div>
      <div>
        {showUsername && <div className="post-username">@{row.username}</div>}
        <div className="post-caption">{String(row.caption || "").slice(0, 220)}{String(row.caption||"").length > 220 ? "…" : ""}</div>
        {row.post_time && <div className="post-time">🕒 {fmtDateTime(row.post_time)} IST</div>}
        {row.post_link && String(row.post_link).startsWith("http") &&
          <a className="post-link" href={row.post_link} target="_blank" rel="noreferrer">🔗 View Original Post</a>
        }
      </div>
      <div className="post-stats">
        <div className="post-stat">
          <div className="post-stat-icon">❤️</div>
          <div className="post-stat-val">{(Number(row.likes)||0).toLocaleString()}</div>
        </div>
        <div className="post-stat">
          <div className="post-stat-icon">💬</div>
          <div className="post-stat-val">{(Number(row.comments)||0).toLocaleString()}</div>
        </div>
      </div>
    </div>
  );
}

function AccountsPage({ rows, onSelectAccount }) {
  const { start, end, setStart, setEnd, filtered, minDate, maxDate, reset } = useDateFilter(rows);

  const stats = {};
  for (const row of filtered) {
    const u = row.username;
    if (!stats[u]) stats[u] = { posts: 0, likes: 0 };
    stats[u].posts++;
    stats[u].likes += Number(row.likes) || 0;
  }
  const sorted = Object.entries(stats).sort((a,b) => b[1].likes - a[1].likes);

  return (
    <div>
      <div className="page-header">
        <div className="page-title">Accounts</div>
      </div>
      <DateBar minDate={minDate} maxDate={maxDate}
        start={start} end={end}
        onStart={setStart} onEnd={setEnd} onReset={reset} />
      <div className="account-grid">
        {sorted.map(([uname, s]) => (
          <div key={uname} className="account-card"
            onClick={() => onSelectAccount(uname)}>
            <div className="account-avatar">{uname[0].toUpperCase()}</div>
            <div>
              <div className="account-name">@{uname}</div>
              <div className="account-meta">{s.posts} posts · {formatNumber(s.likes)} likes</div>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

function AccountDetailPage({ rows, username, onBack }) {
  const { start, end, setStart, setEnd, filtered, minDate, maxDate, reset } = useDateFilter(rows);
  const arows = filtered.filter(r => r.username === username)
    .sort((a,b) => new Date(b.post_time) - new Date(a.post_time));

  const totalLikes    = arows.reduce((s,r) => s + (Number(r.likes)||0), 0);
  const totalComments = arows.reduce((s,r) => s + (Number(r.comments)||0), 0);
  const avgLikes      = arows.length ? Math.round(totalLikes / arows.length) : 0;

  return (
    <div>
      <button className="back-btn" onClick={onBack}>← Back</button>
      <div className="page-header">
        <div className="page-title">@{username}</div>
      </div>
      <DateBar minDate={minDate} maxDate={maxDate}
        start={start} end={end}
        onStart={setStart} onEnd={setEnd} onReset={reset} />
      <div className="metrics-row cols-4" style={{ marginBottom: 24 }}>
        {[["Posts", arows.length.toLocaleString()],
          ["Total Likes", formatNumber(totalLikes)],
          ["Total Comments", formatNumber(totalComments)],
          ["Avg Likes", formatNumber(avgLikes)]
        ].map(([label, value]) => (
          <div key={label} className="metric-card">
            <div className="metric-label">{label}</div>
            <div className="metric-value">{value}</div>
          </div>
        ))}
      </div>
      <div className="section-title" style={{ marginTop: 0 }}>Recent Posts</div>
      {arows.length === 0
        ? <p style={{ color: "var(--text-muted)" }}>No posts in selected range.</p>
        : arows.map((row, i) => <PostCard key={i} row={row} />)
      }
    </div>
  );
}

function AnalyticsPage({ rows }) {
  const { start, end, setStart, setEnd, filtered, minDate, maxDate, reset } = useDateFilter(rows);

  const stats = {};
  for (const row of filtered) {
    const u = row.username;
    if (!stats[u]) stats[u] = { posts: 0, likes: 0, comments: 0, followers: Number(row.followers)||0 };
    stats[u].posts++;
    stats[u].likes    += Number(row.likes)    || 0;
    stats[u].comments += Number(row.comments) || 0;
    if (row.followers) stats[u].followers = Number(row.followers) || 0;
  }

  const rows2 = Object.entries(stats).map(([u, s]) => ({
    username: u,
    posts: s.posts,
    total_likes: s.likes,
    avg_likes: s.posts ? Math.round(s.likes / s.posts) : 0,
    total_comments: s.comments,
    followers: s.followers,
    engagement_rate: s.followers ? +((s.likes + 10 * s.comments) / s.followers * 100).toFixed(2) : 0,
  })).sort((a,b) => b.total_likes - a.total_likes);

  const maxLikes = Math.max(...rows2.map(r => r.total_likes), 1);
  const maxEng   = Math.max(...rows2.map(r => r.engagement_rate), 1);

  const totalPosts    = filtered.length;
  const totalLikes    = filtered.reduce((s,r) => s+(Number(r.likes)||0), 0);
  const totalComments = filtered.reduce((s,r) => s+(Number(r.comments)||0), 0);
  const accounts      = new Set(filtered.map(r => r.username)).size;

  return (
    <div>
      <div className="page-header">
        <div className="page-title">Analytics Overview</div>
      </div>
      <DateBar minDate={minDate} maxDate={maxDate}
        start={start} end={end}
        onStart={setStart} onEnd={setEnd} onReset={reset} />

      {filtered.length === 0
        ? <p style={{ color: "var(--text-muted)" }}>No data for selected range.</p>
        : <>
          <div className="metrics-row cols-4">
            {[["Total Posts", totalPosts.toLocaleString()],
              ["Accounts", accounts],
              ["Total Likes", formatNumber(totalLikes)],
              ["Total Comments", formatNumber(totalComments)]
            ].map(([label, value]) => (
              <div key={label} className="metric-card">
                <div className="metric-label">{label}</div>
                <div className="metric-value">{value}</div>
              </div>
            ))}
          </div>

          <div className="section-title">Performance by Account</div>
          <div className="table-wrap">
            <table className="data-table">
              <thead>
                <tr>
                  {["Account","Posts","Total Likes","Avg Likes","Comments","Followers","Eng. Rate"].map(h => (
                    <th key={h}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {rows2.map(r => (
                  <tr key={r.username}>
                    <td style={{ color: r.username === TOI_USERNAME ? "var(--accent)" : "var(--text)", fontWeight: r.username === TOI_USERNAME ? 700 : 400 }}>
                      {r.username === TOI_USERNAME ? "⭐ " : ""}@{r.username}
                    </td>
                    <td>{r.posts.toLocaleString()}</td>
                    <td>{r.total_likes.toLocaleString()}</td>
                    <td>{r.avg_likes.toLocaleString()}</td>
                    <td>{r.total_comments.toLocaleString()}</td>
                    <td>{r.followers.toLocaleString()}</td>
                    <td>{r.engagement_rate}%</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div className="two-col">
            <div>
              <div className="section-title" style={{ marginTop: 0 }}>🥧 Likes Share by Channel</div>
              <div className="bar-chart-wrap" style={{ background: "var(--bg2)", border: "1px solid var(--border)", borderRadius: "var(--radius)", padding: 16 }}>
                <div className="bar-chart">
                  {rows2.map(r => (
                    <div key={r.username} className="bar-row">
                      <div className="bar-label" style={{ color: r.username === TOI_USERNAME ? "var(--accent)" : undefined }}>
                        @{r.username}
                      </div>
                      <div className="bar-track">
                        <div className="bar-fill" style={{
                          width: `${(r.total_likes / maxLikes * 100).toFixed(1)}%`,
                          background: r.username === TOI_USERNAME ? "var(--accent2)" : "var(--accent)"
                        }} />
                      </div>
                      <div className="bar-val">{formatNumber(r.total_likes)}</div>
                    </div>
                  ))}
                </div>
              </div>
            </div>

            <div>
              <div className="section-title" style={{ marginTop: 0 }}>📈 Engagement Rate by Channel</div>
              <div className="bar-chart-wrap" style={{ background: "var(--bg2)", border: "1px solid var(--border)", borderRadius: "var(--radius)", padding: 16 }}>
                <div className="bar-chart">
                  {[...rows2].sort((a,b)=>b.engagement_rate-a.engagement_rate).map(r => (
                    <div key={r.username} className="bar-row">
                      <div className="bar-label">@{r.username}</div>
                      <div className="bar-track">
                        <div className="bar-fill" style={{
                          width: `${(r.engagement_rate / maxEng * 100).toFixed(1)}%`,
                          background: "var(--success)"
                        }} />
                      </div>
                      <div className="bar-val">{r.engagement_rate}%</div>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          </div>
        </>
      }
    </div>
  );
}

function AIInsightsPage({ rows, isGuest }) {
  const [analysis, setAnalysis] = useState(null);
  const [loading, setLoading]   = useState(false);
  const [error, setError]       = useState(null);
  const cacheKey = useRef(null);

  const today   = todayStr();
  let trows = rows.filter(r => postDate(r) === today);
  let dataDate = today;
  if (!trows.length) {
    const dates = rows.map(r => postDate(r)).filter(Boolean).sort();
    const last = dates[dates.length - 1];
    trows = rows.filter(r => postDate(r) === last);
    dataDate = last || today;
  }

  const thisKey = `${dataDate}_${trows.length}`;

  const runAnalysis = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const payload = buildPayload(rows);
      if (payload.total_posts === 0) { setError("No posts found for analysis."); return; }
      const raw = await callGemini(payload);
      if (raw.startsWith("ERROR")) { setError(raw); return; }
      const secs = extractSections(raw);
      secs._channels = payload.channels;
      secs._raw = raw;
      cacheKey.current = thisKey;
      setAnalysis(secs);
    } finally {
      setLoading(false);
    }
  }, [rows, thisKey]);

  useEffect(() => {
    if (!isGuest && !analysis && !loading) runAnalysis();
  }, []);

  if (isGuest) return (
    <div>
      <div className="page-header"><div className="page-title">AI Insights</div></div>
      <GuestLock />
    </div>
  );

  const totalLikes    = trows.reduce((s,r) => s+(Number(r.likes)||0), 0);
  const totalComments = trows.reduce((s,r) => s+(Number(r.comments)||0), 0);
  const accounts      = new Set(trows.map(r => r.username)).size;

  return (
    <div>
      <div className="page-header">
        <div className="page-title">AI Insights</div>
      </div>
      <div className="ai-badge">✨ Powered by Gemini</div>
      <div style={{ fontSize: 13, color: "var(--text-muted)", marginBottom: 16 }}>
        📅 Analysing: <span style={{ color: "var(--accent2)", fontWeight: 600 }}>{dataDate === today ? `Today · ${today}` : `Latest available: ${dataDate}`}</span>
      </div>

      <div className="metrics-row cols-4">
        {[["Posts Today", trows.length.toLocaleString()],
          ["Channels Active", accounts],
          ["Total Likes", formatNumber(totalLikes)],
          ["Total Comments", formatNumber(totalComments)]
        ].map(([label, value]) => (
          <div key={label} className="stat-card">
            <div className="stat-label">{label}</div>
            <div className="stat-value">{value}</div>
          </div>
        ))}
      </div>

      <div style={{ display: "flex", alignItems: "center", gap: 12, margin: "20px 0" }}>
        <button className="rerun-btn" onClick={() => { setAnalysis(null); runAnalysis(); }} disabled={loading}>
          🔄 Re-run Analysis
        </button>
        <span style={{ fontSize: 12, color: "var(--text-muted)" }}>Cached per snapshot. Click Re-run to refresh.</span>
      </div>

      {loading && <Spinner label="🤖 Analysing… 20–40 seconds" />}
      {error && <div style={{ color: "var(--danger)", background: "var(--bg2)", border: "1px solid var(--border)", borderRadius: 10, padding: 16, marginBottom: 16 }}>{error}</div>}

      {analysis && !loading && (
        <>
          <div className="two-col">
            <div className="ai-box">
              <div className="ai-box-title">📊 TOI vs Competition</div>
              <div className="ai-box-body">{analysis.TOI_VS_COMPETITION || "No data."}</div>
            </div>
            <div className="ai-box">
              <div className="ai-box-title">🎯 Biggest Threat Today</div>
              <div className="ai-box-body">{analysis.BIGGEST_THREAT || "No data."}</div>
            </div>
          </div>

          <MissedOpportunities
            missedRaw={analysis.MISSED_OPPORTUNITIES || ""}
            captionRaw={analysis.CAPTION_IDEAS || ""}
          />

          <CategoryHeatmap
            catRaw={analysis.CATEGORY_BREAKDOWN || ""}
            channels={analysis._channels || []}
          />

          <div className="two-col" style={{ marginTop: 24 }}>
            <HashtagsBox raw={analysis.HASHTAGS || ""} />
            <ActionPlanBox raw={analysis.ACTION_PLAN || ""} />
          </div>

          <ChannelStatsToday trows={trows} />
        </>
      )}
    </div>
  );
}

function MissedOpportunities({ missedRaw, captionRaw }) {
  const rows = [];
  for (const line of missedRaw.split("\n")) {
    const m = line.match(/TOPIC:\s*(.+?)\s*\|\s*RIVAL:\s*@?(\S+)\s*\|\s*THEIR LIKES:\s*([\d,KkMm]+)/);
    if (m) {
      const [, topic, rival, lk] = m;
      const capMatch = captionRaw.match(new RegExp(`CAPTION FOR ${topic.toUpperCase().replace(/[.*+?^${}()|[\]\\]/g,'\\$&')}[:\\s]*\n([\\s\\S]*?)(?=\nCAPTION FOR|$)`, "i"));
      rows.push({ topic: topic.trim(), rival: rival.trim(), lk: lk.trim(), caption: capMatch ? capMatch[1].trim() : "" });
    }
  }

  return (
    <>
      <div className="section-title">⚡ Missed Opportunities</div>
      {rows.length > 0 ? (
        <div className="table-wrap">
          <table className="missed-table">
            <thead>
              <tr>
                <th>Rival</th><th>Likes</th><th>Topic Missed</th><th>✍️ Caption for TOI</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((r, i) => (
                <tr key={i}>
                  <td style={{ color: "#f97316", fontWeight: 600 }}>@{r.rival}</td>
                  <td style={{ color: "#ef4444", fontWeight: 700 }}>{r.lk}</td>
                  <td style={{ color: "var(--warning)", fontWeight: 600 }}>{r.topic}</td>
                  <td style={{ color: "var(--accent2)", fontStyle: "italic" }}>{r.caption}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <div className="two-col">
          <div className="ai-box">
            <div className="ai-box-title">⚡ Missed Opportunities</div>
            <div className="ai-box-body">{missedRaw}</div>
          </div>
          <div className="ai-box">
            <div className="ai-box-title">✍️ Caption Ideas</div>
            <div className="ai-box-body">{captionRaw}</div>
          </div>
        </div>
      )}
    </>
  );
}

function CategoryHeatmap({ catRaw, channels }) {
  const catData = parseCategoryBreakdown(catRaw, channels);
  const hasData = Object.keys(catData).length > 0;

  if (!hasData) return (
    <div className="ai-box">
      <div className="ai-box-title">📊 Category Breakdown</div>
      <div className="ai-box-body">{catRaw}</div>
    </div>
  );

  return (
    <>
      <div className="section-title">📊 Content Category Heatmap</div>
      <div className="table-wrap">
        <table className="heatmap-tbl">
          <thead>
            <tr>
              <th>Channel</th>
              {CATEGORIES.map(c => <th key={c} title={c}>{c.slice(0,5)}.</th>)}
            </tr>
          </thead>
          <tbody>
            {channels.map(ch => {
              const cats = catData[ch] || Object.fromEntries(CATEGORIES.map(c=>[c,0]));
              const total = Object.values(cats).reduce((a,b)=>a+b,0) || 1;
              const isToi = ch === TOI_USERNAME;
              return (
                <tr key={ch}>
                  <td style={{ color: isToi ? "var(--accent)" : "var(--text)", fontWeight: isToi ? 700 : 400, whiteSpace: "nowrap" }}>
                    {isToi ? "⭐ " : ""}@{ch}
                  </td>
                  {CATEGORIES.map(cat => {
                    const n = cats[cat] || 0;
                    const pct = n / total;
                    let bg = "transparent", fg = "var(--border2)", fw = "400";
                    if (pct > 0 && pct < 0.15) { bg = CAT_COLORS[cat]+"22"; fg = "var(--text-muted)"; fw = "400"; }
                    else if (pct >= 0.15 && pct < 0.30) { bg = CAT_COLORS[cat]+"55"; fg = "var(--text)"; fw = "600"; }
                    else if (pct >= 0.30) { bg = CAT_COLORS[cat]+"99"; fg = "var(--text)"; fw = "700"; }
                    return (
                      <td key={cat} style={{ background: bg, color: fg, fontWeight: fw }}>
                        {n || ""}
                      </td>
                    );
                  })}
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      <div style={{ marginTop: 8 }}>
        {CATEGORIES.map(c => (
          <span key={c} className="cat-pill" style={{ background: CAT_COLORS[c]+"22", color: CAT_COLORS[c], border: `1px solid ${CAT_COLORS[c]}55` }}>
            {c}
          </span>
        ))}
      </div>
      <div style={{ fontSize: 11, color: "var(--text-muted)", marginTop: 6 }}>
        Numbers = estimated posts per category. Darker = higher share. ⭐ = TOI
      </div>
    </>
  );
}

function HashtagsBox({ raw }) {
  const tags = raw.split("\n").map(t => t.trim()).filter(t => t.startsWith("#"));
  return (
    <div className="ai-box">
      <div className="ai-box-title">#️⃣ Hashtags for TOI Today</div>
      {tags.length > 0
        ? <div style={{ marginTop: 8, lineHeight: 2.4 }}>
            {tags.map(t => <span key={t} className="tag-pill">{t}</span>)}
          </div>
        : <div className="ai-box-body">{raw}</div>
      }
    </div>
  );
}

function ActionPlanBox({ raw }) {
  const actions = raw.split("\n").map(l => l.trim()).filter(Boolean);
  return (
    <div className="ai-box">
      <div className="ai-box-title">💡 Action Plan for TOI</div>
      {actions.map((a, i) => (
        <div key={i} className="action-item">{a}</div>
      ))}
    </div>
  );
}

function ChannelStatsToday({ trows }) {
  const stats = {};
  for (const row of trows) {
    const u = row.username;
    if (!stats[u]) stats[u] = { posts: 0, likes: 0, comments: 0 };
    stats[u].posts++;
    stats[u].likes    += Number(row.likes)    || 0;
    stats[u].comments += Number(row.comments) || 0;
  }
  const sorted = Object.entries(stats).sort((a,b) => b[1].likes - a[1].likes);

  return (
    <>
      <div className="section-title">📋 Channel Stats (Today)</div>
      <div className="ch-stat-grid">
        {sorted.map(([u, s]) => {
          const isToi = u === TOI_USERNAME;
          return (
            <div key={u} className={`ch-stat-card${isToi ? " toi" : ""}`}>
              <div className={`ch-stat-name${isToi ? " toi" : ""}`}>{isToi ? "⭐ " : ""}@{u}</div>
              <div className="ch-stat-row">
                {[["Posts", s.posts], ["Likes", formatNumber(s.likes)], ["Comments", formatNumber(s.comments)], ["Avg", formatNumber(Math.round(s.likes/Math.max(s.posts,1)))]].map(([label, val]) => (
                  <div key={label} className="ch-stat-item">
                    <div className="stat-label">{label}</div>
                    <div className="stat-num">{val}</div>
                  </div>
                ))}
              </div>
            </div>
          );
        })}
      </div>
    </>
  );
}

// ─── Login ────────────────────────────────────────────────────────────────────

function LoginPage({ onLogin }) {
  const [email, setEmail]     = useState("");
  const [password, setPassword] = useState("");
  const [error, setError]     = useState("");
  const [loading, setLoading] = useState(false);

  const handleSubmit = async () => {
    if (!email || !password) { setError("Please enter email and password."); return; }
    setLoading(true);
    setError("");
    const user = await loginUser(email, password);
    setLoading(false);
    if (user) {
      onLogin(user);
    } else {
      setError("❌ Incorrect email or password. Try again.");
    }
  };

  const handleKey = e => { if (e.key === "Enter") handleSubmit(); };

  return (
    <div className="login-page">
      <div className="login-card">
        <div className="login-title">🔐 Instagram Analytics</div>
        <div className="login-sub">Channel Performance Dashboard</div>
        <input
          className="login-field" type="email" placeholder="Email address"
          value={email} onChange={e => setEmail(e.target.value)} onKeyDown={handleKey}
        />
        <input
          className="login-field" type="password" placeholder="Password"
          value={password} onChange={e => setPassword(e.target.value)} onKeyDown={handleKey}
        />
        <button className="login-btn" onClick={handleSubmit} disabled={loading}>
          {loading ? "Signing in…" : "Login"}
        </button>
        {error && <div className="login-error">{error}</div>}
      </div>
    </div>
  );
}

// ─── App Shell ────────────────────────────────────────────────────────────────

const NAV = [
  { id: "top_posts",   icon: "🏆", label: "Top Posts" },
  { id: "accounts",    icon: "🏠", label: "Accounts" },
  { id: "analytics",   icon: "📊", label: "Analytics" },
  { id: "ai_insights", icon: "🤖", label: "AI Insights" },
];

export default function App() {
  const [user, setUser]           = useState(() => {
    try { const u = localStorage.getItem("toi_user"); return u ? JSON.parse(u) : null; } catch { return null; }
  });
  const [page, setPage]           = useState("top_posts");
  const [selectedAccount, setSelectedAccount] = useState(null);
  const [rows, setRows]           = useState([]);
  const [loading, setLoading]     = useState(true);
  const [lastScrape, setLastScrape] = useState("Unknown");
  const [channels, setChannels]   = useState([]);
  const [selChannels, setSelChannels] = useState([]);
  const [toast, setToast]         = useState(null);
  const [scraperBusy, setScraperBusy] = useState(false);
  const [onlineUsers, setOnlineUsers] = useState([]);

  const isGuest = user?.user_email === "guest@toi.com";

  const showToast = (message, type = "success") => setToast({ message, type });

  const handleLogin = (u) => {
    localStorage.setItem("toi_user", JSON.stringify(u));
    setUser(u);
  };

  const handleLogout = () => {
    if (user) removeSession(user.user_email);
    localStorage.removeItem("toi_user");
    setUser(null);
  };

  // Load data
  useEffect(() => {
    if (!user) return;
    (async () => {
      setLoading(true);
      try {
        const data = await fetchPosts();
        const processed = data.map(r => ({
          ...r,
          likes:    Number(r.likes)    || 0,
          comments: Number(r.comments) || 0,
          username: String(r.username || ""),
        })).filter(r => r.username);
        setRows(processed);
        const chs = [...new Set(processed.map(r => r.username))].sort();
        setChannels(chs);
        setSelChannels(chs);
      } catch (e) {
        showToast(`❌ Failed to load data: ${e.message}`, "error");
      } finally {
        setLoading(false);
      }
      const t = await fetchLastScrapeTime();
      setLastScrape(t);
    })();
  }, [user]);

  // Session heartbeat — ping every 60s, poll online users every 30s
  useEffect(() => {
    if (!user) return;
    upsertSession(user);
    const heartbeat = setInterval(() => upsertSession(user), 60000);
    const poll = setInterval(async () => {
      const sessions = await fetchActiveSessions();
      setOnlineUsers(sessions);
    }, 30000);
    fetchActiveSessions().then(setOnlineUsers);
    return () => {
      clearInterval(heartbeat);
      clearInterval(poll);
      removeSession(user.user_email);
    };
  }, [user]);

  

  const filteredRows = rows.filter(r => selChannels.includes(r.username));
  const otherActiveUsers = onlineUsers.filter(u => u.user_email !== user?.user_email);
  const scraperWarning = otherActiveUsers.length > 0
    ? `⚠️ ${otherActiveUsers.map(u => u.user_name.split(" ")[0]).join(", ")} ${otherActiveUsers.length === 1 ? "is" : "are"} also online.`
    : null;

  const handleRunScraper = async () => {
    if (isGuest) {
      showToast("Members only feature. Please login with your TOI account.", "error");
      return;
    }
    setScraperBusy(true);
    const { ok, msg } = await triggerScraper();
    showToast(msg, ok ? "success" : "error");
    setScraperBusy(false);
  };

  const handleRefresh = async () => {
    setLoading(true);
    try {
      const data = await fetchPosts();
      const processed = data.map(r => ({
        ...r,
        likes: Number(r.likes)||0,
        comments: Number(r.comments)||0,
        username: String(r.username||""),
      })).filter(r => r.username);
      setRows(processed);
      const chs = [...new Set(processed.map(r=>r.username))].sort();
      setChannels(chs);
      setSelChannels(chs);
      showToast("✅ Data refreshed");
    } catch(e) {
      showToast(`❌ ${e.message}`, "error");
    } finally {
      setLoading(false);
    }
    const t = await fetchLastScrapeTime();
    setLastScrape(t);
  };

  if (!user) return (
    <>
      <style>{GLOBAL_CSS}</style>
      <LoginPage onLogin={handleLogin} />
    </>
  );

  const navigate = (p) => {
    setPage(p);
    if (p !== "account_detail") setSelectedAccount(null);
  };

  return (
    <>
      <style>{GLOBAL_CSS}</style>
      <div className="app-shell">
        {/* Sidebar */}
        <aside className="sidebar">
          <div className="sidebar-brand">
            <h1>{APP_TITLE}</h1>
            <p>{APP_SUBTITLE}</p>
          </div>

          <div className="last-scraped">🕒 Last scraped: {lastScrape}</div>

          {/* Online users */}
          <div className="online-section">
            <div className="online-title">
              <span className="online-dot" />
              Online ({onlineUsers.length})
            </div>
            {onlineUsers.map(u => (
              <div key={u.user_email} className="online-user">
                <div className="online-avatar">{u.user_name[0].toUpperCase()}</div>
                <span>{u.user_name.split(" ")[0]}</span>
                {u.user_email === user?.user_email && <span className="online-you">you</span>}
              </div>
            ))}
          </div>

          {scraperWarning && !isGuest && (
            <div className="scraper-busy-warn">{scraperWarning} Coordinate before running scraper.</div>
          )}

          <button
            className="action-btn"
            onClick={handleRunScraper}
            disabled={scraperBusy}
            style={isGuest ? { opacity: 0.7 } : {}}
            title={isGuest ? "Members only" : ""}
          >
            {scraperBusy ? "⏳ Triggering…" : "🚀 Run Scraper Now"}
            {isGuest && <span style={{ fontSize: 12, color: "var(--text-muted)" }}> 🔒</span>}
          </button>

          <div className="sidebar-divider" />

          {NAV.map(({ id, icon, label }) => (
            <button
              key={id}
              className={`nav-btn ${(page === id || (id === "accounts" && page === "account_detail")) ? "active" : ""}`}
              onClick={() => navigate(id)}
            >
              <span className="icon">{icon}</span>
              {label}
              {id === "ai_insights" && isGuest && <span style={{ marginLeft: "auto", fontSize: 11, color: "var(--text-muted)" }}>🔒</span>}
            </button>
          ))}

          <div className="sidebar-divider" />

          <button className="action-btn" onClick={handleRefresh} disabled={loading}>
            🔄 Refresh Data
          </button>

          <div className="sidebar-divider" />

          <div className="channel-filter-label">Filter Channels</div>
          <div className="filter-row">
            <button onClick={() => setSelChannels([...channels])}>Select All</button>
            <button onClick={() => setSelChannels([])}>Clear</button>
          </div>
          <div className="channel-list">
            {channels.map(ch => (
              <label key={ch} className={`channel-chip ${selChannels.includes(ch) ? "selected" : ""}`}>
                <input type="checkbox" style={{ display: "none" }}
                  checked={selChannels.includes(ch)}
                  onChange={e => {
                    if (e.target.checked) setSelChannels(s => [...s, ch]);
                    else setSelChannels(s => s.filter(c => c !== ch));
                  }}
                />
                <div className="ch-dot">{ch[0].toUpperCase()}</div>
                <span>{ch}</span>
              </label>
            ))}
          </div>

          <div style={{ marginTop: "auto", paddingTop: 16 }}>
            <div style={{ fontSize: 11, color: "var(--text-muted)", padding: "0 8px", marginBottom: 6 }}>
              Logged in as<br />
              <span style={{ color: "var(--text)", fontWeight: 600 }}>{user.user_name}</span>
              {isGuest && <span style={{ color: "var(--warning)", fontSize: 10, marginLeft: 4 }}>(Guest)</span>}
            </div>
            <button className="action-btn" onClick={handleLogout} style={{ fontSize: 12 }}>
              🚪 Logout
            </button>
          </div>
        </aside>

        {/* Main */}
        <main className="main-content">
          {loading
            ? <Spinner label="Loading dashboard data…" />
            : (
              <>
                {page === "top_posts"      && <TopPostsPage rows={filteredRows} />}
                {page === "accounts"       && <AccountsPage rows={filteredRows} onSelectAccount={u => { setSelectedAccount(u); setPage("account_detail"); }} />}
                {page === "account_detail" && selectedAccount && <AccountDetailPage rows={filteredRows} username={selectedAccount} onBack={() => navigate("accounts")} />}
                {page === "analytics"      && <AnalyticsPage rows={filteredRows} />}
                {page === "ai_insights"    && <AIInsightsPage rows={filteredRows} isGuest={isGuest} />}
              </>
            )
          }
        </main>
      </div>

      {toast && <Toast {...toast} onClose={() => setToast(null)} />}
    </>
  );
}