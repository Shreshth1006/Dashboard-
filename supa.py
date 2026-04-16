import os
import re
import json
import time
import random
import httpx
from datetime import datetime, timezone, timedelta
from scrapfly import ScrapflyClient, ScrapeConfig
from supabase import create_client
from dotenv import load_dotenv
load_dotenv()

# =============================
# CONFIGURATION
# =============================

SCRAPFLY_KEY = os.getenv("SCRAPFLY_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

client   = ScrapflyClient(key=SCRAPFLY_KEY)
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

IST = timezone(timedelta(hours=5, minutes=30))

# Hardcoded user_ids — no scrape credit wasted resolving these
TARGET_ACCOUNTS = [
    {"username": "indiatoday",       "user_id": "1542430186",  "followers": 6800000},
    {"username": "hindustantimes",   "user_id": "1072450671",  "followers": 5200000},
    {"username": "ndtv",             "user_id": "176062718",   "followers": 7100000},
    {"username": "ndtvindia",        "user_id": "31254711281", "followers": 4500000},
    {"username": "news24official",   "user_id": "5433640349",  "followers": 1200000},
    {"username": "timesnow",         "user_id": "573878215",   "followers": 3800000},
    {"username": "abpnewstv",        "user_id": "1412650800",  "followers": 2900000},
    {"username": "brut.india",       "user_id": "8012421289",  "followers": 9200000},
    {"username": "timesofindia",     "user_id": "1691326988",  "followers": 8400000},
    {"username": "ani_trending",     "user_id": "8712374554",  "followers": 1500000},
    {"username": "bbcnews",          "user_id": "16278726",    "followers": 11000000},
    {"username": "freepressjournal", "user_id": "3179979830",    "followers": 180000},
]

# =============================
# SCRAPING CONTROLS
# =============================
MAX_PAGES_PER_ACCOUNT = 3
MIN_DELAY             = 3.0
MAX_DELAY             = 6.0
ACCOUNT_DELAY_MIN     = 6
ACCOUNT_DELAY_MAX     = 12
MAX_RETRIES           = 3
RETRY_DELAY           = 10

BASE_HEADERS = {
    "x-ig-app-id": "936619743392459",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.instagram.com/",
}

# =============================
# IMAGE UPLOAD TO SUPABASE
# =============================

def upload_image(image_url, post_code):
    if not image_url or not post_code:
        return image_url

    filename = f"{post_code}.jpg"

    try:
        resp = httpx.get(image_url, timeout=10, follow_redirects=True)
        if resp.status_code != 200:
            return image_url

        supabase.storage.from_("post-images").upload(
            path=filename,
            file=resp.content,
            file_options={"content-type": "image/jpeg", "upsert": "true"}
        )

        return supabase.storage.from_("post-images").get_public_url(filename)

    except Exception as e:
        print(f"  Image upload failed for {post_code}: {e}")
        return image_url

# =============================
# HELPERS
# =============================

def extract_hashtags(caption):
    if not caption:
        return ""
    return ", ".join(re.findall(r'#(\w+)', caption))

def get_caption(item):
    try:
        return item['caption']['text'].replace('\n', ' ').strip()
    except (KeyError, TypeError):
        return ""

def scrape_url(url):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            result = client.scrape(ScrapeConfig(
                url=url,
                asp=False,
                headers=BASE_HEADERS,
            ))
            return json.loads(result.content)
        except Exception as e:
            wait = RETRY_DELAY * attempt + random.uniform(0, 4)
            if attempt < MAX_RETRIES:
                print(f"\nAttempt {attempt} failed: {e} → retry in {wait:.1f}s")
                time.sleep(wait)
            else:
                print("Final failure:", e)
                return None

# =============================
# SCRAPE FEED (no user_id lookup needed)
# =============================

def get_posts_for_account(account, scraped_time):
    username  = account["username"]
    user_id   = account["user_id"]
    followers = account["followers"]

    print(f"@{username} (ID={user_id})")

    posts_collected = []
    max_id = None
    page   = 1

    cutoff = datetime.now(IST).timestamp() - (24 * 60 * 60)

    while page <= MAX_PAGES_PER_ACCOUNT:
        # count=50 reduces pages needed vs old count=12
        base = f"https://www.instagram.com/api/v1/feed/user/{user_id}/?count=50"
        url  = f"{base}&max_id={max_id}" if max_id else base

        print(f"  Page {page}...", end=" ", flush=True)

        data = scrape_url(url)
        if not data:
            print("FAILED request")
            break

        items  = data.get('items', [])
        more   = data.get('more_available', False)
        max_id = data.get('next_max_id')

        print(f"{len(items)} posts", end="")

        if not items:
            print(" | empty, stopping.")
            break

        oldest_on_page = None

        for item in items:
            ts        = item.get('taken_at', 0)
            post_time = datetime.fromtimestamp(ts, tz=IST)
            post_code = item.get('code', '')

            if oldest_on_page is None or ts < oldest_on_page:
                oldest_on_page = ts

            if ts < cutoff:
                continue

            caption    = get_caption(item)
            cdn_url    = item.get('image_versions2', {}).get('candidates', [{}])[0].get('url', '')
            stable_url = upload_image(cdn_url, post_code)

            posts_collected.append({
                "username":     username,
                "followers":    followers,
                "post_link":    f"https://www.instagram.com/p/{post_code}/",
                "media_url":    stable_url,
                "post_time":    post_time.isoformat(),
                "caption":      caption,
                "hashtags":     extract_hashtags(caption),
                "likes":        item.get('like_count', 0),
                "comments":     item.get('comment_count', 0),
                "scraped_time": scraped_time,
            })

        if oldest_on_page and oldest_on_page < cutoff:
            print(f" | oldest post beyond 24h, stopping.")
            break

        if not more or not max_id:
            print(" | no more pages")
            break

        delay = random.uniform(MIN_DELAY, MAX_DELAY)
        print(f" | wait {delay:.1f}s")
        time.sleep(delay)
        page += 1

    return posts_collected

# =============================
# ALL ACCOUNTS
# =============================

def get_all_posts():
    scraped_time = datetime.now(IST).isoformat()
    print(f"Scraped time: {scraped_time}\n")

    all_data = []

    for i, account in enumerate(TARGET_ACCOUNTS):
        print(f"\n[{i+1}/{len(TARGET_ACCOUNTS)}] ", end="")

        try:
            posts = get_posts_for_account(account, scraped_time)
            all_data.extend(posts)
            print(f"  Collected {len(posts)} posts")
        except Exception as e:
            print(f"  Skipped: {e}")

        if i < len(TARGET_ACCOUNTS) - 1:
            delay = random.uniform(ACCOUNT_DELAY_MIN, ACCOUNT_DELAY_MAX)
            print(f"  Waiting {delay:.1f}s...")
            time.sleep(delay)

    return all_data

# =============================
# SUPABASE PUSH
# =============================

def push_to_supabase(posts):
    if not posts:
        print("No posts to push.")
        return

    BATCH_SIZE = 50
    for i in range(0, len(posts), BATCH_SIZE):
        batch = posts[i:i + BATCH_SIZE]
        try:
            supabase.table("posts").upsert(batch, on_conflict="post_link").execute()
            print(f"  Batch {i//BATCH_SIZE + 1} pushed ({len(batch)} rows)")
        except Exception as e:
            print(f"  Insert error: {e}")

# =============================
# MAIN
# =============================

def main():
    print("Starting scrape...\n")
    posts = get_all_posts()
    print(f"\nTotal posts: {len(posts)}")
    push_to_supabase(posts)
    print("Done.")

if __name__ == "__main__":
    main()