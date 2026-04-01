import os
import re
import json
import time
import random
from datetime import datetime, timedelta
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

client = ScrapflyClient(key=SCRAPFLY_KEY)
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

TARGET_ACCOUNTS = [
    "indiatoday", "hindustantimes", "ndtv", "ndtvindia", "news24official",
    "timesnow", "abpnewstv", "news9live", "the_hindu", "brut.india",
    "timesofindia", "ani_trending"
]

# =============================
# SCRAPING CONTROLS
# =============================
SCRAPE_SINCE_HOURS    = 24
MAX_PAGES_PER_ACCOUNT = 1
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
# HELPERS
# =============================

def extract_hashtags(caption):
    if not caption:
        return ""
    return ", ".join(re.findall(r'#(\w+)', caption))

def get_media_url(item):
    return item.get('image_versions2', {}).get('candidates', [{}])[0].get('url', '')

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
                asp=True,
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
# STEP 1 - Resolve user_id
# =============================

def get_user_id(username):
    url = f"https://www.instagram.com/api/v1/users/web_profile_info/?username={username}"
    data = scrape_url(url)
    if not data:
        raise Exception("Failed to fetch user")
    user = data['data']['user']
    return user['id'], user['edge_followed_by']['count']

# =============================
# STEP 2 - Paginate feed
# =============================

def get_posts_for_account(username, scraped_time):
    cutoff = (
        datetime.now() - timedelta(hours=SCRAPE_SINCE_HOURS)
        if SCRAPE_SINCE_HOURS else None
    )

    print(f"Resolving @{username}...", end=" ", flush=True)

    try:
        user_id, followers = get_user_id(username)
        print(f"ID={user_id} Followers={followers:,}")
    except Exception as e:
        print(f"FAILED: {e}")
        return []

    posts_collected = []
    max_id = None
    page = 1

    while page <= MAX_PAGES_PER_ACCOUNT:
        base = f"https://www.instagram.com/api/v1/feed/user/{user_id}/?count=12"
        url = f"{base}&max_id={max_id}" if max_id else base

        print(f"Page {page}...", end=" ", flush=True)

        data = scrape_url(url)
        if not data:
            print("FAILED request")
            break

        items = data.get('items', [])
        more = data.get('more_available', False)
        max_id = data.get('next_max_id')

        print(f"{len(items)} posts", end="")

        if not items:
            print(" | empty, stopping.")
            break

        oldest_on_page = None

        for item in items:
            ts = item.get('taken_at', 0)
            post_time = datetime.fromtimestamp(ts)

            if oldest_on_page is None or post_time < oldest_on_page:
                oldest_on_page = post_time

            if cutoff and post_time < cutoff:
                continue

            caption = get_caption(item)
            posts_collected.append({
                "username":     username,
                "followers":    followers,
                "post_link":    f"https://www.instagram.com/p/{item['code']}/",
                "media_url":    get_media_url(item),
                "post_time":    post_time.isoformat(),
                "caption":      caption,
                "hashtags":     extract_hashtags(caption),
                "likes":        item.get('like_count', 0),
                "comments":     item.get('comment_count', 0),
                "scraped_time": scraped_time,  # ✅ when this scrape run happened
            })

        past_cutoff = bool(cutoff and oldest_on_page and oldest_on_page < cutoff)

        if past_cutoff:
            print(" | reached cutoff")
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
    # ✅ One single timestamp for the entire run
    scraped_time = datetime.now().isoformat()
    print(f"Scraped time: {scraped_time}\n")

    all_data = []

    for i, username in enumerate(TARGET_ACCOUNTS):
        print(f"\n[{i+1}/{len(TARGET_ACCOUNTS)}] @{username}")

        try:
            posts = get_posts_for_account(username, scraped_time)
            all_data.extend(posts)
            print(f"Collected {len(posts)} posts")
        except Exception as e:
            print(f"Skipped: {e}")

        if i < len(TARGET_ACCOUNTS) - 1:
            delay = random.uniform(ACCOUNT_DELAY_MIN, ACCOUNT_DELAY_MAX)
            print(f"Waiting {delay:.1f}s...")
            time.sleep(delay)

    return all_data

# =============================
# SUPABASE PUSH
# =============================

def push_to_supabase(posts):
    if not posts:
        print("No new posts to push.")
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