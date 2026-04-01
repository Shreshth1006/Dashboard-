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

client = ScrapflyClient(key=os.getenv("SCRAPFLY_KEY"))
supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))

# =============================
# ✅ EASY CONFIG — edit here
# =============================

TARGET_ACCOUNTS = [
    "indiatoday", "hindustantimes", "ndtv", "ndtvindia", "news24official",
    "timesnow", "abpnewstv", "news9live", "the_hindu", "brut.india",
    "timesofindia", "ani_trending"
]

SCRAPE_SINCE_HOURS    = 3    # cutoff for cheap scraper
MAX_PAGES_PER_ACCOUNT = 3    # max pages for feed API fallback

ACCOUNT_DELAY_MIN = 4
ACCOUNT_DELAY_MAX = 8
MIN_DELAY         = 3.0
MAX_DELAY         = 6.0
MAX_RETRIES       = 3
RETRY_DELAY       = 10

# =============================

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

def get_caption_cheap(node):
    try:
        edges = node['edge_media_to_caption']['edges']
        return edges[0]['node']['text'].replace('\n', ' ').strip() if edges else ""
    except (KeyError, IndexError):
        return ""

def get_caption_feed(item):
    try:
        return item['caption']['text'].replace('\n', ' ').strip()
    except (KeyError, TypeError):
        return ""

def get_media_url_cheap(node):
    return node.get('display_url', '')

def get_media_url_feed(item):
    return item.get('image_versions2', {}).get('candidates', [{}])[0].get('url', '')

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
                print(f"\n  Attempt {attempt} failed: {e} → retry in {wait:.1f}s")
                time.sleep(wait)
            else:
                print(f"  Final failure: {e}")
                return None

# =============================
# METHOD 1 — Cheap (web_profile_info)
# =============================

def get_posts_cheap(username, cutoff, scraped_time):
    url = f"https://www.instagram.com/api/v1/users/web_profile_info/?username={username}"

    try:
        result = client.scrape(ScrapeConfig(url=url, asp=True, headers=BASE_HEADERS))
        data = json.loads(result.content)
    except Exception as e:
        print(f"  Cheap request failed: {e}")
        return None, None, None  # signal to fallback

    try:
        user_info = data['data']['user']
        followers = user_info['edge_followed_by']['count']
        user_id   = user_info['id']
        posts     = user_info['edge_owner_to_timeline_media']['edges']
    except (KeyError, TypeError) as e:
        print(f"  Cheap parse failed: {e}")
        return None, None, None

    print(f"  [cheap] {len(posts)} posts found | Followers: {followers:,}", end="")

    if not posts:
        print(" | 0 posts → switching to feed API")
        return [], user_id, followers  # empty but we have user_id for fallback

    collected = []
    skipped_old = 0

    for post in posts:
        node = post['node']
        timestamp = node.get('taken_at_timestamp', 0)
        post_time = datetime.fromtimestamp(timestamp)

        if cutoff and post_time < cutoff:
            skipped_old += 1
            continue

        caption = get_caption_cheap(node)
        collected.append({
            "username":     username,
            "followers":    followers,
            "post_link":    f"https://www.instagram.com/p/{node['shortcode']}/",
            "media_url":    get_media_url_cheap(node),
            "post_time":    post_time.isoformat(),
            "caption":      caption,
            "hashtags":     extract_hashtags(caption),
            "likes":        node.get('edge_liked_by', {}).get('count', 0),
            "comments":     node.get('edge_media_to_comment', {}).get('count', 0),
            "scraped_time": scraped_time,
        })

    print(f" | {len(collected)} new | {skipped_old} skipped (old)")
    return collected, user_id, followers

# =============================
# METHOD 2 — Feed API fallback (with pagination)
# =============================

def get_posts_feed(username, user_id, followers, scraped_time):
    posts_collected = []
    max_id = None
    page   = 1

    while page <= MAX_PAGES_PER_ACCOUNT:
        base = f"https://www.instagram.com/api/v1/feed/user/{user_id}/?count=12"
        url  = f"{base}&max_id={max_id}" if max_id else base

        print(f"  [feed] Page {page}...", end=" ", flush=True)

        data = scrape_url(url)
        if not data:
            print("FAILED")
            break

        items  = data.get('items', [])
        more   = data.get('more_available', False)
        max_id = data.get('next_max_id')

        print(f"{len(items)} posts", end="")

        if not items:
            print(" | empty, stopping.")
            break

        for item in items:
            ts        = item.get('taken_at', 0)
            post_time = datetime.fromtimestamp(ts)
            caption   = get_caption_feed(item)

            posts_collected.append({
                "username":     username,
                "followers":    followers,
                "post_link":    f"https://www.instagram.com/p/{item['code']}/",
                "media_url":    get_media_url_feed(item),
                "post_time":    post_time.isoformat(),
                "caption":      caption,
                "hashtags":     extract_hashtags(caption),
                "likes":        item.get('like_count', 0),
                "comments":     item.get('comment_count', 0),
                "scraped_time": scraped_time,
            })

        if not more or not max_id:
            print(" | no more pages")
            break

        delay = random.uniform(MIN_DELAY, MAX_DELAY)
        print(f" | wait {delay:.1f}s")
        time.sleep(delay)
        page += 1

    return posts_collected

# =============================
# HYBRID — tries cheap first, falls back to feed
# =============================

def get_posts_for_account(username, cutoff, scraped_time):
    # Step 1 — try cheap endpoint
    collected, user_id, followers = get_posts_cheap(username, cutoff, scraped_time)

    # Step 2 — if cheap returned None (request failed), skip account
    if collected is None:
        print(f"  Skipping @{username} — both methods unavailable")
        return []

    # Step 3 — if cheap returned 0 posts, fallback to feed API
    if len(collected) == 0 and user_id:
        print(f"  Falling back to feed API for @{username}...")
        collected = get_posts_feed(username, user_id, followers, scraped_time)
        print(f"  [feed] {len(collected)} posts collected")

    return collected

# =============================
# ALL ACCOUNTS
# =============================

def get_all_posts():
    cutoff       = datetime.now() - timedelta(hours=SCRAPE_SINCE_HOURS)
    scraped_time = datetime.now().isoformat()

    print(f"Cutoff: {cutoff.strftime('%Y-%m-%d %H:%M:%S')} (last {SCRAPE_SINCE_HOURS}h)")
    print(f"Scraped time: {scraped_time}\n")

    all_data = []

    for i, username in enumerate(TARGET_ACCOUNTS):
        print(f"[{i+1}/{len(TARGET_ACCOUNTS)}] @{username}")

        try:
            posts = get_posts_for_account(username, cutoff, scraped_time)
            all_data.extend(posts)
            print(f"  → {len(posts)} posts collected")
        except Exception as e:
            print(f"  Skipped: {e}")

        if i < len(TARGET_ACCOUNTS) - 1:
            delay = random.uniform(ACCOUNT_DELAY_MIN, ACCOUNT_DELAY_MAX)
            print(f"  Waiting {delay:.1f}s...\n")
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
            supabase.table("posts").upsert(
                batch,
                on_conflict="post_link",
                ignore_duplicates=False   # ✅ always update likes/comments/scraped_time
            ).execute()
            print(f"  Batch {i//BATCH_SIZE + 1} pushed ({len(batch)} rows)")
        except Exception as e:
            print(f"  Insert error: {e}")

# =============================
# MAIN
# =============================

def main():
    print("Starting hybrid scrape...\n")
    posts = get_all_posts()
    print(f"\nTotal posts collected: {len(posts)}")
    push_to_supabase(posts)
    print("Done.")

if __name__ == "__main__":
    main()