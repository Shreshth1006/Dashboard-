import os
import re
import json
import time
import random
from datetime import datetime, timedelta
from scrapfly import ScrapflyClient
from scrapfly.scrape_config import ScrapeConfig
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

# How far back to look for posts (set this to gap between your runs)
# e.g. runs at 9am, 11am → gap is 2 hours → set 3 to be safe
SCRAPE_SINCE_HOURS = 3

ACCOUNT_DELAY_MIN = 4
ACCOUNT_DELAY_MAX = 8

# =============================

BASE_HEADERS = {
    "x-ig-app-id": "936619743392459",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.instagram.com/",
}

def extract_hashtags(caption):
    if not caption:
        return ""
    return ", ".join(re.findall(r'#(\w+)', caption))

def get_media_url(node):
    return node.get('display_url', '')

def get_caption(node):
    try:
        edges = node['edge_media_to_caption']['edges']
        return edges[0]['node']['text'].replace('\n', ' ').strip() if edges else ""
    except (KeyError, IndexError):
        return ""

def get_posts_for_account(username, cutoff):
    url = f"https://www.instagram.com/api/v1/users/web_profile_info/?username={username}"

    try:
        result = client.scrape(ScrapeConfig(
            url=url,
            asp=True,
            headers=BASE_HEADERS,
        ))
        data = json.loads(result.content)
    except Exception as e:
        print(f"  Request failed: {e}")
        return []

    try:
        user_info = data['data']['user']
        followers = user_info['edge_followed_by']['count']
        posts = user_info['edge_owner_to_timeline_media']['edges']
    except (KeyError, TypeError) as e:
        print(f"  Parse failed: {e}")
        return []

    print(f"  {len(posts)} posts found | Followers: {followers:,}")

    collected = []
    skipped_old = 0

    for post in posts:
        node = post['node']
        timestamp = node.get('taken_at_timestamp', 0)
        post_time = datetime.fromtimestamp(timestamp)

        if cutoff and post_time < cutoff:
            skipped_old += 1
            continue

        caption = get_caption(node)
        collected.append({
            "username": username,
            "followers": followers,
            "post_link": f"https://www.instagram.com/p/{node['shortcode']}/",
            "media_url": get_media_url(node),
            "post_time": post_time.isoformat(),
            "caption": caption,
            "hashtags": extract_hashtags(caption),
            "likes": node.get('edge_liked_by', {}).get('count', 0),
            "comments": node.get('edge_media_to_comment', {}).get('count', 0),
        })

    print(f"  {len(collected)} new | {skipped_old} skipped (old)")
    return collected

def get_all_posts():
    cutoff = datetime.now() - timedelta(hours=SCRAPE_SINCE_HOURS)
    print(f"Cutoff: {cutoff.strftime('%Y-%m-%d %H:%M:%S')} (last {SCRAPE_SINCE_HOURS}h)\n")

    all_data = []
    for i, username in enumerate(TARGET_ACCOUNTS):
        print(f"[{i+1}/{len(TARGET_ACCOUNTS)}] @{username}")
        posts = get_posts_for_account(username, cutoff)
        all_data.extend(posts)

        if i < len(TARGET_ACCOUNTS) - 1:
            delay = random.uniform(ACCOUNT_DELAY_MIN, ACCOUNT_DELAY_MAX)
            print(f"  Waiting {delay:.1f}s...\n")
            time.sleep(delay)

    return all_data

def push_to_supabase(posts):
    if not posts:
        print("No new posts to push.")
        return

    BATCH_SIZE = 50
    for i in range(0, len(posts), BATCH_SIZE):
        batch = posts[i:i + BATCH_SIZE]
        try:
            supabase.table("posts").upsert(batch).execute()
            print(f"  Batch {i//BATCH_SIZE + 1} pushed ({len(batch)} rows)")
        except Exception as e:
            print(f"  Insert error: {e}")

def main():
    print("Starting scrape...\n")
    posts = get_all_posts()
    print(f"\nTotal new posts: {len(posts)}")
    push_to_supabase(posts)
    print("Done.")

if __name__ == "__main__":
    main()
