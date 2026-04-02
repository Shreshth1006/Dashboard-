import os
import re
import json
import time
import random
import httpx
from datetime import datetime
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

client  = ScrapflyClient(key=SCRAPFLY_KEY)
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

TARGET_ACCOUNTS = [
    "indiatoday", "hindustantimes", "ndtv", "ndtvindia", "news24official",
    "timesnow", "abpnewstv", "news9live", "the_hindu", "brut.india",
    "timesofindia", "ani_trending"
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
    """
    Download image from Instagram CDN and upload to Supabase Storage.
    Returns permanent public URL, or original CDN URL as fallback.
    """
    if not image_url or not post_code:
        return image_url

    filename = f"{post_code}.jpg"

    try:
        # Download image directly from CDN (no Scrapfly needed)
        resp = httpx.get(image_url, timeout=10, follow_redirects=True)
        if resp.status_code != 200:
            return image_url

        # Upload to Supabase Storage bucket "post-images"
        supabase.storage.from_("post-images").upload(
            path=filename,
            file=resp.content,
            file_options={"content-type": "image/jpeg", "upsert": "true"}
        )

        # Return permanent public URL
        public_url = supabase.storage.from_("post-images").get_public_url(filename)
        return public_url

    except Exception as e:
        print(f"  Image upload failed for {post_code}: {e}")
        return image_url  # fallback to original CDN url

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
    url  = f"https://www.instagram.com/api/v1/users/web_profile_info/?username={username}"
    data = scrape_url(url)
    if not data:
        raise Exception("Failed to fetch user")
    user = data['data']['user']
    return user['id'], user['edge_followed_by']['count']

# =============================
# STEP 2 - Paginate feed
# =============================

def get_posts_for_account(username, scraped_time):
    print(f"Resolving @{username}...", end=" ", flush=True)

    try:
        user_id, followers = get_user_id(username)
        print(f"ID={user_id} Followers={followers:,}")
    except Exception as e:
        print(f"FAILED: {e}")
        return []

    posts_collected = []
    max_id = None
    page   = 1

    # Cutoff: only posts from last 24 hours
    cutoff = datetime.now().timestamp() - (24 * 60 * 60)

    while page <= MAX_PAGES_PER_ACCOUNT:
        base = f"https://www.instagram.com/api/v1/feed/user/{user_id}/?count=12"
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
            post_time = datetime.fromtimestamp(ts)
            post_code = item.get('code', '')

            if oldest_on_page is None or ts < oldest_on_page:
                oldest_on_page = ts

            # Skip posts older than 24h but finish the page
            if ts < cutoff:
                continue

            caption = get_caption(item)

            # Get CDN url then upload to Supabase Storage for permanence
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

        # Stop paginating if oldest post on this page is beyond 24h
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
    scraped_time = datetime.now().isoformat()
    print(f"Scraped time: {scraped_time}\n")

    all_data = []

    for i, username in enumerate(TARGET_ACCOUNTS):
        print(f"\n[{i+1}/{len(TARGET_ACCOUNTS)}] @{username}")

        try:
            posts = get_posts_for_account(username, scraped_time)
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