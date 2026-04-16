import os
import json
import random
import time
from scrapfly import ScrapflyClient, ScrapeConfig
from dotenv import load_dotenv
load_dotenv()

SCRAPFLY_KEY = os.getenv("SCRAPFLY_KEY")
client = ScrapflyClient(key=SCRAPFLY_KEY)

TARGET_ACCOUNTS = [
    "freepressjournal"
]

BASE_HEADERS = {
    "x-ig-app-id": "936619743392459",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.instagram.com/",
}

def get_user_id(username):
    url = f"https://www.instagram.com/api/v1/users/web_profile_info/?username={username}"
    result = client.scrape(ScrapeConfig(url=url, asp=True, headers=BASE_HEADERS))
    data = json.loads(result.content)
    user = data['data']['user']
    print(f'"{username}": "{user["id"]}"')

for username in TARGET_ACCOUNTS:
    get_user_id(username)
    time.sleep(random.uniform(2, 4))  # small delay to avoid rate limit