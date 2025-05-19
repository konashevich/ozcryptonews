#!/usr/bin/env python3
import requests
import datetime
import json
import os
import sys

# === CONFIGURATION ===
API_URL    = 'https://ausblock.com.au/wp-json/wp/v2/posts'
STATE_FILE = 'au_block_state.json'
PAGE_SIZE  = 100   # max number of posts per API call

def load_state():
    """Load last_checked; if missing, default to one month ago."""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            js = json.load(f)
            return datetime.datetime.fromisoformat(js['last_checked'])
    # first run: look back one month
    return datetime.datetime.now() - datetime.timedelta(days=30)

def save_state(ts):
    """Save an ISO-format timestamp to disk."""
    with open(STATE_FILE, 'w') as f:
        json.dump({'last_checked': ts.isoformat()}, f)

def fetch_all_posts():
    """
    Page through the WP API, returning a list of (pub_date: datetime, url: str),
    sorted newest→oldest.
    """
    all_posts = []
    page = 1

    while True:
        resp = requests.get(API_URL, params={
            'per_page': PAGE_SIZE,
            'page': page,
            'orderby': 'date',
            'order': 'desc',
            '_fields': 'date,link'
        })
        resp.raise_for_status()
        data = resp.json()
        if not data:
            break

        for item in data:
            # WP returns dates like "2025-04-08T02:00:24"
            dt = datetime.datetime.fromisoformat(item['date'])
            all_posts.append((dt, item['link']))

        if len(data) < PAGE_SIZE:
            break
        page += 1

    return all_posts

def main():
    # 1) Load or initialise state
    first_run    = not os.path.exists(STATE_FILE)
    last_checked = load_state()

    # On very first run, write the one-month-ago baseline out
    if first_run:
        save_state(last_checked)

    # 2) Fetch everything from the API
    try:
        posts = fetch_all_posts()
    except Exception as e:
        print("❌ Failed to fetch posts:", e, file=sys.stderr)
        sys.exit(1)

    # 3) Filter for new posts
    now = datetime.datetime.now()
    new_posts = [(dt, url) for dt, url in posts
                 if last_checked < dt <= now]

    if new_posts:
        print(f"New articles since {last_checked.isoformat()}:")
        for dt, url in sorted(new_posts, key=lambda x: x[0]):
            print(f"{dt.date()}  →  {url}")
        # 4) Advance state to the most recent one we just saw
        newest = max(dt for dt, _ in new_posts)
        save_state(newest)
    else:
        print(f"No new articles since {last_checked.isoformat()}.")

if __name__ == '__main__':
    main()
