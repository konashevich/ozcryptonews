#!/usr/bin/env python3
"""
Check the Australian FinTech newsfeed for new articles by URL alone.
Always uses the latest-5-URLs fallback approach.
"""

import requests
from bs4 import BeautifulSoup
import datetime
import json
import os

# — Configuration —
FEED_URL   = 'https://australianfintech.com.au/newsfeed-page/'
STATE_FILE = 'fintech_news_state.json'
TOP_N      = 5   # how many top links to track

def load_state():
    """Load previously seen URLs; on first run returns empty list."""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            data = json.load(f)
            return data.get('seen_urls', [])
    return []

def save_state(seen_urls):
    """Persist current top-N URLs for next comparison."""
    with open(STATE_FILE, 'w') as f:
        json.dump({'seen_urls': seen_urls}, f)

def fetch_latest_links():
    """
    Scrape the feed page and return the first TOP_N article hrefs.
    We look for all <a> tags with the "Read more" text.
    """
    resp = requests.get(FEED_URL)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.content, 'html.parser')
    # find_all returns a list of <a> tags; we extract hrefs :contentReference[oaicite:0]{index=0}
    links = [
        a['href']
        for a in soup.find_all('a', string='Read more')
        if a.get('href')
    ]
    return links[:TOP_N]

def main():
    prev_seen = load_state()
    latest = fetch_latest_links()

    # Determine which URLs are new (i.e. not in prev_seen)
    new_urls = [url for url in latest if url not in prev_seen]

    # Always print any new URLs (including on first run)
    for url in new_urls:
        print(url)

    # Save current batch for next time
    save_state(latest)

if __name__ == '__main__':
    main()
