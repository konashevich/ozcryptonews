#!/usr/bin/env python3
import requests
from bs4 import BeautifulSoup
import datetime
import json
import os
from urllib.parse import urljoin

# URL of the news page
URL = 'https://regtechglobal.org/news'
# File where we store the timestamp of the last check
STATE_FILE = 'regtech_state.json'

def load_state():
    """
    Load last_checked from STATE_FILE.
    If it doesn't exist, default to one month ago.
    """
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            data = json.load(f)
            return datetime.datetime.fromisoformat(data['last_checked'])
    return datetime.datetime.now() - datetime.timedelta(days=30)

def save_state(timestamp):
    """
    Save the given timestamp (ISO string) into STATE_FILE.
    """
    with open(STATE_FILE, 'w') as f:
        json.dump({'last_checked': timestamp.isoformat()}, f)

def fetch_new_articles(since):
    """
    Scrape the news page for articles newer than `since`.
    Returns a list of absolute URLs.
    """
    resp = requests.get(URL)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, 'html.parser')

    headings = soup.find_all('h4')
    metas    = soup.find_all('h5')
    new_urls = []

    for h4, h5 in zip(headings, metas):
        # Extract the date part (before the '|')
        meta_text = h5.get_text(strip=True)
        date_str  = meta_text.split('|', 1)[0].strip()  # e.g. "12 Feb 2025 10:10 AM"
        try:
            dt = datetime.datetime.strptime(date_str, '%d %b %Y %I:%M %p')
        except ValueError:
            # If it doesn’t match, skip this entry
            continue

        if dt <= since:
            break  # older or equal, so we can stop

        link = h4.find('a', href=True)
        if link:
            new_urls.append(urljoin(URL, link['href']))

    return new_urls

def main():
    last_checked = load_state()
    new_articles = fetch_new_articles(last_checked)

    if new_articles:
        for url in new_articles:
            print(url)
    else:
        print('No new articles found since', last_checked.isoformat())

    # Update state to now for the next run
    save_state(datetime.datetime.now())

if __name__ == '__main__':
    main()
