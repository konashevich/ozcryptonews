#!/usr/bin/env python3
"""
Check the Australian FinTech newsfeed for new articles by URL alone.
Uses the latest-5-URLs fallback approach, logs new entries to CSV (full ISO-8601 datetime format).
State is derived from existing CSV; filters by source 'australianfintech.com.au'.
New articles are appended chronologically.
"""

import requests
from bs4 import BeautifulSoup
import os
import csv
import re
import datetime
from dateutil import parser as dateparser

# — Configuration —
FEED_URL = 'https://australianfintech.com.au/newsfeed-page/'
CSV_FILE = 'articles.csv'
TOP_N = 10
SOURCE = 'australianfintech.com.au'


def ensure_csv_header():
    """Create CSV file with header if it doesn't yet exist."""
    if not os.path.exists(CSV_FILE):
        with open(CSV_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(
                f,
                fieldnames=['date', 'source', 'url', 'title', 'done']
            )
            writer.writeheader()


def load_seen_urls():
    """
    Read existing CSV and return list of URLs where source matches.
    """
    seen = []
    if not os.path.exists(CSV_FILE):
        return seen
    with open(CSV_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get('source') == SOURCE and row.get('url'):
                seen.append(row['url'])
    return seen


def fetch_latest_links():
    """
    Scrape the feed page and return the first TOP_N article hrefs.
    We look for all <a> tags with the "Read more" text.
    """
    resp = requests.get(FEED_URL)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.content, 'html.parser')
    links = [
        a['href']
        for a in soup.find_all('a', string='Read more')
        if a.get('href')
    ]
    return links[:TOP_N]


def parse_article_date_and_title(url):
    """
    Fetches an article page and extracts its publication date and title.
    Date is parsed from <time datetime> or first matching "Month DD, YYYY" text.
    Returns (datetime object, ISO-8601 datetime string, title string).
    """
    resp = requests.get(url)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.content, 'html.parser')

    # Extract title
    title_tag = soup.find(['h1', 'h2'])
    title = title_tag.get_text(strip=True) if title_tag else ''

    # Extract date
    date_obj = None
    time_tag = soup.find('time')
    if time_tag and time_tag.get('datetime'):
        try:
            date_obj = dateparser.parse(time_tag['datetime'])
        except:
            date_obj = None
    if not date_obj:
        for text in soup.stripped_strings:
            if re.match(r'^[A-Za-z]+ \d{1,2}, \d{4}$', text):
                try:
                    date_obj = dateparser.parse(text)
                    break
                except:
                    pass
    date_iso = date_obj.isoformat() if date_obj else ''
    return date_obj or datetime.datetime.min, date_iso, title


def append_to_csv(date_iso, source, url, title):
    """Append a row to the CSV file."""
    with open(CSV_FILE, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(
            f,
            fieldnames=['date', 'source', 'url', 'title', 'done']
        )
        writer.writerow({
            'date': date_iso,
            'source': source,
            'url': url,
            'title': title,
            'done': ''
        })


def main():
    # Ensure CSV and read seen URLs
    ensure_csv_header()
    seen_urls = load_seen_urls()

    # Fetch latest links
    latest = fetch_latest_links()

    # Determine new URLs not yet seen
    new_urls = [u for u in latest if u not in seen_urls]

    # Parse details, filter by 2025 and sort chronologically
    entries = []
    for url in new_urls:
        date_obj, date_iso, title = parse_article_date_and_title(url)
        # Enforce articles from 2025 onward
        if date_obj.year < 2025:
            continue
        entries.append({'date_obj': date_obj, 'iso': date_iso, 'url': url, 'title': title})
    entries.sort(key=lambda e: e['date_obj'])

    # Append entries to CSV in order and print URLs
    for e in entries:
        append_to_csv(e['iso'], SOURCE, e['url'], e['title'])
        print(e['url'])


if __name__ == '__main__':
    main()