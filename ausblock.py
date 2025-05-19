#!/usr/bin/env python3
import requests
import datetime
import os
import csv
import sys
from bs4 import BeautifulSoup

# === CONFIG ===
API_URL   = 'https://ausblock.com.au/wp-json/wp/v2/posts'
PAGE_SIZE = 100
CSV_FILE  = 'articles.csv'
SOURCE    = 'ausblock.com.au'

def load_last_date():
    """
    Read CSV_FILE and return the max date (as datetime) for rows
    where source == SOURCE. If no such rows, return one month ago.
    """
    if not os.path.exists(CSV_FILE):
        # no CSV yet
        return datetime.datetime.now() - datetime.timedelta(days=30)

    last = None
    with open(CSV_FILE, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get('source') != SOURCE:
                continue
            try:
                dt = datetime.datetime.fromisoformat(row['date'])
            except Exception:
                continue
            if last is None or dt > last:
                last = dt

    if last:
        return last
    return datetime.datetime.now() - datetime.timedelta(days=60)

def fetch_all_posts():
    """
    Page through the WP API, returning list of (dt, url, title),
    newest→oldest.
    """
    out = []
    page = 1
    while True:
        resp = requests.get(API_URL, params={
            'per_page': PAGE_SIZE,
            'page': page,
            'orderby': 'date',
            'order': 'desc'
        })
        resp.raise_for_status()
        data = resp.json()
        if not data:
            break

        for item in data:
            # parse date
            dt = datetime.datetime.fromisoformat(item['date'])
            url = item['link']
            # clean up HTML in title
            raw = item['title'].get('rendered', '')
            title = BeautifulSoup(raw, 'html.parser').get_text(strip=True)
            out.append((dt, url, title))

        if len(data) < PAGE_SIZE:
            break
        page += 1

    return out

def ensure_csv_header():
    """If CSV_FILE doesn’t exist, create it with the proper header."""
    if not os.path.exists(CSV_FILE):
        with open(CSV_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'date', 'source', 'url', 'title', 'done'
            ])
            writer.writeheader()

def main():
    # 1) Find our cutoff
    last_checked = load_last_date()
    MIN_YEAR = 2025  # Only process posts from 2025 onward

    # 2) Fetch posts
    try:
        posts = fetch_all_posts()
    except Exception as e:
        print("Error fetching posts:", e, file=sys.stderr)
        sys.exit(1)

    # 3) Filter new ones (enforcing 2025+)
    new_posts = [(dt, url, title)
                 for dt, url, title in posts
                 if dt > last_checked and dt.year >= MIN_YEAR]

    if not new_posts:
        print(f"No new articles since {last_checked.isoformat()} from {MIN_YEAR} onward.")
        return

    # 4) Append them in chronological order
    new_posts.sort(key=lambda x: x[0])
    ensure_csv_header()
    with open(CSV_FILE, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'date', 'source', 'url', 'title', 'done'
        ])
        for dt, url, title in new_posts:
            writer.writerow({
                'date':   dt.isoformat(),
                'source': SOURCE,
                'url':    url,
                'title':  title,
                'done':   ''
            })

    # 5) Report
    print(f"Appended {len(new_posts)} new articles:")
    for dt, url, _ in new_posts:
        print(f"{dt.date()} → {url}")

if __name__ == '__main__':
    main()

