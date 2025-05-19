# rss_checker.py
# Updated script to fetch RSS feed entries from https://australiandefiassociation.substack.com/feed
# and store them in articles.csv with columns: date, source, url, title, done.
# Maintains state by reading existing CSV and only adding new entries for this source.

import csv
import os
from datetime import datetime
import feedparser

# Constants
RSS_URL = 'https://australiandefiassociation.substack.com/feed'
CSV_FILE = 'articles.csv'
SOURCE = 'australiandefiassociation.substack.com'


def load_existing_urls():
    """
    Load existing URLs for our source from the CSV.
    Returns a set of URLs already present.
    """
    urls = set()
    if not os.path.exists(CSV_FILE):
        return urls

    with open(CSV_FILE, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if row.get('source') == SOURCE:
                urls.add(row.get('url'))
    return urls


def append_articles(entries):
    """
    Append new entries to the CSV in chronological order (oldest first).
    Each entry: date (ISO-8601), source, url, title, done (blank)
    """
    file_exists = os.path.exists(CSV_FILE)
    mode = 'a' if file_exists else 'w'

    with open(CSV_FILE, mode, newline='', encoding='utf-8') as csvfile:
        fieldnames = ['date', 'source', 'url', 'title', 'done']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        if not file_exists:
            writer.writeheader()

        for entry in entries:
            # Convert published date to ISO-8601
            published = getattr(entry, 'published', None) or entry.get('updated')
            # Parse using feedparser's structured time
            dt = datetime(*entry.published_parsed[:6]) if entry.get('published_parsed') else datetime.utcnow()
            iso_date = dt.isoformat()

            writer.writerow({
                'date': iso_date,
                'source': SOURCE,
                'url': entry.link,
                'title': entry.title,
                'done': ''
            })


def main():
    # Fetch and parse feed
    feed = feedparser.parse(RSS_URL)
    entries = feed.entries

    if not entries:
        print("⚠️ No entries found in feed.")
        return

    # Load existing URLs to detect new ones
    existing_urls = load_existing_urls()

    # Filter only new entries and ensure they are from 2025 onwards, in chronological order (oldest first)
    new_entries = []
    for entry in reversed(entries):
        # Skip if already processed
        if entry.link in existing_urls:
            continue

        # Retrieve publication date
        if entry.get('published_parsed'):
            dt = datetime(*entry.published_parsed[:6])
        else:
            dt = datetime.utcnow()

        # Enforce articles from 2025 onwards
        if dt.year < 2025:
            continue

        new_entries.append(entry)

    if not new_entries:
        print("No new articles from 2025 onwards to add.")
        return

    # Append new entries
    append_articles(new_entries)

    # Output added URLs
    for entry in new_entries:
        print(entry.link)


if __name__ == '__main__':
    main()
