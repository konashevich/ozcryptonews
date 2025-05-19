import csv
import os
from datetime import datetime, timezone # Added timezone
import feedparser

# Constants
RSS_URL = 'https://australiandefiassociation.substack.com/feed'
CSV_FILE = 'articles.csv'
SOURCE = 'australiandefiassociation.substack.com'
CSV_HEADERS = ['date', 'source', 'url', 'title', 'done'] # Define headers

def load_existing_urls():
    """Load existing URLs for our source from the CSV."""
    urls = set()
    if not os.path.exists(CSV_FILE) or os.path.getsize(CSV_FILE) == 0:
        # Create file with header if it doesn't exist or is empty
        try:
            with open(CSV_FILE, 'w', newline='', encoding='utf-8') as csvfile_init:
                writer = csv.DictWriter(csvfile_init, fieldnames=CSV_HEADERS)
                writer.writeheader()
            print(f"Initialized CSV file '{CSV_FILE}' with headers.")
        except IOError as e:
            print(f"Error initializing CSV file '{CSV_FILE}': {e}")
        return urls

    try:
        with open(CSV_FILE, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            if not reader.fieldnames or not all(h in reader.fieldnames for h in ['url', 'source']):
                print(f"Warning: CSV file '{CSV_FILE}' is missing 'url' or 'source' headers.")
                return urls # Cannot reliably read
            for row in reader:
                if row.get('source') == SOURCE and row.get('url'):
                    urls.add(row.get('url'))
    except Exception as e:
        print(f"Error loading existing URLs from '{CSV_FILE}': {e}")
    return urls


def append_articles_to_csv(entries_to_append):
    """
    Append new entries to the CSV.
    Date format: YYYY-MM-DDTHH:MM:SS+00:00 (UTC).
    """
    # File existence and header are handled by load_existing_urls or initial creation
    # We open in append mode.
    try:
        with open(CSV_FILE, 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=CSV_HEADERS)
            for entry_data in entries_to_append: # Expecting a list of dicts
                writer.writerow(entry_data)
        print(f"Successfully appended {len(entries_to_append)} new articles to '{CSV_FILE}'.")
    except IOError as e:
        print(f"Error appending articles to '{CSV_FILE}': {e}")


def main():
    print("--- Starting Australian DeFi Association Scraper (Date Format UTC) ---")
    feed = feedparser.parse(RSS_URL)
    if not feed.entries:
        print("⚠️ No entries found in feed.")
        return

    existing_urls = load_existing_urls()
    new_articles_for_csv = []
    MIN_YEAR = 2025

    # Process entries (feedparser usually provides them newest first, so iterate normally or reverse if needed for chronological add)
    # To add them chronologically (oldest first to CSV), we can collect and then sort.
    
    collected_entries = []
    for entry in feed.entries:
        if entry.link in existing_urls:
            continue

        # Parse date from feedparser's structured time tuple (assumed UTC by feedparser)
        dt_obj_utc = None
        if entry.get('published_parsed'):
            # Create datetime object from tuple: (year, month, day, hour, minute, second, ...)
            # and make it timezone-aware UTC
            try:
                pp = entry.published_parsed
                dt_obj_utc = datetime(pp[0], pp[1], pp[2], pp[3], pp[4], pp[5], tzinfo=timezone.utc)
            except (TypeError, IndexError, ValueError) as e:
                print(f"Warning: Could not parse 'published_parsed' for {entry.link}: {e}. Using current UTC time as fallback.")
                dt_obj_utc = datetime.now(timezone.utc) # Fallback
        else:
            # Fallback if 'published_parsed' is missing
            print(f"Warning: 'published_parsed' missing for {entry.link}. Using current UTC time.")
            dt_obj_utc = datetime.now(timezone.utc)

        if dt_obj_utc.year < MIN_YEAR:
            # print(f"Debug: Skipping article from {dt_obj_utc.year}: {entry.title}")
            continue
        
        # Format to YYYY-MM-DDTHH:MM:SS+00:00
        iso_date_utc = dt_obj_utc.strftime('%Y-%m-%dT%H:%M:%S+00:00')

        collected_entries.append({
            'date': iso_date_utc,
            'source': SOURCE,
            'url': entry.link,
            'title': entry.title.strip() if entry.title else "No Title",
            'done': '',
            '_sort_date_obj': dt_obj_utc # For sorting
        })

    if not collected_entries:
        print("No new articles from 2025 onwards to add.")
        return

    # Sort new entries by date (oldest first)
    collected_entries.sort(key=lambda x: x['_sort_date_obj'])

    # Prepare for CSV by removing the temporary sort key
    for item in collected_entries:
        del item['_sort_date_obj']
        
    append_articles_to_csv(collected_entries)

    # Output added URLs (optional)
    # for entry_dict in collected_entries:
    #     print(f"Added: {entry_dict['url']}")
    print("--- Australian DeFi Association Scraper Finished ---")

if __name__ == '__main__':
    main()
