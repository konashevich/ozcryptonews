#!/usr/bin/env python3
"""
Check the Australian FinTech newsfeed.
Dates are parsed and stored in ISO 8601 UTC format: YYYY-MM-DDTHH:MM:SS+00:00.
"""

import requests
from bs4 import BeautifulSoup
import os
import csv
import re
import datetime # Standard datetime
from dateutil import parser as dateparser
from datetime import timezone # Import timezone

# — Configuration —
FEED_URL = 'https://australianfintech.com.au/newsfeed-page/'
CSV_FILE = 'articles.csv'
TOP_N = 10 # How many latest links to check from the feed page
SOURCE = 'australianfintech.com.au'
CSV_HEADERS = ['date', 'source', 'url', 'title', 'done']

def ensure_csv_header():
    """Create CSV file with header if it doesn't yet exist or is empty."""
    if not os.path.exists(CSV_FILE) or os.path.getsize(CSV_FILE) == 0:
        with open(CSV_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
            writer.writeheader()
        print(f"Initialized CSV file '{CSV_FILE}' with headers.")

def load_seen_urls():
    """Read existing CSV and return set of URLs where source matches."""
    seen = set() # Use a set for faster lookups
    if not os.path.exists(CSV_FILE) or os.path.getsize(CSV_FILE) == 0:
        return seen # No file or empty file, no seen URLs

    try:
        with open(CSV_FILE, 'r', encoding='utf-8', newline='') as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames or not all(h in reader.fieldnames for h in ['url', 'source']):
                print(f"Warning: CSV file '{CSV_FILE}' is missing 'url' or 'source' headers.")
                return seen
            for row in reader:
                if row.get('source') == SOURCE and row.get('url'):
                    seen.add(row['url'])
    except Exception as e:
        print(f"Error loading seen URLs from '{CSV_FILE}': {e}")
    return seen


def fetch_latest_links():
    """Scrape the feed page for the first TOP_N article hrefs."""
    links = []
    try:
        resp = requests.get(FEED_URL, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, 'html.parser')
        # Look for <a> tags with "Read more" text, common on this site
        read_more_links = soup.find_all('a', string=re.compile(r'Read more', re.IGNORECASE))
        for link_tag in read_more_links:
            if link_tag.get('href'):
                links.append(link_tag['href'])
            if len(links) >= TOP_N:
                break
    except requests.exceptions.RequestException as e:
        print(f"Error fetching latest links from {FEED_URL}: {e}")
    return links


def parse_article_date_and_title(url):
    """
    Fetches an article page, extracts its publication date and title.
    Returns (datetime object UTC, ISO-8601 UTC string, title string).
    Returns (None, "", "") on failure.
    """
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, 'html.parser')
    except requests.exceptions.RequestException as e:
        print(f"Error fetching article page {url}: {e}")
        return None, "", ""

    title_tag = soup.find(['h1', 'h2']) # Common tags for main title
    title = title_tag.get_text(strip=True) if title_tag else 'No Title Found'

    date_obj_utc = None
    date_iso_utc = ""

    # Attempt 1: <time datetime="..."> attribute
    time_tag = soup.find('time', datetime=True)
    if time_tag and time_tag.get('datetime'):
        try:
            # dateparser.parse can handle various formats and timezone info if present
            parsed_dt = dateparser.parse(time_tag['datetime'])
            # Convert to UTC
            if parsed_dt.tzinfo is None: # If naive, assume local and convert (or assume UTC)
                # For simplicity, let's assume naive dates from here are UTC if not specified
                # A more robust solution might try to infer local timezone or require config
                date_obj_utc = parsed_dt.replace(tzinfo=timezone.utc)
            else:
                date_obj_utc = parsed_dt.astimezone(timezone.utc)
        except (ValueError, TypeError) as e:
            print(f"Warning: Could not parse datetime attribute '{time_tag['datetime']}' from {url}: {e}")
            date_obj_utc = None # Reset if parsing failed

    # Attempt 2: Find date string in text (e.g., "Month DD, YYYY")
    if not date_obj_utc:
        # Look for common date patterns in the text content
        # This regex is an example, might need refinement for australianfintech.com.au
        date_pattern = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}'
        text_content = soup.get_text()
        match = re.search(date_pattern, text_content)
        if match:
            try:
                parsed_dt = dateparser.parse(match.group(0))
                # Assume UTC if parsed as naive
                date_obj_utc = parsed_dt.replace(tzinfo=timezone.utc)
            except (ValueError, TypeError) as e:
                print(f"Warning: Could not parse date string '{match.group(0)}' from text on {url}: {e}")

    if date_obj_utc:
        date_iso_utc = date_obj_utc.strftime('%Y-%m-%dT%H:%M:%S+00:00')
    else:
        print(f"Warning: Could not determine publication date for {url}. Using current UTC time as fallback.")
        date_obj_utc = datetime.datetime.now(timezone.utc) # Fallback to current UTC time
        date_iso_utc = date_obj_utc.strftime('%Y-%m-%dT%H:%M:%S+00:00')
        
    return date_obj_utc, date_iso_utc, title


def append_to_csv(articles_data):
    """Append a list of article data (dictionaries) to the CSV file."""
    if not articles_data:
        return
    try:
        with open(CSV_FILE, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
            for article_row in articles_data:
                 writer.writerow(article_row)
        print(f"Appended {len(articles_data)} new articles to '{CSV_FILE}'.")
    except IOError as e:
        print(f"Error writing to CSV '{CSV_FILE}': {e}")


def main():
    print("--- Starting Australian FinTech Scraper (Date Format UTC) ---")
    ensure_csv_header()
    seen_urls = load_seen_urls()
    print(f"Loaded {len(seen_urls)} seen URLs for source '{SOURCE}'.")

    latest_links = fetch_latest_links()
    if not latest_links:
        print("No links found on the newsfeed page.")
        return

    new_articles_to_add = []
    MIN_YEAR = 2025

    for url in latest_links:
        if url in seen_urls:
            # print(f"Skipping already seen URL: {url}")
            continue

        date_obj_utc, date_iso_utc, title = parse_article_date_and_title(url)

        if date_obj_utc is None: # Should not happen with fallback, but defensive check
            print(f"Skipping article due to parsing failure (no date): {url}")
            continue
            
        if date_obj_utc.year < MIN_YEAR:
            # print(f"Skipping article from {date_obj_utc.year}: {url}")
            seen_urls.add(url) # Add to seen so we don't re-process old ones next time
            continue
        
        print(f"Found new article: '{title}' ({date_iso_utc}) URL: {url}")
        new_articles_to_add.append({
            'date_obj': date_obj_utc, # For sorting
            'date': date_iso_utc,     # ISO string for CSV
            'source': SOURCE,
            'url': url,
            'title': title,
            'done': ''
        })
        seen_urls.add(url) # Add to seen set for current run

    if not new_articles_to_add:
        print("No new articles found to add (or all were older than 2025).")
        return

    # Sort entries by date_obj (oldest first)
    new_articles_to_add.sort(key=lambda e: e['date_obj'])

    # Prepare for CSV by removing the temporary sort key
    csv_ready_articles = []
    for item in new_articles_to_add:
        del item['date_obj']
        csv_ready_articles.append(item)

    append_to_csv(csv_ready_articles)
    print("--- Australian FinTech Scraper Finished ---")

if __name__ == '__main__':
    main()
