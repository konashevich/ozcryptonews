#!/usr/bin/env python3
import requests
import datetime # Keep standard datetime
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
    Read CSV_FILE and return the max date (as datetime.datetime object, UTC)
    for rows where source == SOURCE. If no such rows, return one month ago (UTC).
    """
    if not os.path.exists(CSV_FILE):
        return datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=30)

    last = None
    with open(CSV_FILE, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get('source') != SOURCE:
                continue
            try:
                # Assume dates in CSV are ISO format and convert to datetime object
                dt_obj = datetime.datetime.fromisoformat(row['date'])
                # Ensure it's UTC if it's naive or convert if it has other offset
                if dt_obj.tzinfo is None:
                    dt_obj = dt_obj.replace(tzinfo=datetime.timezone.utc) # Assume UTC if naive
                else:
                    dt_obj = dt_obj.astimezone(datetime.timezone.utc)

            except Exception:
                continue # Skip rows with invalid date format

            if last is None or dt_obj > last:
                last = dt_obj

    if last:
        return last
    # Default if no matching entries or all dates were invalid
    return datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=60)


def fetch_all_posts():
    """
    Page through the WP API, returning list of (datetime_object_utc, url, title),
    newest→oldest.
    """
    out = []
    page = 1
    while True:
        try:
            resp = requests.get(API_URL, params={
                'per_page': PAGE_SIZE,
                'page': page,
                'orderby': 'date',
                'order': 'desc'
            }, timeout=30) # Added timeout
            resp.raise_for_status()
            data = resp.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching page {page} from WP API: {e}", file=sys.stderr)
            break # Stop if a page fetch fails
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON from WP API on page {page}: {e}", file=sys.stderr)
            break


        if not data:
            break

        for item in data:
            # WP API usually provides dates in ISO 8601 format with timezone (often UTC or server local)
            # Example: "2024-05-15T10:00:00" or "2024-05-15T10:00:00+10:00"
            date_str = item.get('date_gmt') or item.get('date') # Prefer GMT/UTC date
            if not date_str:
                print(f"Warning: Missing date for post: {item.get('link')}", file=sys.stderr)
                continue
            
            try:
                # fromisoformat handles timezone offsets correctly if present
                dt_obj = datetime.datetime.fromisoformat(date_str)
                # Ensure it's converted to UTC for consistency
                dt_obj_utc = dt_obj.astimezone(datetime.timezone.utc)
            except ValueError:
                print(f"Warning: Could not parse date string '{date_str}' for post: {item.get('link')}", file=sys.stderr)
                continue # Skip if date is unparsable

            url = item['link']
            raw_title = item.get('title', {}).get('rendered', '')
            title = BeautifulSoup(raw_title, 'html.parser').get_text(strip=True)
            out.append((dt_obj_utc, url, title))

        if len(data) < PAGE_SIZE:
            break
        page += 1
    return out

def ensure_csv_header():
    """If CSV_FILE doesn’t exist or is empty, create it with the proper header."""
    if not os.path.exists(CSV_FILE) or os.path.getsize(CSV_FILE) == 0:
        with open(CSV_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'date', 'source', 'url', 'title', 'done'
            ])
            writer.writeheader()
            print(f"Initialized CSV file '{CSV_FILE}' with headers.")

def main():
    print("--- Starting Ausblock Scraper (Date Format UTC) ---")
    # 1) Find our cutoff (ensure it's a UTC datetime object)
    last_checked_utc = load_last_date()
    MIN_YEAR = 2025  # Only process posts from 2025 onward

    print(f"Checking for new articles since (UTC): {last_checked_utc.strftime('%Y-%m-%dT%H:%M:%S+00:00')}")

    # 2) Fetch posts (dt_utc, url, title)
    posts = fetch_all_posts()
    if not posts:
        print("No posts fetched from API.")
        return

    # 3) Filter new ones (dt_utc is already UTC, last_checked_utc is UTC)
    new_posts_data = []
    for dt_utc, url, title in posts:
        if dt_utc > last_checked_utc and dt_utc.year >= MIN_YEAR:
            new_posts_data.append((dt_utc, url, title))

    if not new_posts_data:
        print(f"No new articles since {last_checked_utc.strftime('%Y-%m-%dT%H:%M:%S+00:00')} from {MIN_YEAR} onward.")
        return

    # 4) Append them in chronological order (oldest first)
    new_posts_data.sort(key=lambda x: x[0]) # Sort by datetime object
    ensure_csv_header()

    appended_count = 0
    try:
        with open(CSV_FILE, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'date', 'source', 'url', 'title', 'done'
            ])
            for dt_utc, url, title in new_posts_data:
                # Format to YYYY-MM-DDTHH:MM:SS+00:00
                iso_date_utc = dt_utc.strftime('%Y-%m-%dT%H:%M:%S+00:00')
                writer.writerow({
                    'date':   iso_date_utc,
                    'source': SOURCE,
                    'url':    url,
                    'title':  title,
                    'done':   ''
                })
                appended_count +=1
    except IOError as e:
        print(f"Error writing to CSV file '{CSV_FILE}': {e}", file=sys.stderr)
        return # Stop if writing fails

    # 5) Report
    print(f"Appended {appended_count} new articles:")
    for dt_utc, url, _ in new_posts_data:
        print(f"{dt_utc.strftime('%Y-%m-%d %H:%M:%S UTC')} → {url}")
    print("--- Ausblock Scraper Finished ---")

if __name__ == '__main__':
    main()
