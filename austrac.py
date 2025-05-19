import csv
import requests
import feedparser
import os
import time
from datetime import datetime, timezone, timedelta

# --- Configuration ---
RSS_URL = "https://www.austrac.gov.au/media-release/rss.xml"
KEYWORDS_TXT = "web3keywords.txt" # Changed from CSV to TXT for keywords
ARTICLES_CSV = "articles.csv"
SOURCE_NAME = "austrac.gov.au"
CSV_HEADERS = ['date', 'source', 'url', 'title', 'done']
REQUEST_TIMEOUT = 30

# --- Functions ---

def load_keywords(filename):
    """Loads keywords from the specified TXT file (one keyword/phrase per line)."""
    keywords = set()
    if not os.path.exists(filename):
        print(f"Warning: Keywords file '{filename}' not found. No keyword filtering will be applied.")
        return keywords
    try:
        with open(filename, mode='r', encoding='utf-8-sig') as infile: # utf-8-sig for potential BOM
            for line in infile:
                keyword = line.strip().lower()
                if keyword: # Ensure keyword is not empty
                    keywords.add(keyword)
        print(f"Loaded {len(keywords)} unique keywords/phrases from {filename}.")
    except FileNotFoundError:
         print(f"Error: Keywords file '{filename}' not found.") # Should be caught by os.path.exists
    except Exception as e:
        print(f"Error loading keywords from {filename}: {e}")
    return keywords

def load_existing_urls(filename, source_filter):
    """Loads existing article URLs for a specific source from the articles CSV file."""
    existing_urls = set()
    if not os.path.exists(filename) or os.path.getsize(filename) == 0:
        try:
            with open(filename, mode='w', newline='', encoding='utf-8') as outfile: # Create if not exists
                writer = csv.writer(outfile)
                writer.writerow(CSV_HEADERS)
            print(f"Created or initialized articles file '{filename}' with headers.")
        except IOError as e:
            print(f"Error: Could not create/initialize articles file '{filename}': {e}")
        return existing_urls

    try:
        with open(filename, mode='r', newline='', encoding='utf-8') as infile:
            reader = csv.DictReader(infile)
            if not all(header in reader.fieldnames for header in ['url', 'source']):
                 print(f"Warning: Existing file '{filename}' is missing 'url' or 'source' column headers.")
            else:
                for row in reader:
                    if row.get('source') == source_filter and row.get('url'):
                        existing_urls.add(row['url'].strip())
    except Exception as e:
        print(f"Error reading existing articles from '{filename}': {e}")
        print(f"Warning: Proceeding with potentially incomplete duplicate checking for '{source_filter}'.")

    print(f"Found {len(existing_urls)} existing URLs for source '{source_filter}' in {filename}")
    return existing_urls

def fetch_and_parse_feed(url, timeout):
    """Fetches and parses the RSS feed."""
    print(f"Fetching RSS feed from: {url}")
    headers = {'User-Agent': 'Python RSS Collector Script/1.0'}
    try:
        response = requests.get(url, timeout=timeout, headers=headers)
        response.raise_for_status()
        content = response.content # Use content for feedparser
        feed = feedparser.parse(content)
        if feed.bozo:
            print(f"Warning: Feed may be ill-formed. Parser issue: {feed.bozo_exception}")
        if not feed.entries:
             print("Warning: Feed parsed, but no entries found.")
        else:
             print(f"Successfully parsed feed. Found {len(feed.entries)} entries.")
        return feed
    except requests.exceptions.Timeout:
        print(f"Error: Request timed out fetching feed: {url}")
    except requests.exceptions.RequestException as e:
        print(f"Error: Failed to fetch RSS feed: {e}")
    except Exception as e:
        print(f"Error: Unexpected error during feed fetching/parsing: {e}")
    return None

def check_match(entry, keywords):
    """Checks if an entry's title or summary contains keywords (case-insensitive substring)."""
    title = entry.get('title', '').lower()
    summary = entry.get('summary', entry.get('description', '')).lower()
    content_to_check = title + " " + summary
    for keyword in keywords:
        if keyword in content_to_check:
            return True
    return False

def format_date_to_iso_utc(parsed_date_tuple):
    """
    Converts feedparser's time tuple to an ISO 8601 UTC string: YYYY-MM-DDTHH:MM:SS+00:00.
    Feedparser's published_parsed is assumed to be in UTC if no timezone info is present.
    """
    if not parsed_date_tuple:
        return None
    try:
        # Create a datetime object from the time tuple.
        # time.mktime assumes local time, so we need to be careful.
        # A better way is to use datetime directly if the tuple represents UTC.
        # feedparser.struct_time is a time.struct_time object.
        # Index 0-5 are year, month, day, hour, minute, second.
        dt_obj = datetime(
            parsed_date_tuple[0], parsed_date_tuple[1], parsed_date_tuple[2],
            parsed_date_tuple[3], parsed_date_tuple[4], parsed_date_tuple[5]
        )
        # Assume the parsed time from feedparser is UTC, make it timezone-aware.
        dt_utc = dt_obj.replace(tzinfo=timezone.utc)
        return dt_utc.strftime('%Y-%m-%dT%H:%M:%S+00:00')
    except (TypeError, ValueError, IndexError) as e:
        print(f"Warning: Could not convert date tuple {parsed_date_tuple} to ISO UTC format: {e}")
        return None


def main():
    print("--- Starting AUSTRAC RSS Collector (Date Format UTC) ---")
    keywords = load_keywords(KEYWORDS_TXT)
    if not keywords:
        print("No keywords loaded. Exiting.")
        return

    existing_urls = load_existing_urls(ARTICLES_CSV, SOURCE_NAME)
    feed = fetch_and_parse_feed(RSS_URL, REQUEST_TIMEOUT)
    if not feed:
        print("Failed to fetch/parse RSS feed. Exiting.")
        return

    new_articles = []
    processed_count = 0
    MIN_YEAR = 2025

    print("Processing feed entries...")
    for entry in feed.entries:
        processed_count += 1
        url = entry.get('link', '').strip()
        title = entry.get('title', 'No Title Available').strip()
        published_parsed_tuple = entry.get('published_parsed')

        if not url or not published_parsed_tuple:
            print(f"Warning: Skipping entry #{processed_count} due to missing URL or date. URL: '{url}', Title: '{title}'")
            continue
        if url in existing_urls:
            continue

        # Convert feedparser's time tuple to datetime object to check year
        try:
            entry_dt = datetime(
                published_parsed_tuple[0], published_parsed_tuple[1], published_parsed_tuple[2],
                tzinfo=timezone.utc # Assume UTC for year check
            )
            if entry_dt.year < MIN_YEAR:
                # print(f"Debug: Skipping article from {entry_dt.year}: {title}")
                continue
        except Exception as e:
            print(f"Warning: Could not parse date for year check for '{title}': {e}")
            continue


        if check_match(entry, keywords):
            iso_date_utc_str = format_date_to_iso_utc(published_parsed_tuple)
            if not iso_date_utc_str:
                 print(f"Warning: Skipping matched article due to date formatting error. URL: <{url}>")
                 continue

            # For sorting, convert the ISO string back to a datetime object (aware)
            # This ensures sorting works correctly with full timestamps.
            try:
                sort_dt_utc = datetime.fromisoformat(iso_date_utc_str)
            except ValueError:
                print(f"Warning: Could not parse ISO date '{iso_date_utc_str}' for sorting. Skipping article '{title}'.")
                continue


            article_data = {
                'date': iso_date_utc_str, # YYYY-MM-DDTHH:MM:SS+00:00
                'source': SOURCE_NAME,
                'url': url,
                'title': title,
                'done': '',
                '_sort_date_obj': sort_dt_utc # Use datetime object for sorting
            }
            new_articles.append(article_data)

    print(f"Finished processing {processed_count} entries. Found {len(new_articles)} new matching articles.")

    if new_articles:
        new_articles.sort(key=lambda x: x['_sort_date_obj']) # Sort by datetime object
        print(f"Appending {len(new_articles)} new articles to '{ARTICLES_CSV}'...")
        try:
            with open(ARTICLES_CSV, mode='a', newline='', encoding='utf-8') as outfile:
                writer = csv.DictWriter(outfile, fieldnames=CSV_HEADERS, extrasaction='ignore')
                # Header is written by load_existing_urls if file is new/empty
                for article in new_articles:
                    writer.writerow({k: article[k] for k in CSV_HEADERS}) # Write only specified headers
            print(f"Successfully appended {len(new_articles)} new articles.")
        except IOError as e:
            print(f"Error: Could not write new articles to '{ARTICLES_CSV}': {e}")
        except Exception as e:
            print(f"Error: Unexpected error while writing to CSV: {e}")
    else:
        print("No new matching articles to add.")
    print("--- AUSTRAC RSS Collector Finished ---")

if __name__ == "__main__":
    main()
