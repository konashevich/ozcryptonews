import csv
import requests
import feedparser
import os
import time
from datetime import datetime, timezone, timedelta

# --- Configuration ---
RSS_URL = "https://www.austrac.gov.au/media-release/rss.xml"
KEYWORDS_CSV = "web3keywords.txt" # Input file with keywords (one per line)
ARTICLES_CSV = "articles.csv"     # Output file for matched articles
SOURCE_NAME = "austrac.gov.au"    # Identifier for this source
CSV_HEADERS = ['date', 'source', 'url', 'title', 'done'] # Output CSV columns
REQUEST_TIMEOUT = 30 # Seconds to wait for the RSS feed request

# --- Functions ---

def load_keywords(filename):
    """
    Loads keywords from the specified file.

    If the file ends with .txt, assumes one keyword/phrase per line.
    Otherwise, falls back to CSV file loading.

    Args:
        filename (str): The path to the keywords file.

    Returns:
        set: A set of unique keywords in lowercase.
    """
    keywords = set()
    try:
        if filename.lower().endswith('.txt'):
            with open(filename, mode='r', encoding='utf-8') as infile:
                for line in infile:
                    keyword = line.strip()
                    if keyword:
                        keywords.add(keyword.lower())
            print(f"Loaded {len(keywords)} keywords/phrases from {filename}")
        else:
            # Fallback to CSV logic (for backwards compatibility)
            with open(filename, mode='r', encoding='utf-8-sig', newline='') as infile:
                reader = csv.DictReader(infile)
                if 'Keyword' not in reader.fieldnames or 'Variants' not in reader.fieldnames:
                    print(f"Error: Missing 'Keyword' or 'Variants' columns in {filename}.")
                    return set()
                for row_number, row in enumerate(reader, start=2):
                    main_keyword = row.get('Keyword', '').strip().lower()
                    if main_keyword:
                        keywords.add(main_keyword)
                    variants_str = row.get('Variants', '').strip()
                    if variants_str:
                        variants = [v.strip().lower() for v in variants_str.split(',') if v.strip()]
                        keywords.update(variants)
            print(f"Loaded {len(keywords)} keywords/variants from {filename}")
        return keywords
    except FileNotFoundError:
        print(f"Error: Keywords file '{filename}' not found. Please create it or check the path.")
        return set()
    except Exception as e:
        print(f"Error reading keywords file '{filename}': {e}")
        return set()

def load_existing_urls(filename, source_filter):
    """
    Loads existing article URLs for a specific source from the articles CSV file.
    Creates the file with headers if it doesn't exist.

    Args:
        filename (str): The path to the articles CSV file.
        source_filter (str): The source name to filter by (e.g., 'austrac.gov.au').

    Returns:
        set: A set of existing URLs for the specified source.
    """
    existing_urls = set()
    file_exists = os.path.exists(filename)

    # Create the file with headers if it doesn't exist
    if not file_exists:
        try:
            with open(filename, mode='w', newline='', encoding='utf-8') as outfile:
                writer = csv.writer(outfile)
                writer.writerow(CSV_HEADERS)
            print(f"Created articles file '{filename}' with headers.")
            return existing_urls # Return empty set as the file was just created
        except IOError as e:
            print(f"Error: Could not create articles file '{filename}': {e}")
            return existing_urls # Return empty set on creation error

    # Read existing URLs if the file exists
    try:
        with open(filename, mode='r', newline='', encoding='utf-8') as infile:
            reader = csv.DictReader(infile)
            # Verify required columns are present in the existing file
            if not all(header in reader.fieldnames for header in ['url', 'source']):
                 print(f"Warning: Existing file '{filename}' is missing 'url' or 'source' column headers. Cannot reliably check for duplicates for source '{source_filter}'.")
                 # Continue cautiously, might add duplicates if headers are wrong
            else:
                for row in reader:
                    # Check if the row has the 'source' key and its value matches the filter
                    # Also check if the 'url' key exists and is not empty
                    if row.get('source') == source_filter and row.get('url'):
                        existing_urls.add(row['url'].strip())

    except FileNotFoundError:
         # This case is handled by the os.path.exists check, but good practice to include
         print(f"Info: Articles file '{filename}' not found, will be created.")
         pass
    except Exception as e:
        print(f"Error reading existing articles from '{filename}': {e}")
        # Decide how to proceed: stop, or continue with potentially incomplete duplicate checking.
        # Let's continue but warn the user.
        print(f"Warning: Proceeding without complete duplicate checking for '{source_filter}' due to read error.")

    print(f"Found {len(existing_urls)} existing URLs for source '{source_filter}' in {filename}")
    return existing_urls

def fetch_and_parse_feed(url, timeout):
    """
    Fetches and parses the RSS feed from the given URL.

    Args:
        url (str): The URL of the RSS feed.
        timeout (int): Request timeout in seconds.

    Returns:
        feedparser.FeedParserDict: The parsed feed object, or None if fetching/parsing fails.
    """
    print(f"Fetching RSS feed from: {url}")
    headers = {'User-Agent': 'Python RSS Collector Script (https://github.com/; contact your-email@example.com)'} # Be polite
    try:
        response = requests.get(url, timeout=timeout, headers=headers)
        response.raise_for_status() # Raise an HTTPError for bad status codes (4xx or 5xx)

        # Decode explicitly using UTF-8, fallback to requests' detection if needed
        try:
            content = response.content.decode('utf-8')
        except UnicodeDecodeError:
            print("Warning: Could not decode feed as UTF-8, using requests' detected encoding.")
            content = response.text # Use requests' decoded text

        feed = feedparser.parse(content)

        # Check for bozo flag which indicates potential parsing issues
        if feed.bozo:
            print(f"Warning: Feed may be ill-formed. Parser issue: {feed.bozo_exception}")
        if not feed.entries:
             print("Warning: Feed parsed successfully, but no entries were found.")
        else:
             print(f"Successfully parsed feed. Found {len(feed.entries)} entries.")
        return feed

    except requests.exceptions.Timeout:
        print(f"Error: Request timed out after {timeout} seconds while fetching feed: {url}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"Error: Failed to fetch RSS feed: {e}")
        return None
    except Exception as e:
        # Catch other potential errors during parsing
        print(f"Error: An unexpected error occurred during feed fetching or parsing: {e}")
        return None

def check_match(entry, keywords):
    """
    Checks if an RSS feed entry's title or summary/description contains any of the keywords.
    Performs case-insensitive substring matching.

    Args:
        entry (feedparser.FeedParserDict): A single entry from the parsed feed.
        keywords (set): A set of lowercase keywords to search for.

    Returns:
        bool: True if a match is found, False otherwise.
    """
    # Get title, default to empty string if missing
    title = entry.get('title', '').lower()

    # Get summary, fall back to description if summary is missing, default to empty string
    summary = entry.get('summary', entry.get('description', '')).lower()

    # Combine title and summary for searching
    content_to_check = title + " " + summary

    # Check if any keyword exists as a substring in the combined content
    for keyword in keywords:
        if keyword in content_to_check:
            # print(f"Debug: Match found! Keyword '{keyword}' in entry: {entry.get('title', 'No Title')}") # Uncomment for debugging matches
            return True # Found a match, no need to check further keywords for this entry
    return False # No keywords matched this entry

def format_date_iso(parsed_date_tuple):
    """
    Converts feedparser's time tuple (assumed UTC/GMT) to an ISO 8601 UTC string.

    Args:
        parsed_date_tuple (time.struct_time): The time tuple from feedparser (e.g., entry.published_parsed).

    Returns:
        str: The date and time in ISO 8601 format (YYYY-MM-DDTHH:MM:SS+00:00),
             or None if the input tuple is invalid.
    """
    if not parsed_date_tuple:
        return None
    try:
        # Convert time tuple to seconds since epoch (assuming it's UTC/GMT)
        epoch_seconds = time.mktime(parsed_date_tuple)
        # Create a naive datetime object from the epoch seconds in UTC
        dt_naive_utc = datetime.fromtimestamp(epoch_seconds, tz=timezone.utc)

        # Format as ISO 8601 string with UTC offset (+00:00)
        # timespec='seconds' ensures HH:MM:SS format
        iso_string = dt_naive_utc.isoformat(timespec='seconds')

        # Ensure the format includes the '+00:00' for UTC explicitly if needed
        # dt_naive_utc.isoformat() sometimes omits it if microseconds are zero.
        # Let's ensure it's always there for consistency.
        if not iso_string.endswith('+00:00'):
             # This might happen if the time tuple conversion somehow lost tz info,
             # but fromtimestamp with tz=timezone.utc should handle it.
             # As a fallback, manually ensure UTC representation:
             dt_aware_utc = datetime.fromtimestamp(epoch_seconds).replace(tzinfo=timezone.utc)
             iso_string = dt_aware_utc.isoformat(timespec='seconds')


        return iso_string
    except (TypeError, ValueError, OverflowError) as e:
        # Catch potential errors during time conversion
        print(f"Warning: Could not convert date tuple {parsed_date_tuple} to ISO format: {e}")
        return None


def main():
    """Main function to orchestrate the feed fetching, filtering, and saving."""
    print("--- Starting AUSTRAC RSS Collector ---")

    # 1. Load Keywords
    keywords = load_keywords(KEYWORDS_CSV)
    if not keywords:
        print("Error: No keywords loaded. Please check 'web3keywords.csv'. Exiting.")
        return # Exit if keywords couldn't be loaded

    # 2. Load Existing URLs for this specific source
    existing_urls = load_existing_urls(ARTICLES_CSV, SOURCE_NAME)

    # 3. Fetch and Parse RSS Feed
    feed = fetch_and_parse_feed(RSS_URL, REQUEST_TIMEOUT)
    if not feed:
        print("Error: Failed to fetch or parse the RSS feed. Exiting.")
        return # Exit if feed fetching failed

    # 4. Process Feed Entries
    new_articles = []
    processed_count = 0
    matched_count = 0
    skipped_duplicate_count = 0
    skipped_error_count = 0

    print("Processing feed entries...")
    MIN_YEAR = 2025
    for entry in feed.entries:
        processed_count += 1

        # --- Basic Validation ---
        url = entry.get('link', '').strip()
        title = entry.get('title', 'No Title Available').strip()
        # Use 'published_parsed' as it's a structured time tuple
        published_parsed = entry.get('published_parsed')

        if not url:
            print(f"Warning: Skipping entry #{processed_count} due to missing URL. Title: '{title}'")
            skipped_error_count += 1
            continue
        if not published_parsed:
             print(f"Warning: Skipping entry #{processed_count} due to missing publication date. URL: <{url}>")
             skipped_error_count += 1
             continue

        # --- Check for Duplicates (based on URL for this source) ---
        if url in existing_urls:
            # print(f"Debug: Skipping already existing article: {url}") # Uncomment for verbose logging
            skipped_duplicate_count += 1
            continue # Skip this entry, it's already in the CSV for this source

        # --- Check for Keyword Match ---
        if check_match(entry, keywords):
            matched_count += 1

            # --- Format Date ---
            iso_date_str = format_date_iso(published_parsed)
            if not iso_date_str:
                 print(f"Warning: Skipping matched article due to date formatting error. URL: <{url}>")
                 skipped_error_count += 1
                 continue # Skip if date couldn't be formatted

            try:
                sort_dt = datetime.fromtimestamp(time.mktime(published_parsed), tz=timezone.utc)
            except Exception:
                sort_dt = datetime.now(timezone.utc)

            # Enforce articles from 2025 onward
            if sort_dt.year < MIN_YEAR:
                continue  # Skip articles before 2025

            # --- Prepare Data for CSV ---
            # Store the original datetime object for sorting purposes
            article_data = {
                'date': iso_date_str,
                'source': SOURCE_NAME,
                'url': url,
                'title': title,
                'done': '',
                '_sort_date': sort_dt # Temporary key for sorting
            }
            new_articles.append(article_data)
            # print(f"Found new matching article: '{title}' ({iso_date_str})") # Debugging log

    print(f"Finished processing {processed_count} entries.")
    print(f" - Skipped {skipped_duplicate_count} entries already present in '{ARTICLES_CSV}' for source '{SOURCE_NAME}'.")
    print(f" - Skipped {skipped_error_count} entries due to missing data or errors.")
    print(f" - Found {len(new_articles)} new articles matching keywords.")

    # 5. Sort and Append New Articles to CSV
    if new_articles:
        # Sort the newly found articles chronologically (oldest first) based on publication date
        new_articles.sort(key=lambda x: x['_sort_date'])
        print(f"Appending {len(new_articles)} new articles to '{ARTICLES_CSV}'...")

        try:
            # Open in append mode ('a')
            with open(ARTICLES_CSV, mode='a', newline='', encoding='utf-8') as outfile:
                # Use DictWriter, ensuring fieldnames match the desired order
                writer = csv.DictWriter(outfile, fieldnames=CSV_HEADERS, extrasaction='ignore')
                # The header is only written when the file is first created by load_existing_urls
                # No need to write header in append mode

                for article in new_articles:
                    # The '_sort_date' key was temporary; DictWriter with extrasaction='ignore'
                    # will automatically skip it if it's not in fieldnames.
                    # Alternatively, explicitly remove it: del article['_sort_date']
                    writer.writerow(article)

            print(f"Successfully appended {len(new_articles)} new articles.")
        except IOError as e:
            print(f"Error: Could not write new articles to '{ARTICLES_CSV}': {e}")
        except Exception as e:
            print(f"Error: An unexpected error occurred while writing to CSV: {e}")
    else:
        print("No new matching articles to add.")

    print("--- Script finished ---")

# --- Run the main function ---
if __name__ == "__main__":
    main()
