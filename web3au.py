import feedparser
import csv
import re
import requests
import os
from dateutil import parser as date_parser # Renamed for clarity
from datetime import timezone # Import timezone
import html  # Add this import

# Configuration
RSS_FEED_URL = 'https://www.web3au.media/feed'
CSV_OUTPUT_FILE = "articles.csv" # Renamed for clarity
SOURCE_IDENTIFIER_NAME = 'www.web3au.media' # Renamed for clarity
CSV_COLUMN_HEADERS = ['date', 'source', 'url', 'title', 'done'] # Renamed for clarity

def clean_html_tags(raw_html_text):
  """Removes HTML tags from a string and decodes HTML entities."""
  if raw_html_text is None: return ""
  clean_regex = re.compile('<.*?>')
  cleaned_text = re.sub(clean_regex, '', raw_html_text)
  # Decode HTML entities
  cleaned_text = html.unescape(cleaned_text)
  return cleaned_text.strip()

def fetch_and_parse_rss_feed(feed_url_to_fetch):
  """Fetches the RSS feed and returns parsed entries."""
  try:
    response = requests.get(feed_url_to_fetch, timeout=20) # Increased timeout
    response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
    feed_data = feedparser.parse(response.content) # Use response.content for feedparser

    if feed_data.bozo:
        print(f"Warning: Feed '{feed_url_to_fetch}' may be malformed: {feed_data.bozo_exception}")
    if not feed_data.entries:
        print(f"No entries found in feed: {feed_url_to_fetch}")
    return feed_data.entries
  except requests.exceptions.RequestException as e_req:
    print(f"Error fetching feed {feed_url_to_fetch}: {e_req}")
  except Exception as e_parse: # Catch other potential errors during parsing
    print(f"Error parsing feed {feed_url_to_fetch}: {e_parse}")
  return None


def get_existing_urls_from_csv(csv_filename):
    """Reads existing URLs from the CSV file for the current source."""
    existing_urls_set = set()
    if not os.path.exists(csv_filename) or os.path.getsize(csv_filename) == 0:
        # Create CSV with header if it doesn't exist or is empty
        try:
            with open(csv_filename, mode='w', newline='', encoding='utf-8') as init_file:
                writer = csv.DictWriter(init_file, fieldnames=CSV_COLUMN_HEADERS)
                writer.writeheader()
            print(f"Initialized CSV file '{csv_filename}' with headers.")
        except IOError as e_io_init:
            print(f"Error initializing CSV file '{csv_filename}': {e_io_init}")
        return existing_urls_set

    try:
        with open(csv_filename, mode='r', newline='', encoding='utf-8') as file_handle:
            reader = csv.DictReader(file_handle)
            if not reader.fieldnames or not all(h in reader.fieldnames for h in ['url', 'source']):
                print(f"Warning: CSV '{csv_filename}' missing 'url' or 'source' headers.")
                return existing_urls_set # Cannot reliably read
            for row_data in reader:
                # Only consider URLs from the current source
                if row_data.get('source') == SOURCE_IDENTIFIER_NAME and row_data.get('url'):
                    existing_urls_set.add(row_data['url'])
    except Exception as e_read_csv:
        print(f"Error reading existing CSV file '{csv_filename}': {e_read_csv}")
    return existing_urls_set


def save_new_articles_to_csv(articles_list_to_save, csv_filename):
  """Saves new parsed article data to the CSV file."""
  if not articles_list_to_save:
      print("No new articles found to save for Web3AU.")
      return

  # File existence and header are handled by get_existing_urls_from_csv
  # Open in append mode.
  try:
      with open(csv_filename, mode='a', newline='', encoding='utf-8') as file_handle:
        writer = csv.DictWriter(file_handle, fieldnames=CSV_COLUMN_HEADERS)
        # Header should have been written by get_existing_urls_from_csv if file was new/empty
        
        appended_count = 0
        for article_dict_data in articles_list_to_save:
            # Ensure only defined columns are written
            row_for_csv = {col: article_dict_data.get(col, '') for col in CSV_COLUMN_HEADERS}
            writer.writerow(row_for_csv)
            appended_count += 1
        print(f"Successfully added {appended_count} new articles for '{SOURCE_IDENTIFIER_NAME}' to '{csv_filename}'")
  except IOError as e_io_write:
      print(f"Error writing to CSV file '{csv_filename}' for Web3AU: {e_io_write}")
  except Exception as e_csv_w:
      print(f"Unexpected error writing Web3AU articles to CSV: {e_csv_w}")


if __name__ == "__main__":
  print(f"--- Starting Web3AU Media Scraper ({SOURCE_IDENTIFIER_NAME}, Date Format UTC) ---")
  
  rss_entries = fetch_and_parse_rss_feed(RSS_FEED_URL)
  if not rss_entries:
    print(f"Failed to retrieve or parse articles from {RSS_FEED_URL}. Exiting.")
    exit()

  existing_article_urls = get_existing_urls_from_csv(CSV_OUTPUT_FILE)
  print(f"Found {len(existing_article_urls)} existing URLs for '{SOURCE_IDENTIFIER_NAME}'.")
  
  new_web3au_articles = []
  MIN_YEAR = 2025

  for entry in rss_entries:
      article_url = getattr(entry, 'link', None)
      if not article_url or article_url in existing_article_urls:
          continue

      published_date_str = getattr(entry, 'published', None)
      dt_obj_utc = None
      if published_date_str:
          try:
              # date_parser.parse is flexible with formats
              parsed_dt_naive_or_aware = date_parser.parse(published_date_str)
              # Ensure it's UTC
              if parsed_dt_naive_or_aware.tzinfo is None: # If naive
                  dt_obj_utc = parsed_dt_naive_or_aware.replace(tzinfo=timezone.utc) # Assume UTC
              else: # If already timezone-aware
                  dt_obj_utc = parsed_dt_naive_or_aware.astimezone(timezone.utc) # Convert to UTC
          except (ValueError, date_parser.ParserError, TypeError) as e_date:
              print(f"Warning: Could not parse date '{published_date_str}' for '{article_url}'. Error: {e_date}. Using current UTC time.")
              dt_obj_utc = datetime.now(timezone.utc) # Fallback
      else: # Fallback if no published date string
          print(f"Warning: No published date for '{article_url}'. Using current UTC time.")
          dt_obj_utc = datetime.now(timezone.utc)


      if dt_obj_utc.year < MIN_YEAR:
          # print(f"Debug: Skipping article from {dt_obj_utc.year}: {article_url}")
          existing_article_urls.add(article_url) # Add old ones to seen to prevent re-check
          continue
      
      # Format to YYYY-MM-DDTHH:MM:SS+00:00
      iso_date_utc_formatted = dt_obj_utc.strftime('%Y-%m-%dT%H:%M:%S+00:00')

      title_raw = getattr(entry, 'title', '')
      summary_raw = getattr(entry, 'summary', '') # 'summary' often contains HTML

      cleaned_title_text = clean_html_tags(title_raw)
      cleaned_summary_text = clean_html_tags(summary_raw)

      # Combine title and summary if both exist, otherwise use what's available
      combined_display_title = cleaned_title_text
      if cleaned_summary_text:
          combined_display_title = f"{cleaned_title_text} - {cleaned_summary_text}" if cleaned_title_text else cleaned_summary_text
      if not combined_display_title: combined_display_title = "No Title or Summary"
      
      new_web3au_articles.append({
          'date': iso_date_utc_formatted,
          'source': SOURCE_IDENTIFIER_NAME,
          'url': article_url,
          'title': combined_display_title,
          'done': '',
          '_sort_date_obj': dt_obj_utc # For sorting
      })
      existing_article_urls.add(article_url) # Add to seen for current run

  if new_web3au_articles:
      # Sort new articles by their datetime object (oldest first)
      new_web3au_articles.sort(key=lambda x: x['_sort_date_obj'])
      
      # Prepare for CSV by removing the temporary sort key
      articles_for_csv_output = []
      for item_dict in new_web3au_articles:
          del item_dict['_sort_date_obj']
          articles_for_csv_output.append(item_dict)
          
      save_new_articles_to_csv(articles_for_csv_output, CSV_OUTPUT_FILE)
  else:
      print(f"No new articles from '{SOURCE_IDENTIFIER_NAME}' (>= {MIN_YEAR}) to add.")
      
  print(f"--- Web3AU Media Scraper Finished ---")
