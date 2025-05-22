#!/usr/bin/env python3
"""
Scrapes DFRC CBDC Acacia project updates and media releases.
Dates are stored in ISO 8601 UTC format: YYYY-MM-DDTHH:MM:SS+00:00.
"""

import requests
from bs4 import BeautifulSoup
import os
import csv
import re
from dateutil import parser as dateparser
import datetime # Keep standard datetime
from urllib.parse import urljoin, urlparse
import unicodedata
from datetime import timezone # Import timezone

def clean_text(text):
    """Cleans and normalizes text for consistency."""
    if not text: return ""
    try:
        # Normalize Unicode characters to their canonical forms
        normalized_text = unicodedata.normalize('NFKC', text)
    except TypeError:
        # Fallback if text is not a string (e.g., already a number or None)
        normalized_text = str(text)
    # Replace common typographic variants with standard ASCII equivalents
    replacements = {'’': "'", '‘': "'", '”': '"', '“': '"', '–': '-', '—': '-'}
    for char, replacement in replacements.items():
        normalized_text = normalized_text.replace(char, replacement)
    # Remove non-printable characters, allowing newline and tab
    cleaned_text = ''.join(char for char in normalized_text if char.isprintable() or char in '\n\t')
    # Replace multiple whitespace characters with a single space and strip leading/trailing whitespace
    return re.sub(r'\s+', ' ', cleaned_text).strip()

def ensure_csv_header():
    """Ensures the CSV file exists and has the correct headers. Creates it if not."""
    if not os.path.exists(CSV_FILE) or os.path.getsize(CSV_FILE) == 0:
        with open(CSV_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
            writer.writeheader()
        print(f"Initialized CSV file '{CSV_FILE}' with headers.")

def load_seen_data():
    """
    Loads seen URLs and (title, date_str) tuples for duplicate checking.
    Only considers items from DFRC sources relevant to this script.
    """
    seen_urls = set()
    seen_title_date_strs = set() # Store date as original ISO string from CSV for exact match

    if not os.path.exists(CSV_FILE) or os.path.getsize(CSV_FILE) == 0:
        return seen_urls, seen_title_date_strs

    # Get the processed source paths for accurate comparison
    source_path_project = get_source_path(PROJECT_URL)
    source_path_media = get_source_path(MEDIA_RELEASES_URL)

    try:
        with open(CSV_FILE, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames or not all(h in reader.fieldnames for h in ['url', 'title', 'date', 'source']):
                print(f"Warning: CSV '{CSV_FILE}' missing required headers (url, title, date, source). Skipping loading seen data for DFRC.")
                return seen_urls, seen_title_date_strs
            for row in reader:
                current_row_source = row.get('source')
                # Only consider items from DFRC sources for this script's duplicate check
                if current_row_source == source_path_project or current_row_source == source_path_media:
                    if row.get('url') and row['url'].strip():
                        seen_urls.add(row['url'])
                    # For items without a URL, or as an additional check
                    if row.get('title') and row.get('date'): # Ensure date exists for the tuple
                        seen_title_date_strs.add((clean_text(row['title']), row['date']))
    except Exception as e:
        print(f"Error loading seen data from {CSV_FILE}: {e}")
    return seen_urls, seen_title_date_strs


def append_to_csv(articles_list):
    """Appends a list of article dictionaries to the CSV file."""
    if not articles_list: return
    # Header is ensured by ensure_csv_header() before this function is typically called if file is new
    try:
        with open(CSV_FILE, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
            for article_dict in articles_list:
                 # Ensure only columns in CSV_HEADERS are written, and in correct order
                row_to_write = {col: article_dict.get(col, '') for col in CSV_HEADERS}
                writer.writerow(row_to_write)
        print(f"Appended {len(articles_list)} new items to '{CSV_FILE}'.")
    except Exception as e:
        print(f"Error appending to CSV: {e}")


def get_source_path(url):
    """Extracts the 'netloc/path' part of a URL to use as a consistent source identifier."""
    parsed = urlparse(url)
    # Remove trailing slash from path for consistency if it's not the root path
    path = parsed.path
    if path != '/' and path.endswith('/'):
        path = path[:-1]
    return parsed.netloc + path

# --- Configuration ---
PROJECT_URL = 'https://dfcrc.com.au/projects-cbdc-acacia/'
MEDIA_RELEASES_URL = 'https://dfcrc.com.au/news/media-releases/'
CSV_FILE = 'articles.csv' # The shared CSV file
STOP_HEADING_TEXT = "Previous IAG Material" # Stop scraping project updates when this heading is found
CSV_HEADERS = ['date', 'source', 'url', 'title', 'done'] # Define the order and names of CSV columns

def fetch_project_updates():
    """Fetches and parses project updates from the DFRC Acacia project page."""
    print(f"Fetching project updates from {PROJECT_URL}...")
    items_for_csv = []
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (DFCRC Scraper; +http://example.com/botinfo)'} # Added a more descriptive User-Agent
        resp = requests.get(PROJECT_URL, timeout=30, headers=headers)
        resp.raise_for_status() # Raises HTTPError for bad responses (4XX or 5XX)
    except requests.exceptions.RequestException as e:
        print(f"Error fetching {PROJECT_URL}: {e}")
        return items_for_csv

    soup = BeautifulSoup(resp.content, 'html.parser')
    all_h3_tags = soup.find_all('h3')

    for h3_tag in all_h3_tags:
        h3_text_cleaned = clean_text(h3_tag.get_text(strip=True))
        if h3_text_cleaned == STOP_HEADING_TEXT:
            print(f"Reached stop heading: '{STOP_HEADING_TEXT}'. No more project updates will be processed from this page.")
            break # Stop processing further h3 tags

        # Heuristic to identify relevant project update sections
        if "Meeting" in h3_text_cleaned or "Update" in h3_text_cleaned or "Summary" in h3_text_cleaned:
            current_update_title_cleaned = h3_text_cleaned
            # Attempt to extract date from the h3 title itself
            date_match = re.search(r'(\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{4})', current_update_title_cleaned, re.IGNORECASE)
            raw_date_str_from_title = date_match.group(1) if date_match else None

            parsed_date_obj_utc = None
            if raw_date_str_from_title:
                try:
                    # Parse the date string; dateparser handles various formats
                    parsed_dt_naive = dateparser.parse(raw_date_str_from_title)
                    # Assume the parsed date is local and convert to UTC.
                    # For DFRC (Australia), if timezone is critical and known, specify it.
                    # For simplicity, if date only, it's treated as start of day.
                    parsed_date_obj_utc = parsed_dt_naive.replace(tzinfo=timezone.utc) # Ensure UTC
                except (dateparser.ParserError, ValueError) as e_date:
                    print(f"  - Could not parse date from title component '{raw_date_str_from_title}' for '{current_update_title_cleaned}'. Error: {e_date}")
            # else: # No date in title, will rely on PDF link or use current time as last resort
                # print(f"  - No clear date in title for '{current_update_title_cleaned}'.")

            pdf_link_found = None
            pdf_title_text = None # Initialize pdf_title_text
            # Look for a 'ul' sibling that might contain PDF links
            ul_sibling = h3_tag.find_next_sibling('ul')
            if ul_sibling:
                for li in ul_sibling.find_all('li'):
                    a_tag = li.find('a', href=lambda href: href and href.lower().endswith('.pdf'))
                    if a_tag:
                        pdf_link_found = urljoin(PROJECT_URL, a_tag['href']) # Make URL absolute
                        pdf_title_text = clean_text(a_tag.get_text(strip=True))
                        # If the main title was generic like "Update", use PDF title for more specificity
                        if "Update" == current_update_title_cleaned and pdf_title_text:
                            current_update_title_cleaned = f"Update: {pdf_title_text}"
                        elif "Summary" == current_update_title_cleaned and pdf_title_text:
                             current_update_title_cleaned = f"Summary: {pdf_title_text}"
                        break # Found a PDF, assume it's the primary one for this section

            # If no date was parsed from title and no PDF link, it's hard to date this item.
            # We will use current UTC time as a fallback if a PDF is found but no date in title.
            if not parsed_date_obj_utc and pdf_link_found: # Only if PDF exists but title had no date
                 print(f"  - No date in title for '{current_update_title_cleaned}' with PDF. Using current UTC as fallback.")
                 parsed_date_obj_utc = datetime.datetime.now(timezone.utc)
            elif not parsed_date_obj_utc and not pdf_link_found and ("Meeting" in current_update_title_cleaned or "Update" in current_update_title_cleaned):
                 print(f"  - No date in title for '{current_update_title_cleaned}' and no PDF. Using current UTC as fallback for this entry.")
                 parsed_date_obj_utc = datetime.datetime.now(timezone.utc)


            # Only add if we have a date (parsed or fallback) AND either a PDF or it's a "Meeting" type
            if parsed_date_obj_utc and (pdf_link_found or "Meeting" in current_update_title_cleaned):
                iso_date_utc_str = parsed_date_obj_utc.strftime('%Y-%m-%dT%H:%M:%S+00:00')
                
                # Construct a meaningful title for the CSV
                csv_title = f"DFCRC Acacia: {current_update_title_cleaned}"
                # If there's a PDF and the title isn't already specific (like a meeting title),
                # and it's not an "Update: PDF title" type, use the PDF name.
                if pdf_link_found and not ("Meeting" in current_update_title_cleaned or "Update: " in current_update_title_cleaned or "Summary: " in current_update_title_cleaned):
                    pdf_name = os.path.basename(urlparse(pdf_link_found).path) # Get PDF filename
                    csv_title = f"DFCRC Acacia PDF: {pdf_name}"
                elif not pdf_link_found and "Meeting" in current_update_title_cleaned: # Ensure meeting titles are clear
                    csv_title = f"DFCRC Acacia: {current_update_title_cleaned}"


                items_for_csv.append({
                    'parsed_date_obj_utc': parsed_date_obj_utc, # For sorting, will be removed before CSV write
                    'date': iso_date_utc_str,
                    'source': get_source_path(PROJECT_URL),
                    'url': pdf_link_found if pdf_link_found else PROJECT_URL, # Use project URL if no specific PDF
                    'title': csv_title,
                    'done': '' # Default 'done' status
                })
            # else:
                # print(f"  - Skipping '{current_update_title_cleaned}' due to missing date or insufficient info (no PDF for non-meeting).")

    return items_for_csv

def fetch_media_releases():
    """Fetches and parses media releases from the DFRC news page."""
    print(f"\nFetching media releases from {MEDIA_RELEASES_URL}...")
    items_for_csv = []
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (DFCRC Scraper; +http://example.com/botinfo)'}
        resp = requests.get(MEDIA_RELEASES_URL, timeout=30, headers=headers)
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching {MEDIA_RELEASES_URL}: {e}")
        return items_for_csv

    soup = BeautifulSoup(resp.content, 'html.parser')
    # Find all elements with class "latest_post" which seems to wrap each media release
    latest_posts_elements = soup.find_all(class_="latest_post")

    for post_element in latest_posts_elements:
        date_tag_element = post_element.find(class_="date entry_date updated") # Standard class for date
        link_tag_element = post_element.find(class_="latest_post_title", name='a') # Title is usually a link
        
        # Fallback if the link is nested differently
        if not link_tag_element:
            title_container = post_element.find(class_="latest_post_title")
            if title_container:
                link_tag_element = title_container.find('a')

        parsed_date_obj_utc = None
        iso_date_utc_str = ""
        article_url_val = None
        article_title_cleaned = "N/A" # Default title if not found

        if date_tag_element:
            raw_date_str = date_tag_element.get_text(strip=True)
            try:
                parsed_dt_naive = dateparser.parse(raw_date_str)
                parsed_date_obj_utc = parsed_dt_naive.replace(tzinfo=timezone.utc) # Ensure UTC
                iso_date_utc_str = parsed_date_obj_utc.strftime('%Y-%m-%dT%H:%M:%S+00:00')
            except (dateparser.ParserError, ValueError) as e_date:
                print(f"  - Could not parse media release date '{raw_date_str}'. Error: {e_date}. Skipping this item.")
                continue # Skip this item if date parsing fails

        if link_tag_element and link_tag_element.has_attr('href'):
            article_url_val = urljoin(MEDIA_RELEASES_URL, link_tag_element['href']) # Make URL absolute
            article_title_cleaned = clean_text(link_tag_element.get_text(strip=True))
        else:
            print(f"  - Could not find a valid link or title for a media release item. Skipping.")
            continue # Skip if no link/title

        # Only add if we successfully parsed a date, URL, and title
        if article_url_val and article_title_cleaned != "N/A" and iso_date_utc_str:
            items_for_csv.append({
                'parsed_date_obj_utc': parsed_date_obj_utc, # For sorting
                'date': iso_date_utc_str,
                'source': get_source_path(MEDIA_RELEASES_URL),
                'url': article_url_val,
                'title': article_title_cleaned,
                'done': ''
            })
    return items_for_csv

def main():
    """Main function to orchestrate the scraping and CSV updating process."""
    print("--- Starting DFRC Scraper (Project Updates & Media Releases, Date Format UTC) ---")
    ensure_csv_header() # Make sure CSV file and headers are ready
    seen_urls, seen_title_date_strs = load_seen_data()
    print(f"Loaded {len(seen_urls)} seen URLs and {len(seen_title_date_strs)} seen (title, date_str) combos for DFRC sources from '{CSV_FILE}'.")

    all_new_items_to_add = []
    
    # Fetch and filter project updates
    project_updates_data = fetch_project_updates()
    print(f"\nFetched {len(project_updates_data)} potential project updates.")
    new_project_updates_count = 0
    for item_data in project_updates_data:
        is_duplicate = False
        # Check by URL if available and not empty
        if item_data.get('url') and item_data['url'].strip() and item_data['url'] in seen_urls:
            is_duplicate = True
        # If no URL, or as fallback, check by cleaned title and ISO date string
        # Ensure title and date are present for this check
        elif item_data.get('title') and item_data.get('date') and \
             (item_data['title'], item_data['date']) in seen_title_date_strs:
            is_duplicate = True
        
        if not is_duplicate:
            all_new_items_to_add.append(item_data)
            # Add to current run's seen set to prevent duplicates from within this scrape session
            if item_data.get('url') and item_data['url'].strip():
                seen_urls.add(item_data['url'])
            if item_data.get('title') and item_data.get('date'):
                 seen_title_date_strs.add((item_data['title'], item_data['date']))
            new_project_updates_count +=1
        # else:
            # print(f"Skipping duplicate project update: \"{item_data['title']}\" ({item_data.get('url', 'No URL')})")
    if new_project_updates_count > 0:
        print(f"Found {new_project_updates_count} new project updates.")


    # Fetch and filter media releases
    media_releases_data = fetch_media_releases()
    print(f"\nFetched {len(media_releases_data)} potential media releases.")
    new_media_releases_count = 0
    for item_data in media_releases_data:
        is_duplicate = False
        if item_data.get('url') and item_data['url'].strip() and item_data['url'] in seen_urls:
            is_duplicate = True
        elif item_data.get('title') and item_data.get('date') and \
             (item_data['title'], item_data['date']) in seen_title_date_strs:
            is_duplicate = True

        if not is_duplicate:
            all_new_items_to_add.append(item_data)
            # Add to current run's seen set
            if item_data.get('url') and item_data['url'].strip():
                seen_urls.add(item_data['url'])
            if item_data.get('title') and item_data.get('date'):
                seen_title_date_strs.add((item_data['title'], item_data['date']))
            new_media_releases_count += 1
        # else:
            # print(f"Skipping duplicate media release: \"{item_data['title']}\" ({item_data.get('url', 'No URL')})")
    if new_media_releases_count > 0:
        print(f"Found {new_media_releases_count} new media releases.")


    if not all_new_items_to_add:
        print("\nNo new DFRC updates or releases to add to CSV.")
    else:
        # Sort all new items by their datetime object (oldest first)
        # Fallback to a very old date if 'parsed_date_obj_utc' is somehow missing (should not happen with current logic)
        all_new_items_to_add.sort(key=lambda x: x.get('parsed_date_obj_utc', datetime.datetime.min.replace(tzinfo=timezone.utc)))
        
        # Prepare for CSV: remove the temporary 'parsed_date_obj_utc' key as it's not in CSV_HEADERS
        items_for_csv_final = []
        for item in all_new_items_to_add:
            item_copy = item.copy()
            item_copy.pop('parsed_date_obj_utc', None) # Safely remove the key
            items_for_csv_final.append(item_copy)
        
        print(f"\nFound {len(items_for_csv_final)} new DFRC items in total. Appending to '{CSV_FILE}'...")
        append_to_csv(items_for_csv_final)

    print(f"--- DFRC Scraper Finished ---")

if __name__ == '__main__':
    main()
