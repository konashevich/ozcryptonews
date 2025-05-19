#!/usr/bin/env python3
"""
Scrapes project updates from the DFRC CBDC Acacia project page and
media releases from the DFRC news page.
Parses meeting updates and news articles, extracts details, cleans text, 
and saves to a CSV file with the structure: date,source,url,title,done.
Stops parsing project updates when it encounters the "Previous IAG Material" section.
"""

import requests
from bs4 import BeautifulSoup
import os
import csv
import re
from dateutil import parser as dateparser
import datetime
from urllib.parse import urljoin
import unicodedata # Added for Unicode normalization

# --- Configuration ---
PROJECT_URL = 'https://dfcrc.com.au/projects-cbdc-acacia/'
MEDIA_RELEASES_URL = 'https://dfcrc.com.au/news/media-releases/'
CSV_FILE = 'articles.csv'
STOP_HEADING_TEXT = "Previous IAG Material" # For project updates page

# --- Text Cleaning Function ---
def clean_text(text):
    """
    Cleans text by normalizing Unicode, replacing specific problematic characters,
    and removing non-printable characters.
    """
    if not text:
        return ""
    
    # Normalize Unicode to NFKC form (Compatibility Composition)
    # This can help with various Unicode quirks, e.g., converting non-breaking spaces to regular spaces.
    try:
        normalized_text = unicodedata.normalize('NFKC', text)
    except TypeError: # Handle if text is not a string (though it should be)
        normalized_text = str(text)

    # Replace specific typographic punctuation with ASCII equivalents
    replacements = {
        '’': "'",  # Right single quotation mark
        '‘': "'",  # Left single quotation mark
        '”': '"',  # Right double quotation mark
        '“': '"',  # Left double quotation mark
        '–': '-',  # En dash
        '—': '-',  # Em dash
        # Add more replacements if needed
    }
    for char, replacement in replacements.items():
        normalized_text = normalized_text.replace(char, replacement)
    
    # Remove non-printable characters (except for common whitespace like space, tab, newline)
    # This helps remove invisible characters.
    # Allow newline and tab for potential multi-line text if desired, though titles are usually single line.
    cleaned_text = ''.join(char for char in normalized_text if char.isprintable() or char in '\n\t')
    
    # Replace multiple spaces with a single space and strip leading/trailing whitespace
    cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip()
    
    return cleaned_text

# --- CSV Functions ---

def ensure_csv_header():
    """Create CSV file with header if it doesn't yet exist."""
    fieldnames = ['date', 'source', 'url', 'title', 'done']
    if not os.path.exists(CSV_FILE):
        with open(CSV_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
        print(f"CSV file '{CSV_FILE}' created with headers: {', '.join(fieldnames)}")

def load_seen_article_urls():
    """
    Read existing CSV and return a set of article URLs (from 'url' column) already recorded.
    This helps in avoiding duplicate entries from both project updates and media releases.
    """
    seen_urls = set()
    if not os.path.exists(CSV_FILE):
        return seen_urls
    try:
        with open(CSV_FILE, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            if reader.fieldnames and 'url' in reader.fieldnames:
                for row in reader:
                    if row.get('url') and row['url'].strip(): 
                        seen_urls.add(row['url'])
            elif not reader.fieldnames:
                print(f"Warning: CSV file '{CSV_FILE}' appears to be empty (no headers found).")
            else: 
                print(f"Warning: CSV file '{CSV_FILE}' header is missing 'url' column.")
    except FileNotFoundError:
        print(f"Info: CSV file '{CSV_FILE}' not found. Will be created.")
        pass 
    except Exception as e:
        print(f"Error loading seen article URLs from {CSV_FILE}: {e}")
    return seen_urls


def append_to_csv(iso_date, source_url, item_url, item_title):
    """Append a row to the CSV file."""
    fieldnames = ['date', 'source', 'url', 'title', 'done']
    try:
        with open(CSV_FILE, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writerow({
                'date': iso_date,
                'source': source_url,
                'url': item_url or '', 
                'title': item_title, # Already cleaned before this function is called
                'done': '' 
            })
    except Exception as e:
        print(f"Error appending to CSV: {e}")

# --- Web Scraping and Parsing Functions ---

def fetch_project_updates():
    """
    Fetches the DFRC CBDC Acacia project page, parses meeting updates.
    Returns a list of dictionaries, each formatted for CSV writing.
    """
    print(f"Fetching project updates from {PROJECT_URL}...")
    items_for_csv = []
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        resp = requests.get(PROJECT_URL, timeout=30, headers=headers)
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching URL {PROJECT_URL}: {e}")
        return items_for_csv

    soup = BeautifulSoup(resp.content, 'html.parser')
    all_h3_tags = soup.find_all('h3')
    print(f"Found {len(all_h3_tags)} h3 tags on the project page.")

    for h3_tag in all_h3_tags:
        h3_text_raw = h3_tag.get_text(strip=True) 
        h3_text_cleaned = clean_text(h3_text_raw) # Clean the h3 text

        if h3_text_cleaned == STOP_HEADING_TEXT: # Compare with cleaned stop heading
            print(f"Reached stop heading: '{STOP_HEADING_TEXT}'. Stopping parse for further project updates.")
            break

        strong_tag_in_h3 = h3_tag.find('strong')
        candidate_title_text_for_meeting_check_raw = h3_text_raw
        if strong_tag_in_h3:
            candidate_title_text_for_meeting_check_raw = strong_tag_in_h3.get_text(strip=True)
        
        candidate_title_text_for_meeting_check_cleaned = clean_text(candidate_title_text_for_meeting_check_raw)

        if "Meeting" in candidate_title_text_for_meeting_check_cleaned: # Check cleaned text
            current_update_h3_title = h3_text_cleaned # Use cleaned h3 text
            print(f"Processing potential project update: \"{current_update_h3_title}\"")

            raw_date_str = ""
            # Extract date from the cleaned candidate text
            if "Meeting" in candidate_title_text_for_meeting_check_cleaned:
                raw_date_str = re.sub(r'Meeting\s*', '', candidate_title_text_for_meeting_check_cleaned, flags=re.IGNORECASE).strip()
            elif "Meeting" in current_update_h3_title: # Fallback to cleaned h3 title
                raw_date_str = re.sub(r'Meeting\s*', '', current_update_h3_title, flags=re.IGNORECASE).strip()


            parsed_date_obj = datetime.datetime.min 
            parsed_date_iso = ""
            if raw_date_str:
                try:
                    date_to_parse = raw_date_str.split(" and ")[0].strip()
                    date_to_parse_cleaned = re.sub(r'(\d+)(st|nd|rd|th)', r'\1', date_to_parse, flags=re.IGNORECASE)
                    parsed_date_obj = dateparser.parse(date_to_parse_cleaned)
                    parsed_date_iso = parsed_date_obj.isoformat() 
                except (dateparser.ParserError, ValueError) as e:
                    print(f"  - Could not parse date from '{raw_date_str}' for project update '{current_update_h3_title}'. Error: {e}")
            else:
                print(f"  - No date string extracted for project update '{current_update_h3_title}'.")

            pdf_link_found = None
            for sibling in h3_tag.find_next_siblings():
                if sibling.name == 'h3':
                    break
                if sibling.name == 'ul':
                    for li in sibling.find_all('li'):
                        a_tag = li.find('a', href=True)
                        if a_tag and a_tag['href'].lower().endswith('.pdf'):
                            pdf_url = a_tag['href']
                            pdf_link_found = urljoin(PROJECT_URL, pdf_url) 
                            print(f"  - Found PDF: {pdf_link_found}")
            
            # Use the cleaned h3 title for the CSV
            combined_csv_title = f"DFCRC Acacia {current_update_h3_title}"

            items_for_csv.append({
                'parsed_date_obj': parsed_date_obj, 
                'csv_date': parsed_date_iso,
                'csv_source': PROJECT_URL,
                'csv_url': pdf_link_found, 
                'csv_title': combined_csv_title, # This title is now cleaned
            })
            print(f"  --- Collected project update for CSV: Title='{combined_csv_title}', Date='{parsed_date_iso or 'N/A'}', PDF URL='{pdf_link_found or 'N/A'}'")
    return items_for_csv

def fetch_media_releases():
    """
    Fetches the DFRC media releases page, parses news articles.
    Returns a list of dictionaries, each formatted for CSV writing.
    """
    print(f"Fetching media releases from {MEDIA_RELEASES_URL}...")
    items_for_csv = []
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        resp = requests.get(MEDIA_RELEASES_URL, timeout=30, headers=headers)
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching URL {MEDIA_RELEASES_URL}: {e}")
        return items_for_csv

    soup = BeautifulSoup(resp.content, 'html.parser')
    latest_posts = soup.find_all(class_="latest_post") 
    print(f"Found {len(latest_posts)} 'latest_post' items on the media releases page.")

    for post in latest_posts:
        date_tag = post.find(class_="date entry_date updated")
        link_tag = post.find(class_="latest_post_title entry_title") 

        raw_date_str = ""
        parsed_date_obj = datetime.datetime.min
        parsed_date_iso = ""
        article_url = None
        article_title_raw = "N/A"
        article_title_cleaned = "N/A"


        if date_tag:
            raw_date_str = date_tag.get_text(strip=True) # Date usually doesn't need extensive cleaning
            try:
                parsed_date_obj = dateparser.parse(raw_date_str)
                parsed_date_iso = parsed_date_obj.isoformat()
            except (dateparser.ParserError, ValueError) as e:
                print(f"  - Could not parse date from '{raw_date_str}' for a media release. Error: {e}")
        else:
            print("  - Date tag not found for a media release item.")

        if link_tag and link_tag.name == 'a' and link_tag.has_attr('href'):
            article_url = urljoin(MEDIA_RELEASES_URL, link_tag['href']) 
            article_title_raw = link_tag.get_text(strip=True)
        else:
            if link_tag: 
                actual_a_tag = link_tag.find('a')
                if actual_a_tag and actual_a_tag.has_attr('href'):
                    article_url = urljoin(MEDIA_RELEASES_URL, actual_a_tag['href'])
                    article_title_raw = actual_a_tag.get_text(strip=True)
                else:
                    print("  - Link tag or href not found for a media release item's title.")
            else:
                 print("  - 'latest_post_title' element not found for a media release item.")
        
        article_title_cleaned = clean_text(article_title_raw) # Clean the extracted title

        print(f"Processing media release: Title='{article_title_cleaned}', Date='{raw_date_str}'")
        
        items_for_csv.append({
            'parsed_date_obj': parsed_date_obj,
            'csv_date': parsed_date_iso,
            'csv_source': MEDIA_RELEASES_URL,
            'csv_url': article_url,
            'csv_title': article_title_cleaned, # Use cleaned title for CSV
        })
        print(f"  --- Collected media release for CSV: Title='{article_title_cleaned}', Date='{parsed_date_iso or 'N/A'}', URL='{article_url or 'N/A'}'")
    return items_for_csv

# --- Main Execution ---

def main():
    """Main function to orchestrate the scraping from both sources and CSV writing."""
    print("Starting DFRC project update and media release scraper...")
    ensure_csv_header()
    seen_article_urls = load_seen_article_urls() 
    print(f"Loaded {len(seen_article_urls)} seen article URLs (from 'url' column) from '{CSV_FILE}'.")

    all_new_items_for_csv = []
    
    # Prepare a set of (title, date) tuples from existing CSV for duplicate checking of URL-less items
    # This is done once to avoid re-reading CSV in the loop
    existing_title_dates = set()
    if os.path.exists(CSV_FILE) and os.path.getsize(CSV_FILE) > 50 : # Check if CSV has content
        try:
            with open(CSV_FILE, 'r', newline='', encoding='utf-8') as f_read:
                reader = csv.DictReader(f_read)
                if reader.fieldnames and 'title' in reader.fieldnames and 'date' in reader.fieldnames:
                    for row in reader:
                        existing_title_dates.add((row['title'], row['date']))
        except Exception as e:
            print(f"Error reading existing CSV for duplicate check: {e}")


    # Fetch and filter project updates
    project_updates = fetch_project_updates()
    print(f"\nFetched {len(project_updates)} potential project updates.")
    for item in project_updates:
        # item['csv_title'] is already cleaned
        is_duplicate = False
        if item['csv_url']:
            if item['csv_url'] in seen_article_urls:
                is_duplicate = True
                print(f"Skipping already seen project update (by URL): \"{item['csv_title']}\"")
        elif (item['csv_title'], item['csv_date']) in existing_title_dates:
            is_duplicate = True
            print(f"Skipping already seen project update (by title and date, no URL): \"{item['csv_title']}\"")
        
        if not is_duplicate:
            all_new_items_for_csv.append(item)
            if item['csv_url']: 
                 seen_article_urls.add(item['csv_url']) # Add to current run's seen URLs
            # Add to current run's seen title/dates as well to prevent adding from media_releases if it's a duplicate
            existing_title_dates.add((item['csv_title'], item['csv_date']))


    # Fetch and filter media releases
    media_releases = fetch_media_releases()
    print(f"\nFetched {len(media_releases)} potential media releases.")
    for item in media_releases:
        # item['csv_title'] is already cleaned
        is_duplicate = False
        if item['csv_url']:
            if item['csv_url'] in seen_article_urls:
                is_duplicate = True
                print(f"Skipping already seen media release (by URL): \"{item['csv_title']}\"")
        elif (item['csv_title'], item['csv_date']) in existing_title_dates:
            is_duplicate = True
            print(f"Skipping already seen media release (by title and date, no URL): \"{item['csv_title']}\"")

        if not is_duplicate:
            all_new_items_for_csv.append(item)
            # No need to add to seen_article_urls or existing_title_dates here again,
            # as all_new_items_for_csv is the final list for appending.
            # However, if an item was added from project_updates without a URL,
            # and media_releases has the same title/date WITH a URL, this logic is fine.

    # Sort all new items from both sources by date
    all_new_items_for_csv.sort(key=lambda x: x['parsed_date_obj'])
    
    new_updates_count = 0
    if not all_new_items_for_csv:
        print("\nNo new updates or releases to add to the CSV.")
    else:
        print(f"\nFound {len(all_new_items_for_csv)} new items in total to add after filtering. Appending to CSV...")
        for item_to_add in all_new_items_for_csv: # Renamed to avoid conflict
            append_to_csv(
                item_to_add['csv_date'],
                item_to_add['csv_source'],
                item_to_add['csv_url'],
                item_to_add['csv_title']
            )
            print(f"Appended to CSV: \"{item_to_add['csv_title']}\" (Source: {item_to_add['csv_source']}, URL: {item_to_add['csv_url'] or 'N/A'})")
            new_updates_count += 1

    print(f"\nScript finished. Added {new_updates_count} new item(s) to '{CSV_FILE}'.")

if __name__ == '__main__':
    main()
