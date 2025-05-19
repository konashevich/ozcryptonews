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
from urllib.parse import urljoin
import unicodedata
from datetime import timezone # Import timezone

# --- Configuration ---
PROJECT_URL = 'https://dfcrc.com.au/projects-cbdc-acacia/'
MEDIA_RELEASES_URL = 'https://dfcrc.com.au/news/media-releases/'
CSV_FILE = 'articles.csv'
STOP_HEADING_TEXT = "Previous IAG Material"
CSV_HEADERS = ['date', 'source', 'url', 'title', 'done']

def clean_text(text):
    if not text: return ""
    try:
        normalized_text = unicodedata.normalize('NFKC', text)
    except TypeError:
        normalized_text = str(text)
    replacements = {'’': "'", '‘': "'", '”': '"', '“': '"', '–': '-', '—': '-'}
    for char, replacement in replacements.items():
        normalized_text = normalized_text.replace(char, replacement)
    cleaned_text = ''.join(char for char in normalized_text if char.isprintable() or char in '\n\t')
    return re.sub(r'\s+', ' ', cleaned_text).strip()

def ensure_csv_header():
    if not os.path.exists(CSV_FILE) or os.path.getsize(CSV_FILE) == 0:
        with open(CSV_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
            writer.writeheader()
        print(f"Initialized CSV file '{CSV_FILE}' with headers.")

def load_seen_data():
    """Loads seen URLs and (title, date_str) tuples for duplicate checking."""
    seen_urls = set()
    seen_title_date_strs = set() # Store date as original ISO string from CSV for exact match
    if not os.path.exists(CSV_FILE) or os.path.getsize(CSV_FILE) == 0:
        return seen_urls, seen_title_date_strs
    try:
        with open(CSV_FILE, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames or not all(h in reader.fieldnames for h in ['url', 'title', 'date', 'source']):
                print(f"Warning: CSV '{CSV_FILE}' missing required headers.")
                return seen_urls, seen_title_date_strs
            for row in reader:
                # Only consider items from DFRC sources for this script's duplicate check
                if row.get('source') == PROJECT_URL or row.get('source') == MEDIA_RELEASES_URL:
                    if row.get('url') and row['url'].strip():
                        seen_urls.add(row['url'])
                    # For items without a URL, or as an additional check
                    if row.get('title') and row.get('date'):
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


def fetch_project_updates():
    print(f"Fetching project updates from {PROJECT_URL}...")
    items_for_csv = []
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (DFCRC Scraper)'}
        resp = requests.get(PROJECT_URL, timeout=30, headers=headers)
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching {PROJECT_URL}: {e}")
        return items_for_csv

    soup = BeautifulSoup(resp.content, 'html.parser')
    all_h3_tags = soup.find_all('h3')
    MIN_YEAR = 2025

    for h3_tag in all_h3_tags:
        h3_text_cleaned = clean_text(h3_tag.get_text(strip=True))
        if h3_text_cleaned == STOP_HEADING_TEXT:
            print(f"Reached stop heading: '{STOP_HEADING_TEXT}'.")
            break

        # Focus on h3 tags that seem to represent meeting updates
        if "Meeting" in h3_text_cleaned or "Update" in h3_text_cleaned: # Broader check
            current_update_title_cleaned = h3_text_cleaned
            
            # Date parsing from title (e.g., "Meeting 22 May 2025", "Update 22 May 2025")
            # This is a simplified regex; might need adjustment if format varies widely
            date_match = re.search(r'(\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{4})', current_update_title_cleaned, re.IGNORECASE)
            raw_date_str_from_title = date_match.group(1) if date_match else None

            parsed_date_obj_utc = None
            if raw_date_str_from_title:
                try:
                    # dateparser.parse is flexible
                    parsed_dt_naive = dateparser.parse(raw_date_str_from_title)
                    # Assume naive date is UTC for project updates, or local if known
                    parsed_date_obj_utc = parsed_dt_naive.replace(tzinfo=timezone.utc)
                except (dateparser.ParserError, ValueError) as e_date:
                    print(f"  - Could not parse date from title '{raw_date_str_from_title}' for '{current_update_title_cleaned}'. Error: {e_date}")
            else: # Fallback: try to find date in subsequent text if not in H3
                # This would require more complex logic to associate text with H3
                print(f"  - No clear date in title for '{current_update_title_cleaned}'. Using current UTC as fallback if PDF found.")
                # If no date found in title, but a PDF is linked, we might use current time or skip.
                # For now, let's require a date from title for project updates.
                # If a PDF is found but no date, it's hard to timestamp accurately.
                # Defaulting to a placeholder or skipping might be options.
                # Let's try to make a fallback to current time if a PDF is found.
                if not parsed_date_obj_utc: # If still no date
                    parsed_date_obj_utc = datetime.datetime.now(timezone.utc) # Fallback to now if PDF found later

            if parsed_date_obj_utc and parsed_date_obj_utc.year < MIN_YEAR:
                # print(f"  - Skipping old project update from {parsed_date_obj_utc.year}: {current_update_title_cleaned}")
                continue

            pdf_link_found = None
            # Look for PDF links in <ul> following the <h3>
            ul_sibling = h3_tag.find_next_sibling('ul')
            if ul_sibling:
                for li in ul_sibling.find_all('li'):
                    a_tag = li.find('a', href=lambda href: href and href.lower().endswith('.pdf'))
                    if a_tag:
                        pdf_link_found = urljoin(PROJECT_URL, a_tag['href'])
                        # Use the PDF link's text as part of title if H3 is too generic like "Update"
                        pdf_title_text = clean_text(a_tag.get_text(strip=True))
                        if "Update" == current_update_title_cleaned and pdf_title_text:
                             current_update_title_cleaned = f"Update: {pdf_title_text}"
                        break # Take first PDF link under this H3

            # Only add if we have a PDF link or a very specific title indicating an update
            if pdf_link_found or "Meeting" in current_update_title_cleaned :
                iso_date_utc_str = parsed_date_obj_utc.strftime('%Y-%m-%dT%H:%M:%S+00:00') if parsed_date_obj_utc else ""
                
                # Construct a meaningful title
                csv_title = f"DFCRC Acacia: {current_update_title_cleaned}"
                if pdf_link_found and not ("Meeting" in current_update_title_cleaned or "Update" in current_update_title_cleaned):
                    # If title was generic and we found a PDF, try to use PDF name
                    pdf_name = os.path.basename(pdf_link_found)
                    csv_title = f"DFCRC Acacia PDF: {pdf_name}"


                items_for_csv.append({
                    'parsed_date_obj_utc': parsed_date_obj_utc,
                    'date': iso_date_utc_str, # ISO UTC string
                    'source': PROJECT_URL, # Source is the project page itself
                    'url': pdf_link_found, # URL of the PDF if found
                    'title': csv_title,
                    'done': ''
                })
    return items_for_csv


def fetch_media_releases():
    print(f"Fetching media releases from {MEDIA_RELEASES_URL}...")
    items_for_csv = []
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (DFCRC Scraper)'}
        resp = requests.get(MEDIA_RELEASES_URL, timeout=30, headers=headers)
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching {MEDIA_RELEASES_URL}: {e}")
        return items_for_csv

    soup = BeautifulSoup(resp.content, 'html.parser')
    # Assuming media releases are in elements with class "latest_post"
    latest_posts_elements = soup.find_all(class_="latest_post")
    MIN_YEAR = 2025

    for post_element in latest_posts_elements:
        date_tag_element = post_element.find(class_="date entry_date updated")
        # Title link is usually within a specific class like "latest_post_title" and is an <a> tag
        link_tag_element = post_element.find(class_="latest_post_title", name='a') 
        if not link_tag_element: # Fallback if title is not directly 'a' but inside another tag
            title_container = post_element.find(class_="latest_post_title")
            if title_container:
                link_tag_element = title_container.find('a')


        parsed_date_obj_utc = None
        iso_date_utc_str = ""
        article_url_val = None
        article_title_cleaned = "N/A"

        if date_tag_element:
            raw_date_str = date_tag_element.get_text(strip=True)
            try:
                parsed_dt_naive = dateparser.parse(raw_date_str)
                # Assume naive date from media releases is UTC or convert if local TZ known
                parsed_date_obj_utc = parsed_dt_naive.replace(tzinfo=timezone.utc)
                iso_date_utc_str = parsed_date_obj_utc.strftime('%Y-%m-%dT%H:%M:%S+00:00')
            except (dateparser.ParserError, ValueError) as e_date:
                print(f"  - Could not parse media release date '{raw_date_str}'. Error: {e_date}")
        
        if link_tag_element and link_tag_element.has_attr('href'):
            article_url_val = urljoin(MEDIA_RELEASES_URL, link_tag_element['href'])
            article_title_cleaned = clean_text(link_tag_element.get_text(strip=True))
        
        if parsed_date_obj_utc and parsed_date_obj_utc.year < MIN_YEAR:
            # print(f"  - Skipping old media release from {parsed_date_obj_utc.year}: {article_title_cleaned}")
            continue
            
        if article_url_val and article_title_cleaned != "N/A" and iso_date_utc_str: # Ensure essential data is present
            items_for_csv.append({
                'parsed_date_obj_utc': parsed_date_obj_utc,
                'date': iso_date_utc_str, # ISO UTC string
                'source': MEDIA_RELEASES_URL, # Source is the media releases page
                'url': article_url_val,
                'title': article_title_cleaned,
                'done': ''
            })
    return items_for_csv

def main():
    print("--- Starting DFRC Scraper (Project Updates & Media Releases, Date Format UTC) ---")
    ensure_csv_header()
    seen_urls, seen_title_date_strs = load_seen_data()
    print(f"Loaded {len(seen_urls)} seen URLs and {len(seen_title_date_strs)} seen (title, date_str) combos for DFRC sources.")

    all_new_items_to_add = []
    
    # Fetch and filter project updates
    project_updates_data = fetch_project_updates()
    print(f"\nFetched {len(project_updates_data)} potential project updates.")
    for item_data in project_updates_data:
        is_duplicate = False
        # Check by URL if available
        if item_data.get('url') and item_data['url'] in seen_urls:
            is_duplicate = True
        # If no URL, or as fallback, check by cleaned title and ISO date string
        elif (item_data['title'], item_data['date']) in seen_title_date_strs:
            is_duplicate = True
        
        if not is_duplicate:
            all_new_items_to_add.append(item_data)
            if item_data.get('url'): seen_urls.add(item_data['url'])
            seen_title_date_strs.add((item_data['title'], item_data['date'])) # Add to current run's seen set
        # else:
            # print(f"Skipping duplicate project update: \"{item_data['title']}\"")

    # Fetch and filter media releases
    media_releases_data = fetch_media_releases()
    print(f"\nFetched {len(media_releases_data)} potential media releases.")
    for item_data in media_releases_data:
        is_duplicate = False
        if item_data.get('url') and item_data['url'] in seen_urls:
            is_duplicate = True
        elif (item_data['title'], item_data['date']) in seen_title_date_strs:
            is_duplicate = True

        if not is_duplicate:
            all_new_items_to_add.append(item_data)
            # No need to add to seen_urls/seen_title_date_strs again here,
            # as all_new_items_to_add is the final list.
        # else:
            # print(f"Skipping duplicate media release: \"{item_data['title']}\"")

    if not all_new_items_to_add:
        print("\nNo new DFRC updates or releases to add.")
    else:
        # Sort all new items by their datetime object (oldest first)
        all_new_items_to_add.sort(key=lambda x: x['parsed_date_obj_utc'] if x['parsed_date_obj_utc'] else datetime.datetime.min.replace(tzinfo=timezone.utc))
        
        # Prepare for CSV (remove temporary sort key if it was consistently added)
        # The current structure directly uses dict keys matching CSV_HEADERS for append_to_csv
        
        print(f"\nFound {len(all_new_items_to_add)} new DFRC items in total. Appending...")
        append_to_csv(all_new_items_to_add)

    print(f"--- DFRC Scraper Finished ---")

if __name__ == '__main__':
    main()
