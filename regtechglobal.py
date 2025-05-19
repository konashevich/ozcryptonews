#!/usr/bin/env python3
import requests
from bs4 import BeautifulSoup
import datetime # Keep standard datetime
import csv
import os
import sys
from urllib.parse import urljoin
from datetime import timezone # Import timezone

# Configuration
NEWS_PAGE_URL = 'https://regtechglobal.org/news' # Renamed for clarity
CSV_FILE_PATH = 'articles.csv' # Renamed for clarity
SOURCE_IDENTIFIER = 'regtechglobal.org' # Renamed for clarity
KEYWORDS_FILE_PATH = 'australia_keywords.txt' # Renamed for clarity
CSV_COLUMNS_LIST = ['date', 'source', 'url', 'title', 'done'] # Renamed for clarity

def load_keywords():
    """Loads keywords from KEYWORDS_FILE_PATH, one per line, lowercase."""
    keywords_set = set() # Use a set for keywords
    if not os.path.exists(KEYWORDS_FILE_PATH):
        print(f"Keywords file '{KEYWORDS_FILE_PATH}' not found.")
        return list(keywords_set) # Return empty list if no file
    try:
        with open(KEYWORDS_FILE_PATH, 'r', encoding='utf-8') as f:
            for line in f:
                kw = line.strip().lower()
                if kw: keywords_set.add(kw)
        print(f"Loaded {len(keywords_set)} keywords from '{KEYWORDS_FILE_PATH}'.")
    except Exception as e:
        print(f"Error loading keywords from '{KEYWORDS_FILE_PATH}': {e}")
    return list(keywords_set)


def load_last_check_date_utc():
    """
    Determines the most recent date in CSV_FILE_PATH for SOURCE_IDENTIFIER.
    Returns a timezone-aware UTC datetime object. Defaults to 30 days ago (UTC) if no entry.
    """
    default_date = datetime.datetime.now(timezone.utc) - datetime.timedelta(days=30)
    if not os.path.exists(CSV_FILE_PATH) or os.path.getsize(CSV_FILE_PATH) == 0:
        return default_date

    last_dt_utc = None
    try:
        with open(CSV_FILE_PATH, mode='r', newline='', encoding='utf-8') as f_read:
            reader = csv.DictReader(f_read)
            if not reader.fieldnames or not all(h in reader.fieldnames for h in ['source', 'date']):
                print(f"Warning: CSV '{CSV_FILE_PATH}' missing 'source' or 'date' headers.")
                return default_date
                
            for row in reader:
                if row.get('source') == SOURCE_IDENTIFIER and row.get('date'):
                    try:
                        # Assume dates in CSV are ISO format and convert to datetime object
                        dt_obj = datetime.datetime.fromisoformat(row['date'])
                        # Ensure it's UTC
                        current_dt_utc = dt_obj.astimezone(timezone.utc) if dt_obj.tzinfo else dt_obj.replace(tzinfo=timezone.utc)
                        
                        if last_dt_utc is None or current_dt_utc > last_dt_utc:
                            last_dt_utc = current_dt_utc
                    except ValueError:
                        # print(f"Warning: Invalid date format in CSV for RegTech: {row['date']}")
                        continue # Skip rows with invalid date format
    except Exception as e:
        print(f"Error reading last check date from '{CSV_FILE_PATH}': {e}")
        return default_date # Fallback on error

    return last_dt_utc or default_date


def append_articles_to_csv(articles_list):
    """Appends a list of article dicts to CSV_FILE_PATH."""
    if not articles_list: return

    file_exists_and_has_content = os.path.exists(CSV_FILE_PATH) and os.path.getsize(CSV_FILE_PATH) > 0
    try:
        with open(CSV_FILE_PATH, 'a', newline='', encoding='utf-8') as f_append:
            writer = csv.DictWriter(f_append, fieldnames=CSV_COLUMNS_LIST)
            if not file_exists_and_has_content: # Write header if new/empty file
                writer.writeheader()
                print(f"Wrote headers to '{CSV_FILE_PATH}'.")
            
            appended_count = 0
            for article_dict in articles_list:
                # Ensure only columns in CSV_COLUMNS_LIST are written
                row_to_write = {col: article_dict.get(col, '') for col in CSV_COLUMNS_LIST}
                writer.writerow(row_to_write)
                appended_count +=1
            print(f"Appended {appended_count} new articles for '{SOURCE_IDENTIFIER}' to '{CSV_FILE_PATH}'.")
    except IOError as e_io:
        print(f"IOError writing to CSV '{CSV_FILE_PATH}': {e_io}")
    except Exception as e_csv:
        print(f"Unexpected error writing to CSV for '{SOURCE_IDENTIFIER}': {e_csv}")


def fetch_new_articles_data(since_utc_dt, keywords_list):
    """
    Scrapes the news page, returns articles newer than `since_utc_dt` containing any keyword.
    Date format for CSV: YYYY-MM-DDTHH:MM:SS+00:00.
    """
    print(f"Fetching articles from '{NEWS_PAGE_URL}' since {since_utc_dt.strftime('%Y-%m-%d %H:%M UTC')}")
    new_articles_collected = []
    try:
        resp = requests.get(NEWS_PAGE_URL, timeout=30)
        resp.raise_for_status()
    except requests.exceptions.RequestException as e_req:
        print(f"Error fetching '{NEWS_PAGE_URL}': {e_req}")
        return new_articles_collected

    soup = BeautifulSoup(resp.text, 'html.parser')
    
    # Assuming articles are within some identifiable container, e.g., <article> or specific divs
    # This site uses <h4> for title and <h5> for meta. Let's find pairs.
    # This is a bit fragile; a more robust selector for article blocks would be better.
    article_elements_h4 = soup.find_all('h4') # Titles
    article_elements_h5 = soup.find_all('h5') # Meta (date)

    MIN_YEAR = 2025

    # Iterate based on the shorter list to avoid IndexError if counts mismatch
    for i in range(min(len(article_elements_h4), len(article_elements_h5))):
        h4_tag = article_elements_h4[i]
        h5_tag = article_elements_h5[i] # Corresponding meta tag

        meta_text_content = h5_tag.get_text(strip=True)
        # Date is usually before a '|' separator in meta, e.g., "23 Apr 2024 10:00 AM | Category"
        date_string_from_meta = meta_text_content.split('|')[0].strip()
        
        dt_obj_utc = None
        try:
            # Parse the date string (e.g., "23 Apr 2024 10:00 AM")
            parsed_dt_naive = datetime.datetime.strptime(date_string_from_meta, '%d %b %Y %I:%M %p')
            # Assume the parsed naive datetime is local, convert to UTC
            # This is an assumption. If the site provides UTC or another known TZ, adjust.
            # For now, let's assume it's local and make it UTC for consistency.
            # A better way would be to know the site's timezone.
            # If we assume the site's times are, for example, Sydney time:
            # from dateutil import tz
            # sydney_tz = tz.gettz('Australia/Sydney')
            # parsed_dt_local = parsed_dt_naive.replace(tzinfo=sydney_tz)
            # dt_obj_utc = parsed_dt_local.astimezone(timezone.utc)
            # For simplicity, if timezone is unknown, we might treat naive as UTC directly:
            dt_obj_utc = parsed_dt_naive.replace(tzinfo=timezone.utc)

        except ValueError:
            # print(f"Warning: Could not parse date: '{date_string_from_meta}'")
            continue # Skip if date is unparsable

        if dt_obj_utc.year < MIN_YEAR:
            # print(f"Debug: Skipping article from {dt_obj_utc.year}")
            continue
        
        # Compare with 'since_utc_dt' which is already UTC
        if dt_obj_utc <= since_utc_dt: # If article date is not newer
            # print(f"Debug: Article date {dt_obj_utc} not newer than {since_utc_dt}. Stopping for this type.")
            # Assuming articles are listed newest first, we can break if we hit an old one.
            # However, if keyword filtering is active, we might need to continue.
            # For now, let's continue checking all if keywords are used. If not, we could break.
            if not keywords_list: # If no keywords, assume chronological order and break
                 break
            continue


        article_title_text = h4_tag.get_text(strip=True)
        link_tag_element = h4_tag.find('a', href=True)
        if not link_tag_element: continue # Skip if no link in title

        article_page_url = urljoin(NEWS_PAGE_URL, link_tag_element['href'])

        # Keyword check (if keywords are provided)
        if keywords_list:
            try:
                article_content_resp = requests.get(article_page_url, timeout=20)
                article_content_resp.raise_for_status()
                article_soup = BeautifulSoup(article_content_resp.text, 'html.parser')
                # Try to find a main content area
                main_content_div = article_soup.find('div', class_='entry-content') or \
                                   article_soup.find('article') or \
                                   article_soup.find('div', class_='post-content') # Common content classes
                
                content_text_to_search = article_title_text # Start with title
                if main_content_div:
                    content_text_to_search += " " + main_content_div.get_text(separator=' ', strip=True)
                else: # Fallback to all text if specific content area not found
                    content_text_to_search += " " + article_soup.get_text(separator=' ', strip=True)
                
                content_text_lower = content_text_to_search.lower()
                if not any(kw.lower() in content_text_lower for kw in keywords_list):
                    # print(f"Debug: Keyword not found in: {article_title_text}")
                    continue # Skip if no keyword match
            except requests.exceptions.RequestException as e_art:
                print(f"Warning: Failed to fetch content for keyword check: {article_page_url}. Error: {e_art}")
                continue # Skip if article content fetch fails
            except Exception as e_kw_check:
                print(f"Error during keyword check for {article_page_url}: {e_kw_check}")
                continue


        # Format date to YYYY-MM-DDTHH:MM:SS+00:00
        iso_date_utc_str = dt_obj_utc.strftime('%Y-%m-%dT%H:%M:%S+00:00')
        
        new_articles_collected.append({
            'date': iso_date_utc_str,
            'source': SOURCE_IDENTIFIER,
            'url': article_page_url,
            'title': article_title_text,
            'done': '',
            '_sort_date_obj': dt_obj_utc # For sorting
        })

    # Sort collected articles by date (oldest first)
    new_articles_collected.sort(key=lambda x: x['_sort_date_obj'])
    
    # Remove temporary sort key before returning
    for item in new_articles_collected:
        del item['_sort_date_obj']
        
    return new_articles_collected


def main():
    print(f"--- Starting RegTech Global Scraper ({SOURCE_IDENTIFIER}, Date Format UTC) ---")
    active_keywords = load_keywords()
    # if not active_keywords: # Decide if script should run without keywords
    #     print("No keywords loaded. Exiting, or define behavior.")
    #     # sys.exit(0) # Or continue to fetch all articles if desired

    last_checked_datetime_utc = load_last_check_date_utc()
    found_articles = fetch_new_articles_data(last_checked_datetime_utc, active_keywords)

    if found_articles:
        append_articles_to_csv(found_articles)
        # for art_info in found_articles:
        #     print(f"Logged: {art_info['url']}")
    else:
        print(f"No new articles matching criteria found since {last_checked_datetime_utc.isoformat()}.")
    
    print(f"--- RegTech Global Scraper Finished ---")

if __name__ == '__main__':
    main()
