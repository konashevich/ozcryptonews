#!/usr/bin/env python3
"""
decrypt_news_scraper.py
- Parses rendered HTML for articles, extracting full ISO-8601 UTC timestamp.
- Appends new, unique articles into articles.csv with 'date' column in YYYY-MM-DDTHH:MM:SS+00:00 format.
"""

import os
import csv
from dateutil import parser as date_parser # Renamed for clarity
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime, timezone

# Configuration
CSV_FILE = 'articles.csv'
KEYWORDS_FILE = 'australia_keywords.txt' # Ensure this file exists or default is used
SEARCH_URL_TEMPLATE = 'https://decrypt.co/search/all/{}' # Use a template string
COOKIEBOT_ACCEPT_XPATH = '//button[@id="CybotCookiebotDialogBodyButtonAccept"]'
SEARCH_BOX_ID = 'q' # ID of the search input field
ARTICLE_TAG_SELECTOR = 'article' # Main selector for article blocks
CSV_HEADERS = ['date', 'source', 'url', 'title', 'done'] # Standardized headers

def load_keywords():
    """Loads keywords from file, defaults to ['australia'] if file not found."""
    try:
        with open(KEYWORDS_FILE, 'r', encoding='utf-8') as f:
            keywords = [line.strip() for line in f if line.strip()]
            if keywords:
                print(f"Loaded keywords: {keywords}")
                return keywords
    except FileNotFoundError:
        print(f"⚠️ Keywords file '{KEYWORDS_FILE}' not found.")
    print("Defaulting to keyword: ['australia']")
    return ['australia']

def setup_driver():
    """Initializes and returns a headless Selenium WebDriver."""
    opts = Options()
    opts.add_argument('--headless')
    opts.add_argument('--disable-gpu')
    opts.add_argument('--no-sandbox')
    # opts.add_argument('--enable-unsafe-swiftshader') # Usually not needed
    opts.add_argument('--log-level=3') # Suppress console noise
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36")
    try:
        driver = webdriver.Chrome(options=opts) # Assumes chromedriver is in PATH
        driver.set_page_load_timeout(40) # Increased timeout
        print("WebDriver initialized.")
        return driver
    except Exception as e:
        print(f"Error setting up WebDriver: {e}")
        return None


def accept_cookies_if_present(driver):
    """Attempts to click the Cookiebot accept button if visible."""
    try:
        accept_button = WebDriverWait(driver, 7).until( # Wait a bit longer
            EC.element_to_be_clickable((By.XPATH, COOKIEBOT_ACCEPT_XPATH))
        )
        accept_button.click()
        print("Cookiebot dialog accepted.")
        WebDriverWait(driver, 3).until_not( # Wait for dialog to disappear
            EC.presence_of_element_located((By.XPATH, COOKIEBOT_ACCEPT_XPATH))
        )
    except TimeoutException:
        print("Cookiebot dialog not found or did not disappear after click.")
    except NoSuchElementException:
        print("Cookiebot accept button not found (NoSuchElement).")
    except Exception as e:
        print(f"Error interacting with Cookiebot dialog: {e}")


def fetch_and_parse_search_results(driver, keyword):
    """Fetches search results for a keyword and parses articles."""
    search_page_url = SEARCH_URL_TEMPLATE.format(keyword)
    print(f"Fetching search results for '{keyword}' from: {search_page_url}")
    try:
        driver.get(search_page_url)
    except TimeoutException:
        print(f"⚠️ Timeout loading search page for '{keyword}': {search_page_url}")
        return []

    accept_cookies_if_present(driver) # Handle cookies

    # The search might already be triggered by the URL, or might need interaction.
    # Decrypt.co's search page usually loads results based on the URL path.
    # Let's ensure results are loaded by waiting for article tags.
    try:
        WebDriverWait(driver, 15).until( # Wait for article elements to be present
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, ARTICLE_TAG_SELECTOR))
        )
        print(f"Article elements found for '{keyword}'.")
    except TimeoutException:
        print(f"⚠️ No <{ARTICLE_TAG_SELECTOR}> elements found on search results page for '{keyword}'.")
        # Save debug HTML
        # with open(f"debug_decrypt_{keyword}.html", "w", encoding="utf-8") as f_debug:
        #     f_debug.write(driver.page_source)
        # print(f"Saved debug HTML to debug_decrypt_{keyword}.html")
        return []

    html_content = driver.page_source
    return parse_search_page_articles(html_content)


def parse_search_page_articles(html_content):
    """Parses articles from the search results page HTML."""
    # Define the threshold date (January 1, 2025) as a timezone-aware UTC datetime object
    threshold_date_utc = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    
    soup = BeautifulSoup(html_content, 'html.parser')
    articles_data = []
    
    for article_block in soup.select(ARTICLE_TAG_SELECTOR):
        link_tag = article_block.find('a', href=True)
        if not link_tag: continue

        # Skip non-article links like "collection" or "price" pages if they appear in <article>
        href_value = link_tag['href']
        if '/price/' in href_value or '/collections/' in href_value or '/category/' in href_value:
            continue
        
        article_url = href_value if href_value.startswith('http') else 'https://decrypt.co' + href_value
        
        # Try to get a more specific title, e.g., from an <h2> or <h3> inside the link
        title_element = link_tag.find(['h2', 'h3', 'span'], class_=lambda x: x and 'title' in x.lower())
        article_title = title_element.get_text(strip=True) if title_element else link_tag.get_text(strip=True)
        if not article_title: article_title = "No Title Found"

        time_tag = article_block.find('time', datetime=True) # Prefer <time datetime="...">
        date_str_to_parse = None
        if time_tag:
            date_str_to_parse = time_tag['datetime']
        else: # Fallback if <time> with datetime attr is not found
            time_tag_text = article_block.find('time')
            if time_tag_text: date_str_to_parse = time_tag_text.get_text(strip=True)

        if not date_str_to_parse:
            # print(f"Warning: No date found for article: {article_title} ({article_url})")
            continue
        
        try:
            # date_parser.parse is good at handling various formats
            # FIX: Add default=datetime.now(timezone.utc) for relative date parsing
            parsed_dt_obj = date_parser.parse(date_str_to_parse, default=datetime.now(timezone.utc))
            # Ensure the datetime object is UTC
            if parsed_dt_obj.tzinfo is None: # If naive
                dt_utc = parsed_dt_obj.replace(tzinfo=timezone.utc) # Assume UTC
            else: # If timezone-aware
                dt_utc = parsed_dt_obj.astimezone(timezone.utc) # Convert to UTC
            
            # Skip articles older than the threshold year (2025)
            if dt_utc < threshold_date_utc:
                # print(f"Debug: Skipping article from {dt_utc.year}: {article_title}")
                continue

            # Format to YYYY-MM-DDTHH:MM:SS+00:00
            iso_timestamp_utc = dt_utc.strftime('%Y-%m-%dT%H:%M:%S+00:00')
            
            articles_data.append({
                'date': iso_timestamp_utc, # Changed from 'timestamp' to 'date' for consistency
                'source': 'decrypt.co',
                'url': article_url,
                'title': article_title,
                'done': '',
                '_sort_date_obj': dt_utc # For sorting
            })
        except (ValueError, date_parser.ParserError, TypeError) as e:
            print(f"Warning: Could not parse date string '{date_str_to_parse}' for '{article_title}'. Error: {e}")
        except Exception as e_gen:
            print(f"General error parsing date for '{article_title}': {e_gen}")
            
    return articles_data


def main():
    print("--- Starting Decrypt.co Scraper (Date Format UTC) ---")
    # Load existing URLs from articles.csv for 'decrypt.co'
    seen_urls = set()
    is_csv_new_or_empty = not os.path.exists(CSV_FILE) or os.path.getsize(CSV_FILE) == 0
    
    if not is_csv_new_or_empty:
        try:
            with open(CSV_FILE, mode='r', newline='', encoding='utf-8') as csvfile_read:
                reader = csv.DictReader(csvfile_read)
                if reader.fieldnames and all(h in reader.fieldnames for h in ['source', 'url']):
                    for row in reader:
                        if row.get('source') == 'decrypt.co' and row.get('url'):
                            seen_urls.add(row['url'])
                else: # CSV exists but headers are missing/wrong
                    print(f"Warning: '{CSV_FILE}' has missing/incorrect headers. Will treat as new for writing header.")
                    is_csv_new_or_empty = True # Force header write
        except Exception as e_read:
            print(f"Error reading existing CSV '{CSV_FILE}': {e_read}. Treating as new.")
            is_csv_new_or_empty = True


    driver_instance = setup_driver()
    if not driver_instance:
        print("Exiting due to WebDriver setup failure.")
        return

    all_new_articles_found = []
    keywords_list = load_keywords()

    for kw in keywords_list:
        articles_from_keyword_search = fetch_and_parse_search_results(driver_instance, kw)
        for article_item in articles_from_keyword_search:
            if article_item['url'] not in seen_urls:
                all_new_articles_found.append(article_item)
                seen_urls.add(article_item['url']) # Add to seen set to avoid duplicates from other keywords in this run
    
    driver_instance.quit()
    print("WebDriver closed.")

    if not all_new_articles_found:
        print("No new articles found across all keywords.")
        return

    # Sort new articles by their datetime object (oldest first)
    all_new_articles_found.sort(key=lambda x: x['_sort_date_obj'])

    # Append to CSV
    try:
        with open(CSV_FILE, 'a', newline='', encoding='utf-8') as csvfile_append:
            writer = csv.DictWriter(csvfile_append, fieldnames=CSV_HEADERS)
            if is_csv_new_or_empty: # Write header if file was new, empty, or had bad headers
                writer.writeheader()
                print(f"Wrote headers to '{CSV_FILE}'.")
            
            appended_count = 0
            for article_to_write in all_new_articles_found:
                # Prepare row, removing the temporary sort key
                row_data = {
                    'date': article_to_write['date'], # Already formatted ISO string
                    'source': article_to_write['source'],
                    'url': article_to_write['url'],
                    'title': article_to_write['title'],
                    'done': article_to_write['done']
                }
                writer.writerow(row_data)
                appended_count +=1
            print(f"✅ Added {appended_count} new article(s) to '{CSV_FILE}'.")
    except IOError as e_io:
        print(f"Error writing to CSV '{CSV_FILE}': {e_io}")
    except Exception as e_csv_write:
        print(f"Unexpected error writing to CSV: {e_csv_write}")
        
    print("--- Decrypt.co Scraper Finished ---")

if __name__ == '__main__':
    main()