import time
import os
import csv
import re
from datetime import datetime, timezone # Import timezone
from urllib.parse import urljoin

import pandas as pd
from bs4 import BeautifulSoup # Needed for link finding and date extraction
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
# Import newspaper
from newspaper import Article as NewspaperArticle
from newspaper.article import ArticleException

# --- Configuration ---
BASE_URL = "https://asic.gov.au"
MEDIA_RELEASES_URL = "https://asic.gov.au/newsroom/media-releases/"
OUTPUT_CSV = "articles.csv"
KEYWORDS_TXT = "web3keywords.txt"
CHECKED_URLS_FILE = "asic_checked.txt"
SOURCE_IDENTIFIER = "asic.gov.au"
OUTPUT_COLUMNS = ['date', 'source', 'url', 'title', 'done']
MAIN_PAGE_LOAD_WAIT = 10
REQUEST_DELAY = 1.5
USER_AGENT = 'Python Selenium Scraper Bot (Educational Use)'
MIN_YEAR_YY = 25 # Corresponds to 2025
CONTEXT_CHARS = 50

# --- Helper Functions ---

def load_keywords(filename):
    """Loads keywords from the specified TXT file."""
    keywords = set()
    if not os.path.exists(filename):
        print(f"Warning: Keywords file '{filename}' not found. No keyword filtering will be applied.")
        return keywords
    try:
        with open(filename, mode='r', encoding='utf-8-sig') as infile:
            for line in infile:
                keyword = line.strip().lower()
                if keyword:
                    keywords.add(keyword)
        print(f"Loaded {len(keywords)} unique keywords/phrases from {filename}.")
        keywords.discard('')
        return keywords
    except FileNotFoundError:
         print(f"Error: Keywords file '{filename}' not found.")
         return set()
    except Exception as e:
        print(f"Error loading keywords from {filename}: {e}")
        return set()

def load_checked_urls(filename):
    """Loads previously checked URLs from the specified text file."""
    checked_urls = set()
    if not os.path.exists(filename):
        try:
            with open(filename, mode='w', encoding='utf-8') as f:
                pass
            print(f"Created checked URLs file: '{filename}'")
        except IOError as e:
            print(f"Warning: Could not create checked URLs file '{filename}': {e}")
        return checked_urls

    try:
        with open(filename, mode='r', encoding='utf-8') as infile:
            for line in infile:
                url = line.strip()
                if url:
                    checked_urls.add(url)
        print(f"Loaded {len(checked_urls)} previously checked URLs from {filename}.")
    except Exception as e:
        print(f"Error reading checked URLs file '{filename}': {e}")
        print("Warning: Proceeding without knowledge of previously checked URLs.")
    return checked_urls

def save_checked_url(filename, url):
    """Appends a URL to the checked URLs file."""
    try:
        with open(filename, mode='a', encoding='utf-8') as outfile:
            outfile.write(url + '\n')
    except IOError as e:
        print(f"Warning: Could not write URL '{url}' to checked file '{filename}': {e}")

def find_matching_keywords(text, keywords):
    """
    Checks if the text contains any keywords and returns a list of those found.
    Performs case-insensitive, whole-word matching. Includes context on match.
    """
    found_keywords_details = []
    if not keywords or not text:
        return []

    text_lower = text.lower()
    for keyword in keywords:
        pattern = r'\b' + re.escape(keyword) + r'\b'
        for match in re.finditer(pattern, text_lower):
            start, end = match.span()
            context_start = max(0, start - CONTEXT_CHARS)
            context_end = min(len(text), end + CONTEXT_CHARS)
            context_snippet = text[context_start:context_end].replace('\n', ' ').replace('\r', '')
            if keyword not in [k for k, c in found_keywords_details]:
                 found_keywords_details.append((keyword, context_snippet))
    return [k for k, c in found_keywords_details]


def extract_sort_key_from_url(url):
    """
    Extracts a sort key (year, article_number) from an ASIC URL.
    Expected format: .../YY-NNNMR-...
    Returns (99, 9999) if the pattern is not found or year is invalid.
    """
    match = re.search(r'/(\d{2})-(\d{3})mr', url, re.IGNORECASE)
    if match:
        try:
            year_yy = int(match.group(1))
            article_num = int(match.group(2))
            # Assuming MIN_YEAR_YY is the threshold for '20YY'
            # e.g., if MIN_YEAR_YY is 25, it means 2025
            if year_yy < MIN_YEAR_YY: # Check against the two-digit year
                 return (99, 9999) # Invalid year, sort last
            return (year_yy, article_num)
        except ValueError:
             print(f"Warning: Invalid number format in sort key for URL: {url}")
             return (99, 9999)
    else:
        return (99, 9999) # No pattern match, sort last

def extract_and_format_date(page_source):
    """
    Extracts date from HTML source using the <time class="nh-mr-date"> tag
    and returns it in YYYY-MM-DDTHH:MM:SS+00:00 format (UTC).
    """
    try:
        soup = BeautifulSoup(page_source, 'html.parser')
        date_tag = soup.select_one('time.nh-mr-date')

        if date_tag:
            date_str = date_tag.get_text(strip=True)
            if date_str:
                try:
                    # Parse the extracted string (e.g., "14 April 2025")
                    parsed_date = datetime.strptime(date_str, '%d %B %Y')
                    # Make it timezone-aware (UTC) and format
                    utc_date = parsed_date.replace(tzinfo=timezone.utc)
                    iso_date_full = utc_date.strftime('%Y-%m-%dT%H:%M:%S+00:00')
                    print(f"    Extracted and formatted date: {iso_date_full}")
                    return iso_date_full
                except ValueError:
                    print(f"    Warning: Could not parse date string from tag: '{date_str}'")
                    return None
            else:
                print("    Warning: Found date tag but it was empty.")
                return None
        else:
            # Fallback: Try to find date in <p class="text-sm text-gray-500"> if time tag fails
            # This is a hypothetical fallback, adjust selector if needed
            fallback_date_tag = soup.select_one('p.text-sm.text-gray-500') # Example selector
            if fallback_date_tag:
                date_str_fallback = fallback_date_tag.get_text(strip=True)
                # Attempt to parse common date formats, e.g., "Published: April 14, 2025"
                # This part would need more robust parsing if the format varies
                match = re.search(r'(\w+\s\d{1,2},\s\d{4})', date_str_fallback)
                if match:
                    try:
                        parsed_date = datetime.strptime(match.group(1), '%B %d, %Y')
                        utc_date = parsed_date.replace(tzinfo=timezone.utc)
                        iso_date_full = utc_date.strftime('%Y-%m-%dT%H:%M:%S+00:00')
                        print(f"    Extracted and formatted date (fallback): {iso_date_full}")
                        return iso_date_full
                    except ValueError:
                        print(f"    Warning: Could not parse fallback date string: '{match.group(1)}'")
                        return None
            print("    Warning: <time class='nh-mr-date'> tag not found and no fallback date extracted.")
            return None
    except Exception as e:
        print(f"    Error during date extraction from HTML: {e}")
        return None

# --- Selenium Specific Functions ---

def setup_driver():
    """Initializes and returns a Selenium WebDriver instance."""
    options = webdriver.ChromeOptions()
    options.add_argument(f'user-agent={USER_AGENT}')
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920x1080')
    options.add_argument('--log-level=3')
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    try:
        service = ChromeService() # Assumes chromedriver is in PATH
        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(60)
        print("WebDriver initialized successfully (Headless Mode). Page load timeout set to 60s.")
        return driver
    except WebDriverException as e:
         print("\n--- WebDriver Error ---")
         print(f"Error initializing WebDriver: {e}")
         print("Ensure ChromeDriver is installed and accessible (in PATH or script directory).")
         print("Download: https://chromedriver.chromium.org/downloads")
         print("-----------------------\n")
         return None

def fetch_and_check_article_content_selenium(driver, article_url, keywords):
    """
    Fetches article page using Selenium, uses newspaper3k to extract main text,
    extracts the publication date from HTML source, and checks for keywords.

    Returns:
        tuple: (list_of_found_keywords, iso_formatted_date_or_None)
               Returns ([], None) on error or if keywords not found.
    """
    print(f"  Checking article: {article_url}")
    extracted_iso_date = None
    found_keywords_list = []
    try:
        time.sleep(REQUEST_DELAY)
        print(f"    Navigating to article page...")
        driver.get(article_url)
        print(f"    Page loaded. Processing...")

        page_source = driver.page_source

        extracted_iso_date = extract_and_format_date(page_source) # Ensures YYYY-MM-DDTHH:MM:SS+00:00
        if not extracted_iso_date:
             print(f"    Warning: Could not extract publication date using primary or fallback methods.")

        print(f"    Processing text with newspaper3k...")
        article = NewspaperArticle(article_url, language='en')
        article.download(input_html=page_source)
        article.parse()
        article_text = article.text

        if not article_text:
             print(f"    Warning: newspaper3k could not extract main text from {article_url}. Keyword check might be incomplete.")
             article_text = "" # Ensure article_text is a string for find_matching_keywords

        print(f"    Extracted {len(article_text)} characters using newspaper3k for keyword check.")

        found_keywords_list = find_matching_keywords(article_text, keywords)

        if found_keywords_list:
             print(f"    DEBUG: Matched keywords for {article_url}: {found_keywords_list}")

        return (found_keywords_list, extracted_iso_date)

    except TimeoutException:
        print(f"  Error: Page load timed out for {article_url} after {driver.timeouts.page_load} seconds.")
        return ([], None)
    except ArticleException as e:
         print(f"  Error: newspaper3k failed to process article {article_url}: {e}")
         return ([], extracted_iso_date)
    except WebDriverException as e:
        print(f"  Error navigating to or processing {article_url}: {e}")
        return ([], None)
    except Exception as e:
        print(f"  Unexpected error processing article {article_url}: {e}")
        return ([], None)

# --- Main Script ---

print(f"--- Starting ASIC Article Scraper (Selenium - ISO Date Format UTC) ---")

keywords_to_check = load_keywords(KEYWORDS_TXT)
if not keywords_to_check:
     print("Proceeding without keyword filtering as no keywords were loaded.")

checked_urls = load_checked_urls(CHECKED_URLS_FILE)
driver = setup_driver()
if not driver:
    print("--- Script Finished (WebDriver Setup Error) ---")
    exit()

urls_to_process = set()
articles_to_add = []

try:
    print(f"Fetching main list from {MEDIA_RELEASES_URL}...")
    driver.get(MEDIA_RELEASES_URL)
    print(f"Pausing for {MAIN_PAGE_LOAD_WAIT} seconds for page to potentially finish rendering...")
    time.sleep(MAIN_PAGE_LOAD_WAIT)

    print("Extracting links from main page source...")
    page_source = driver.page_source
    soup = BeautifulSoup(page_source, 'html.parser')
    all_links = soup.find_all('a', href=True)
    print(f"Found {len(all_links)} total links on the page.")

    print(f"Filtering links for 'news-centre', year >= 20{MIN_YEAR_YY}, and not previously checked...")
    skipped_year_count = 0
    skipped_checked_count = 0
    skipped_other_count = 0

    current_year_full = datetime.now().year # e.g., 2025
    min_year_full = 2000 + MIN_YEAR_YY # e.g., 2025

    for link in all_links:
        href = link['href']
        try:
            full_url = urljoin(BASE_URL, href)
            if "news-centre" not in full_url or not full_url.endswith('/'): # ASIC URLs often end with /
                skipped_other_count += 1
                continue
            if full_url in checked_urls:
                skipped_checked_count += 1
                continue

            # Year check based on URL pattern
            year_match_in_url = re.search(r'/(\d{2})-\d{3}mr', full_url, re.IGNORECASE)
            if year_match_in_url:
                year_yy_from_url = int(year_match_in_url.group(1))
                if year_yy_from_url < MIN_YEAR_YY: # Compare two-digit years
                    skipped_year_count += 1
                    save_checked_url(CHECKED_URLS_FILE, full_url) # Mark old ones as checked
                    checked_urls.add(full_url)
                    continue
            else:
                # If no year pattern in URL, we might skip or try to infer differently.
                # For now, if it doesn't match ASIC's typical MR URL, skip.
                skipped_other_count +=1
                continue

            urls_to_process.add(full_url)
        except Exception as e:
            print(f"Warning: Error processing link href '{href}': {e}")
            skipped_other_count += 1
            continue

    print(f"Filtering complete. Skipped: {skipped_year_count} (wrong year), {skipped_checked_count} (already checked), {skipped_other_count} (other reasons).")
    print(f"Identified {len(urls_to_process)} unique, relevant (>= 20{MIN_YEAR_YY}), and unchecked article URLs to process.")


    if urls_to_process:
        print("Fetching and checking content of relevant URLs...")
        processed_count = 0
        # Sort URLs by the extracted year and article number for chronological processing
        urls_to_process_list = sorted(list(urls_to_process), key=extract_sort_key_from_url)

        for url in urls_to_process_list:
            processed_count += 1
            print(f"Processing URL {processed_count}/{len(urls_to_process_list)}...")

            found_keywords_list, article_date_iso_full = fetch_and_check_article_content_selenium(driver, url, keywords_to_check)

            if found_keywords_list:
                title_content = ", ".join(sorted(found_keywords_list))
                if article_date_iso_full: # This is already YYYY-MM-DDTHH:MM:SS+00:00
                    date_to_save = article_date_iso_full
                else:
                    # Fallback to current UTC time if date extraction failed
                    current_utc_dt = datetime.now(timezone.utc)
                    date_to_save = current_utc_dt.strftime('%Y-%m-%dT%H:%M:%S+00:00')
                    print(f"    -> Using current UTC timestamp as fallback date: {date_to_save}")

                articles_to_add.append({
                    'date': date_to_save,
                    'source': SOURCE_IDENTIFIER,
                    'url': url,
                    'title': title_content, # Storing keywords in title column
                    'done': ''
                })
            save_checked_url(CHECKED_URLS_FILE, url)
            checked_urls.add(url)
        print("Finished checking individual articles.")

    print(f"Found {len(articles_to_add)} new articles containing keywords.")

    if articles_to_add:
        # Sorting is already implicitly handled by processing sorted URLs,
        # but if dates were inconsistent, explicit sort by date would be needed here.
        # articles_to_add.sort(key=lambda x: x['date']) # Ensure final sort by date string

        print(f"Appending {len(articles_to_add)} new articles to {OUTPUT_CSV}...")
        file_exists = os.path.exists(OUTPUT_CSV)
        is_empty = (not file_exists) or (os.path.getsize(OUTPUT_CSV) == 0)

        try:
            # Convert to DataFrame for easier CSV writing with pandas
            new_df = pd.DataFrame(articles_to_add)
            new_df.to_csv(OUTPUT_CSV,
                          mode='a',
                          header=is_empty, # Write header only if file is new/empty
                          index=False,
                          columns=OUTPUT_COLUMNS,
                          encoding='utf-8-sig',
                          quoting=csv.QUOTE_MINIMAL)
            print(f"Successfully appended {len(articles_to_add)} articles.")
        except Exception as e:
            print(f"Error writing to CSV file {OUTPUT_CSV}: {e}")
    else:
        print("No new articles matching the criteria were found to add.")

except WebDriverException as e:
    print(f"\nFatal WebDriver error during script execution: {e}")
except TimeoutException as e:
    print(f"\nPage load timeout during script execution (likely on main list page): {e}")
except Exception as e:
    print(f"\nAn unexpected error occurred during the main process: {e}")
finally:
    if 'driver' in locals() and driver:
        print("Closing WebDriver...")
        driver.quit()
        print("WebDriver closed.")

print("--- Script Finished ---")
