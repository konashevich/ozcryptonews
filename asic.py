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
OUTPUT_COLUMNS = ['date', 'source', 'url', 'title', 'done'] # 'title' now holds found keywords
MAIN_PAGE_LOAD_WAIT = 10
REQUEST_DELAY = 1.5
USER_AGENT = 'Python Selenium Scraper Bot (Educational Use)'
MIN_YEAR_YY = 25
CONTEXT_CHARS = 50 # Keep for debug printing in find_matching_keywords

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
            # print(f"    DEBUG: Found keyword '{keyword}' -> Context: ...{context_snippet}...") # Keep commented unless debugging
            if keyword not in [k for k, c in found_keywords_details]:
                 found_keywords_details.append((keyword, context_snippet))
            # break # Optional: only report first occurrence

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
            if year_yy < MIN_YEAR_YY:
                 return (99, 9999)
            return (year_yy, article_num)
        except ValueError:
             print(f"Warning: Invalid number format in sort key for URL: {url}")
             return (99, 9999)
    else:
        return (99, 9999)

def extract_and_format_date(page_source):
    """
    Extracts date from HTML source using the <time class="nh-mr-date"> tag
    and returns it in YYYY-MM-DDTHH:MM:SS+00:00 format.
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
                    # Format into ISO YYYY-MM-DD and add zeroed time + UTC offset
                    iso_date_full = parsed_date.strftime('%Y-%m-%dT00:00:00+00:00')
                    print(f"    Extracted and formatted date: {iso_date_full}")
                    return iso_date_full
                except ValueError:
                    print(f"    Warning: Could not parse date string from tag: '{date_str}'")
                    return None
            else:
                print("    Warning: Found date tag but it was empty.")
                return None
        else:
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
        service = ChromeService()
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

        extracted_iso_date = extract_and_format_date(page_source)
        if not extracted_iso_date:
             print(f"    Warning: Could not extract publication date using <time> tag.")

        print(f"    Processing text with newspaper3k...")
        article = NewspaperArticle(article_url, language='en')
        article.download(input_html=page_source)
        article.parse()
        article_text = article.text

        if not article_text:
             print(f"    Warning: newspaper3k could not extract main text from {article_url}. Keyword check might be incomplete.")
             article_text = ""

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
         return ([], extracted_iso_date) # Return date if found before newspaper error
    except WebDriverException as e:
        print(f"  Error navigating to or processing {article_url}: {e}")
        return ([], None)
    except Exception as e:
        print(f"  Unexpected error processing article {article_url}: {e}")
        return ([], None)

# --- Main Script ---

print(f"--- Starting ASIC Article Scraper (Selenium - ISO Date Format) ---") # Updated title

# 1. Load Keywords
keywords_to_check = load_keywords(KEYWORDS_TXT)
if not keywords_to_check:
     print("Proceeding without keyword filtering as no keywords were loaded.")

# 2. Load Previously Checked URLs
checked_urls = load_checked_urls(CHECKED_URLS_FILE)

# 3. Setup Selenium WebDriver
driver = setup_driver()
if not driver:
    print("--- Script Finished (WebDriver Setup Error) ---")
    exit()

urls_to_process = set()
articles_to_add = [] # List to store dicts for matched articles before sorting

try:
    # 4. Fetch Main Media Releases Page using Selenium
    print(f"Fetching main list from {MEDIA_RELEASES_URL}...")
    driver.get(MEDIA_RELEASES_URL)

    print(f"Pausing for {MAIN_PAGE_LOAD_WAIT} seconds for page to potentially finish rendering...")
    time.sleep(MAIN_PAGE_LOAD_WAIT)

    # 5. Get page source and find all relevant links
    print("Extracting links from main page source...")
    page_source = driver.page_source
    soup = BeautifulSoup(page_source, 'html.parser') # Use BS for link finding

    all_links = soup.find_all('a', href=True)
    print(f"Found {len(all_links)} total links on the page.")

    # 6. Filter links
    print(f"Filtering links for 'news-centre', year >= {MIN_YEAR_YY}, and not previously checked...")
    skipped_year_count = 0
    skipped_checked_count = 0
    skipped_other_count = 0
    for link in all_links:
        href = link['href']
        try:
            full_url = urljoin(BASE_URL, href)
            if "news-centre" not in full_url or not full_url.endswith('/'):
                skipped_other_count += 1
                continue
            if full_url in checked_urls:
                skipped_checked_count += 1
                continue
            year_match = re.search(r'/(\d{2})-\d{3}mr', full_url, re.IGNORECASE)
            if not year_match or int(year_match.group(1)) < MIN_YEAR_YY:
                 skipped_year_count += 1
                 if year_match and int(year_match.group(1)) < MIN_YEAR_YY:
                      save_checked_url(CHECKED_URLS_FILE, full_url)
                      checked_urls.add(full_url)
                 continue
            urls_to_process.add(full_url)
        except Exception as e:
            print(f"Warning: Error processing link href '{href}': {e}")
            skipped_other_count += 1
            continue

    print(f"Filtering complete. Skipped: {skipped_year_count} (wrong year), {skipped_checked_count} (already checked), {skipped_other_count} (other reasons).")
    print(f"Identified {len(urls_to_process)} unique, relevant (>= {MIN_YEAR_YY}), and unchecked article URLs to process.")


    # 7. Fetch and Check Full Content of Potential New Articles using Selenium
    if urls_to_process:
        print("Fetching and checking content of relevant URLs...")
        processed_count = 0
        urls_to_process_list = sorted(list(urls_to_process), key=extract_sort_key_from_url)

        for url in urls_to_process_list:
            processed_count += 1
            print(f"Processing URL {processed_count}/{len(urls_to_process_list)}...")

            # Check content, get keywords AND date
            found_keywords_list, article_date_iso_full = fetch_and_check_article_content_selenium(driver, url, keywords_to_check)

            # Add to list if keywords were found (date can be None/empty initially)
            if found_keywords_list:
                title_content = ", ".join(sorted(found_keywords_list))
                # --- Use extracted date if available, otherwise use CURRENT date (formatted) ---
                if article_date_iso_full:
                    date_to_save = article_date_iso_full
                else:
                    # Get current UTC time and format as YYYY-MM-DDTHH:MM:SS+00:00
                    # Note: Using timezone.utc ensures the offset is +00:00
                    current_ts_iso_full = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S+00:00')
                    date_to_save = current_ts_iso_full
                    print(f"    -> Using current timestamp as fallback date: {date_to_save}")
                # --- End Date Fallback Logic ---

                articles_to_add.append({
                    'date': date_to_save, # Use extracted date or current timestamp
                    'source': SOURCE_IDENTIFIER,
                    'url': url,
                    'title': title_content,
                    'done': ''
                })

            # Save URL to checked file *after* processing, regardless of keyword/date success
            save_checked_url(CHECKED_URLS_FILE, url)
            checked_urls.add(url)

        print("Finished checking individual articles.")

    print(f"Found {len(articles_to_add)} new articles containing keywords.")

    # 8. Sort New Articles based on URL pattern (YY-NNNMR)
    if articles_to_add:
        print("Sorting found articles by URL year and number...")
        articles_to_add.sort(key=lambda x: extract_sort_key_from_url(x['url']))
        print("Sorting complete.")


    # 9. Append to CSV
    if articles_to_add:
        print(f"Appending {len(articles_to_add)} new articles to {OUTPUT_CSV}...")
        file_exists = os.path.exists(OUTPUT_CSV)
        is_empty = (not file_exists) or (os.path.getsize(OUTPUT_CSV) == 0)

        try:
            new_df = pd.DataFrame(articles_to_add)
            new_df.to_csv(OUTPUT_CSV,
                          mode='a',
                          header=is_empty,
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
    # Ensure the browser is closed even if errors occur
    if 'driver' in locals() and driver:
        print("Closing WebDriver...")
        driver.quit()
        print("WebDriver closed.")

print("--- Script Finished ---")
