import time
import os
import csv
import re
from datetime import datetime, timezone # Import timezone
from urllib.parse import urljoin

import pandas as pd
from bs4 import BeautifulSoup
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
MIN_YEAR_YY = 24  # Updated to extend search starting from 2024
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
        keywords.discard('') # Ensure empty strings are not treated as keywords
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
            # Create the file if it doesn't exist
            with open(filename, mode='w', encoding='utf-8') as f:
                pass # Just create an empty file
            print(f"Created checked URLs file: '{filename}'")
        except IOError as e:
            print(f"Warning: Could not create checked URLs file '{filename}': {e}")
        return checked_urls # Return empty set if creation failed or it's new

    try:
        with open(filename, mode='r', encoding='utf-8') as infile:
            for line in infile:
                url = line.strip()
                if url: # Ensure non-empty lines are added
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
    if not keywords or not text: # If no keywords or text, return empty list
        return []

    text_lower = text.lower()
    for keyword in keywords:
        if not keyword: # Skip empty keywords
            continue
        # Use word boundaries to match whole words
        pattern = r'\b' + re.escape(keyword) + r'\b'
        for match in re.finditer(pattern, text_lower):
            start, end = match.span()
            # Extract context around the found keyword
            context_start = max(0, start - CONTEXT_CHARS)
            context_end = min(len(text), end + CONTEXT_CHARS)
            context_snippet = text[context_start:context_end].replace('\n', ' ').replace('\r', '')
            # Add keyword and its context if not already added
            if keyword not in [k for k, c in found_keywords_details]:
                 found_keywords_details.append((keyword, context_snippet))
    return [k for k, c in found_keywords_details] # Return only the keywords


def extract_sort_key_from_url(url):
    """
    Extracts a sort key (year, article_number) from an ASIC URL.
    Expected format: .../YY-NNNMR-...
    Returns (99, 9999) if the pattern is not found or year is invalid, to sort these last.
    """
    match = re.search(r'/(\d{2})-(\d{3})mr', url, re.IGNORECASE)
    if match:
        try:
            year_yy = int(match.group(1))
            article_num = int(match.group(2))
            # Filter out years before the minimum specified year
            if year_yy < MIN_YEAR_YY:
                 return (99, 9999) # Invalid year (too old), sort last
            return (year_yy, article_num)
        except ValueError:
             print(f"Warning: Invalid number format in sort key for URL: {url}")
             return (99, 9999) # Error in parsing, sort last
    else:
        # print(f"Debug: No sort key pattern found in URL: {url}") # Optional: for debugging non-matching URLs
        return (99, 9999) # No pattern match, sort last

# --- Selenium Specific Functions ---

def setup_driver():
    """Initializes and returns a Selenium WebDriver instance."""
    options = webdriver.ChromeOptions()
    options.add_argument(f'user-agent={USER_AGENT}')
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920x1080')
    options.add_argument('--log-level=3') # Suppress console logs from Chrome/ChromeDriver
    options.add_experimental_option('excludeSwitches', ['enable-logging']) # Further suppress logs
    try:
        # Assumes chromedriver is in PATH or specify executable_path
        service = ChromeService()
        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(60) # Increased timeout for robustness
        print("WebDriver initialized successfully (Headless Mode). Page load timeout set to 60s.")
        return driver
    except WebDriverException as e:
         print("\n--- WebDriver Error ---")
         print(f"Error initializing WebDriver: {e}")
         print("Ensure ChromeDriver is installed and accessible (in PATH or specify executable_path in ChromeService).")
         print("Download: https://chromedriver.chromium.org/downloads")
         print("-----------------------\n")
         return None

def fetch_and_check_article_content_selenium(driver, article_url, keywords):
    """
    Fetches article page using Selenium, extracts the publication date and title
    from the <header class="media-release"> element, processes the article text
    via newspaper3k, and checks for keywords.

    Returns:
        tuple: (found_keywords_list, iso_date_or_None, article_title_string)
               article_title_string will be empty if not found.
               iso_date_or_None will be None if not found or parsing fails.
    """
    print(f"  Checking article: {article_url}")
    extracted_iso_date = None
    article_title = "" # Initialize article_title as an empty string
    found_keywords_list = []

    try:
        time.sleep(REQUEST_DELAY) # Be respectful to the server
        print(f"    Navigating to article page...")
        driver.get(article_url)
        print(f"    Page loaded. Processing...")

        page_source = driver.page_source # Get page source after JS rendering
        soup = BeautifulSoup(page_source, 'html.parser')

        # Find the specific <header class="media-release"> element
        media_release_header = soup.find('header', class_='media-release')

        if media_release_header:
            print(f"    Found <header class='media-release'> for {article_url}.")
            # Extract title from H1 within this header
            h1_tag = media_release_header.find('h1')
            if h1_tag:
                article_title = h1_tag.get_text(strip=True)
                print(f"    Extracted article title: {article_title}")
            else:
                print(f"    Warning: No <h1> tag found within <header class='media-release'> for {article_url}.")
                # article_title remains ""

            # Extract date from <time class="nh-mr-date"> within this header
            date_tag = media_release_header.find('time', class_='nh-mr-date')
            if date_tag:
                date_str = date_tag.get_text(strip=True)
                if date_str:
                    try:
                        # Parse the extracted string (e.g., "8 March 2024")
                        parsed_date = datetime.strptime(date_str, '%d %B %Y')
                        # Make it timezone-aware (UTC) and format
                        utc_date = parsed_date.replace(tzinfo=timezone.utc)
                        extracted_iso_date = utc_date.strftime('%Y-%m-%dT%H:%M:%S+00:00')
                        print(f"    Extracted and formatted date: {extracted_iso_date}")
                    except ValueError:
                        print(f"    Warning: Could not parse date string '{date_str}' from <header class='media-release'> for {article_url}.")
                        # extracted_iso_date remains None
                else:
                    print(f"    Warning: Found <time class='nh-mr-date'> within <header class='media-release'> but it was empty for {article_url}.")
                    # extracted_iso_date remains None
            else:
                print(f"    Warning: No <time class='nh-mr-date'> tag found within <header class='media-release'> for {article_url}.")
                # extracted_iso_date remains None
        else:
            print(f"    Warning: <header class='media-release'> not found on page {article_url}. Title and date may be missing or incorrect.")
            # article_title remains ""
            # extracted_iso_date remains None

        # Process article text using newspaper3k for keyword checking
        print(f"    Processing text with newspaper3k...")
        article_parser = NewspaperArticle(article_url, language='en')
        article_parser.download(input_html=page_source) # Use Selenium's source
        article_parser.parse()
        article_text = article_parser.text

        if not article_text:
            print(f"    Warning: newspaper3k could not extract main text from {article_url}. Keyword check might be incomplete.")
            article_text = ""  # Ensure article_text is a string for find_matching_keywords

        print(f"    Extracted {len(article_text)} characters using newspaper3k for keyword check.")

        # Find keywords in the extracted text
        found_keywords_list = find_matching_keywords(article_text, keywords)

        if found_keywords_list:
            print(f"    DEBUG: Matched keywords for {article_url}: {found_keywords_list}")
        else:
            print(f"    DEBUG: No keywords matched for {article_url}.")

        return (found_keywords_list, extracted_iso_date, article_title)

    except TimeoutException:
        print(f"  Error: Page load timed out for {article_url} after {driver.timeouts.page_load} seconds.")
        return ([], None, article_title) # Return current article_title (likely empty)
    except ArticleException as e: # newspaper3k specific error
        print(f"  Error: newspaper3k failed to process article {article_url}: {e}")
        return ([], extracted_iso_date, article_title) # Return date if extracted, and current title
    except WebDriverException as e: # Selenium specific errors
        print(f"  Error navigating to or processing {article_url} with Selenium: {e}")
        return ([], None, article_title) # Return current article_title
    except Exception as e: # Catch-all for other unexpected errors
        print(f"  Unexpected error processing article {article_url}: {e}")
        return ([], None, article_title) # Return current article_title

# --- Main Script ---

print(f"--- Starting ASIC Article Scraper (Selenium - ISO Date Format UTC) ---")

keywords_to_check = load_keywords(KEYWORDS_TXT)
if not keywords_to_check:
     print("Proceeding without keyword filtering as no keywords were loaded or file was empty.")

checked_urls = load_checked_urls(CHECKED_URLS_FILE)
driver = setup_driver()

if not driver:
    print("--- Script Finished (WebDriver Setup Error) ---")
    exit() # Exit if WebDriver fails to initialize

urls_to_process = set()
articles_to_add = [] # List to hold dictionaries of articles to be added to CSV

try:
    print(f"Fetching main list from {MEDIA_RELEASES_URL}...")
    driver.get(MEDIA_RELEASES_URL)
    print(f"Pausing for {MAIN_PAGE_LOAD_WAIT} seconds for page to potentially finish dynamic content loading...")
    time.sleep(MAIN_PAGE_LOAD_WAIT)

    print("Extracting links from main page source...")
    page_source = driver.page_source # Get source after waiting
    soup = BeautifulSoup(page_source, 'html.parser')
    all_links = soup.find_all('a', href=True) # Find all links with an href attribute
    print(f"Found {len(all_links)} total links on the page.")

    print(f"Filtering links for 'news-centre', year >= 20{MIN_YEAR_YY}, and not previously checked...")
    skipped_year_count = 0
    skipped_checked_count = 0
    skipped_other_count = 0 # For links not matching 'news-centre' or other criteria

    for link_tag in all_links:
        href = link_tag['href']
        try:
            full_url = urljoin(BASE_URL, href) # Construct absolute URL

            # Basic filtering criteria
            if "news-centre" not in full_url or not full_url.endswith('/'): # ASIC media releases often end with '/'
                skipped_other_count += 1
                continue
            if full_url in checked_urls:
                skipped_checked_count += 1
                continue

            # Year check based on URL pattern (e.g., /24-123mr/)
            year_match_in_url = re.search(r'/(\d{2})-\d{3}mr', full_url, re.IGNORECASE)
            if year_match_in_url:
                year_yy_from_url = int(year_match_in_url.group(1))
                # Compare two-digit years directly with MIN_YEAR_YY
                if year_yy_from_url < MIN_YEAR_YY:
                    skipped_year_count += 1
                    save_checked_url(CHECKED_URLS_FILE, full_url) # Mark old ones as checked to avoid re-processing
                    checked_urls.add(full_url)
                    continue
            else:
                # If no year pattern typical of ASIC MRs, skip it.
                skipped_other_count +=1
                continue

            urls_to_process.add(full_url) # Add to set for processing
        except Exception as e: # Catch errors during URL processing
            print(f"Warning: Error processing link href '{href}': {e}")
            skipped_other_count += 1
            continue

    print(f"Filtering complete. Skipped: {skipped_year_count} (year before 20{MIN_YEAR_YY}), {skipped_checked_count} (already checked), {skipped_other_count} (other reasons).")
    print(f"Identified {len(urls_to_process)} unique, relevant (>= 20{MIN_YEAR_YY}), and unchecked article URLs to process.")


    if urls_to_process:
        print("Fetching and checking content of relevant URLs...")
        processed_count = 0
        # Sort URLs by the extracted year and article number for chronological processing (best effort)
        urls_to_process_list = sorted(list(urls_to_process), key=extract_sort_key_from_url)

        for url in urls_to_process_list:
            processed_count += 1
            print(f"Processing URL {processed_count}/{len(urls_to_process_list)}: {url}")

            # Returns: (found_keywords_list, iso_date_string_or_None, actual_article_title_string)
            found_keywords_list, article_date_iso_full, actual_article_title = fetch_and_check_article_content_selenium(driver, url, keywords_to_check)

            # Add to CSV if keywords are found OR if no keywords are being checked
            if found_keywords_list or not keywords_to_check:
                
                title_for_csv = actual_article_title
                if not actual_article_title: # If title was not found or was empty
                    print(f"    -> Warning: No valid title found for {url}. Using 'Title not found' as placeholder.")
                    title_for_csv = "Title not found"

                date_to_save = article_date_iso_full
                if not article_date_iso_full: # If date extraction failed
                    print(f"    -> Warning: Date extraction failed for {url}. Using current UTC timestamp as fallback.")
                    current_utc_dt = datetime.now(timezone.utc)
                    date_to_save = current_utc_dt.strftime('%Y-%m-%dT%H:%M:%S+00:00')
                    print(f"    -> Fallback date: {date_to_save}")

                articles_to_add.append({
                    'date': date_to_save,
                    'source': SOURCE_IDENTIFIER,
                    'url': url,
                    'title': title_for_csv,
                    'done': ''
                })
            else: # This else block is for when keywords_to_check is not empty AND found_keywords_list is empty
                print(f"    No matching keywords found in {url}. Skipping CSV entry for this article.")

            save_checked_url(CHECKED_URLS_FILE, url) # Mark URL as checked
            checked_urls.add(url)

        print("Finished checking individual articles.")

    print(f"Found {len(articles_to_add)} new articles to add to CSV (matching criteria or no keyword filter).")

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
        print("No new articles matching the criteria were found to add to the CSV.")

except WebDriverException as e:
    print(f"\nFatal WebDriver error during script execution: {e}")
except TimeoutException as e:
    print(f"\nPage load timeout during script execution (likely on main list page): {e}")
except Exception as e:
    print(f"\nAn unexpected error occurred during the main process: {e}")
    import traceback
    print(traceback.format_exc())
finally:
    if 'driver' in locals() and driver:
        print("Closing WebDriver...")
        driver.quit()
        print("WebDriver closed.")

print("--- Script Finished ---")
