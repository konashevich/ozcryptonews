import csv
import os
import time
from datetime import datetime, timezone # Added timezone
import requests 
from bs4 import BeautifulSoup
from dateutil import parser as date_parser
from selenium import webdriver
from selenium.common.exceptions import (ElementClickInterceptedException,
                                        NoSuchElementException,
                                        TimeoutException, WebDriverException)
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

# --- Configuration ---
URL = "https://cointelegraph.com/tags/australia"  # Main URL for tag-based scraping
CSV_FILENAME = "articles.csv"
SOURCE_NAME = "cointelegraph.com"
BASE_URL = "https://cointelegraph.com"
HEADERS = ['date', 'source', 'url', 'title', 'done']
SELENIUM_TIMEOUT_SECONDS = 25
SCROLL_PAUSE_TIME = 2
SCROLL_ATTEMPTS = 5
ACCEPT_BUTTON_TIMEOUT_SECONDS = 10
MIN_ARTICLE_YEAR = 2025 # Year to filter articles from (inclusive)

# Keywords to check for in the article title (case-insensitive)
TITLE_KEYWORDS = ["australia", "australian"] 

# Cookie/Consent Accept Button Selectors
ACCEPT_BUTTON_SELECTORS = [
    (By.ID, "CybotCookiebotDialogBodyButtonAccept"), 
    (By.ID, "onetrust-accept-btn-handler"),
    (By.XPATH, "//button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'accept')]"),
    (By.CSS_SELECTOR, 'button[data-testid*="accept"], button[aria-label*="Accept"]')
]

# --- Selectors for TAG pages ---
ENABLE_TAG_SEARCH = True # Set to True to enable scraping the main URL (tag page)
TAG_PAGE_ARTICLE_CONTAINER_SELECTOR = 'article.post-card-inline'
TAG_PAGE_ARTICLE_CONTAINER_SELECTOR_FALLBACK = 'li.posts-listing__item'
TAG_PAGE_LINK_SELECTOR = 'a.post-card-inline__title-link[href]'
TAG_PAGE_DATE_SELECTOR = 'time.post-card-inline__date[datetime]' # Expects 'datetime' attribute
TAG_PAGE_TITLE_IN_LINK_SELECTOR = 'span.post-card-inline__title'

# --- Selectors for SEARCH RESULT pages ---
# Container for each search result item
SEARCH_PAGE_ARTICLE_CONTAINER_SELECTOR = 'div.search-page__post-item'
# Fallback container for search results (can be None if primary is reliable)
SEARCH_PAGE_ARTICLE_CONTAINER_SELECTOR_FALLBACK = 'div[data-testid="search-article"]' 
# Selector for the <a> tag within h2.header that contains the link and the title's <span>
SEARCH_PAGE_LINK_SELECTOR = 'h2.header a[href]'
# Selector for the <time> tag containing the date text (text content, not 'datetime' attribute)
SEARCH_PAGE_DATE_SELECTOR = 'time.date'
# Selector for the <span> tag (within the link_tag from SEARCH_PAGE_LINK_SELECTOR) that holds the title text
SEARCH_PAGE_TITLE_IN_LINK_SELECTOR = 'span' 


# --- Helper Functions ---

def load_existing_urls(filename, source_filter):
    """Loads existing URLs from the CSV file for a specific source to avoid duplicates."""
    existing_urls = set()
    if not os.path.exists(filename) or os.path.getsize(filename) == 0:
        try:
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile_init:
                writer = csv.DictWriter(csvfile_init, fieldnames=HEADERS)
                writer.writeheader()
            print(f"Initialized CSV file '{filename}' with headers.")
        except IOError as e:
            print(f"Error initializing CSV file '{filename}': {e}")
        return existing_urls
        
    try:
        with open(filename, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            if not reader.fieldnames or not all(header in reader.fieldnames for header in ['url', 'source']):
                print(f"Warning: CSV file '{filename}' is missing required columns. Cannot load existing URLs for '{source_filter}'.")
                return existing_urls
            for row in reader:
                if row and row.get('source') == source_filter and row.get('url'):
                    existing_urls.add(row['url'])
        print(f"Loaded {len(existing_urls)} existing URLs for source '{source_filter}' from '{filename}'.")
    except Exception as e:
        print(f"Error reading CSV file '{filename}' for '{source_filter}': {e}.")
    return existing_urls

def setup_driver():
    """Sets up and returns a headless Chrome WebDriver instance."""
    print("Setting up Chrome WebDriver (Headless Mode)...")
    try:
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--log-level=3') # Suppress non-critical console logs from WebDriver
        options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36')
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        service = ChromeService(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})") # Attempt to bypass bot detection
        print("WebDriver setup complete.")
        return driver
    except Exception as e:
        print(f"Error setting up WebDriver: {e}")
        return None

def click_accept_button(driver, selectors, timeout):
    """Attempts to find and click an "Accept Cookies" or similar button."""
    print("Checking for and attempting to click Accept button...")
    button_clicked = False
    for by, selector_value in selectors:
        try:
            accept_button = WebDriverWait(driver, timeout).until(
                EC.element_to_be_clickable((by, selector_value))
            )
            driver.execute_script("arguments[0].scrollIntoView(true);", accept_button) # Scroll into view
            time.sleep(0.5) # Brief pause after scroll
            accept_button.click() 
            print(f"Successfully clicked Accept button using selector: {by}='{selector_value}'")
            time.sleep(1) # Pause for any overlay to disappear
            button_clicked = True
            break 
        except ElementClickInterceptedException:
             print(f"Accept button click intercepted for {by}='{selector_value}'. Trying JS click.")
             try:
                 intercepted_button = driver.find_element(by, selector_value) 
                 driver.execute_script("arguments[0].click();", intercepted_button) # JavaScript click fallback
                 print(f"Successfully clicked Accept button using JavaScript fallback.")
                 time.sleep(1)
                 button_clicked = True
                 break
             except Exception as js_e:
                 print(f"JavaScript click also failed for {by}='{selector_value}': {js_e}")
                 continue
        except TimeoutException:
            # print(f"Timeout for Accept button selector: {by}='{selector_value}'") # Less verbose
            continue
        except Exception as e:
            print(f"An error occurred trying to click Accept button with {by}='{selector_value}': {e}")
            continue
    if not button_clicked:
        print("Could not find or click the Accept button (or it wasn't present).")
    return button_clicked


def fetch_page_source_with_selenium(driver, url, wait_selector, fallback_selector, timeout_val):
    """Fetches page source using Selenium, handling scrolling and waiting for elements."""
    print(f"Fetching data from: {url} using Selenium...")
    page_source = None
    current_selector_used = wait_selector # Assume primary selector will be used
    try:
        driver.get(url)
        click_accept_button(driver, ACCEPT_BUTTON_SELECTORS, ACCEPT_BUTTON_TIMEOUT_SECONDS)

        print(f"Waiting up to {timeout_val}s for elements matching: '{wait_selector}'")
        try:
            WebDriverWait(driver, timeout_val).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, wait_selector))
            )
            print(f"Initial elements loaded with primary selector: '{wait_selector}'.")
        except TimeoutException:
            print(f"Timeout for primary selector '{wait_selector}'. Trying fallback '{fallback_selector}'...")
            if fallback_selector: # Only try if a fallback is provided
                try:
                     WebDriverWait(driver, 5).until( # Shorter timeout for fallback
                        EC.presence_of_all_elements_located((By.CSS_SELECTOR, fallback_selector))
                     )
                     print(f"Initial elements loaded with fallback selector: '{fallback_selector}'.")
                     current_selector_used = fallback_selector # Update to reflect fallback was used
                except TimeoutException:
                     print(f"Timeout for fallback selector '{fallback_selector}' too.")
                     print("Attempting scroll, but extraction may fail if no key elements loaded.")
            else:
                print("No fallback selector provided. Proceeding with scroll.")
        
        print(f"Scrolling {SCROLL_ATTEMPTS} times...")
        last_h = driver.execute_script("return document.body.scrollHeight")
        for i in range(SCROLL_ATTEMPTS):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(SCROLL_PAUSE_TIME)
            new_h = driver.execute_script("return document.body.scrollHeight")
            if new_h == last_h: 
                print(f"Scrolling stopped early at attempt {i+1} as height did not change.")
                break
            last_h = new_h
        time.sleep(2) # Final pause after scrolling
        print("Scrolling done.")
        page_source = driver.page_source
        print("Page source retrieved.")
    except WebDriverException as e:
        print(f"WebDriver error during page fetch for {url}: {e}")
    except Exception as e:
        print(f"Unexpected error in Selenium fetching for {url}: {e}")
    return page_source, current_selector_used


def extract_articles(page_source, effective_container_selector, fallback_container_selector, base_url_val,
                     link_selector_css, date_selector_css, title_in_link_selector_css=None):
    """Extracts article details from page source using provided CSS selectors."""
    if not page_source:
        print("No page source provided to extract_articles.")
        return []
    articles = []
    soup = BeautifulSoup(page_source, 'html.parser')
    
    # Attempt to find articles using the primary container selector
    article_elements = soup.select(effective_container_selector)
    print(f"Extracting with container selector '{effective_container_selector}'. Found {len(article_elements)} potential article elements.")

    # If no articles found with primary, try fallback container selector (if provided)
    if not article_elements and fallback_container_selector:
        print(f"No articles found with '{effective_container_selector}'. Trying fallback container selector '{fallback_container_selector}'...")
        article_elements = soup.select(fallback_container_selector)
        print(f"Found {len(article_elements)} potential article elements with fallback container selector.")
        if not article_elements:
            print(f"No articles found with fallback container selector either. Debug HTML if issues persist.")
            # Consider saving HTML for debugging:
            # with open(f"debug_extract_failed_{time.time()}.html", "w", encoding="utf-8") as f_debug:
            #    f_debug.write(page_source)
            return []
    elif not article_elements:
        print(f"No articles found with '{effective_container_selector}' and no fallback provided.")
        return []


    extracted_count = 0
    for i, element in enumerate(article_elements):
        try:
            link_tag = element.select_one(link_selector_css)
            date_tag = element.select_one(date_selector_css)
            
            date_str = None
            if date_tag:
                if date_tag.get('datetime'):  # Primarily for tag pages with 'datetime' attribute
                    date_str = date_tag.get('datetime')
                else:  # Fallback for search results using text content of <time> tag
                    date_str = date_tag.get_text(strip=True)

            if link_tag and link_tag.get('href') and date_str:
                relative_url = link_tag['href']
                # Construct full URL carefully
                if relative_url.startswith('//'):
                    full_url = "https:" + relative_url
                elif relative_url.startswith('/'):
                    full_url = base_url_val + relative_url
                else:
                    full_url = relative_url # Assume it's already a full URL
                
                title_text = ""
                # Title extraction:
                # For search pages, title is in a span directly within the link_tag
                # For tag pages, it might be in a specific span or the link_tag itself
                if title_in_link_selector_css:
                    title_element = link_tag.select_one(title_in_link_selector_css)
                    if title_element:
                        title_text = title_element.get_text(strip=True) # Gets text from <span> including <em>
                
                if not title_text: # Fallback to the link_tag's direct text if specific title element not found/specified
                    title_text = link_tag.get_text(strip=True) 
                
                title = title_text.strip() # Ensure no leading/trailing whitespace

                if not full_url or not title: 
                    # print(f"Debug (Element {i}): Skipping - missing full_url or title. URL: '{full_url}', Title: '{title}'")
                    continue

                try:
                    # dateutil.parser.parse is robust for various formats like "May 19, 2025" or ISO
                    # FIX: Add default=datetime.now(timezone.utc) for relative date parsing
                    parsed_dt_obj = date_parser.parse(date_str, default=datetime.now(timezone.utc))
                    # Convert to UTC if naive, or ensure it's UTC
                    if parsed_dt_obj.tzinfo is None or parsed_dt_obj.tzinfo.utcoffset(parsed_dt_obj) is None:
                        dt_utc = parsed_dt_obj.replace(tzinfo=timezone.utc) 
                    else:
                        dt_utc = parsed_dt_obj.astimezone(timezone.utc)
                    
                    articles.append({
                        'url': full_url,
                        'title': title,
                        'parsed_date_utc': dt_utc 
                    })
                    extracted_count += 1
                except (ValueError, date_parser.ParserError, OverflowError, TypeError) as e:
                    print(f"Warning (Element {i}): Could not parse date: '{date_str}' for title '{title}'. Error: {e}")
            # else: # Debugging for missing critical info
            #     debug_missing = []
            #     if not link_tag: debug_missing.append(f"link_tag (selector: {link_selector_css})")
            #     elif not link_tag.get('href'): debug_missing.append("link_href")
            #     if not date_tag: debug_missing.append(f"date_tag (selector: {date_selector_css})")
            #     elif not date_str: debug_missing.append("date_str (parsed from date_tag)")
            #     # print(f"Debug (Element {i}): Skipping - missing: {', '.join(debug_missing)}. Element HTML (partial): {str(element)[:200]}")


        except AttributeError as e:
            print(f"Debug (Element {i}): Skipping due to AttributeError (likely structure mismatch): {e}. Element HTML (partial): {str(element)[:200]}")
        except Exception as e:
            print(f"Error processing an article element (Element {i}): {e}")

    print(f"Successfully extracted details for {extracted_count} out of {len(article_elements)} processed article elements using container '{effective_container_selector}'.")
    return articles

def append_to_csv(filename, articles_data_list, headers_config, source_id):
    """Appends new, valid articles to the CSV file, sorted by date."""
    file_exists = os.path.exists(filename)
    is_empty_or_new_file = not file_exists or os.path.getsize(filename) == 0

    valid_articles_for_csv_write = []
    for article_item in articles_data_list:
        # Ensure essential data is present, especially the parsed_date_utc
        if article_item.get('url') and article_item.get('title') and article_item.get('parsed_date_utc') and isinstance(article_item['parsed_date_utc'], datetime):
            valid_articles_for_csv_write.append(article_item)
    
    if not valid_articles_for_csv_write:
        print(f"No valid new articles with all required data (URL, Title, Date) to append for {source_id}.")
        return

    # Sort articles by date before writing
    valid_articles_for_csv_write.sort(key=lambda x: x['parsed_date_utc'])

    try:
        with open(filename, 'a', newline='', encoding='utf-8') as csv_file_handle:
            writer_obj = csv.DictWriter(csv_file_handle, fieldnames=headers_config)
            if is_empty_or_new_file: # Write header if file is new or empty
                writer_obj.writeheader()
                print(f"Wrote header to '{filename}' for {source_id}.")
            
            num_appended = 0
            for article_item in valid_articles_for_csv_write:
                # Format date to ISO 8601 UTC for CSV
                iso_utc_date_string = article_item['parsed_date_utc'].strftime('%Y-%m-%dT%H:%M:%S+00:00')
                csv_row = {
                    'date': iso_utc_date_string,
                    'source': source_id,
                    'url': article_item['url'],
                    'title': article_item['title'],
                    'done': '' # 'done' field is initially empty
                }
                writer_obj.writerow(csv_row)
                num_appended += 1
            print(f"Appended {num_appended} new articles for '{source_id}' to '{filename}'.")
    except IOError as e_io:
        print(f"IOError writing to CSV '{filename}' for {source_id}: {e_io}")
    except Exception as e_gen:
        print(f"Unexpected error during CSV writing for '{source_id}': {e_gen}")


# --- Main Execution ---
if __name__ == "__main__":
    start_time = time.time()
    print(f"--- Starting CoinTelegraph Scraper ({SOURCE_NAME}, Date Format UTC) ---")
    driver_instance = None
    try:
        driver_instance = setup_driver()
        if not driver_instance:
            raise Exception("WebDriver setup failed. Exiting.")

        existing_article_urls = load_existing_urls(CSV_FILENAME, SOURCE_NAME)
        combined_extracted_data = []

        # 1. Conditionally process main tag search (using TAG page selectors)
        if ENABLE_TAG_SEARCH:
            print(f"\n--- Processing Main Tag URL: {URL} ---")
            main_page_source, main_effective_selector = fetch_page_source_with_selenium(
                driver_instance, URL, 
                TAG_PAGE_ARTICLE_CONTAINER_SELECTOR, 
                TAG_PAGE_ARTICLE_CONTAINER_SELECTOR_FALLBACK, 
                SELENIUM_TIMEOUT_SECONDS
            )
            if main_page_source:
                main_articles = extract_articles(
                    main_page_source, main_effective_selector, # Pass the selector actually used by fetch_page
                    TAG_PAGE_ARTICLE_CONTAINER_SELECTOR_FALLBACK, # Still pass fallback for extract_articles' internal logic
                    BASE_URL,
                    TAG_PAGE_LINK_SELECTOR,
                    TAG_PAGE_DATE_SELECTOR,
                    TAG_PAGE_TITLE_IN_LINK_SELECTOR
                )
                combined_extracted_data.extend(main_articles)
            else:
                print(f"Could not retrieve page source for main URL: {URL}")
        else:
            print("\nSkipping main tag search as per configuration (ENABLE_TAG_SEARCH=False).")

        # 2. Process additional search queries (using SEARCH page selectors)
        additional_queries = [
            "https://cointelegraph.com/search?query=australian",
            "https://cointelegraph.com/search?query=australia"
        ]
        print(f"\n--- Processing {len(additional_queries)} Additional Search Queries ---")
        for query_url in additional_queries:
            print(f"\nProcessing search query: {query_url}")
            query_page_source, query_effective_selector = fetch_page_source_with_selenium(
                driver_instance, query_url, 
                SEARCH_PAGE_ARTICLE_CONTAINER_SELECTOR, 
                SEARCH_PAGE_ARTICLE_CONTAINER_SELECTOR_FALLBACK, 
                SELENIUM_TIMEOUT_SECONDS
            )
            if query_page_source:
                query_articles = extract_articles(
                    query_page_source, query_effective_selector, # Pass the selector actually used by fetch_page
                    SEARCH_PAGE_ARTICLE_CONTAINER_SELECTOR_FALLBACK, # Still pass fallback for extract_articles
                    BASE_URL,
                    SEARCH_PAGE_LINK_SELECTOR,         
                    SEARCH_PAGE_DATE_SELECTOR,         
                    SEARCH_PAGE_TITLE_IN_LINK_SELECTOR 
                )
                combined_extracted_data.extend(query_articles)
            else:
                print(f"Could not retrieve page source for query: {query_url}")
        
        print(f"\n--- Filtering and CSV Appending ---")
        print(f"Found {len(combined_extracted_data)} articles in total from scraping before filtering.")

        # Filter out articles that are duplicates or older than MIN_ARTICLE_YEAR
        articles_to_add_to_csv = []
        num_filtered_out = 0 # Renamed for clarity
        
        # Deduplicate based on URL from combined_extracted_data first
        # Ensure art_data has 'url' and 'parsed_date_utc' and 'title' before adding to unique_articles_by_url
        unique_articles_by_url_dict = {}
        for art in combined_extracted_data:
            if art.get('url') and art.get('parsed_date_utc') and art.get('title'):
                 unique_articles_by_url_dict[art['url']] = art # Overwrites duplicates, keeping the last seen
        
        unique_articles_by_url = list(unique_articles_by_url_dict.values())
        print(f"Reduced to {len(unique_articles_by_url)} unique articles by URL before further filtering.")


        for art_data in unique_articles_by_url: # Iterate over de-duplicated articles
            # Check 1: Not already in CSV
            if art_data['url'] not in existing_article_urls:
                # Check 2: Meets minimum year requirement
                if art_data['parsed_date_utc'].year >= MIN_ARTICLE_YEAR:
                    # Check 3: Title contains one of the keywords (case-insensitive)
                    title_lower = art_data['title'].lower()
                    if any(keyword.lower() in title_lower for keyword in TITLE_KEYWORDS):
                        articles_to_add_to_csv.append(art_data)
                    else:
                        # print(f"Filtered out by title keyword: '{art_data['title'][:60]}...'") # Optional: for debugging
                        num_filtered_out += 1
                else:
                    # print(f"Filtered out by year: {art_data['parsed_date_utc'].year} < {MIN_ARTICLE_YEAR} - {art_data['title'][:60]}...")
                    num_filtered_out += 1
            else:
                num_filtered_out += 1
        
        print(f"Found {len(articles_to_add_to_csv)} new articles matching all criteria (year >= {MIN_ARTICLE_YEAR}, non-duplicate, title keywords).")
        print(f"Filtered out {num_filtered_out} articles (already existing, older, or no title keyword).")


        if articles_to_add_to_csv:
            append_to_csv(CSV_FILENAME, articles_to_add_to_csv, HEADERS, SOURCE_NAME)
        else:
            print(f"No new valid articles found to append for {SOURCE_NAME} matching all criteria.")
            
    except Exception as main_exec_e:
        print(f"An critical error occurred in the main execution for {SOURCE_NAME}: {main_exec_e}")
    finally:
        if driver_instance:
            print(f"\nClosing browser for {SOURCE_NAME}...")
            driver_instance.quit()
            print(f"Browser closed for {SOURCE_NAME}.")
    end_time = time.time()
    print(f"--- CoinTelegraph Scraper Finished ({SOURCE_NAME}) in {end_time - start_time:.2f} seconds ---")