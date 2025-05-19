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
URL = "https://cointelegraph.com/tags/australia"
CSV_FILENAME = "articles.csv"
SOURCE_NAME = "cointelegraph.com"
BASE_URL = "https://cointelegraph.com"
HEADERS = ['date', 'source', 'url', 'title', 'done']
SELENIUM_TIMEOUT_SECONDS = 25
SCROLL_PAUSE_TIME = 2
SCROLL_ATTEMPTS = 5
ACCEPT_BUTTON_TIMEOUT_SECONDS = 10

ARTICLE_CONTAINER_SELECTOR = 'article.post-card-inline'
ARTICLE_CONTAINER_SELECTOR_FALLBACK = 'li.posts-listing__item'
ACCEPT_BUTTON_SELECTORS = [
    (By.ID, "CybotCookiebotDialogBodyButtonAccept"), # Specific to Cointelegraph sometimes
    (By.ID, "onetrust-accept-btn-handler"),
    (By.XPATH, "//button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'accept')]"),
    (By.CSS_SELECTOR, 'button[data-testid*="accept"], button[aria-label*="Accept"]')
]

# --- Helper Functions --- (load_existing_urls, setup_driver, click_accept_button, fetch_page_source_with_selenium are similar to coindesk.py)

def load_existing_urls(filename, source_filter):
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
    print("Setting up Chrome WebDriver (Headless Mode)...")
    try:
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--log-level=3')
        options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36')
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        service = ChromeService(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        print("WebDriver setup complete.")
        return driver
    except Exception as e:
        print(f"Error setting up WebDriver: {e}")
        return None

def click_accept_button(driver, selectors, timeout):
    # (Identical to coindesk.py's click_accept_button - no date changes)
    print("Checking for and attempting to click Accept button...")
    button_clicked = False
    for by, selector_value in selectors:
        try:
            accept_button = WebDriverWait(driver, timeout).until(
                EC.element_to_be_clickable((by, selector_value))
            )
            # Scroll into view and click with JS if direct click is intercepted
            driver.execute_script("arguments[0].scrollIntoView(true);", accept_button)
            time.sleep(0.5)
            accept_button.click() # Try direct click first
            print(f"Successfully clicked Accept button using selector: {by}='{selector_value}'")
            time.sleep(1) 
            button_clicked = True
            break 
        except ElementClickInterceptedException:
             print(f"Accept button click intercepted for {by}='{selector_value}'. Trying JS click.")
             try:
                 intercepted_button = driver.find_element(by, selector_value) # Re-find
                 driver.execute_script("arguments[0].click();", intercepted_button)
                 print(f"Successfully clicked Accept button using JavaScript fallback.")
                 time.sleep(1)
                 button_clicked = True
                 break
             except Exception as js_e:
                 print(f"JavaScript click also failed for {by}='{selector_value}': {js_e}")
                 continue
        except TimeoutException:
            # print(f"Timeout for Accept button selector: {by}='{selector_value}'")
            continue
        except Exception as e:
            print(f"An error occurred trying to click Accept button with {by}='{selector_value}': {e}")
            continue
    if not button_clicked:
        print("Could not find or click the Accept button (or it wasn't present).")
    return button_clicked


def fetch_page_source_with_selenium(driver, url, wait_selector, fallback_selector, timeout_val):
    # (Identical to coindesk.py's function - no date changes)
    print(f"Fetching data from: {url} using Selenium...")
    page_source = None
    current_selector_used = wait_selector
    try:
        driver.get(url)
        click_accept_button(driver, ACCEPT_BUTTON_SELECTORS, ACCEPT_BUTTON_TIMEOUT_SECONDS)

        print(f"Waiting up to {timeout_val}s for elements matching: '{wait_selector}'")
        try:
            WebDriverWait(driver, timeout_val).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, wait_selector))
            )
            print(f"Initial elements loaded with '{wait_selector}'.")
        except TimeoutException:
            print(f"Timeout for '{wait_selector}'. Trying fallback '{fallback_selector}'...")
            try:
                 WebDriverWait(driver, 5).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, fallback_selector))
                 )
                 print(f"Initial elements loaded with fallback '{fallback_selector}'.")
                 current_selector_used = fallback_selector
            except TimeoutException:
                 print(f"Timeout for fallback selector '{fallback_selector}' too.")
                 print("Attempting scroll, but extraction may fail.")
        
        print(f"Scrolling {SCROLL_ATTEMPTS} times...")
        last_h = driver.execute_script("return document.body.scrollHeight")
        for _ in range(SCROLL_ATTEMPTS):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(SCROLL_PAUSE_TIME)
            new_h = driver.execute_script("return document.body.scrollHeight")
            if new_h == last_h: break
            last_h = new_h
        time.sleep(2)
        print("Scrolling done.")
        page_source = driver.page_source
        print("Page source retrieved.")
    except WebDriverException as e:
        print(f"WebDriver error: {e}")
    except Exception as e:
        print(f"Unexpected error in Selenium fetching: {e}")
    return page_source, current_selector_used


def extract_articles(page_source, effective_selector, fallback_selector_val, base_url_val):
    if not page_source:
        print("No page source for extract_articles.")
        return []
    articles = []
    soup = BeautifulSoup(page_source, 'html.parser')
    article_elements = soup.select(effective_selector)
    print(f"Extracting with '{effective_selector}'. Found {len(article_elements)} elements.")

    if not article_elements:
        print(f"No articles with '{effective_selector}'. Trying fallback '{fallback_selector_val}'...")
        article_elements = soup.select(fallback_selector_val)
        print(f"Found {len(article_elements)} with fallback.")
        if not article_elements:
            # (Debug save logic as before)
            return []

    extracted_count = 0
    for element in article_elements:
        try:
            link_tag = element.select_one('a.post-card-inline__title-link[href]')
            date_tag = element.select_one('time.post-card-inline__date[datetime]')
            date_str = date_tag.get('datetime') if date_tag else None

            if link_tag and link_tag.get('href') and date_str:
                relative_url = link_tag['href']
                full_url = base_url_val + relative_url if relative_url.startswith('/') else relative_url
                
                title_span = link_tag.select_one('span.post-card-inline__title')
                title = title_span.get_text(strip=True) if title_span else link_tag.get_text(strip=True)

                if not full_url or not title: continue

                try:
                    # dateutil.parser.parse is robust for various ISO-like formats
                    parsed_dt_obj = date_parser.parse(date_str)
                    # Convert to UTC
                    if parsed_dt_obj.tzinfo is None:
                        dt_utc = parsed_dt_obj.replace(tzinfo=timezone.utc) # Assume UTC if naive
                    else:
                        dt_utc = parsed_dt_obj.astimezone(timezone.utc)
                    
                    articles.append({
                        'url': full_url,
                        'title': title,
                        'parsed_date_utc': dt_utc # Store UTC datetime object
                    })
                    extracted_count += 1
                except (ValueError, date_parser.ParserError, OverflowError, TypeError) as e:
                    print(f"Warning: Could not parse date: '{date_str}' for '{title}'. Error: {e}")
            # else:
                # print(f"Debug: Skipping element - missing link or date. Link: {link_tag is not None}, Date: {date_str}")

        except AttributeError as e:
            print(f"Debug: Skipping element due to AttributeError (structure mismatch): {e}")
        except Exception as e:
            print(f"Error processing an article element: {e}")

    print(f"Successfully extracted details for {extracted_count} articles.")
    return articles

def append_to_csv(filename, articles_data_list, headers_config, source_id):
    # (This function is identical to coindesk.py's append_to_csv - no date changes here, date is formatted before call)
    file_exists = os.path.exists(filename)
    is_empty_or_new_file = not file_exists or os.path.getsize(filename) == 0

    valid_articles_for_csv_write = []
    for article_item in articles_data_list:
        if article_item.get('parsed_date_utc') and isinstance(article_item['parsed_date_utc'], datetime):
            valid_articles_for_csv_write.append(article_item)
    
    if not valid_articles_for_csv_write:
        print(f"No valid new articles with dates to append for {source_id}.")
        return

    valid_articles_for_csv_write.sort(key=lambda x: x['parsed_date_utc'])

    # print(f"--- Articles for {source_id} to append (Sorted) ---")
    # for article_item in valid_articles_for_csv_write:
    #     print(f"- {article_item['parsed_date_utc'].strftime('%Y-%m-%d')}: {article_item['title'][:60]}...")
    # print("-------------------------------------------")

    try:
        with open(filename, 'a', newline='', encoding='utf-8') as csv_file_handle:
            writer_obj = csv.DictWriter(csv_file_handle, fieldnames=headers_config)
            if is_empty_or_new_file:
                writer_obj.writeheader()
                print(f"Wrote header to '{filename}' for {source_id}.")
            
            num_appended = 0
            for article_item in valid_articles_for_csv_write:
                iso_utc_date_string = article_item['parsed_date_utc'].strftime('%Y-%m-%dT%H:%M:%S+00:00')
                csv_row = {
                    'date': iso_utc_date_string,
                    'source': source_id,
                    'url': article_item['url'],
                    'title': article_item['title'],
                    'done': ''
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
    print(f"--- Starting CoinTelegraph Scraper ({SOURCE_NAME}, Date Format UTC) ---")
    driver_instance = None
    try:
        driver_instance = setup_driver()
        if not driver_instance:
            raise Exception("WebDriver setup failed.")

        existing_article_urls = load_existing_urls(CSV_FILENAME, SOURCE_NAME)
        html_page_source, selector_that_worked = fetch_page_source_with_selenium(
            driver_instance, URL, ARTICLE_CONTAINER_SELECTOR,
            ARTICLE_CONTAINER_SELECTOR_FALLBACK, SELENIUM_TIMEOUT_SECONDS
        )

        if html_page_source:
            all_extracted_data = extract_articles(
                html_page_source, selector_that_worked,
                ARTICLE_CONTAINER_SELECTOR_FALLBACK, # Pass fallback again
                BASE_URL
            )
            
            articles_to_add_to_csv = []
            num_duplicates = 0
            MIN_ARTICLE_YEAR = 2025

            for art_data in all_extracted_data:
                if art_data.get('url') and art_data.get('parsed_date_utc'):
                    if art_data['url'] not in existing_article_urls:
                        if art_data['parsed_date_utc'].year >= MIN_ARTICLE_YEAR:
                            articles_to_add_to_csv.append(art_data)
                        # else:
                            # print(f"Skipping old article (Year {art_data['parsed_date_utc'].year}): {art_data['url']}")
                    else:
                        num_duplicates += 1
            
            print(f"Found {len(articles_to_add_to_csv)} new articles (>= {MIN_ARTICLE_YEAR}) for {SOURCE_NAME} (filtered out {num_duplicates} existing/old).")

            if articles_to_add_to_csv:
                append_to_csv(CSV_FILENAME, articles_to_add_to_csv, HEADERS, SOURCE_NAME)
            else:
                print(f"No new valid articles found for {SOURCE_NAME} for 2025 onward.")
        else:
            print(f"Could not retrieve page source for {SOURCE_NAME}. Exiting.")

    except Exception as main_exec_e:
        print(f"An error occurred in the main execution for {SOURCE_NAME}: {main_exec_e}")
    finally:
        if driver_instance:
            print(f"Closing browser for {SOURCE_NAME}...")
            driver_instance.quit()
            print(f"Browser closed for {SOURCE_NAME}.")
    print(f"--- CoinTelegraph Scraper Finished ({SOURCE_NAME}) ---")
