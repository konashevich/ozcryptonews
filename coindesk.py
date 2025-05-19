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
URL = "https://www.coindesk.com/tag/australia"
CSV_FILENAME = "articles.csv"
SOURCE_NAME = "coindesk.com"
HEADERS = ['date', 'source', 'url', 'title', 'done']
SELENIUM_TIMEOUT_SECONDS = 25
SCROLL_PAUSE_TIME = 2
SCROLL_ATTEMPTS = 5
ACCEPT_BUTTON_TIMEOUT_SECONDS = 10

ARTICLE_CONTAINER_SELECTOR = 'div.bg-white.flex.gap-6.w-full.shrink.justify-between'
ARTICLE_CONTAINER_SELECTOR_FALLBACK = 'div.flex.flex-col.gap-4'
ACCEPT_BUTTON_SELECTORS = [
    (By.ID, "onetrust-accept-btn-handler"),
    (By.XPATH, "//button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'accept')]"),
    (By.CSS_SELECTOR, 'button[data-testid*="accept"], button[aria-label*="Accept"]')
]

# --- Helper Functions ---

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
                print(f"Warning: CSV file '{filename}' is missing required columns. Cannot load existing URLs.")
                return existing_urls
            for row in reader:
                if row and row.get('source') == source_filter and row.get('url'):
                    existing_urls.add(row['url'])
        print(f"Loaded {len(existing_urls)} existing URLs for source '{source_filter}' from '{filename}'.")
    except Exception as e:
        print(f"Error reading CSV file '{filename}': {e}. Check file encoding and format.")
    return existing_urls

def setup_driver():
    print("Setting up Chrome WebDriver...")
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
    # (No changes needed in this function for date formatting)
    print("Checking for and attempting to click Accept button...")
    button_clicked = False
    for by, selector_value in selectors:
        try:
            accept_button = WebDriverWait(driver, timeout).until(
                EC.element_to_be_clickable((by, selector_value))
            )
            driver.execute_script("arguments[0].scrollIntoView(true);", accept_button)
            time.sleep(0.5) # Brief pause after scroll
            accept_button.click()
            print(f"Successfully clicked Accept button using selector: {by}='{selector_value}'")
            time.sleep(1) # Wait for overlay to disappear
            button_clicked = True
            break 
        except TimeoutException:
            continue
        except ElementClickInterceptedException:
             print(f"Accept button click intercepted for {by}='{selector_value}'. Trying JS click.")
             try:
                 # Re-find element before JS click to ensure it's the same one
                 intercepted_button = driver.find_element(by, selector_value)
                 driver.execute_script("arguments[0].click();", intercepted_button)
                 print(f"Successfully clicked Accept button using JavaScript fallback.")
                 time.sleep(1)
                 button_clicked = True
                 break
             except Exception as js_e:
                 print(f"JavaScript click also failed for {by}='{selector_value}': {js_e}")
                 continue
        except Exception as e:
            print(f"An error occurred trying to click Accept button with {by}='{selector_value}': {e}")
            continue
    if not button_clicked:
        print("Could not find or click the Accept button (or it wasn't present).")
    return button_clicked


def fetch_page_source_with_selenium(driver, url, wait_selector, fallback_selector, timeout):
    # (No changes needed in this function for date formatting)
    print(f"Fetching data from: {url} using Selenium...")
    page_source = None
    primary_selector_used = wait_selector 
    try:
        driver.get(url)
        click_accept_button(driver, ACCEPT_BUTTON_SELECTORS, ACCEPT_BUTTON_TIMEOUT_SECONDS)

        print(f"Waiting up to {timeout} seconds for initial elements matching selector: '{wait_selector}'")
        try:
            WebDriverWait(driver, timeout).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, wait_selector))
            )
            print(f"Initial article element(s) potentially loaded using '{wait_selector}'.")
        except TimeoutException:
            print(f"Timeout waiting for primary selector '{wait_selector}'. Trying fallback '{fallback_selector}'...")
            try:
                 WebDriverWait(driver, 5).until( # Shorter wait for fallback
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, fallback_selector))
                 )
                 print(f"Initial fallback element(s) potentially loaded using '{fallback_selector}'.")
                 primary_selector_used = fallback_selector 
            except TimeoutException:
                 print(f"Timeout waiting for fallback selector '{fallback_selector}' as well.")
                 print("Attempting scrolling, but extraction might fail if elements don't load.")

        print(f"Attempting to scroll down {SCROLL_ATTEMPTS} times...")
        last_height = driver.execute_script("return document.body.scrollHeight")
        for i in range(SCROLL_ATTEMPTS):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(SCROLL_PAUSE_TIME)
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height
        time.sleep(2) # Final pause after scrolling
        print("Scrolling finished.")
        page_source = driver.page_source
        print("Page source retrieved.")
    except WebDriverException as e:
        print(f"WebDriver error during Selenium processing: {e}")
    except Exception as e:
        print(f"Unexpected error during Selenium fetching: {e}")
    return page_source, primary_selector_used


def extract_articles(page_source, effective_selector, fallback_selector, base_url="https://www.coindesk.com"):
    if not page_source:
        print("No page source provided to extract_articles.")
        return []
    articles = []
    soup = BeautifulSoup(page_source, 'html.parser')
    article_elements = soup.select(effective_selector)
    print(f"Attempting extraction with selector '{effective_selector}'. Found {len(article_elements)} elements.")

    if not article_elements:
        print(f"No articles with '{effective_selector}'. Trying fallback '{fallback_selector}'...")
        article_elements = soup.select(fallback_selector)
        print(f"Found {len(article_elements)} elements with fallback selector.")
        if not article_elements:
            # Save debug HTML if still no articles
            # ... (debug save logic as before)
            return []

    extracted_count = 0
    for element in article_elements:
        try:
            link_tag = element.select_one('a[class*="text-color-charcoal-900"][href]')
            date_container = element.select_one('p.flex.gap-2.flex-col') # More specific
            date_str = None
            if date_container:
                date_span = date_container.select_one('span.font-metadata.text-color-charcoal-600') # More specific
                if date_span:
                    date_str = date_span.get_text(strip=True)
            
            # Fallback for date if specific span not found, try <time> tag within element
            if not date_str:
                time_tag_fallback = element.select_one('time[datetime]')
                if time_tag_fallback and time_tag_fallback.get('datetime'):
                    date_str = time_tag_fallback['datetime'] # Use the datetime attribute value
            
            if link_tag and link_tag.get('href') and date_str:
                relative_url = link_tag['href']
                full_url = base_url + relative_url if relative_url.startswith('/') else relative_url
                
                title_tag_h2 = link_tag.select_one('h2') # Prefer h2 if present
                title = title_tag_h2.get_text(strip=True) if title_tag_h2 else link_tag.get_text(strip=True)

                if not full_url or not title: continue

                try:
                    # Parse date string. date_parser.parse is good at handling various formats.
                    parsed_dt_obj = date_parser.parse(date_str)
                    # Convert to UTC
                    if parsed_dt_obj.tzinfo is None: # If naive
                        dt_utc = parsed_dt_obj.replace(tzinfo=timezone.utc) # Assume UTC
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
                # print(f"Debug: Skipping element - missing link_tag or date_str. Link: {link_tag is not None}, Date: {date_str}")


        except AttributeError as e:
            print(f"Debug: Skipping element due to AttributeError (structure mismatch): {e}")
        except Exception as e:
            print(f"Error processing an article element: {e}")
    
    print(f"Successfully extracted details for {extracted_count} articles from {len(article_elements)} potential elements.")
    return articles


def append_to_csv(filename, articles_data, headers_list, source_name_val):
    file_exists = os.path.exists(filename)
    is_empty_or_new = not file_exists or os.path.getsize(filename) == 0

    valid_articles_for_csv = []
    for article in articles_data:
        if article.get('parsed_date_utc') and isinstance(article['parsed_date_utc'], datetime):
            valid_articles_for_csv.append(article)
    
    if not valid_articles_for_csv:
        print("No valid new articles with dates to append.")
        return

    valid_articles_for_csv.sort(key=lambda x: x['parsed_date_utc']) # Sort by datetime object

    print(f"--- Articles to be appended for {source_name_val} (Sorted Chronologically) ---")
    # for article in valid_articles_for_csv:
    #     print(f"- {article['parsed_date_utc'].strftime('%Y-%m-%d')}: {article['title'][:60]}...")
    # print("-------------------------------------------------------")

    try:
        with open(filename, 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers_list)
            if is_empty_or_new:
                writer.writeheader()
                print(f"Wrote header to '{filename}'.")
            
            appended_count = 0
            for article in valid_articles_for_csv:
                iso_date_utc_str = article['parsed_date_utc'].strftime('%Y-%m-%dT%H:%M:%S+00:00')
                row = {
                    'date': iso_date_utc_str,
                    'source': source_name_val,
                    'url': article['url'],
                    'title': article['title'],
                    'done': ''
                }
                writer.writerow(row)
                appended_count += 1
            print(f"Appended {appended_count} new articles for '{source_name_val}' to '{filename}'.")
    except IOError as e:
        print(f"Error writing to CSV '{filename}': {e}")
    except Exception as e:
        print(f"Unexpected error during CSV writing for '{source_name_val}': {e}")


# --- Main Execution ---
if __name__ == "__main__":
    print("--- Starting CoinDesk Scraper (Date Format UTC) ---")
    driver = None
    try:
        driver = setup_driver()
        if not driver:
            raise Exception("WebDriver setup failed.")

        existing_urls = load_existing_urls(CSV_FILENAME, SOURCE_NAME)
        page_source, effective_selector_used = fetch_page_source_with_selenium(
            driver, URL, ARTICLE_CONTAINER_SELECTOR, 
            ARTICLE_CONTAINER_SELECTOR_FALLBACK, SELENIUM_TIMEOUT_SECONDS
        )

        if page_source:
            all_extracted = extract_articles(
                page_source, effective_selector_used, 
                ARTICLE_CONTAINER_SELECTOR_FALLBACK # Pass fallback again for the function's own retry
            )
            
            new_articles_to_process = []
            duplicate_count = 0
            MIN_YEAR = 2025

            for article_data in all_extracted:
                if article_data.get('url') and article_data.get('parsed_date_utc'):
                    if article_data['url'] not in existing_urls:
                        if article_data['parsed_date_utc'].year >= MIN_YEAR:
                            new_articles_to_process.append(article_data)
                        # else:
                            # print(f"Skipping article from {article_data['parsed_date_utc'].year}: {article_data['url']}")
                    else:
                        duplicate_count += 1
            
            print(f"Found {len(new_articles_to_process)} new articles (>= {MIN_YEAR}) to add (filtered out {duplicate_count} existing/old).")

            if new_articles_to_process:
                append_to_csv(CSV_FILENAME, new_articles_to_process, HEADERS, SOURCE_NAME)
            else:
                print("No new valid articles found to add.")
        else:
            print("Could not retrieve page source. Exiting.")

    except Exception as e:
        print(f"An error occurred in the main execution: {e}")
    finally:
        if driver:
            print("Closing browser...")
            driver.quit()
            print("Browser closed.")
    print("--- CoinDesk Scraper Finished ---")
