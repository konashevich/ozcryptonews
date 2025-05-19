import csv
import os
import time
from datetime import datetime

import requests # Keep requests for potential future use or fallback
from bs4 import BeautifulSoup
from dateutil import parser as date_parser # Use alias to avoid confusion
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
SOURCE_NAME = "cointelegraph.com" # Updated source name
BASE_URL = "https://cointelegraph.com" # Base URL for constructing full URLs
HEADERS = ['date', 'source', 'url', 'title', 'done']
# Selenium Configuration
SELENIUM_TIMEOUT_SECONDS = 25
SCROLL_PAUSE_TIME = 2
SCROLL_ATTEMPTS = 5 # Adjust as needed, CoinTelegraph might load more on scroll
ACCEPT_BUTTON_TIMEOUT_SECONDS = 10

# --- Selectors (UPDATED based on user-provided debug HTML - May 2025 v2) ---
# Main container for each article in the list
ARTICLE_CONTAINER_SELECTOR = 'article.post-card-inline'
# Fallback selector (less likely needed but good practice)
ARTICLE_CONTAINER_SELECTOR_FALLBACK = 'li.posts-listing__item' # Previous attempt as fallback
# Privacy/Cookie Accept Button Selectors (Keep as is)
ACCEPT_BUTTON_SELECTORS = [
    (By.ID, "CybotCookiebotDialogBodyButtonAccept"),
    (By.ID, "onetrust-accept-btn-handler"),
    (By.XPATH, "//button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'accept')]"),
    (By.CSS_SELECTOR, 'button[data-testid*="accept"], button[aria-label*="Accept"]')
]


# --- Helper Functions ---

def load_existing_urls(filename, source_filter):
    """Loads existing article URLs from the CSV file for a specific source."""
    existing_urls = set()
    try:
        if os.path.exists(filename) and os.path.getsize(filename) > 0:
            with open(filename, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                if not reader.fieldnames or not all(header in reader.fieldnames for header in ['url', 'source']):
                    print(f"Warning: CSV file '{filename}' is missing required columns ('url', 'source') or is malformed. Skipping loading existing URLs.")
                    return existing_urls
                for row in reader:
                    if row and row.get('source') == source_filter and row.get('url'):
                        existing_urls.add(row['url'])
            print(f"Loaded {len(existing_urls)} existing URLs for source '{source_filter}' from '{filename}'.")
        else:
            print(f"CSV file '{filename}' not found or is empty. Starting fresh for source '{source_filter}'.")
    except FileNotFoundError:
        print(f"CSV file '{filename}' not found. Starting fresh for source '{source_filter}'.")
    except Exception as e:
        print(f"Error reading CSV file '{filename}': {e}. Check file encoding and format.")
    return existing_urls

def setup_driver():
    """Sets up the Selenium WebDriver in headless mode."""
    print("Setting up Chrome WebDriver (Headless Mode)...")
    try:
        options = webdriver.ChromeOptions()
        options.add_argument('--headless') # Uncommented: Runs Chrome in headless mode
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
    except WebDriverException as e:
        print(f"Error setting up WebDriver: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred during WebDriver setup: {e}")
        return None

def click_accept_button(driver, selectors, timeout):
    """Attempts to find and click the Accept button using multiple selectors."""
    print("Checking for and attempting to click Accept button...")
    button_clicked = False
    for by, selector_value in selectors:
        try:
            accept_button = WebDriverWait(driver, timeout).until(
                EC.element_to_be_clickable((by, selector_value))
            )
            accept_button.click()
            print(f"Successfully clicked Accept button using selector: {by}='{selector_value}'")
            time.sleep(1)
            button_clicked = True
            break
        except ElementClickInterceptedException:
             print(f"Accept button click intercepted using selector: {by}='{selector_value}'. Trying JavaScript click...")
             try:
                 accept_button_js = driver.find_element(by, selector_value)
                 driver.execute_script("arguments[0].click();", accept_button_js)
                 print(f"Successfully clicked Accept button using JavaScript fallback.")
                 time.sleep(1)
                 button_clicked = True
                 break
             except Exception as js_e:
                 print(f"JavaScript click also failed: {js_e}")
                 continue
        except TimeoutException:
            continue
        except Exception as e:
            print(f"An error occurred trying to click Accept button with {by}='{selector_value}': {e}")
            continue

    if not button_clicked:
        print("Could not find or click the Accept button using any provided selectors (or it wasn't present).")
    return button_clicked

def fetch_page_source_with_selenium(driver, url, wait_selector, fallback_selector, timeout):
    """Fetches the page source using Selenium after handling accept button, waiting, and scrolling."""
    print(f"Fetching data from: {url} using Selenium...")
    page_source = None
    primary_selector_used = wait_selector
    try:
        driver.get(url)
        click_accept_button(driver, ACCEPT_BUTTON_SELECTORS, ACCEPT_BUTTON_TIMEOUT_SECONDS)

        print(f"Waiting up to {timeout} seconds for initial elements matching selector: '{wait_selector}'")
        try:
            WebDriverWait(driver, timeout).until(
                # Wait for presence, not visibility, as scrolling might be needed
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, wait_selector))
            )
            print(f"Initial article element(s) potentially loaded using '{wait_selector}'.")
            primary_selector_used = wait_selector
        except TimeoutException:
            print(f"Timeout waiting for initial element with primary selector '{wait_selector}'. Trying fallback selector '{fallback_selector}'...")
            try:
                 WebDriverWait(driver, 5).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, fallback_selector))
                 )
                 print(f"Initial fallback element(s) potentially loaded using '{fallback_selector}'.")
                 primary_selector_used = fallback_selector
            except TimeoutException:
                 print(f"Timeout waiting for initial elements with fallback selector '{fallback_selector}' as well.")
                 print("Will attempt scrolling anyway, but extraction might fail if elements never load.")

        print(f"Attempting to scroll down {SCROLL_ATTEMPTS} times to load more content...")
        last_height = driver.execute_script("return document.body.scrollHeight")
        for i in range(SCROLL_ATTEMPTS):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(SCROLL_PAUSE_TIME)
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height
        time.sleep(2)
        print("Scrolling finished.")

        page_source = driver.page_source
        print("Page source retrieved successfully after waiting and scrolling.")

    except WebDriverException as e:
        print(f"Error during Selenium navigation or processing: {e}")
        return None, wait_selector
    except Exception as e:
        print(f"An unexpected error occurred during Selenium fetching: {e}")
        return None, wait_selector

    return page_source, primary_selector_used


def extract_articles(page_source, effective_selector, fallback_selector, base_url):
    """
    Extracts article details (URL, title, date object) from the HTML page source
    for CoinTelegraph using updated selectors.
    """
    if not page_source:
        print("No page source provided to extract_articles.")
        return []

    articles = []
    soup = BeautifulSoup(page_source, 'html.parser')

    # Use the selector determined to be present (or the primary if wait failed)
    article_elements = soup.select(effective_selector)
    print(f"Attempting extraction using selector '{effective_selector}'. Found {len(article_elements)} potential article elements.")

    if not article_elements:
        print(f"No articles found using the determined selector ('{effective_selector}').")
        print(f"Trying explicit fallback selector '{fallback_selector}' again...")
        article_elements = soup.select(fallback_selector)
        print(f"Found {len(article_elements)} elements using explicit fallback selector.")

        if not article_elements:
            try:
                debug_filename = "debug_page_cointelegraph.html"
                with open(debug_filename, "w", encoding="utf-8") as f:
                    f.write(soup.prettify())
                print(f"*** Saved the page source Selenium received to '{debug_filename}' for manual inspection. ***")
            except Exception as e:
                print(f"Error saving debug HTML file: {e}")
            return []

    # --- Extraction Logic (UPDATED for CoinTelegraph based on debug HTML) ---
    extracted_count = 0
    for element in article_elements:
        try:
            # Find the link and title tag
            # Target: <a class="post-card-inline__title-link" href="...">
            link_tag = element.select_one('a.post-card-inline__title-link[href]')

            # Find the date tag
            # Target: <time datetime="ISO-LIKE-STRING" class="post-card-inline__date">
            date_tag = element.select_one('time.post-card-inline__date[datetime]')
            date_str = None
            if date_tag:
                date_str = date_tag.get('datetime') # Use the reliable datetime attribute

            # --- Data Extraction ---
            if link_tag and link_tag.get('href') and date_str:
                relative_url = link_tag['href']
                full_url = base_url + relative_url if relative_url.startswith('/') else relative_url

                # Title is the text content of the link tag (or its inner span)
                title_span = link_tag.select_one('span.post-card-inline__title')
                title = title_span.get_text(strip=True) if title_span else link_tag.get_text(strip=True)

                if not full_url or not title:
                    continue

                try:
                    parsed_date = date_parser.parse(date_str)
                    articles.append({
                        'url': full_url,
                        'title': title,
                        'parsed_date': parsed_date
                    })
                    extracted_count += 1
                except (ValueError, date_parser.ParserError, OverflowError, TypeError) as e:
                    print(f"Warning: Could not parse date string: '{date_str}' for article '{title}'. Error: {e}. Skipping article.")
                except Exception as e:
                    print(f"Error parsing date '{date_str}' for article '{title}': {e}. Skipping article.")
            else:
                pass # Skip if link or date missing in this element

        except AttributeError as e:
            print(f"Debug: Skipping element due to AttributeError: {e}")
            continue
        except Exception as e:
            print(f"Error processing an article element: {e}")
            continue

    print(f"Successfully extracted details for {extracted_count} articles from {len(article_elements)} potential elements.")
    return articles

def append_to_csv(filename, articles_data, headers, source_name):
    """Appends new article data to the CSV file in chronological order."""
    file_exists = os.path.exists(filename)
    is_effectively_empty = not file_exists or os.path.getsize(filename) == 0
    if file_exists and not is_effectively_empty:
        try:
            with open(filename, 'r', newline='', encoding='utf-8') as csvfile:
                 sniffer = csv.Sniffer()
                 has_header = sniffer.has_header(csvfile.read(1024))
                 csvfile.seek(0)
                 reader = csv.reader(csvfile)
                 row_count = sum(1 for row in reader)
                 if has_header and row_count <= 1:
                     is_effectively_empty = True
        except Exception as e:
            print(f"Could not accurately determine if file '{filename}' is empty due to error: {e}. Assuming it might need a header.")

    valid_articles = [a for a in articles_data if a.get('parsed_date') and isinstance(a['parsed_date'], datetime)]
    if len(valid_articles) != len(articles_data):
        print(f"Warning: {len(articles_data) - len(valid_articles)} articles skipped due to missing/invalid dates before sorting.")

    valid_articles.sort(key=lambda x: x['parsed_date'])

    if valid_articles:
        print("--- Articles to be appended (Sorted Chronologically) ---")
        for article in valid_articles:
            print(f"- {article['parsed_date'].strftime('%Y-%m-%d %H:%M')}: {article['title'][:60]}...")
        print("-------------------------------------------------------")
    else:
        print("No valid new articles to append after date check.")
        return

    try:
        with open(filename, 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            if is_effectively_empty:
                writer.writeheader()
                print(f"Wrote header to '{filename}'.")

            count = 0
            for article in valid_articles:
                 iso_date = article['parsed_date'].isoformat()
                 row = {
                    'date': iso_date,
                    'source': source_name,
                    'url': article['url'],
                    'title': article['title'],
                    'done': ''
                 }
                 writer.writerow(row)
                 count += 1
            print(f"Appended {count} new articles for source '{source_name}' to '{filename}'.")
    except IOError as e:
        print(f"Error writing to CSV file '{filename}': {e}")
    except Exception as e:
        print(f"An unexpected error occurred during CSV writing: {e}")


# --- Main Execution ---
if __name__ == "__main__":
    print(f"--- Starting CoinTelegraph Scraper ({SOURCE_NAME}) ---")
    driver = None
    try:
        driver = setup_driver()
        if not driver:
            raise Exception("WebDriver setup failed.")

        existing_urls = load_existing_urls(CSV_FILENAME, SOURCE_NAME)

        page_source, effective_selector = fetch_page_source_with_selenium(
            driver,
            URL,
            ARTICLE_CONTAINER_SELECTOR, # Use updated CoinTelegraph selector
            ARTICLE_CONTAINER_SELECTOR_FALLBACK,
            SELENIUM_TIMEOUT_SECONDS
        )

        if page_source:
            all_extracted_articles = extract_articles(
                page_source,
                effective_selector,
                ARTICLE_CONTAINER_SELECTOR_FALLBACK,
                BASE_URL
            )

            new_articles = []
            duplicate_count = 0
            for article in all_extracted_articles:
                if article.get('url') and article['url'] not in existing_urls and article.get('parsed_date'):
                    new_articles.append(article)
                elif article.get('url') and article['url'] in existing_urls:
                    duplicate_count += 1

            print(f"Found {len(new_articles)} new articles for {SOURCE_NAME} to add (filtered out {duplicate_count} existing).")

            # After extracting new_articles, add a filtering step:

            filtered_articles = []
            for article in new_articles:
                if article['parsed_date'].year >= 2025:
                    filtered_articles.append(article)
                else:
                    print(f"Skipping article from {article['parsed_date'].year}: {article['url']}")

            if filtered_articles:
                append_to_csv(CSV_FILENAME, filtered_articles, HEADERS, SOURCE_NAME)
            else:
                print("No new valid articles found for 2025 onward.")
        else:
            print("Could not retrieve page source using Selenium. Exiting.")

    except Exception as e:
        print(f"An error occurred in the main execution block: {e}")

    finally:
        if driver:
            print("Closing the browser...")
            driver.quit()
            print("Browser closed.")
        # Optional: Clean up debug file
        # if os.path.exists("debug_page_cointelegraph.html"):
        #     os.remove("debug_page_cointelegraph.html")

    print(f"--- Scraper finished ({SOURCE_NAME}) ---")
