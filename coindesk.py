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
URL = "https://www.coindesk.com/tag/australia"
CSV_FILENAME = "articles.csv"
SOURCE_NAME = "coindesk.com"
HEADERS = ['date', 'source', 'url', 'title', 'done']
# Selenium Configuration
SELENIUM_TIMEOUT_SECONDS = 25
SCROLL_PAUSE_TIME = 2
SCROLL_ATTEMPTS = 5
ACCEPT_BUTTON_TIMEOUT_SECONDS = 10

# --- Selectors ---
# Article Selectors (UPDATED based on debug_page.html - May 2025 v5 - More Specific Container)
# Target the div directly containing the text content and image link
ARTICLE_CONTAINER_SELECTOR = 'div.bg-white.flex.gap-6.w-full.shrink.justify-between'
# Fallback: The previous outer container (less likely to be needed now)
ARTICLE_CONTAINER_SELECTOR_FALLBACK = 'div.flex.flex-col.gap-4'
# Privacy/Cookie Accept Button Selectors (Keep as is)
ACCEPT_BUTTON_SELECTORS = [
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
            # Ensure file is read with UTF-8 encoding
            with open(filename, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                # Check for required headers robustly
                if not reader.fieldnames or not all(header in reader.fieldnames for header in ['url', 'source']):
                    print(f"Warning: CSV file '{filename}' is missing required columns ('url', 'source') or is malformed. Skipping loading existing URLs.")
                    # Attempt to create a new file with headers if it's completely broken? Or just return empty set.
                    return existing_urls # Safer to return empty and potentially duplicate than crash

                for row in reader:
                    # Check if row is not empty and has the required keys
                    if row and row.get('source') == source_filter and row.get('url'):
                        existing_urls.add(row['url'])
            print(f"Loaded {len(existing_urls)} existing URLs for source '{source_filter}' from '{filename}'.")
        else:
            print(f"CSV file '{filename}' not found or is empty. Starting fresh.")
    except FileNotFoundError:
        print(f"CSV file '{filename}' not found. Starting fresh.")
    except Exception as e:
        # Catch potential decoding errors or other file issues
        print(f"Error reading CSV file '{filename}': {e}. Check file encoding and format.")
    return existing_urls

def setup_driver():
    """Sets up the Selenium WebDriver."""
    print("Setting up Chrome WebDriver...")
    try:
        options = webdriver.ChromeOptions()
        # options.add_argument('--headless') # Run headless for background operation
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
            driver.execute_script("arguments[0].scrollIntoView(true);", accept_button)
            time.sleep(0.5)
            accept_button.click()
            print(f"Successfully clicked Accept button using selector: {by}='{selector_value}'")
            time.sleep(1)
            button_clicked = True
            break # Exit loop once button is clicked
        except TimeoutException:
            continue
        except ElementClickInterceptedException:
             print(f"Accept button click intercepted using selector: {by}='{selector_value}'. Trying JavaScript click...")
             try:
                 accept_button_js = driver.find_element(by, selector_value)
                 driver.execute_script("arguments[0].click();", accept_button_js)
                 print(f"Successfully clicked Accept button using JavaScript fallback.")
                 time.sleep(1)
                 button_clicked = True
                 break # Exit loop once button is clicked
             except Exception as js_e:
                 print(f"JavaScript click also failed: {js_e}")
                 continue
        except Exception as e:
            print(f"An error occurred trying to click Accept button with {by}='{selector_value}': {e}")
            continue

    if not button_clicked:
        print("Could not find or click the Accept button using any provided selectors (or it wasn't present).")
    return button_clicked # Return True if clicked, False otherwise

def fetch_page_source_with_selenium(driver, url, wait_selector, fallback_selector, timeout):
    """Fetches the page source using Selenium after handling accept button, waiting, and scrolling."""
    print(f"Fetching data from: {url} using Selenium...")
    page_source = None
    primary_selector_used = wait_selector # Default to primary
    try:
        driver.get(url)
        click_accept_button(driver, ACCEPT_BUTTON_SELECTORS, ACCEPT_BUTTON_TIMEOUT_SECONDS)

        print(f"Waiting up to {timeout} seconds for initial elements matching selector: '{wait_selector}'")
        try:
            WebDriverWait(driver, timeout).until(
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
                 primary_selector_used = fallback_selector # Switch to fallback for extraction
            except TimeoutException:
                 print(f"Timeout waiting for initial elements with fallback selector '{fallback_selector}' as well.")
                 print("Will attempt scrolling anyway, but extraction might fail if elements never load.")

        print(f"Attempting to scroll down {SCROLL_ATTEMPTS} times to load more content...")
        last_height = driver.execute_script("return document.body.scrollHeight")
        for i in range(SCROLL_ATTEMPTS):
            # print(f"Scroll attempt {i+1}/{SCROLL_ATTEMPTS}...") # Less verbose scrolling
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(SCROLL_PAUSE_TIME)
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                # print("Reached bottom of page or no new content loaded.") # Less verbose
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


def extract_articles(page_source, effective_selector, fallback_selector, base_url="https://www.coindesk.com"):
    """
    Extracts article details (URL, title, date object) from the HTML page source.
    Uses the selector determined during the waiting phase.
    Saves debug HTML if no articles are found.
    """
    if not page_source:
        print("No page source provided to extract_articles.")
        return []

    articles = []
    soup = BeautifulSoup(page_source, 'html.parser')

    # Use the selector that was determined to be present (or the primary if wait failed)
    article_elements = soup.select(effective_selector)
    print(f"Attempting extraction using selector '{effective_selector}'. Found {len(article_elements)} potential article elements.")

    # --- DEBUG: Save page source if no elements found ---
    if not article_elements:
        print(f"No articles found using the determined selector ('{effective_selector}').")
        print(f"Trying explicit fallback selector '{fallback_selector}' again...")
        article_elements = soup.select(fallback_selector)
        print(f"Found {len(article_elements)} elements using explicit fallback selector.")

        if not article_elements: # Still no articles found
            try:
                debug_filename = "debug_page.html"
                with open(debug_filename, "w", encoding="utf-8") as f:
                    f.write(soup.prettify())
                print(f"*** Saved the page source Selenium received to '{debug_filename}' for manual inspection. Please check this file. ***")
            except Exception as e:
                print(f"Error saving debug HTML file: {e}")
            return [] # Return empty list

    # --- Extraction Logic ---
    extracted_count = 0
    for element in article_elements:
        try:
            # --- Selectors within the card (Based on May 2025 v5) ---
            # Find the main link/title tag within the current element context
            link_tag = element.select_one('a[class*="text-color-charcoal-900"][href]')

            # Find the date span within the current element context
            date_container = element.select_one('p.flex.gap-2.flex-col')
            date_str = None
            if date_container:
                date_span = date_container.select_one('span.font-metadata.text-color-charcoal-600')
                if date_span:
                    date_str = date_span.get_text(strip=True)
                else:
                    date_tag_fallback = element.select_one('time[datetime]')
                    if date_tag_fallback and date_tag_fallback.get('datetime'):
                        date_str = date_tag_fallback['datetime']

            # --- Data Extraction ---
            if link_tag and link_tag.get('href') and date_str:
                relative_url = link_tag['href']
                if relative_url.startswith('http'):
                     full_url = relative_url
                elif relative_url.startswith('/'):
                     full_url = base_url + relative_url
                else:
                     print(f"Warning: Unexpected relative URL format: {relative_url}")
                     full_url = base_url + "/" + relative_url # Best guess

                title_tag = link_tag.select_one('h2')
                title = title_tag.get_text(strip=True) if title_tag else link_tag.get_text(strip=True).strip() # Ensure title is stripped

                # Basic check: Ensure URL and title look reasonable
                if not full_url or not title:
                    # print(f"Debug: Skipping element - Missing URL or Title derived from link_tag.")
                    continue

                try:
                    # Attempt to parse the date string
                    parsed_date = date_parser.parse(date_str)
                    articles.append({
                        'url': full_url,
                        'title': title,
                        'parsed_date': parsed_date # Keep as datetime object for sorting
                    })
                    extracted_count += 1
                except (ValueError, date_parser.ParserError, OverflowError, TypeError) as e:
                    print(f"Warning: Could not parse date string: '{date_str}' for article '{title}'. Error: {e}. Skipping article.")
                except Exception as e:
                    print(f"Error parsing date '{date_str}' for article '{title}': {e}. Skipping article.")
            else:
                # This handles cases where the element matched the container selector
                # but didn't contain the expected link or date structure.
                # This is normal if the selector matches other similar-looking divs.
                # print(f"Debug: Skipping element - Missing link_tag or date_str.")
                pass

        except AttributeError as e:
            # This might happen if select_one returns None and we try to access attributes
            print(f"Debug: Skipping element due to AttributeError (likely internal structure mismatch): {e}")
            continue
        except Exception as e:
            print(f"Error processing an article element: {e}")
            continue

    print(f"Successfully extracted details for {extracted_count} articles from {len(article_elements)} potential elements.")
    return articles

def append_to_csv(filename, articles_data, headers, source_name):
    """Appends new article data to the CSV file in chronological order."""
    file_exists = os.path.exists(filename)
    is_empty = not file_exists or os.path.getsize(filename) == 0

    # --- Sort the articles_data list chronologically before appending ---
    # Ensure only articles with valid dates are included in sorting/appending
    valid_articles = [a for a in articles_data if a.get('parsed_date') and isinstance(a['parsed_date'], datetime)]
    if len(valid_articles) != len(articles_data):
        print(f"Warning: {len(articles_data) - len(valid_articles)} articles skipped due to missing/invalid dates before sorting.")

    # Sort the valid articles by 'parsed_date' (earliest first)
    valid_articles.sort(key=lambda x: x['parsed_date'])

    # --- Add Debug Print for Sorted Order ---
    if valid_articles:
        print("--- Articles to be appended (Sorted Chronologically) ---")
        for article in valid_articles:
            print(f"- {article['parsed_date'].strftime('%Y-%m-%d')}: {article['title'][:60]}...") # Print date and truncated title
        print("-------------------------------------------------------")
    else:
        print("No valid new articles to append after date check.")
        return # Nothing to append

    try:
        # Use 'a' mode (append) with utf-8 encoding
        with open(filename, 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)

            # Write header only if file is newly created or was empty
            if is_empty:
                writer.writeheader()
                print(f"Created new CSV file '{filename}' and wrote header.")

            count = 0
            for article in valid_articles: # Iterate through the sorted list
                 # Format date to ISO 8601 just before writing
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
            print(f"Appended {count} new articles to '{filename}'.")
    except IOError as e:
        print(f"Error writing to CSV file '{filename}': {e}")
    except Exception as e:
        print(f"An unexpected error occurred during CSV writing: {e}")


# --- Main Execution ---
if __name__ == "__main__":
    print("--- Starting CoinDesk Scraper (Selenium - Fixes for Duplicates & Order) ---")
    driver = None
    try:
        driver = setup_driver()
        if not driver:
            raise Exception("WebDriver setup failed.")

        # 1. Load existing URLs (robustly)
        existing_urls = load_existing_urls(CSV_FILENAME, SOURCE_NAME)

        # 2. Fetch page source
        page_source, effective_selector = fetch_page_source_with_selenium(
            driver,
            URL,
            ARTICLE_CONTAINER_SELECTOR, # Use the more specific selector
            ARTICLE_CONTAINER_SELECTOR_FALLBACK,
            SELENIUM_TIMEOUT_SECONDS
        )

        if page_source:
            # 3. Extract article details
            all_extracted_articles = extract_articles(
                page_source,
                effective_selector,
                ARTICLE_CONTAINER_SELECTOR_FALLBACK
            )

            # 4. Filter out articles already in the CSV
            new_articles = []
            duplicate_count = 0
            for article in all_extracted_articles:
                # Check both URL and that date was parsed successfully
                if article.get('url') and article['url'] not in existing_urls and article.get('parsed_date'):
                    new_articles.append(article)
                elif article.get('url') and article['url'] in existing_urls:
                    duplicate_count += 1

            print(f"Found {len(new_articles)} new articles to add (filtered out {duplicate_count} existing).")

            # Enforce 2025 onwards filtering
            filtered_articles = []
            for article in new_articles:
                if article['parsed_date'].year >= 2025:
                    filtered_articles.append(article)
                else:
                    print(f"Skipping article from {article['parsed_date'].year}: {article['url']}")

            if filtered_articles:
                # 5. Append new articles to the CSV (Sorting is now handled inside append_to_csv)
                append_to_csv(CSV_FILENAME, filtered_articles, HEADERS, SOURCE_NAME)
            else:
                print("No new valid articles found to add (or all found articles were already in the CSV).")

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
        # if os.path.exists("debug_page.html"):
        #     os.remove("debug_page.html")


    print("--- Scraper finished ---")
