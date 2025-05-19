import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timezone # Import timezone for UTC
import os
import logging
import time
import random
from urllib.parse import urljoin

# --- Selenium Imports ---
from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions # Renamed to avoid conflict
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException

# --- Configuration ---
URLS_TO_SCRAPE = [
    "https://cryptonews.com.au/category/australia/",
    "https://cryptonews.com.au/category/austrac/",
    "https://cryptonews.com.au/category/asic/"
]
CSV_FILE = 'articles.csv'
SOURCE_NAME = 'cryptonews.com.au'
CSV_COLUMNS = ['date', 'source', 'url', 'title', 'done']
# How long Selenium should wait for elements to appear (seconds)
SELENIUM_TIMEOUT = 15
# Selector for the main article container we expect to find
ARTICLE_SELECTOR_CSS = 'div.article'

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Helper Functions ---

def load_existing_articles(filename, source_filter):
    """Loads existing articles from CSV, filtering by source."""
    existing_urls = set()
    df = pd.DataFrame(columns=CSV_COLUMNS)
    if os.path.exists(filename):
        try:
            df = pd.read_csv(filename, on_bad_lines='warn')
            if df.empty:
                 logging.warning(f"{filename} is empty or contains only invalid lines after parsing. Starting fresh for source '{source_filter}'.")
                 return pd.DataFrame(columns=CSV_COLUMNS), existing_urls
            if 'source' in df.columns:
                source_specific_df = df[df['source'] == source_filter].copy()
                if 'url' in source_specific_df.columns:
                     existing_urls = set(source_specific_df['url'].dropna())
                     logging.info(f"Loaded {len(existing_urls)} existing article URLs for source '{source_filter}' from {filename}")
                else:
                     logging.warning(f"'url' column not found in {filename}. Cannot check for existing articles.")
            else:
                 logging.warning(f"'source' column not found in {filename}. Cannot filter by source.")
        except pd.errors.EmptyDataError:
            logging.warning(f"{filename} exists but is empty. Starting fresh for source '{source_filter}'.")
            return pd.DataFrame(columns=CSV_COLUMNS), existing_urls
        except Exception as e:
            logging.error(f"Error loading or processing {filename}: {e}. Check file. Starting fresh for source '{source_filter}'.")
            return pd.DataFrame(columns=CSV_COLUMNS), existing_urls
    else:
        logging.info(f"{filename} not found. Will create a new file.")
    return df, existing_urls

def save_articles(new_articles_df, filename, existing_df):
    """Appends new articles to the CSV file."""
    if new_articles_df.empty:
        logging.info("No new articles to save.")
        return
    try:
        new_articles_df = new_articles_df[CSV_COLUMNS]
    except KeyError as e:
        logging.error(f"Missing expected column in new articles data: {e}. Cannot save.")
        return
    file_exists = os.path.exists(filename)
    write_header = not file_exists or existing_df.empty
    try:
        new_articles_df.to_csv(filename, mode='a', header=write_header, index=False, encoding='utf-8')
        logging.info(f"Successfully appended {len(new_articles_df)} new articles to {filename}")
    except IOError as e:
        logging.error(f"Error writing to {filename}: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred during saving: {e}")

# --- Selenium Driver Setup ---
def setup_driver():
    """Initializes and returns a Selenium WebDriver instance."""
    opts = ChromeOptions()
    opts.add_argument('--headless')  # Run Chrome in headless mode (no GUI)
    opts.add_argument('--disable-gpu') # Often necessary for headless mode
    opts.add_argument('--no-sandbox') # Bypass OS security model, required in some environments
    opts.add_argument('--log-level=3') # Suppress unnecessary console logs from Chrome/ChromeDriver
    opts.add_argument('--window-size=1920,1080') # Specify window size
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36") # Set user agent

    try:
        # Assumes chromedriver is in your PATH.
        # If not, provide the path: webdriver.Chrome(executable_path='/path/to/chromedriver', options=opts)
        driver = webdriver.Chrome(options=opts)
        driver.set_page_load_timeout(45) # Increase page load timeout
        logging.info("Selenium WebDriver initialized successfully.")
        return driver
    except WebDriverException as e:
        logging.error(f"Failed to initialize Selenium WebDriver: {e}")
        logging.error("Ensure chromedriver is installed, matches your Chrome version, and is in your PATH or specified.")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred during WebDriver setup: {e}")
        return None


def scrape_page_with_selenium(driver, page_url):
    """
    Scrapes a single category page using Selenium to handle JavaScript rendering.
    Uses the structure identified from the user's HTML snippet.

    Args:
        driver (webdriver): The Selenium WebDriver instance.
        page_url (str): The URL of the category page to scrape.

    Returns:
        list: A list of dictionaries for found articles.
    """
    articles_found = []
    logging.info(f"Attempting to scrape page with Selenium: {page_url}")
    try:
        # Load the page using Selenium
        driver.get(page_url)

        # --- Wait for article containers to be present ---
        # Wait until at least one element matching the ARTICLE_SELECTOR_CSS is found
        WebDriverWait(driver, SELENIUM_TIMEOUT).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ARTICLE_SELECTOR_CSS))
        )
        logging.info(f"Article container ('{ARTICLE_SELECTOR_CSS}') found on page.")

        # Optional: Add a small extra delay if content loads slightly after container appears
        # time.sleep(2)

        # Get the page source *after* waiting for elements to load
        html_content = driver.page_source
        soup = BeautifulSoup(html_content, 'lxml')

        # --- Find article containers in the Selenium-rendered HTML ---
        article_containers = soup.select(ARTICLE_SELECTOR_CSS)

        if not article_containers:
             # This shouldn't happen if the WebDriverWait succeeded, but check anyway
             logging.warning(f"Found '{ARTICLE_SELECTOR_CSS}' with WebDriverWait, but BeautifulSoup parsing failed to find them.")
             return articles_found

        logging.info(f"Found {len(article_containers)} '{ARTICLE_SELECTOR_CSS}' elements via BeautifulSoup. Processing...")

        # Iterate through each found article container
        for container in article_containers:
            # --- Extract elements using updated selectors ---
            link_tag = container.select_one('div.post-info h4 a')
            date_tag = container.select_one('div.meta div.date')

            # --- Data Extraction and Validation ---
            if link_tag and link_tag.get('href') and date_tag:
                article_url_raw = link_tag['href']
                article_url = urljoin(page_url, article_url_raw) # Ensure URL is absolute
                article_title = link_tag.text.strip()
                date_text = date_tag.text.strip()

                # --- Date Parsing and Formatting ---
                try:
                    parsed_date = datetime.strptime(date_text, '%B %d, %Y')
                    parsed_date_utc = parsed_date.replace(tzinfo=timezone.utc)
                    
                    # Enforce articles from 2025 onwards
                    if parsed_date.year < 2025:
                        logging.info(f"Skipping article from {parsed_date.year}: {article_title} ({article_url})")
                        continue

                    article_date_iso = parsed_date_utc.strftime('%Y-%m-%dT%H:%M:%SZ')

                    articles_found.append({
                        'date': article_date_iso,
                        'source': SOURCE_NAME,
                        'url': article_url,
                        'title': article_title,
                        'done': ''
                    })
                    logging.debug(f"Successfully extracted: {article_title} ({article_url}) Date: {date_text} -> {article_date_iso}")

                except ValueError:
                    logging.warning(f"Could not parse date '{date_text}' using format '%B %d, %Y' for article '{article_title}'. Skipping.")
                except Exception as e:
                     logging.error(f"Error processing date '{date_text}' for article '{article_title}': {e}")
            else:
                missing_parts = []
                if not link_tag or not link_tag.get('href'): missing_parts.append("title link (div.post-info h4 a)")
                if not date_tag: missing_parts.append("date tag (div.meta div.date)")
                if missing_parts:
                    logging.debug(f"Skipping a '{ARTICLE_SELECTOR_CSS}' element: Missing {', '.join(missing_parts)}.")

    # --- Error Handling ---
    except TimeoutException:
        logging.error(f"Timed out waiting for element '{ARTICLE_SELECTOR_CSS}' on {page_url}")
    except WebDriverException as e:
         logging.error(f"Selenium WebDriver error on {page_url}: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred scraping {page_url} with Selenium: {e}", exc_info=True)

    logging.info(f"Finished scraping {page_url}. Found {len(articles_found)} valid articles.")
    return articles_found

# --- Main Execution ---
if __name__ == "__main__":
    logging.info("--- CryptoNews AU Scraper Script Started (Using Selenium) ---")

    # --- Initialize Selenium Driver ---
    driver = setup_driver()
    if not driver:
        logging.error("Exiting script due to WebDriver initialization failure.")
        exit() # Exit if driver setup failed

    # 1. Load existing articles
    existing_df, existing_urls = load_existing_articles(CSV_FILE, SOURCE_NAME)
    logging.info(f"Checking against {len(existing_urls)} existing URLs from source '{SOURCE_NAME}'.")

    # 2. Scrape new articles using Selenium
    all_new_articles_data = []
    for url in URLS_TO_SCRAPE:
        # Pass the driver instance to the scraping function
        scraped_data = scrape_page_with_selenium(driver, url)
        newly_found_count = 0
        for article_data in scraped_data:
            if article_data['url'] not in existing_urls:
                all_new_articles_data.append(article_data)
                existing_urls.add(article_data['url'])
                newly_found_count += 1
                logging.debug(f"Found new article: {article_data['title']} ({article_data['url']})")
            else:
                 logging.debug(f"Skipping already existing article (URL found in CSV): {article_data['url']}")
        logging.info(f"Found {newly_found_count} new articles on {url}.")

        # Add Delay - still good practice even with Selenium
        sleep_time = random.uniform(1.0, 3.0) # Can potentially reduce delay slightly
        logging.info(f"Waiting for {sleep_time:.2f} seconds before next request...")
        time.sleep(sleep_time)

    # --- Quit Selenium Driver ---
    logging.info("Closing Selenium WebDriver...")
    driver.quit()
    logging.info("WebDriver closed.")

    # 3. Process and save new articles
    if not all_new_articles_data:
        logging.info("No new articles found across all specified URLs.")
    else:
        # Deduplicate within this run
        temp_df = pd.DataFrame(all_new_articles_data)
        original_new_count = len(temp_df)
        temp_df.drop_duplicates(subset=['url'], keep='first', inplace=True)
        final_new_count = len(temp_df)
        if final_new_count < original_new_count:
            logging.info(f"Removed {original_new_count - final_new_count} duplicate new articles found during this run.")
        logging.info(f"Found a total of {final_new_count} unique new articles to add.")
        new_articles_df = temp_df

        # Convert 'date' to datetime for sorting
        new_articles_df['date_dt'] = pd.to_datetime(new_articles_df['date'], errors='coerce', utc=True)

        # Drop rows with invalid dates
        original_count = len(new_articles_df)
        new_articles_df.dropna(subset=['date_dt'], inplace=True)
        if len(new_articles_df) < original_count:
            logging.warning(f"Removed {original_count - len(new_articles_df)} articles due to invalid date formats after conversion check.")

        # Sort chronologically
        new_articles_df = new_articles_df.sort_values(by='date_dt', ascending=True)

        # Drop temporary datetime column
        new_articles_df = new_articles_df.drop(columns=['date_dt'])

        # 4. Append to CSV
        save_articles(new_articles_df, CSV_FILE, existing_df)

    logging.info("--- CryptoNews AU Scraper Script Finished ---")
