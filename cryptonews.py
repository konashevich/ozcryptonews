import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timezone # Ensure timezone is imported
import os
import logging
import time
import random
from urllib.parse import urljoin

from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager # For easy driver management

# --- Configuration ---
URLS_TO_SCRAPE = [
    "https://cryptonews.com.au/category/australia/",
    "https://cryptonews.com.au/category/austrac/",
    "https://cryptonews.com.au/category/asic/"
]
CSV_FILE = 'articles.csv'
SOURCE_NAME = 'cryptonews.com.au'
CSV_COLUMNS = ['date', 'source', 'url', 'title', 'done']
SELENIUM_TIMEOUT = 20 # Increased slightly
ARTICLE_SELECTOR_CSS = 'div.article' # Main article container

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Helper Functions ---

def load_existing_articles(filename, source_filter):
    existing_urls = set()
    # Ensure file exists and has header before reading with pandas
    if not os.path.exists(filename) or os.path.getsize(filename) == 0:
        try:
            with open(filename, 'w', newline='', encoding='utf-8') as outfile:
                writer = pd.DataFrame(columns=CSV_COLUMNS).to_csv(outfile, index=False) # Use pandas to write header
            logging.info(f"Initialized CSV file '{filename}' with headers.")
        except IOError as e:
            logging.error(f"Could not create/initialize CSV file '{filename}': {e}")
        return pd.DataFrame(columns=CSV_COLUMNS), existing_urls # Return empty df

    try:
        # Read CSV, handle bad lines by warning and skipping
        df = pd.read_csv(filename, on_bad_lines='warn', encoding='utf-8')
        if df.empty:
             logging.warning(f"'{filename}' is empty or unreadable after parsing. Starting fresh for '{source_filter}'.")
             return pd.DataFrame(columns=CSV_COLUMNS), existing_urls
        if 'source' in df.columns and 'url' in df.columns:
            source_specific_df = df[df['source'] == source_filter].copy()
            existing_urls = set(source_specific_df['url'].dropna())
            logging.info(f"Loaded {len(existing_urls)} existing URLs for '{source_filter}' from {filename}")
        else:
             logging.warning(f"'{filename}' missing 'source' or 'url' columns. Cannot check existing for '{source_filter}'.")
    except pd.errors.EmptyDataError:
        logging.warning(f"'{filename}' is empty. Starting fresh for '{source_filter}'.")
        return pd.DataFrame(columns=CSV_COLUMNS), existing_urls
    except Exception as e:
        logging.error(f"Error loading '{filename}': {e}. Starting fresh for '{source_filter}'.")
        return pd.DataFrame(columns=CSV_COLUMNS), existing_urls
    return df, existing_urls # Return potentially full df for append logic

def save_articles(new_articles_df, filename):
    """Appends new articles (DataFrame) to the CSV file."""
    if new_articles_df.empty:
        logging.info("No new articles to save.")
        return
    try:
        # Ensure columns are in the correct order for appending
        new_articles_df_ordered = new_articles_df[CSV_COLUMNS]
    except KeyError as e:
        logging.error(f"Missing expected column in new articles data: {e}. Cannot save.")
        return

    file_exists_and_has_content = os.path.exists(filename) and os.path.getsize(filename) > 0
    
    try:
        new_articles_df_ordered.to_csv(
            filename, 
            mode='a', 
            header=not file_exists_and_has_content, # Write header only if file is new/empty
            index=False, 
            encoding='utf-8'
        )
        logging.info(f"Successfully appended {len(new_articles_df_ordered)} new articles to {filename}")
    except IOError as e:
        logging.error(f"Error writing to {filename}: {e}")
    except Exception as e:
        logging.error(f"Unexpected error during saving to {filename}: {e}")

def setup_driver():
    opts = ChromeOptions()
    opts.add_argument('--headless')
    opts.add_argument('--disable-gpu')
    opts.add_argument('--no-sandbox')
    opts.add_argument('--log-level=3')
    opts.add_argument('--window-size=1920,1080')
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36")
    try:
        service = webdriver.chrome.service.Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=opts)
        driver.set_page_load_timeout(45)
        logging.info("Selenium WebDriver initialized successfully.")
        return driver
    except WebDriverException as e:
        logging.error(f"Failed to initialize Selenium WebDriver: {e}")
        return None
    except Exception as e:
        logging.error(f"Unexpected error during WebDriver setup: {e}")
        return None


def scrape_page_with_selenium(driver, page_url):
    articles_found = []
    logging.info(f"Scraping page with Selenium: {page_url}")
    try:
        driver.get(page_url)
        WebDriverWait(driver, SELENIUM_TIMEOUT).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, ARTICLE_SELECTOR_CSS)) # Wait for multiple
        )
        logging.info(f"Article containers ('{ARTICLE_SELECTOR_CSS}') found on page.")
        
        html_content = driver.page_source
        soup = BeautifulSoup(html_content, 'lxml') # Use lxml for parsing
        article_containers = soup.select(ARTICLE_SELECTOR_CSS)

        if not article_containers:
             logging.warning(f"WebDriverWait found elements, but BeautifulSoup did not. Page: {page_url}")
             return articles_found

        logging.info(f"Found {len(article_containers)} '{ARTICLE_SELECTOR_CSS}' elements. Processing...")

        for container in article_containers:
            link_tag = container.select_one('div.post-info h4 a')
            date_tag = container.select_one('div.meta div.date') # Selector for date

            if link_tag and link_tag.get('href') and date_tag:
                article_url_raw = link_tag['href']
                article_url = urljoin(page_url, article_url_raw)
                article_title = link_tag.text.strip()
                date_text = date_tag.text.strip() # e.g., "May 15, 2025"

                try:
                    # Parse the date string
                    parsed_date_naive = datetime.strptime(date_text, '%B %d, %Y')
                    # Make it timezone-aware UTC
                    parsed_date_utc = parsed_date_naive.replace(tzinfo=timezone.utc)
                    
                    if parsed_date_utc.year < 2025: # Year filter
                        # logging.debug(f"Skipping article from {parsed_date_utc.year}: {article_title}")
                        continue
                    
                    # Format to YYYY-MM-DDTHH:MM:SS+00:00
                    article_date_iso_utc = parsed_date_utc.strftime('%Y-%m-%dT%H:%M:%S+00:00')

                    articles_found.append({
                        'date': article_date_iso_utc,
                        'source': SOURCE_NAME,
                        'url': article_url,
                        'title': article_title,
                        'done': ''
                    })
                except ValueError:
                    logging.warning(f"Could not parse date '{date_text}' for '{article_title}'. Skipping.")
                except Exception as e:
                     logging.error(f"Error processing date '{date_text}' for '{article_title}': {e}")
            # else:
                # logging.debug(f"Skipping a '{ARTICLE_SELECTOR_CSS}' element: Missing link or date tag.")
        
    except TimeoutException:
        logging.error(f"Timed out waiting for '{ARTICLE_SELECTOR_CSS}' on {page_url}")
    except WebDriverException as e:
         logging.error(f"Selenium WebDriver error on {page_url}: {e}")
    except Exception as e:
        logging.error(f"Unexpected error scraping {page_url} with Selenium: {e}", exc_info=True)

    logging.info(f"Finished scraping {page_url}. Found {len(articles_found)} valid articles.")
    return articles_found

# --- Main Execution ---
if __name__ == "__main__":
    logging.info("--- CryptoNews AU Scraper Started (Selenium, Date Format UTC) ---")
    driver = setup_driver()
    if not driver:
        logging.error("Exiting due to WebDriver failure.")
        exit()

    _, existing_urls = load_existing_articles(CSV_FILE, SOURCE_NAME) # existing_df not used directly in loop
    logging.info(f"Checking against {len(existing_urls)} existing URLs for '{SOURCE_NAME}'.")

    all_new_articles_data_list = []
    for url_to_scrape in URLS_TO_SCRAPE:
        scraped_page_data = scrape_page_with_selenium(driver, url_to_scrape)
        newly_found_on_page = 0
        for article_item in scraped_page_data:
            if article_item['url'] not in existing_urls:
                all_new_articles_data_list.append(article_item)
                existing_urls.add(article_item['url']) # Add to set to avoid duplicates within this run
                newly_found_on_page += 1
        logging.info(f"Found {newly_found_on_page} new articles on {url_to_scrape}.")
        time.sleep(random.uniform(1.0, 2.0)) # Reduced delay slightly

    logging.info("Closing Selenium WebDriver...")
    driver.quit()
    logging.info("WebDriver closed.")

    if not all_new_articles_data_list:
        logging.info("No new articles found across all URLs.")
    else:
        new_articles_df = pd.DataFrame(all_new_articles_data_list)
        # Ensure 'date' is datetime for sorting, then convert back to string if needed by save_articles
        # The 'date' in all_new_articles_data_list is already the target ISO string.
        # We need to parse it to datetime for sorting, then it's saved as string.
        try:
            new_articles_df['parsed_date_dt'] = pd.to_datetime(new_articles_df['date'], errors='coerce', utc=True)
            new_articles_df.dropna(subset=['parsed_date_dt'], inplace=True) # Drop if date parsing failed
            new_articles_df = new_articles_df.sort_values(by='parsed_date_dt', ascending=True)
            new_articles_df = new_articles_df.drop(columns=['parsed_date_dt']) # Remove temp sort column
        except Exception as e:
            logging.error(f"Error during sorting of new articles: {e}. Articles might not be in order.")
            # Continue with unsorted or partially sorted data if absolutely necessary,
            # or handle more gracefully. For now, it will use the DataFrame as is.

        logging.info(f"Found a total of {len(new_articles_df)} unique new articles to add.")
        save_articles(new_articles_df, CSV_FILE)

    logging.info("--- CryptoNews AU Scraper Script Finished ---")
