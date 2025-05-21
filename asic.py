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
REPORTS_PUBLICATIONS_URL = "https://asic.gov.au/newsroom/reports-and-publications/" # New source URL

OUTPUT_CSV = "articles.csv"
KEYWORDS_TXT = "web3keywords.txt"
CHECKED_URLS_FILE = "asic_checked.txt"
SOURCE_IDENTIFIER = "asic.gov.au" # This remains general for all ASIC content
OUTPUT_COLUMNS = ['date', 'source', 'url', 'title', 'done']
MAIN_PAGE_LOAD_WAIT = 10
REQUEST_DELAY = 1.5
USER_AGENT = 'Python Selenium Scraper Bot (Educational Use)'
MIN_YEAR_YY = 24  # Corresponds to 2024
CONTEXT_CHARS = 50 # For keyword context, currently not in CSV but can be useful for debugging

# List of sources to scrape
SOURCE_URLS_TO_SCRAPE = [
    {"name": "Media Releases", "url": MEDIA_RELEASES_URL, "type": "media_release"},
    {"name": "Reports and Publications", "url": REPORTS_PUBLICATIONS_URL, "type": "report_publication"}
]

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
    """Checks if the text contains any keywords and returns a list of those found."""
    found_keywords_list = []
    if not keywords or not text:
        return []

    text_lower = text.lower()
    unique_found_keywords = set() 

    for keyword in keywords:
        if not keyword:
            continue
        pattern = r'\b' + re.escape(keyword) + r'\b'
        if re.search(pattern, text_lower): 
            unique_found_keywords.add(keyword)
    return sorted(list(unique_found_keywords))


def extract_sort_key_from_url(url):
    """Extracts a sort key (year, article_number) from an ASIC Media Release URL."""
    match = re.search(r'/(\d{2})-(\d{3})mr', url, re.IGNORECASE)
    if match:
        try:
            year_yy = int(match.group(1))
            article_num = int(match.group(2))
            return (year_yy, article_num)
        except ValueError:
             return (99, 9999)
    return (99, 9999) 

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
         print(f"\n--- WebDriver Error ---")
         print(f"Error initializing WebDriver: {e}")
         print("Ensure ChromeDriver is installed and accessible (in PATH or specify executable_path in ChromeService).")
         print("Download: https://chromedriver.chromium.org/downloads")
         print("-----------------------\n")
         return None

def fetch_and_check_article_content_selenium(driver, article_url, keywords):
    """
    Fetches article page, extracts title and date adaptively, processes text, and checks for keywords.
    """
    print(f"  Checking article: {article_url}")
    extracted_iso_date = None
    article_title = ""
    found_keywords_list = []

    try:
        time.sleep(REQUEST_DELAY)
        print(f"    Navigating to article page...")
        driver.get(article_url)
        print(f"    Page loaded. Processing...")

        page_source = driver.page_source
        soup = BeautifulSoup(page_source, 'html.parser')

        # Attempt 1: Specific structure for Media Releases
        media_release_header = soup.find('header', class_='media-release')
        if media_release_header:
            print(f"    Found <header class='media-release'>. Extracting title/date for {article_url}.")
            h1_tag = media_release_header.find('h1')
            if h1_tag:
                article_title = h1_tag.get_text(strip=True)
                print(f"    Extracted MR article title: {article_title}")
            
            date_tag_mr = media_release_header.find('time', class_='nh-mr-date')
            if date_tag_mr:
                date_str_mr = date_tag_mr.get_text(strip=True)
                if date_str_mr:
                    try:
                        parsed_date_mr = datetime.strptime(date_str_mr, '%d %B %Y')
                        utc_date_mr = parsed_date_mr.replace(tzinfo=timezone.utc)
                        extracted_iso_date = utc_date_mr.strftime('%Y-%m-%dT%H:%M:%S+00:00')
                        print(f"    Extracted MR date: {extracted_iso_date}")
                    except ValueError:
                        print(f"    Warning: Could not parse MR date string '{date_str_mr}'.")
        else:
            # Attempt 2: Fallback for Reports/Publications or other structures
            print(f"    <header class='media-release'> not found. Attempting fallback extraction for {article_url}.")
            
            main_content_area = soup.find('article') or soup.find('div', role='main') or soup.find('main')
            h1_tag_fallback = None
            if main_content_area:
                h1_tag_fallback = main_content_area.find('h1')
            if not h1_tag_fallback: 
                h1_tag_fallback = soup.find('h1')
            
            if h1_tag_fallback:
                article_title = h1_tag_fallback.get_text(strip=True)
                print(f"    Extracted fallback article title: {article_title}")
            else:
                print(f"    Warning: No <h1> tag found for fallback title extraction on {article_url}.")

            published_date_p = soup.find('p', class_='published-date')
            if published_date_p:
                date_tag_fallback = published_date_p.find('time')
                if date_tag_fallback:
                    date_str_fallback = date_tag_fallback.get_text(strip=True) or date_tag_fallback.get('datetime')
                    if date_str_fallback:
                        try:
                            if '-' in date_str_fallback and len(date_str_fallback.split('-')[0]) == 4 : 
                                parsed_date_fallback = datetime.strptime(date_str_fallback, '%Y-%m-%d')
                            else: 
                                parsed_date_fallback = datetime.strptime(date_str_fallback, '%d %B %Y')
                            utc_date_fallback = parsed_date_fallback.replace(tzinfo=timezone.utc)
                            extracted_iso_date = utc_date_fallback.strftime('%Y-%m-%dT%H:%M:%S+00:00')
                            print(f"    Extracted fallback date (from p.published-date time): {extracted_iso_date}")
                        except ValueError:
                            print(f"    Warning: Could not parse fallback date string '{date_str_fallback}' from p.published-date time.")
        
        print(f"    Processing text with newspaper3k...")
        article_parser = NewspaperArticle(article_url, language='en')
        article_parser.download(input_html=page_source)
        article_parser.parse()
        article_text = article_parser.text

        if not article_text:
            print(f"    Warning: newspaper3k could not extract main text from {article_url}.")
            article_text = ""

        print(f"    Extracted {len(article_text)} characters using newspaper3k for keyword check.")
        found_keywords_list = find_matching_keywords(article_text, keywords)

        if not extracted_iso_date and article_parser.publish_date:
            print(f"    Attempting date extraction from newspaper3k metadata for {article_url}.")
            try:
                publish_dt = article_parser.publish_date
                if publish_dt.tzinfo is None:
                    utc_date_np = publish_dt.replace(tzinfo=timezone.utc)
                else:
                    utc_date_np = publish_dt.astimezone(timezone.utc)
                extracted_iso_date = utc_date_np.strftime('%Y-%m-%dT%H:%M:%S+00:00')
                print(f"    Extracted and formatted date (from newspaper3k): {extracted_iso_date}")
            except Exception as e_np_date:
                print(f"    Warning: Could not format newspaper3k publish_date: {e_np_date}")
        
        if found_keywords_list:
            print(f"    DEBUG: Matched keywords for {article_url}: {found_keywords_list}")
        else:
            print(f"    DEBUG: No keywords matched for {article_url}.")

        return (found_keywords_list, extracted_iso_date, article_title)

    except TimeoutException:
        print(f"  Error: Page load timed out for {article_url}.")
        return ([], None, article_title)
    except ArticleException as e:
        print(f"  Error: newspaper3k failed to process article {article_url}: {e}")
        return ([], extracted_iso_date, article_title)
    except WebDriverException as e:
        print(f"  Error navigating or processing {article_url} with Selenium: {e}")
        return ([], None, article_title)
    except Exception as e:
        print(f"  Unexpected error processing article {article_url}: {e}")
        import traceback
        print(traceback.format_exc()) 
        return ([], None, article_title)

# --- Main Script ---

print(f"--- Starting ASIC Article Scraper (Selenium - ISO Date Format UTC) ---")

keywords_to_check = load_keywords(KEYWORDS_TXT)
if not keywords_to_check:
     print("Proceeding without keyword filtering as no keywords were loaded or file was empty.")

checked_urls = load_checked_urls(CHECKED_URLS_FILE)
driver = setup_driver()

if not driver:
    print("--- Script Finished (WebDriver Setup Error) ---")
    exit()

urls_to_process = set()
articles_to_add = []

try:
    for source_info in SOURCE_URLS_TO_SCRAPE:
        current_page_url = source_info["url"]
        page_type = source_info["type"] 
        print(f"\n--- Processing source: {source_info['name']} from {current_page_url} ---")
        
        try:
            driver.get(current_page_url)
            print(f"Pausing for {MAIN_PAGE_LOAD_WAIT} seconds for {source_info['name']} to load...")
            time.sleep(MAIN_PAGE_LOAD_WAIT)

            page_source = driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            all_links_on_page = soup.find_all('a', href=True)
            print(f"Found {len(all_links_on_page)} total links on {source_info['name']} page.")

            temp_urls_from_this_source = set()
            skipped_year_count_source = 0
            skipped_checked_count_source = 0
            skipped_other_count_source = 0

            for link_tag in all_links_on_page:
                href = link_tag['href']
                try:
                    full_url = urljoin(BASE_URL, href)

                    # 1. Basic Exclusions
                    if not full_url.startswith(BASE_URL) or \
                       full_url.startswith('#') or \
                       full_url.startswith('mailto:') or \
                       full_url.startswith('javascript:') or \
                       any(full_url.lower().endswith(ext) for ext in ['.pdf', '.zip', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx', '.jpg', '.png', '.gif']):
                        skipped_other_count_source += 1
                        continue

                    # 2. Skip if already checked (in-memory for this run)
                    if full_url in checked_urls or full_url in urls_to_process: # also check urls_to_process to avoid duplicates from same page
                        skipped_checked_count_source += 1
                        continue
                    
                    # 3. Skip if it's one of the source listing pages itself
                    if full_url in [s_info["url"] for s_info in SOURCE_URLS_TO_SCRAPE]:
                        skipped_other_count_source += 1
                        continue

                    # 4. Type-specific structural validation
                    is_potential_content_page = False
                    if page_type == "media_release":
                        if full_url.startswith(MEDIA_RELEASES_URL) and \
                           re.search(r'/(\d{2})-(\d{3})mr[-/]', full_url, re.IGNORECASE): # Stricter MR pattern
                            is_potential_content_page = True
                    elif page_type == "report_publication":
                        # Exclude common non-document paths and news-items explicitly
                        if "/news-centre/news-items/" in full_url or \
                           "/about-asic/" in full_url or \
                           any(skip_path in full_url for skip_path in ["/contact-us", "/sitemap", "/privacy", "/freedom-of-information", "/accessibility", "/copyright", "/dealing-with-asic"]):
                            is_potential_content_page = False
                        elif (full_url.startswith(REPORTS_PUBLICATIONS_URL) and full_url != REPORTS_PUBLICATIONS_URL and len(full_url.replace(REPORTS_PUBLICATIONS_URL, "").strip('/').split('/')) >= 1) or \
                             (full_url.startswith(f"{BASE_URL}/regulatory-resources/") and any(sub_path in full_url for sub_path in ["/reports/", "/consultation-papers/", "/information-sheets/", "/key-matters/"])) or \
                             (full_url.startswith(f"{BASE_URL}/consultations/") and full_url != f"{BASE_URL}/consultations/" and len(full_url.replace(f"{BASE_URL}/consultations/", "").strip('/').split('/')) >=1 ):
                            is_potential_content_page = True
                    
                    if not is_potential_content_page:
                        # print(f"    Skipping (structure/type mismatch): {full_url}") # Debug
                        skipped_other_count_source += 1
                        continue

                    # 5. Media Release URL Year Pre-filter (only if it's a potential MR page)
                    if page_type == "media_release": # is_potential_content_page is True here
                        year_match = re.search(r'/(\d{2})-(\d{3})mr', full_url, re.IGNORECASE)
                        if year_match: # Should always match due to stricter check above
                            year_yy_from_url = int(year_match.group(1))
                            if year_yy_from_url < MIN_YEAR_YY:
                                skipped_year_count_source += 1
                                checked_urls.add(full_url) # Add to in-memory set for this session
                                continue 
                        # No else needed, if it passed is_potential_content_page for MR, it's structurally okay

                    temp_urls_from_this_source.add(full_url)
                except Exception as e_link_proc:
                    print(f"Warning: Error processing individual link href '{href}': {e_link_proc}")
                    skipped_other_count_source += 1
            
            print(f"Filtering for {source_info['name']} complete. Skipped: {skipped_year_count_source} (URL year pre-filter), {skipped_checked_count_source} (already checked/queued), {skipped_other_count_source} (other reasons).")
            print(f"Added {len(temp_urls_from_this_source)} unique, unchecked URLs from {source_info['name']} to main processing queue.")
            urls_to_process.update(temp_urls_from_this_source)

        except TimeoutException:
            print(f"Timeout loading main page for {source_info['name']}: {current_page_url}")
        except WebDriverException as e_wd_source:
            print(f"WebDriver error loading main page for {source_info['name']} {current_page_url}: {e_wd_source}")
        except Exception as e_source_proc:
            print(f"Unexpected error processing source {source_info['name']} {current_page_url}: {e_source_proc}")


    if urls_to_process:
        print(f"\nCollected a total of {len(urls_to_process)} unique URLs from all sources to process.")
        urls_to_process_list = sorted(list(urls_to_process), key=extract_sort_key_from_url) 
        
        processed_count = 0
        for url in urls_to_process_list:
            processed_count += 1
            print(f"\nProcessing URL {processed_count}/{len(urls_to_process_list)}: {url}")

            # Check if URL was already processed and added to checked_urls by a pre-filter (e.g. MR year pre-filter)
            # This check is somewhat redundant if the pre-filter correctly adds to checked_urls and we skip above,
            # but acts as a safeguard if a URL somehow got re-added.
            if url in checked_urls and url not in temp_urls_from_this_source: # temp_urls_from_this_source has fresh items for this run
                 # The above condition is tricky. The primary check is `if full_url in checked_urls` during link gathering.
                 # Let's assume if it's in urls_to_process, it passed initial checks.
                 pass


            found_keywords_list, article_date_iso_full, actual_article_title = fetch_and_check_article_content_selenium(driver, url, keywords_to_check)

            valid_year_for_csv = False
            if article_date_iso_full:
                try:
                    article_datetime = datetime.strptime(article_date_iso_full.split('T')[0], '%Y-%m-%d')
                    if article_datetime.year >= (2000 + MIN_YEAR_YY):
                        valid_year_for_csv = True
                    else:
                        print(f"    Skipping: Extracted date {article_datetime.year} is older than 20{MIN_YEAR_YY} for {url}.")
                except ValueError:
                    print(f"    Warning: Could not parse extracted date '{article_date_iso_full}' for year filtering for {url}.")
            else: 
                 print(f"    Skipping: No valid publication date extracted for {url} to verify year.")


            if not valid_year_for_csv:
                save_checked_url(CHECKED_URLS_FILE, url) 
                checked_urls.add(url)
                continue 

            if found_keywords_list or not keywords_to_check:
                title_for_csv = actual_article_title if actual_article_title else "Title not found"
                date_to_save = article_date_iso_full 

                articles_to_add.append({
                    'date': date_to_save,
                    'source': SOURCE_IDENTIFIER,
                    'url': url,
                    'title': title_for_csv,
                    'done': ''
                })
                print(f"    Added to CSV queue: {title_for_csv[:60]}...")
            else:
                print(f"    No matching keywords found in {url} (and keyword filter is active). Skipping CSV entry.")

            save_checked_url(CHECKED_URLS_FILE, url)
            checked_urls.add(url)
        
        print("\nFinished checking all individual articles.")

    print(f"\nFound {len(articles_to_add)} new articles to add to CSV (matching criteria and year).")

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
    print(f"\nPage load timeout during script execution: {e}")
except Exception as e:
    print(f"\nAn unexpected error occurred during the main process: {e}")
    import traceback
    print(traceback.format_exc())
finally:
    if 'driver' in locals() and driver:
        print("\nClosing WebDriver...")
        driver.quit()
        print("WebDriver closed.")

print("\n--- Script Finished ---")
