#!/usr/bin/env python3

"""
decrypt_news_scraper.py

This script:
  - Reads a list of keywords from australia_keywords.txt (or uses ['australia'] if the file is missing)
  - Uses Selenium with headless Chrome to load https://decrypt.co/search/all/[keyword]
      • Executes JavaScript to render search results and handles Cookiebot consent
      • Triggers the in-page React search widget to run the query
  - Parses rendered HTML for articles (<article> tags) extracting full ISO‑8601 timestamp, source, url, title
      • Skips non-article feeds such as price tickers, collections pages, and items without a valid timestamp
  - Appends new, unique articles into articles.csv with columns: timestamp, source, url, title, done
      • 'done' column is left blank for user checkboxes
  - Uses articles.csv itself to track which 'decrypt.co' URLs have already been collected
  - Ensures new entries are written in chronological order (oldest first, newest last)

Dependencies:
  pip install selenium beautifulsoup4 python-dateutil
  • Download Chromedriver matching your Chrome version and ensure it’s in your PATH
"""

import os
import csv
from dateutil import parser
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Configuration
CSV_FILE = 'articles.csv'
KEYWORDS_FILE = 'australia_keywords.txt'
SEARCH_URL = 'https://decrypt.co/search/all/{}'
COOKIEBOT_ACCEPT_BUTTON = '//button[@id="CybotCookiebotDialogBodyButtonAccept"]'
SEARCH_BOX_ID = 'q'
RESULT_TAG = 'article'


def load_keywords():
    try:
        with open(KEYWORDS_FILE, 'r', encoding='utf-8') as f:
            keywords = [line.strip() for line in f if line.strip()]
            if keywords:
                return keywords
    except FileNotFoundError:
        print(f"⚠️ {KEYWORDS_FILE} not found, defaulting to ['australia']")
    return ['australia']


def setup_driver():
    opts = Options()
    opts.add_argument('--headless')
    opts.add_argument('--disable-gpu')
    opts.add_argument('--no-sandbox')
    opts.add_argument('--enable-unsafe-swiftshader')
    opts.add_argument('--log-level=3')
    driver = webdriver.Chrome(options=opts)
    driver.set_page_load_timeout(30)
    return driver


def accept_cookies(driver):
    try:
        btn = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.XPATH, COOKIEBOT_ACCEPT_BUTTON))
        )
        btn.click()
    except (TimeoutException, NoSuchElementException):
        pass


def fetch_and_parse(driver, keyword):
    url = SEARCH_URL.format(keyword)
    try:
        driver.get(url)
    except TimeoutException:
        print(f"⚠️ Timeout loading {url}")
        return []

    accept_cookies(driver)
    # Trigger React search widget
    try:
        search_box = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.ID, SEARCH_BOX_ID))
        )
        search_box.clear()
        search_box.send_keys(keyword)
        search_box.send_keys(Keys.RETURN)
    except TimeoutException:
        print(f"⚠️ Could not trigger search input on {url}")

    # Wait for <article> results
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, RESULT_TAG))
        )
    except TimeoutException:
        print(f"⚠️ No <{RESULT_TAG}> elements found on {url}")

    html = driver.page_source
    return parse_results(html)


def parse_results(html):
    from dateutil import parser
    # Define the threshold date (January 1, 2025)
    threshold_date = parser.isoparse("2025-01-01T00:00:00Z")
    
    soup = BeautifulSoup(html, 'html.parser')
    articles = []
    for block in soup.select(RESULT_TAG):
        link = block.find('a', href=True)
        if not link:
            continue
        # Exclude reference blocks based on their CSS classes
        if 'linkbox__overlay' in link.get('class', []):
            continue

        href = link['href']
        url = href if href.startswith('http') else 'https://decrypt.co' + href
        # Filter out non-article sections
        if '/price/' in url or '/collections/' in url:
            continue

        title = link.get_text(strip=True)
        time_tag = block.find('time')
        if not time_tag:
            continue
        
        dt = None
        # Try parsing using the datetime attribute
        if time_tag.has_attr('datetime'):
            try:
                dt = parser.isoparse(time_tag['datetime'])
            except Exception:
                pass
        # Fallback: parse displayed text
        if not dt:
            try:
                dt = parser.parse(time_tag.get_text(strip=True))
            except Exception:
                pass
        if not dt:
            continue

        # Skip articles older than 2025
        if dt < threshold_date:
            continue

        timestamp = dt.isoformat()
        articles.append({
            'timestamp': timestamp,
            'source': 'decrypt.co',
            'url': url,
            'title': title,
            'done': ''
        })
    return articles


def main():
    # Load existing decrypt.co URLs and their timestamps
    seen = set()
    write_header = not os.path.exists(CSV_FILE)
    if not write_header:
        with open(CSV_FILE, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                if row.get('source') == 'decrypt.co':
                    seen.add(row.get('url'))

    # Fetch new articles
    driver = setup_driver()
    new_articles = []
    for keyword in load_keywords():
        print(f"🔍 Searching for: {keyword}")
        found = fetch_and_parse(driver, keyword)
        for art in found:
            if art['url'] not in seen:
                seen.add(art['url'])
                new_articles.append(art)
    driver.quit()

    # Sort new articles by timestamp (oldest first, newest last)
    new_articles.sort(key=lambda x: parser.isoparse(x['timestamp']))

    # Append to CSV with columns: timestamp, source, url, title, done
    with open(CSV_FILE, 'a', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['timestamp', 'source', 'url', 'title', 'done']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        for art in new_articles:
            writer.writerow(art)

    print(f"✅ Added {len(new_articles)} new article(s).")


if __name__ == '__main__':
    main()
