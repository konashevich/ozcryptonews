#!/usr/bin/env python3
import requests
from bs4 import BeautifulSoup
import datetime
import csv
import os
import sys
from urllib.parse import urljoin

# Configuration
URL = 'https://regtechglobal.org/news'
CSV_FILE = 'articles.csv'
SOURCE_NAME = 'regtechglobal.org'
KEYWORDS_FILE = 'australia_keywords.txt'

# CSV Columns: date (ISO), source, url, title, done
CSV_COLUMNS = ['date', 'source', 'url', 'title', 'done']


def load_keywords():
    """
    Load keywords from KEYWORDS_FILE, one per line, lowercase.
    """
    if not os.path.exists(KEYWORDS_FILE):
        print(f"Keywords file '{KEYWORDS_FILE}' not found.")
        return []
    with open(KEYWORDS_FILE, 'r', encoding='utf-8') as f:
        return [kw.strip().lower() for kw in f if kw.strip()]


def load_last_check_date():
    """
    Determine the most recent date in CSV_FILE for SOURCE_NAME.
    If absent, default to one month ago.
    """
    if not os.path.exists(CSV_FILE):
        return datetime.datetime.now() - datetime.timedelta(days=30)
    last_date = None
    with open(CSV_FILE, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get('source') != SOURCE_NAME:
                continue
            try:
                dt = datetime.datetime.fromisoformat(row['date'])
            except Exception:
                continue
            if last_date is None or dt > last_date:
                last_date = dt
    return last_date or (datetime.datetime.now() - datetime.timedelta(days=30))


def append_articles(articles):
    """
    Append list of article dicts to CSV_FILE.
    """
    file_exists = os.path.exists(CSV_FILE)
    with open(CSV_FILE, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        if not file_exists:
            writer.writeheader()
        for art in articles:
            writer.writerow(art)


def fetch_new_articles(since, keywords):
    """
    Scrape the news page, return articles newer than `since` containing any keyword
    in the title or full article text.
    Returns list of dicts: date, source, url, title, done.
    """
    resp = requests.get(URL)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, 'html.parser')

    headings = soup.find_all('h4')
    metas = soup.find_all('h5')
    new_articles = []

    for h4, h5 in zip(headings, metas):
        # Extract date string before '|'
        meta_text = h5.get_text(strip=True)
        date_str = meta_text.split('|', 1)[0].strip()
        try:
            dt = datetime.datetime.strptime(date_str, '%d %b %Y %I:%M %p')
        except ValueError:
            continue

        # Enforce articles from 2025 onwards
        if dt.year < 2025:
            continue

        if dt <= since:
            break

        title = h4.get_text(strip=True)
        link = h4.find('a', href=True)
        if not link:
            continue
        url = urljoin(URL, link['href'])

        # Fetch full article page to search text
        try:
            art_resp = requests.get(url)
            art_resp.raise_for_status()
            art_soup = BeautifulSoup(art_resp.text, 'html.parser')
            # Try common containers
            content_elem = art_soup.find('div', class_='entry-content') or art_soup.find('article')
            if content_elem:
                content_text = content_elem.get_text(separator=' ', strip=True)
            else:
                content_text = art_soup.get_text(separator=' ', strip=True)
        except Exception:
            # Skip if article fetch fails
            continue

        full_text_lower = (title + ' ' + content_text).lower()
        if not any(kw in full_text_lower for kw in keywords):
            continue

        new_articles.append({
            'date': dt.isoformat(),
            'source': SOURCE_NAME,
            'url': url,
            'title': title,
            'done': ''
        })

    # Ensure chronological: earliest first
    new_articles.reverse()
    return new_articles


def main():
    keywords = load_keywords()
    if not keywords:
        print('No keywords loaded; exiting.')
        sys.exit(1)

    last_checked = load_last_check_date()
    articles = fetch_new_articles(last_checked, keywords)

    if articles:
        append_articles(articles)
        for art in articles:
            print(art['url'])
    else:
        print('No new articles matching keywords since', last_checked.isoformat())


if __name__ == '__main__':
    main()
