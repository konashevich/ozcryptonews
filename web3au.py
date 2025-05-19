import feedparser
import csv
import re
import requests
import os
from dateutil import parser

def clean_html(raw_html):
  """Removes HTML tags from a string."""
  if raw_html is None:
      return ""
  cleanr = re.compile('<.*?>')
  cleantext = re.sub(cleanr, '', raw_html)
  return cleantext.strip()

def fetch_and_parse_rss(feed_url):
  """Fetches the RSS feed and returns parsed entries."""
  try:
    response = requests.get(feed_url, timeout=10)
    response.raise_for_status()
    feed = feedparser.parse(response.content)

    if feed.bozo:
        print(f"Warning: Feed may be malformed: {feed.bozo_exception}")

    return feed.entries
  except requests.exceptions.RequestException as e:
    print(f"Error fetching feed {feed_url}: {e}")
    return None
  except Exception as e:
    print(f"Error parsing feed: {e}")
    return None

def get_existing_urls(filename):
    """Reads the existing URLs from the CSV file."""
    existing_urls = set()
    if os.path.exists(filename):
        try:
            with open(filename, mode='r', newline='', encoding='utf-8') as file:
                reader = csv.reader(file)
                next(reader) # Skip the header row
                for row in reader:
                    if len(row) > 2: # Ensure the row has at least the URL column
                        existing_urls.add(row[2]) # URL is the third column (index 2)
        except Exception as e:
            print(f"Error reading existing CSV file {filename}: {e}")
            # Continue without existing URLs if reading fails
            pass
    return existing_urls


def save_articles_to_csv(articles, filename="articles.csv"):
  """Saves new parsed article data to the CSV file, appending if it exists."""
  if not articles:
      print("No new articles found to save.")
      return

  # Get URLs of articles already in the CSV
  existing_urls = get_existing_urls(filename)
  new_articles_to_save = []

  # Filter out articles that are already in the CSV
  for article in articles:
      url = getattr(article, 'link', '')
      if url and url not in existing_urls:
          new_articles_to_save.append(article)
          existing_urls.add(url) # Prevent duplicates in current fetch

  if not new_articles_to_save:
      print("No new articles to add to the CSV.")
      return

  # Determine if header needs to be written
  write_header = not os.path.exists(filename) or os.stat(filename).st_size == 0

  try:
      # Open the file in append mode ('a')
      with open(filename, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)

        if write_header:
            writer.writerow(['date', 'source', 'url', 'title', 'done'])

        for article in new_articles_to_save:
          # Convert the published date to ISO 8601 format
          published = getattr(article, 'published', '')
          try:
              parsed_date = parser.parse(published)
              date_iso = parsed_date.isoformat()
          except Exception:
              date_iso = published  # Fallback to original if parsing fails

          source = 'www.web3au.media'
          url = getattr(article, 'link', '')

          title_content = getattr(article, 'title', '')
          description_content = getattr(article, 'summary', '')

          cleaned_title = clean_html(title_content)
          cleaned_description = clean_html(description_content)

          if cleaned_title and cleaned_description:
              combined_title = f"{cleaned_title} - {cleaned_description}"
          elif cleaned_title:
              combined_title = cleaned_title
          elif cleaned_description:
              combined_title = cleaned_description
          else:
              combined_title = "No Title or Description"

          done = ''

          writer.writerow([date_iso, source, url, combined_title, done])

      print(f"Successfully added {len(new_articles_to_save)} new articles to {filename}")

  except IOError as e:
      print(f"Error writing to CSV file {filename}: {e}")


if __name__ == "__main__":
  rss_feed_url = 'https://www.web3au.media/feed'

  print(f"Fetching articles from {rss_feed_url}...")
  articles = fetch_and_parse_rss(rss_feed_url)

  if articles:
    save_articles_to_csv(articles)
  else:
    print("Failed to retrieve articles.")
