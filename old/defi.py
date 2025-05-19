#!/usr/bin/env python3
"""
rss_checker.py

Checks the Australian DeFi Association Substack RSS feed:
  https://australiandefiassociation.substack.com/feed

First run:
  - Prints only the latest article's URL.
  - Saves its ID to rss_state.json.

Subsequent runs:
  - Prints all new article URLs since the last run.
  - Updates rss_state.json to the newest article ID.
"""

import os
import json
import feedparser

# URL of the RSS feed
RSS_URL = 'https://australiandefiassociation.substack.com/feed'

# Path to the JSON file where we'll store the last-seen entry ID
STATE_FILE = 'rss_state.json'


def load_last_id():
    """Load the last-seen entry ID from STATE_FILE, or return None if not found."""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            data = json.load(f)
            return data.get('last_id')
    return None


def save_last_id(last_id):
    """Save the given entry ID into STATE_FILE."""
    with open(STATE_FILE, 'w') as f:
        json.dump({'last_id': last_id}, f)


def get_entry_id(entry):
    """
    Return a stable identifier for an entry.
    feedparser populates .id (from <guid>) for RSS+Atom feeds; fall back to .link.
    """
    return getattr(entry, 'id', entry.link)


def main():
    # Load previous state
    last_id = load_last_id()

    # Fetch and parse feed
    feed = feedparser.parse(RSS_URL)
    entries = feed.entries

    if not entries:
        print("⚠️  No entries found in feed.")
        return

    # Identify the newest entry
    newest = entries[0]
    newest_id = get_entry_id(newest)

    # First run: no state file exists
    if last_id is None:
        print(newest.link)
        save_last_id(newest_id)
        return

    # Subsequent runs: collect any entries newer than last_id
    new_entries = []
    for entry in entries:
        if get_entry_id(entry) == last_id:
            break
        new_entries.append(entry)

    if new_entries:
        # Print in chronological order (oldest first)
        for entry in reversed(new_entries):
            print(entry.link)
    else:
        print("No new entries since last check.")

    # Update state to the current newest entry
    save_last_id(newest_id)


if __name__ == '__main__':
    main()
