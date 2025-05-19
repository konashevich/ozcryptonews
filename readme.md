# All Crypto News Australia

## Description

This project is a collection of Python scripts designed to scrape news articles from various financial and cryptocurrency-related websites. The primary focus is on articles relevant to Australia, Web3, and regulatory bodies like ASIC and AUSTRAC within WEB3 topic. The scraped data is aggregated into a single CSV file and then sent through a telegram bot.

## Features

The scraper currently supports the following 12 news sources:

* ASIC (asic.gov.au)
* Ausblock (ausblock.com.au)
* AUSTRAC (austrac.gov.au)
* Australian DeFi Association (australiandefiassociation.substack.com)
* Australian FinTech (australianfintech.com.au)
* CoinDesk (coindesk.com)
* Cointelegraph (cointelegraph.com)
* CryptoNews (cryptonews.com.au)
* Decrypt (decrypt.co)
* RegTech Global (regtechglobal.org)
* Digital Finance CRC (dfcrc.com) 
* Web3AU Newsletter (web3au.media)

The script `telegrambotsender.py` sends notifications via Telegram

## Prerequisites

* Python 3.x
* pip (Python package installer)
* A virtual environment (recommended)
* Google Chrome browser (for Selenium-based scrapers)
* ChromeDriver (compatible with your Chrome version and added to your system's PATH or placed in the project directory)

## Setup

1.  **Clone the repository (or download the files):**
    ```bash
    # If it's a git repository
    git clone <repository_url>
    cd <project_directory>
    ```

2.  **Create and activate a virtual environment:**
    * On Windows:
        ```bash
        python -m venv .venv
        .venv\Scripts\activate
        ```
    * On macOS/Linux:
        ```bash
        python3 -m venv .venv
        source .venv/bin/activate
        ```

3.  **Install dependencies:**
    requirements.txt - install these using pip:
    ```bash
    pip install -r requirements.txt
    ```
    It's highly recommended to create a `requirements.txt` file by running `pip freeze > requirements.txt` after installing all necessary packages.

## Usage

### Running all scrapers

A batch file `crytpo-news-au.bat` is included to automate the process of running all scraper scripts sequentially (Windows).

To run all scripts:

1.  Ensure you are in the project's root directory.
2.  Make sure the virtual environment (`.venv`) has been created and dependencies are installed.
3.  Execute the batch file:
    ```batch
    crytpo-news-au.bat
    ```
    *(The batch file provided in the prompt was unnamed, so I've assumed it would be named something like `run_all.bat`)*

    The batch file performs the following actions:
    * Changes to the script's directory.
    * Activates the virtual environment (`.venv\Scripts\activate.bat`).
    * Runs each Python scraper script in sequence.

## License

This project is released under the **MIT License**.

