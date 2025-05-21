import csv
import logging
import os
import json
import telegram # Ensure this is python-telegram-bot v20+
from telegram.error import TelegramError
import asyncio
from datetime import datetime, timezone # Import datetime and timezone

# --- Configuration from JSON ---
config_path = os.path.join(os.path.dirname(__file__), 'telegrambot.json')
try:
    with open(config_path, 'r', encoding='utf-8') as config_file:
        config = json.load(config_file)
        TELEGRAM_BOT_TOKEN = config['TELEGRAM_BOT_TOKEN']
        TELEGRAM_CHAT_ID = config['TELEGRAM_CHAT_ID']
except FileNotFoundError:
    logging.error(f"Configuration file '{config_path}' not found. Please create it.")
    exit(1)
except KeyError as e:
    logging.error(f"Missing key in '{config_path}': {e}. Ensure TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID are present.")
    exit(1)
except Exception as e:
    logging.error(f"Failed to load configuration from {config_path}: {e}")
    exit(1)

# --- Optional Configuration ---
CSV_FILE_PATH = 'articles.csv'
CHECK_MARK = '+' # Symbol to mark rows as 'done'
CSV_HEADERS = ['date', 'source', 'url', 'title', 'done']

# --- Logging Setup ---
log = logging.getLogger(__name__) # Use __name__ for logger
if not log.handlers: # Setup handlers only if not already configured (e.g., by other modules)
    log.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # File Handler
    try:
        fh = logging.FileHandler("notifier.log", encoding='utf-8')
        fh.setFormatter(formatter)
        log.addHandler(fh)
    except Exception as e_fh:
        print(f"Error setting up file logger: {e_fh}")


    # Stream Handler (Console)
    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    log.addHandler(sh)

log.info("Telegram Bot Sender script started for a single run.")

# --- Functions ---

async def send_telegram_message_async(bot_token, chat_id, message_text):
    """Sends a message to Telegram asynchronously."""
    bot_instance = None
    try:
        # For python-telegram-bot v20+ Application/Bot is the way for context
        # However, for simple message sending, telegram.Bot is still fine.
        bot_instance = telegram.Bot(token=bot_token)
        await bot_instance.send_message(
            chat_id=chat_id,
            text=message_text,
            parse_mode='HTML',
            # Consider disable_web_page_preview=True if previews are noisy
        )
        log.info(f"Successfully sent message to chat ID {chat_id}")
        return True
    except TelegramError as e_tg:
        log.error(f"Telegram API Error sending to {chat_id}: {e_tg}")
        if "bot was blocked by the user" in str(e_tg).lower():
             log.error(f"Bot blocked by user in chat {chat_id}.")
        elif "chat not found" in str(e_tg).lower():
             log.error(f"Chat ID {chat_id} not found.")
        return False
    except Exception as e_send:
        log.error(f"Unexpected error during message sending to {chat_id}: {e_send}", exc_info=True)
        return False
    finally:
        if bot_instance:
            await bot_instance.shutdown() # Gracefully close bot session

def format_date_for_telegram(iso_date_str):
    """Converts an ISO 8601 date string to DD/MM/YYYY format."""
    if not iso_date_str:
        return "N/A"
    try:
        # Parse the ISO 8601 string. fromisoformat handles timezone offsets.
        dt_obj = datetime.fromisoformat(iso_date_str)
        # Format to DD/MM/YYYY
        return dt_obj.strftime('%d/%m/%Y')
    except ValueError:
        log.warning(f"Could not parse date string '{iso_date_str}' for Telegram formatting. Returning as is.")
        return iso_date_str # Fallback to original string if parsing fails
    except Exception as e:
        log.error(f"Unexpected error formatting date '{iso_date_str}': {e}")
        return "Error in date"


async def process_csv_and_notify(bot_token, chat_id_to_notify):
    """Reads CSV, finds new rows, sends notifications, and updates CSV."""
    rows_to_update_indices = []
    all_rows_data_list = []

    # 1. Read the CSV file
    if not os.path.exists(CSV_FILE_PATH) or os.path.getsize(CSV_FILE_PATH) == 0:
        log.warning(f"CSV file '{CSV_FILE_PATH}' missing or empty. Attempting to create/initialize.")
        try:
            with open(CSV_FILE_PATH, 'w', newline='', encoding='utf-8') as outfile_init:
                writer = csv.DictWriter(outfile_init, fieldnames=CSV_HEADERS)
                writer.writeheader()
            log.info(f"Created/initialized empty CSV '{CSV_FILE_PATH}' with headers.")
            return # Nothing to process yet
        except IOError as e_io_create:
            log.error(f"Could not create/write headers to '{CSV_FILE_PATH}': {e_io_create}")
            return

    try:
        with open(CSV_FILE_PATH, 'r', newline='', encoding='utf-8') as csvfile_read:
            reader = csv.DictReader(csvfile_read)
            if not reader.fieldnames:
                 log.error(f"CSV '{CSV_FILE_PATH}' has content but no headers. Expected: {CSV_HEADERS}")
                 return
            if list(reader.fieldnames) != CSV_HEADERS: # Strict check on header order and content
                 log.error(f"CSV headers mismatch! Expected: {CSV_HEADERS}, Found: {reader.fieldnames}. Correct '{CSV_FILE_PATH}'.")
                 return
            all_rows_data_list = list(reader)
    except FileNotFoundError: # Should be caught by os.path.exists
        log.error(f"FileNotFoundError for '{CSV_FILE_PATH}' (should have been handled).")
        return
    except Exception as e_read:
        log.error(f"Unexpected error reading CSV '{CSV_FILE_PATH}': {e_read}", exc_info=True)
        return

    if not all_rows_data_list:
        log.info("CSV has headers but no data rows. Nothing to process.")
        return

    notifications_attempted_this_run = False
    for i, row_dict in enumerate(all_rows_data_list):
        if 'done' not in row_dict: # Robustness for malformed rows
            log.warning(f"Row {i+2} (index {i}) missing 'done' column. Skipping. Data: {row_dict}")
            continue
        if str(row_dict.get('done', '')).strip() != CHECK_MARK:
            log.info(f"Found new entry at row {i+2}: Title: {row_dict.get('title', 'N/A')}")
            notifications_attempted_this_run = True

            # Format date for Telegram message
            human_readable_date = format_date_for_telegram(row_dict.get('date'))

            message_content = (
                f"<b>{row_dict.get('title', 'N/A')}</b>\n\n"
                f"<b>Date:</b> {human_readable_date}\n"
                f"<b>Source:</b> {row_dict.get('source', 'N/A')}\n"
                f"<b>Link:</b> {'<a href=\"' + row_dict.get('url') + '\">' + row_dict.get('url') + '</a>' if row_dict.get('url') else 'No URL'}"
            )
            
            if await send_telegram_message_async(bot_token, chat_id_to_notify, message_content):
                rows_to_update_indices.append(i)
            else:
                log.warning(f"Notification failed for row {i+2}. Will retry next run.")
            await asyncio.sleep(1.2)  # <-- Add this line: 1.2 seconds delay between messages
    
    # 3. Rewrite CSV if rows were successfully processed and marked for update
    if rows_to_update_indices:
        log.info(f"Attempting to update {len(rows_to_update_indices)} rows in CSV.")
        for index_to_update in rows_to_update_indices:
            if 0 <= index_to_update < len(all_rows_data_list):
                 all_rows_data_list[index_to_update]['done'] = CHECK_MARK
        try:
            with open(CSV_FILE_PATH, 'w', newline='', encoding='utf-8') as csvfile_write:
                writer = csv.DictWriter(csvfile_write, fieldnames=CSV_HEADERS)
                writer.writeheader()
                writer.writerows(all_rows_data_list)
            log.info(f"Successfully updated '{CSV_FILE_PATH}' with {len(rows_to_update_indices)} rows marked done.")
        except IOError as e_io_write_all:
            log.error(f"Could not write updates to '{CSV_FILE_PATH}': {e_io_write_all}")
        except Exception as e_write_all:
            log.error(f"Unexpected error writing updates to CSV: {e_write_all}", exc_info=True)
    elif notifications_attempted_this_run: # Notifications sent but none succeeded to mark CSV
        log.info("Notifications were attempted, but no rows were successfully processed to update in CSV this run.")
    else:
        log.info("No new, unprocessed entries found in this run.")


# --- Main Execution (Async Wrapper) ---
async def main_async():
    log.info(f"Starting single run to check '{CSV_FILE_PATH}' for new entries...")
    try:
        await process_csv_and_notify(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)
        log.info("Processing complete for this run.")
    except Exception as e_main:
        log.critical(f"A critical error occurred during the async run: {e_main}", exc_info=True)
    finally:
        log.info("Script finished single async run.")

if __name__ == "__main__":
    # For python-telegram-bot v20+, if using Application, it handles the loop.
    # For direct Bot usage in a script, asyncio.run is fine.
    asyncio.run(main_async())
