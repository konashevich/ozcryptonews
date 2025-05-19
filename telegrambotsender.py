import csv
import logging
import os
import json
import telegram
from telegram.error import TelegramError
import asyncio

# --- Configuration from JSON ---
config_path = os.path.join(os.path.dirname(__file__), 'telegrambot.json')
try:
    with open(config_path, 'r', encoding='utf-8') as config_file:
        config = json.load(config_file)
        TELEGRAM_BOT_TOKEN = config['TELEGRAM_BOT_TOKEN']
        TELEGRAM_CHAT_ID = config['TELEGRAM_CHAT_ID']
except Exception as e:
    logging.error(f"Failed to load configuration from {config_path}: {e}")
    exit(1)

# --- Optional Configuration ---
CSV_FILE_PATH = 'articles.csv'         # Path to your CSV file (Consider changing back from articles_test.csv if needed)
CHECK_MARK = '+'                       # Symbol to mark rows as 'done'
# POLL_INTERVAL is no longer used for waiting, but kept here for potential future use or reference
POLL_INTERVAL = 3                     # Seconds to wait between checking the CSV file (Informational only now)
CSV_HEADERS = ['date', 'source', 'url', 'title', 'done'] # Expected CSV header row

# --- Logging Setup ---
log = logging.getLogger()
if not log.handlers:
    log.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    # File Handler
    fh = logging.FileHandler("notifier.log")
    fh.setFormatter(formatter)
    log.addHandler(fh)

    # Stream Handler (Console)
    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    log.addHandler(sh)

logging.info("Script started for a single run.")

# --- Functions ---

async def send_telegram_message(bot_token, chat_id, message):
    """Sends a message to the specified Telegram chat using an ephemeral bot instance."""
    bot = None # Ensure bot is defined in the scope
    try:
        # Initialize bot inside the async function for compatibility with sync code
        bot = telegram.Bot(token=bot_token)
        await bot.send_message(
            chat_id=chat_id,
            text=message,
            parse_mode='HTML' # Use HTML for basic formatting like bold, italics, links
        )
        logging.info(f"Successfully sent message to chat ID {chat_id}")
        return True
    # --- Corrected Error Handling ---
    except TelegramError as e: # Catch the specific imported error class
        logging.error(f"Telegram API Error: Failed to send message to chat ID {chat_id}: {e}")
        # Specific error handling can be added here (e.g., Unauthorized, ChatNotFound)
        if "bot was blocked by the user" in str(e):
             logging.error(f"Bot was blocked by the user in chat {chat_id}. Please unblock the bot.")
        elif "chat not found" in str(e):
             logging.error(f"Chat ID {chat_id} not found. Is the ID correct and did the bot join/start chat?")
        # Add more specific error checks as needed
        return False
    except Exception as e:
        logging.error(f"An unexpected error occurred during message sending: {e}")
        return False
    finally:
        # Clean up the bot session if initialized
        if bot:
            # Ensure the bot connection is closed properly
            await bot.shutdown()


def process_csv(bot_token, chat_id):
    """Reads the CSV, finds new rows, sends notifications, and updates the CSV."""
    rows_to_update_indices = []
    all_rows_data = []

    # 1. Read the CSV file
    try:
        # Check file size before opening to handle empty file case cleanly
        if not os.path.exists(CSV_FILE_PATH) or os.path.getsize(CSV_FILE_PATH) == 0:
            logging.warning(f"CSV file '{CSV_FILE_PATH}' is missing or empty. Attempting to create/initialize.")
            try:
                with open(CSV_FILE_PATH, 'w', newline='', encoding='utf-8') as outfile:
                    writer = csv.DictWriter(outfile, fieldnames=CSV_HEADERS)
                    writer.writeheader()
                logging.info(f"Created/initialized empty CSV file '{CSV_FILE_PATH}' with headers.")
                return # Exit processing for this run, nothing to read yet
            except IOError as e_write:
                logging.error(f"Could not create/write headers to CSV file '{CSV_FILE_PATH}': {e_write}")
                return # Cannot proceed

        # File exists and is not empty, proceed to read
        with open(CSV_FILE_PATH, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)

            # Check if headers are present after opening
            if not reader.fieldnames:
                 # This case should ideally be caught by the size check, but as a fallback
                 logging.error(f"CSV file '{CSV_FILE_PATH}' has content but no headers detected. Please add headers: {','.join(CSV_HEADERS)}")
                 return # Cannot proceed without headers

            # Check if headers match expected headers
            if reader.fieldnames != CSV_HEADERS:
                 logging.error(f"CSV headers mismatch! Expected: {CSV_HEADERS}, Found in file: {reader.fieldnames}. Please correct '{CSV_FILE_PATH}'.")
                 return # Stop processing until headers are corrected

            all_rows_data = list(reader) # Read all rows into memory

    except FileNotFoundError:
         # This case is now handled by the os.path.exists check above, but kept as safeguard
        logging.error(f"FileNotFoundError occurred unexpectedly for '{CSV_FILE_PATH}'.")
        return # Exit processing for this run
    except Exception as e:
        logging.error(f"An unexpected error occurred while reading CSV '{CSV_FILE_PATH}': {e}", exc_info=True)
        return # Stop processing this run

    # 2. Process rows and identify new ones
    if not all_rows_data:
        logging.info("CSV file contains headers but no data rows. Nothing to process.")
        return # No data to process

    notification_sent_in_cycle = False
    tasks = [] # List to hold potential async tasks if needed, though currently running sequentially

    for i, row in enumerate(all_rows_data):
        # Ensure row has the 'done' key, handle potential malformed rows gracefully
        if 'done' not in row:
            logging.warning(f"Row {i+2} (index {i}) is missing the 'done' column. Skipping. Row data: {row}")
            continue # Skip this malformed row

        # Check if the 'done' column is empty or doesn't contain the check mark
        # Using strip() to handle potential whitespace
        if str(row.get('done', '')).strip() != CHECK_MARK:
            logging.info(f"Found new entry at row {i+2}: Title: {row.get('title', 'N/A')}")

            # Format the message (using HTML for links and basic styling)
            # Added checks for missing keys to prevent errors
            message = (
                f"📰 <b>New Article Found!</b>\n\n"
                f"<b>Date:</b> {row.get('date', 'N/A')}\n"
                f"<b>Source:</b> {row.get('source', 'N/A')}\n"
                f"<b>Title:</b> {row.get('title', 'N/A')}\n"
                # Ensure URL exists and is not empty before creating a link
                f"<b>Link:</b> {'<a href=\"' + row.get('url') + '\">' + row.get('url') + '</a>' if row.get('url') else 'No URL provided'}"
            )

            # Send notification via Telegram
            # Run the async send function. Using asyncio.run() for each is simple for single-run script.
            try:
                if asyncio.run(send_telegram_message(bot_token, chat_id, message)):
                    # Mark index for update only if sending succeeded
                    rows_to_update_indices.append(i)
                    notification_sent_in_cycle = True
                else:
                    logging.warning(f"Notification failed for row {i+2}. It will be retried next time the script is run.")
            except RuntimeError as e:
                 logging.error(f"RuntimeError during asyncio.run (might indicate conflicting event loops): {e}")
            except Exception as e:
                 logging.error(f"Unexpected error calling send_telegram_message for row {i+2}: {e}", exc_info=True)


    # 3. Rewrite the CSV file *only if* notifications were successfully sent and rows marked for update
    if rows_to_update_indices:
        logging.info(f"Attempting to update {len(rows_to_update_indices)} rows in the CSV.")
        # Update the 'done' status in the in-memory data for the successfully processed rows
        for index in rows_to_update_indices:
            # Double check index is valid before access
            if 0 <= index < len(all_rows_data):
                 all_rows_data[index]['done'] = CHECK_MARK
            else:
                 logging.error(f"Attempted to update invalid index {index}. Skipping.") # Should not happen normally

        # Write the entire updated data back to the CSV
        try:
            with open(CSV_FILE_PATH, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=CSV_HEADERS)
                writer.writeheader()
                writer.writerows(all_rows_data)
            logging.info(f"Successfully updated CSV file '{CSV_FILE_PATH}' with {len(rows_to_update_indices)} processed rows marked as done.")
        except IOError as e:
            logging.error(f"Could not write updates to CSV file '{CSV_FILE_PATH}': {e}")
            # Note: If writing fails, the rows are not marked as done and will be retried next run.
        except Exception as e:
            logging.error(f"An unexpected error occurred while writing updates to CSV: {e}", exc_info=True)
    elif notification_sent_in_cycle:
         # This case might happen if sending succeeded but updating indices list failed somehow
         logging.warning("Notifications were sent, but no rows were marked for CSV update. Check for errors.")
    else:
        logging.info("No new, unprocessed entries found in this run.")


# --- Main Execution ---
if __name__ == "__main__":
    logging.info(f"Starting single run to check '{CSV_FILE_PATH}' for new entries...")

    try:
        # Call the processing function directly, once.
        process_csv(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)
        logging.info("Processing complete for this run.")
    except Exception as e:
        # Catch unexpected errors during the single run
        logging.critical(f"A critical error occurred during the run: {e}", exc_info=True)
    finally:
        logging.info("Script finished single run.")

