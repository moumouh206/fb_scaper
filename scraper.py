import os
import time
import random
import uuid
import requests
import mysql.connector
import argparse # For command-line arguments
from mysql.connector import Error
from playwright.sync_api import sync_playwright, TimeoutError
from dotenv import load_dotenv
from datetime import datetime
from pathlib import Path
import re
from urllib.parse import urlparse, parse_qs

# --- CONFIGURATION ---
load_dotenv()

# Database
DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')

# Scraper
FACEBOOK_MARKETPLACE_URL = 'https://www.facebook.com/marketplace/category/search?query=location%20appartement'
# How many links to AIM for during collection phase
TARGET_LINKS_TO_COLLECT = 5000
# How many listings to PROCESS from the DB in one run
PROCESS_LIMIT = 100 
# Folder to save images
IMAGE_DIR = 'images'
# Auth file
AUTH_FILE = Path('playwright_auth_state.json')

# --- DATABASE HELPER FUNCTIONS ---

def get_db_connection():
    try:
        conn = mysql.connector.connect(
            host=DB_HOST, user=DB_USER, passwd=DB_PASSWORD, database=DB_NAME
        )
        return conn
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None

# --- UTILITY FUNCTIONS ---

def slugify(text):
    text = text.lower().strip()
    text = ''.join(c for c in text if c.isalnum() or c == ' ')
    return text.replace(' ', '-')[:95]

# --- NEW HELPER FUNCTION: Commune Lookup ---
def get_commune_id(db_conn, location_name):
    """Queries the database to find a commune ID based on its name."""
    if not location_name:
        return 0 # Default value if no location name was found
    
    commune_id = 0
    try:
        cursor = db_conn.cursor(dictionary=True)
        # Use LIKE to be more forgiving with whitespace or extra details
        sql = "SELECT id FROM communes WHERE name LIKE %s OR ar_name LIKE %s LIMIT 1"
        # The `%%` adds wildcards before and after the name
        search_term = f"%{location_name.strip()}%"
        cursor.execute(sql, (search_term, search_term))
        result = cursor.fetchone()
        if result:
            commune_id = result['id']
            print(f"  > Matched location '{location_name}' to commune ID: {commune_id}")
        else:
            print(f"  > Warning: Could not find a commune ID for '{location_name}'.")
    except Exception as e:
        print(f"  > Error during commune lookup: {e}")
    return commune_id

# --- NEW HELPER FUNCTION: Phone Number Extraction ---
def extract_phone_number(text):
    """Extracts an Algerian phone number from text, handling digits and emojis."""
    if not text:
        return None

    emoji_map = {
        '0️⃣': '0', '1️⃣': '1', '2️⃣': '2', '3️⃣': '3', '4️⃣': '4',
        '5️⃣': '5', '6️⃣': '6', '7️⃣': '7', '8️⃣': '8', '9️⃣': '9'
    }
    # Replace all number emojis with actual digits
    for emoji, digit in emoji_map.items():
        text = text.replace(emoji, digit)

    # Regex to find Algerian phone numbers (e.g., 05/06/07 xx xx xx xx)
    # This looks for a 10-digit number starting with 05, 06, or 07.
    # It allows for spaces, dots, commas, or hyphens between digits.
    match = re.search(r'(0[567])[\s\.,-]?(\d{2})[\s\.,-]?(\d{2})[\s\.,"_]?(\d{2})[\s\.,-]?(\d{2})', text)
    
    if match:
        # Join all the matched groups to form a clean number
        phone_number = "".join(match.groups())
        return phone_number
    
    return None # Return None if no number is found
      
def download_image_with_name(image_url, new_filename):

    """Downloads an image and saves it with a specific filename."""
    if not os.path.exists(IMAGE_DIR):
        os.makedirs(IMAGE_DIR)

    try:
        file_path = os.path.join(IMAGE_DIR, new_filename)
        response = requests.get(image_url, stream=True, timeout=20)
        response.raise_for_status()

        with open(file_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        print(f"    - Downloaded and saved as: {new_filename}")
        return new_filename
    except requests.exceptions.RequestException as e:
        print(f"    - Could not download image {image_url}. Error: {e}")
        return None

def close_popups(page):
    """Proactively finds and closes known pop-up dialogs like chat windows."""
    
    # A list of selectors for various "close" buttons on pop-ups.
    # The most common are for chat, cookies, and notifications.
    popup_close_selectors = [
        'div[aria-label="Close chat"]',
        'div[aria-label="Close"]',
        'div[aria-label="Not now"]'
    ]

    for selector in popup_close_selectors:
        try:
            # Use .all() which doesn't fail if nothing is found.
            all_buttons = page.locator(selector).all()
            if all_buttons:
                print(f"  > Found pop-up with selector '{selector}'. Closing it.")
                # Click the last button found, as it's most likely the top-most one.
                all_buttons[-1].click(timeout=1000)
                time.sleep(1) # Give it a moment to disappear
        except Exception:
            # If clicking fails for any reason, just ignore and continue.
            pass
    

def download_image(image_url, property_id):
    if not os.path.exists(IMAGE_DIR): os.makedirs(IMAGE_DIR)
    try:
        unique_id = uuid.uuid4().hex[:8]
        file_name = f"prop_{property_id}_{unique_id}.jpg"
        file_path = os.path.join(IMAGE_DIR, file_name)
        response = requests.get(image_url, stream=True, timeout=15)
        response.raise_for_status()
        with open(file_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"  > Downloaded image: {file_name}")
        return file_name
    except requests.exceptions.RequestException as e:
        print(f"  > Could not download image {image_url}. Error: {e}")
        return None

# --- PHASE 1: LINK COLLECTION (VERSION 6 - INFINITE SCROLL) ---

def collect_links(page, db_cursor, db_conn):
    """Infinitely scrolls the marketplace page, collecting links until stopped."""
    print("\n--- PHASE 1: COLLECTING LINKS (INFINITE MODE) ---")
    print(">>> The script will run continuously. Close the browser window to stop. <<<")
    
    # --- Navigation and pop-up handling remains the same ---
    print(f"Navigating to {FACEBOOK_MARKETPLACE_URL}...")
    page.goto(FACEBOOK_MARKETPLACE_URL, wait_until='domcontentloaded', timeout=90000)

    print("Page navigated. Looking for pop-ups...")
    try:
        print("Checking for potential pop-ups (waiting up to 10 seconds)...")
        close_button_selector = 'div[aria-label="Close"], div[aria-label="Not now"]'
        close_button = page.locator(close_button_selector).first
        close_button.wait_for(state='visible', timeout=10000)
        print("Detected a potential pop-up. Trying to close it...")
        close_button.click()
        time.sleep(2)
        print("Pop-up should be closed.")
    except TimeoutError:
        print("No pop-up detected within the time limit. Continuing...")
    except Exception as e:
        print(f"An error occurred while trying to close a pop-up: {e}")

    listing_link_selector = "a[href^='/marketplace/item/']"
    try:
        print(f"Waiting for the first listing to appear ('{listing_link_selector}')...")
        page.locator(listing_link_selector).first.wait_for(state='visible', timeout=30000)
        print("First listing is visible. Starting infinite scroll and collect loop.")
    except TimeoutError:
        print("CRITICAL: Could not find any listings on the page within 30 seconds.")
        screenshot_path = "debug_screenshot_no_listings_found.png"
        page.screenshot(path=screenshot_path)
        print(f"A screenshot has been saved to '{screenshot_path}' to help you debug.")
        return

    # --- THE NEW INFINITE SCROLL LOOP ---
    collected_count_session = 0
    consecutive_scrolls_with_no_new_links = 0
    last_link_count = 0
    seen_item_ids = set()

    # The main infinite loop. It will run forever until the page is closed or script is stopped.
    while True: 
        # The 'context.on("close", ...)' event will handle stopping the script gracefully.
        # We don't need a complex 'while' condition anymore.

        links = page.locator(listing_link_selector).all()
        
        current_link_count = len(links)
        if current_link_count == last_link_count and last_link_count > 0:
            consecutive_scrolls_with_no_new_links += 1
            print(f"Scroll did not load new links. Attempt #{consecutive_scrolls_with_no_new_links}")
        else:
            consecutive_scrolls_with_no_new_links = 0

        # If it seems we've reached the bottom, wait a bit longer and try again.
        if consecutive_scrolls_with_no_new_links >= 5:
            print("No new links found after 5 scrolls. Waiting for 30 seconds before retrying...")
            time.sleep(30)
            consecutive_scrolls_with_no_new_links = 0 # Reset counter and try again
        
        last_link_count = current_link_count

        new_links_in_batch = 0
        for link in links:
            try:
                href = link.get_attribute('href')
                if not href: continue
                
                item_id = href.split('/')[3].split('?')[0]
                if item_id in seen_item_ids:
                    continue
                
                seen_item_ids.add(item_id)
                full_url = "https://www.facebook.com" + href

                sql = "INSERT IGNORE INTO marketplace_links (fb_item_id, url) VALUES (%s, %s)"
                db_cursor.execute(sql, (item_id, full_url))
                
                if db_cursor.rowcount > 0:
                    collected_count_session += 1
                    new_links_in_batch += 1
                    print(f"Collected link #{collected_count_session} in this session: {item_id}")

            except Exception as e:
                print(f"Error processing a link element: {e}")
        
        if new_links_in_batch > 0:
            # Commit to the database after each batch of new links is found.
            # This saves progress as we go.
            print(f"Committing {new_links_in_batch} new links to the database...")
            db_conn.commit()

        print(f"Scrolling down... (Total collected this session: {collected_count_session})")
        page.mouse.wheel(0, 20000)
        time.sleep(random.uniform(4, 6))


    """Scrolls the marketplace page and inserts new links into the database."""
    print("\n--- PHASE 1: COLLECTING LINKS ---")
    
    print(f"Navigating to {FACEBOOK_MARKETPLACE_URL}...")
    page.goto(FACEBOOK_MARKETPLACE_URL, wait_until='domcontentloaded', timeout=90000)

    print("Page navigated. Looking for pop-ups...")
    try:
        print("Checking for potential pop-ups (waiting up to 10 seconds)...")
        close_button_selector = 'div[aria-label="Close"], div[aria-label="Not now"]'
        close_button = page.locator(close_button_selector).first
        close_button.wait_for(state='visible', timeout=10000)
        print("Detected a potential pop-up. Trying to close it...")
        close_button.click()
        time.sleep(2)
        print("Pop-up should be closed.")
    except TimeoutError:
        print("No pop-up detected within the time limit. Continuing...")
    except Exception as e:
        print(f"An error occurred while trying to close a pop-up: {e}")

    # --- THIS IS THE KEY CHANGE ---
    # Instead of waiting for a generic container, we wait for the first actual listing link.
    # This is more reliable.
    listing_link_selector = "a[href^='/marketplace/item/']"
    try:
        print(f"Waiting for the first listing to appear ('{listing_link_selector}')...")
        # Wait for the very first element matching the selector to become visible.
        page.locator(listing_link_selector).first.wait_for(state='visible', timeout=30000)
        print("First listing is visible. Starting scroll and collect loop.")
    except TimeoutError:
        print("CRITICAL: Could not find any listings on the page within 30 seconds.")
        print("This could be due to a login issue, a new page layout, or a network problem.")
        screenshot_path = "debug_screenshot_no_listings_found.png"
        page.screenshot(path=screenshot_path)
        print(f"A screenshot has been saved to '{screenshot_path}' to help you debug.")
        return

    # --- THE SCROLLING LOOP ---
    # This part should now work correctly because we've confirmed listings are present.
    collected_count = 0
    consecutive_scrolls_with_no_new_links = 0
    last_link_count = 0
    seen_item_ids = set() # Use a set for faster checking of duplicates

    while collected_count < TARGET_LINKS_TO_COLLECT:
        # Find all currently visible links
        links = page.locator(listing_link_selector).all()
        
        current_link_count = len(links)
        if current_link_count == last_link_count and last_link_count > 0:
            consecutive_scrolls_with_no_new_links += 1
            print(f"Scroll did not load new links. Attempt #{consecutive_scrolls_with_no_new_links}")
        else:
            consecutive_scrolls_with_no_new_links = 0

        if consecutive_scrolls_with_no_new_links >= 3:
            print("No new links found after multiple scrolls. Reached the end of the page.")
            break
        
        last_link_count = current_link_count

        for link in links:
            try:
                href = link.get_attribute('href')
                if not href: continue
                
                # Extract the unique item ID
                item_id = href.split('/')[3].split('?')[0]

                # If we've already processed this ID in this session, skip it
                if item_id in seen_item_ids:
                    continue
                
                seen_item_ids.add(item_id)
                full_url = "https://www.facebook.com" + href

                # Attempt to insert, ignore if fb_item_id is already there (UNIQUE KEY)
                sql = "INSERT IGNORE INTO marketplace_links (fb_item_id, url) VALUES (%s, %s)"
                db_cursor.execute(sql, (item_id, full_url))
                
                if db_cursor.rowcount > 0: # rowcount > 0 means a new row was inserted
                    collected_count += 1
                    print(f"Collected link #{collected_count}: {item_id}")

            except (IndexError, AttributeError) as e:
                # This can happen if the href attribute is malformed.
                print(f"Warning: Could not parse a link. Error: {e}. Skipping.")
            except Exception as e:
                print(f"Error processing a link element: {e}")
        
        if collected_count >= TARGET_LINKS_TO_COLLECT:
            print(f"Target of {TARGET_LINKS_TO_COLLECT} links reached.")
            break
            
        print("Scrolling down to load more items...")
        page.mouse.wheel(0, 20000) # Increased scroll distance
        time.sleep(random.uniform(4, 6)) # Longer wait to ensure content loads

    print(f"\nLink collection finished. Total new links added in this session: {collected_count}")


# --- PHASE 2: DATA EXTRACTION ---
def process_links(page, db_conn):
    """Fetches links, scrapes data including location/phone, and inserts into DB."""
    print("\n--- PHASE 2: PROCESSING LINKS ---")
    
    cursor = db_conn.cursor(dictionary=True)
    cursor.execute(f"SELECT id, url FROM marketplace_links WHERE status = 'new' OR status = 'error' ORDER BY id ASC LIMIT {PROCESS_LIMIT} FOR UPDATE")
    links_to_process = cursor.fetchall()

    if not links_to_process:
        print("No new links to process.")
        return

    link_ids = [link['id'] for link in links_to_process]
    cursor.execute(f"UPDATE marketplace_links SET status = 'processing' WHERE id IN ({','.join(['%s']*len(link_ids))})", tuple(link_ids))
    db_conn.commit()
    print(f"Marked {len(links_to_process)} links as 'processing'.")

    for link in links_to_process:
        print(f"\n[Processing URL]: {link['url']}")
        final_status = 'error'
        
        try:
            if page.is_closed(): raise ConnectionError("Browser page closed.")
            page.goto(link['url'], wait_until='load', timeout=60000)
            close_popups(page) # Proactively close pop-ups
            time.sleep(random.uniform(2, 4))

            # --- DEFINE SELECTORS ---
            container_selector = "div.x12u81az"
            title_selector = f"{container_selector} h1[dir='auto'] > span"
            price_selector = f"{container_selector} span:has-text('DZD'), {container_selector} span:has-text('FREE')"
            location_selector = 'div:has(h2:has-text("Location")) + div span:first-child'
            desc_container_selector = 'div:has(h2:has-text("Description")) + div'
            img_selector = "div.xal61yo img, div[aria-label^='Thumbnail'] img"

            # --- EXTRACT DATA ---
            title = page.locator(title_selector).first.inner_text(timeout=10000)
            price_text = page.locator(price_selector).first.inner_text(timeout=10000)
            price_digits = ''.join(filter(str.isdigit, price_text))
            price = int(price_digits) if price_digits else 1
            
            try:
                location_text = page.locator(location_selector).first.inner_text(timeout=5000)
            except TimeoutError:
                location_text = "Unknown"

            try:
                description = page.locator(desc_container_selector).first.inner_text(timeout=5000)
            except TimeoutError:
                description = title
            
            # --- INTEGRATE HELPER FUNCTIONS ---
            phone_number = extract_phone_number(description)
            commune_id, willaya_id = get_location_ids(db_conn, location_text)
            
            print(f"  > Data Extracted: Title='{title[:30]}...', Price={price}, Location='{location_text}', Phone='{phone_number}'")

            # --- IMAGE EXTRACTION ---
            page.locator(img_selector).first.wait_for(state='visible', timeout=10000)
            image_elements = page.locator(img_selector).all()
            image_urls = sorted(list(set([img.get_attribute('src') for img in image_elements if img and img.get_attribute('src') and 'scontent' in img.get_attribute('src')])))
            if not image_urls: raise ValueError("No valid image URLs found.")
            print(f"  > Found {len(image_urls)} unique images.")
            
            # --- DOWNLOAD & INSERT ---
            downloaded_image_filenames = [download_image_with_name(url, f"{int(time.time())}_{random.randint(100, 999)}.jpg") for url in image_urls]
            if not all(downloaded_image_filenames): raise ConnectionError("An image failed to download.")

            property_data = {
                'slug': slugify(title), 'userid': 1, 'type': 1, 'choice': 'rent',
                'willaya': willaya_id, 'commune': commune_id, 'location_text': location_text,
                'title': title, 'descritpion': description, 'surface': 0, 'telephone': phone_number,
                'price': price, 'priceunite': 'DA', 'bedroom': 0, 'bethroom': 0,
                'pricenegiciae': 0, 'balcony': 0, 'agent': 0, 'latitude': 0, 'longitude': 0,
                'status': 1, 'entry_date': datetime.now(), 'published_at': datetime.now()
            }
            
            prop_cursor = db_conn.cursor()
            prop_sql = """INSERT INTO properties (slug, userid, `type`, choice, willaya, commune, location_text, title, descritpion, surface, telephone, expiredin, price, pricenegiciae, priceunite, bedroom, bethroom, balcony, agent, latitude, longitude, `status`, entry_date, published_at) 
                          VALUES (%(slug)s, %(userid)s, %(type)s, %(choice)s, %(willaya)s, %(commune)s, %(location_text)s, %(title)s, %(descritpion)s, %(surface)s, %(telephone)s, 30, %(price)s, %(pricenegiciae)s, %(priceunite)s, %(bedroom)s, %(bethroom)s, %(balcony)s, %(agent)s, %(latitude)s, %(longitude)s, %(status)s, %(entry_date)s, %(published_at)s)"""
            prop_cursor.execute(prop_sql, property_data)
            property_id = prop_cursor.lastrowid
            
            media_sql = "INSERT INTO media (model_id, model_type, file_name, user_id, created_at) VALUES (%s, 1, %s, %s, %s)"
            media_values = [(property_id, fname, 1, datetime.now()) for fname in downloaded_image_filenames]
            prop_cursor.executemany(media_sql, media_values)
            
            db_conn.commit()
            print(f"  > SUCCESS: Property ID {property_id} and media saved.")
            final_status = 'completed'

        except (ValueError, ConnectionError, TimeoutError) as e:
            print(f"  > controlled_error: SKIPPING - {e}")
            db_conn.rollback()
        except Exception as e:
            print(f"  > script_error: FAILED - {e}")
            db_conn.rollback()
            if "Target page" in str(e): break
        finally:
            update_cursor = db_conn.cursor()
            update_cursor.execute("UPDATE marketplace_links SET status = %s, processed_at = %s WHERE id = %s", (final_status, datetime.now(), link['id']))
            db_conn.commit()
            print(f"  > Marked link as '{final_status}'.")
            time.sleep(random.uniform(3, 7))


# --- MAIN EXECUTION LOGIC (MODIFIED FOR INFINITE SCROLL) ---

def main():
    parser = argparse.ArgumentParser(description="Facebook Marketplace Scraper")
    parser.add_argument('phase', choices=['collect', 'process'], help="Which phase to run: 'collect' links or 'process' them.")
    args = parser.parse_args()

    if not AUTH_FILE.exists():
        print(f"ERROR: Authentication file '{AUTH_FILE}' not found.")
        print("Please run 'python create_auth_state.py' first to log in.")
        return

    db_conn = get_db_connection()
    if not db_conn: return

    # We need a way to signal that the script should stop.
    # A simple list can act as a mutable flag.
    stop_signal = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(storage_state=AUTH_FILE)

        # --- KEY ADDITION: Handle the browser closing event ---
        def on_close():
            print("\nBrowser or context was closed! Signaling script to stop.")
            stop_signal.append(True)
        
        context.on("close", on_close)

        page = context.new_page()

        try:
            if args.phase == 'collect':
                db_cursor = db_conn.cursor()
                # Pass the db_conn to commit transactions intermittently
                collect_links(page, db_cursor, db_conn) 

            elif args.phase == 'process':
                process_links(page, db_conn)

        # Catch the exception that Playwright throws when the browser is closed mid-operation.
        except Exception as e:
            if "Target page, context or browser has been closed" in str(e) or stop_signal:
                 print("Scraper stopped gracefully because the browser was closed.")
            else:
                 print(f"\nAn unexpected error occurred in main execution: {e}")
        finally:
            print("\nClosing database connection.")
            # The browser is already closed at this point.
            if db_conn.is_connected():
                db_conn.close()


if __name__ == '__main__':
    main()