"""This script serves as an example on how to use Python 
   & Playwright to scrape/extract data from Google Maps"""

from playwright.sync_api import sync_playwright
from dataclasses import dataclass, asdict, field
import pandas as pd
import argparse
import os
import sys
import sqlite3
import re
from datetime import datetime

@dataclass
class Business:
    """holds business data"""

    name: str = None
    address: str = None
    website: str = None
    phone_number: str = None
    reviews_count: int = None
    reviews_average: float = None
    latitude: float = None
    longitude: float = None
    tag: str = None


@dataclass
class BusinessList:
    """holds list of Business objects and saves to Excel
    """
    business_list: list[Business] = field(default_factory=list)
    save_at = 'output'

    def dataframe(self):
        """transform business_list to pandas dataframe

        Returns: pandas dataframe with proper column structure
        """
        # Convert business objects to a list of dictionaries
        business_dicts = [asdict(business) for business in self.business_list]
        
        # Create DataFrame with proper orientation (businesses as rows, attributes as columns)
        df = pd.DataFrame(business_dicts)
        
        # Reorder columns for better readability
        column_order = [
            'name', 
            'address', 
            'website', 
            'phone_number', 
            'reviews_count', 
            'reviews_average', 
            'latitude', 
            'longitude'
        ]
        
        # Only include columns that exist in the DataFrame
        column_order = [col for col in column_order if col in df.columns]
        
        return df[column_order]

    def save_to_excel(self, filename):
        """saves pandas dataframe to excel (xlsx) file with proper formatting

        Args:
            filename (str): filename (without extension)
            
        Returns:
            str: Path to the saved Excel file
        """
        # Ensure the output directory exists
        if not os.path.exists(self.save_at):
            os.makedirs(self.save_at)
            
        # Create full path
        filepath = os.path.join(self.save_at, f"{filename}.xlsx")
        
        # Get the dataframe
        df = self.dataframe()
        
        # Create a Pandas Excel writer using XlsxWriter as the engine
        writer = pd.ExcelWriter(filepath, engine='xlsxwriter')
        
        # Convert the dataframe to an XlsxWriter Excel object
        df.to_excel(writer, sheet_name='Businesses', index=False)
        
        # Get the xlsxwriter workbook and worksheet objects
        workbook = writer.book
        worksheet = writer.sheets['Businesses']
        
        # Add a header format
        header_format = workbook.add_format({
            'bold': True,
            'text_wrap': True,
            'valign': 'top',
            'fg_color': '#4F81BD',
            'border': 1,
            'color': 'white'
        })
        
        # Format the header row
        for col_num, value in enumerate(df.columns.values):
            worksheet.write(0, col_num, value, header_format)
        
        # Set column widths to be at least 20 characters wide
        for i, col in enumerate(df.columns):
            # Find the maximum length in the column
            max_length = max(
                df[col].astype(str).apply(len).max(),
                len(str(col))  # Length of column name
            )
            # Set the column width (adding a little extra space)
            worksheet.set_column(i, i, min(max_length + 2, 30))
        
        # Close the Pandas Excel writer and output the Excel file
        writer.close()
        
        return filepath



def extract_coordinates_from_url(url: str) -> tuple[float,float]:
    """helper function to extract coordinates from url"""
    
    coordinates = url.split('/@')[-1].split('/')[0]
    # return latitude, longitude
    return float(coordinates.split(',')[0]), float(coordinates.split(',')[1])

DB_PATH = "scraper_data.sqlite"

def _norm(s: str) -> str:
    if not s:
        return ""
    return re.sub(r"\s+", " ", str(s).strip().lower())

def _phone_key(phone: str) -> str:
    if not phone:
        return ""
    digits = re.sub(r"\D+", "", phone)
    return digits

def make_dedupe_key(business: dict) -> str:
    phone = _phone_key(business.get("phone_number"))
    if not phone:
        return ""
    return f"p:{phone}"

def _db_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn

def init_db():
    conn = _db_conn()
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS businesses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dedupe_key TEXT NOT NULL UNIQUE,
                tag TEXT,
                batch_name TEXT,
                name TEXT,
                address TEXT,
                website TEXT,
                phone_number TEXT,
                reviews_count INTEGER,
                reviews_average REAL,
                latitude REAL,
                longitude REAL,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.commit()
    finally:
        conn.close()

def insert_business(tag: str, batch_name: str, business: dict) -> bool:
    dedupe_key = make_dedupe_key(business)
    if not dedupe_key:
        return False
    conn = _db_conn()
    try:
        cur = conn.execute(
            """
            INSERT OR IGNORE INTO businesses (
                dedupe_key, tag, batch_name, name, address, website, phone_number,
                reviews_count, reviews_average, latitude, longitude, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                dedupe_key,
                tag,
                batch_name or "",
                business.get("name") or "",
                business.get("address") or "",
                business.get("website") or "",
                business.get("phone_number") or "",
                int(business.get("reviews_count")) if str(business.get("reviews_count")).isdigit() else None,
                float(business.get("reviews_average")) if _norm(business.get("reviews_average")) else None,
                business.get("latitude"),
                business.get("longitude"),
                datetime.now().isoformat(timespec="seconds"),
            ),
        )
        conn.commit()
        return cur.rowcount == 1
    finally:
        conn.close()


def main():
    
    ########
    # input 
    ########
    
    # read search from arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--search", type=str)
    parser.add_argument("-t", "--total", type=int)
    parser.add_argument("-d", "--debug", action="store_true", help="Enable debug mode (headful browser)")
    parser.add_argument("-i", "--input_file", type=str, default="input.txt", help="Input file with search queries (default: input.txt)")
    parser.add_argument("-b", "--batch_name", type=str, default="", help="Batch name for the campaign")
    args = parser.parse_args()
    
    init_db()
    
    if args.search:
        search_list = [args.search]
        
    if args.total:
        total = args.total
    else:
        # if no total is passed, we set the value to random big number
        total = 1_000_000

    if not args.search:
        search_list = []
        # read search from input.txt file
        input_file_name = args.input_file
        # Get the absolute path of the file in the current working directory
        input_file_path = os.path.join(os.getcwd(), input_file_name)
        # Check if the file exists
        if os.path.exists(input_file_path):
        # Open the file in read mode
            with open(input_file_path, 'r') as file:
            # Read all lines into a list
                search_list = file.readlines()
                
        if len(search_list) == 0:
            print('Error occured: You must either pass the -s search argument, or add searches to input.txt')
            sys.exit()
        
    ###########
    # scraping
    ###########
    # When spawned by a worker after a crash, SCRAPER_INCOGNITO=1 is set in
    # the environment so we get a completely fresh browser profile with no
    # stale cookies, cache, or session state from a previous crashed run.
    use_incognito = os.environ.get("SCRAPER_INCOGNITO", "0") == "1"

    with sync_playwright() as p:
        # Use headful browser if debug mode is enabled
        headless_mode = not args.debug
        if args.debug:
            print("DEBUG MODE: Running with visible browser")

        browser = p.chromium.launch(
            headless=headless_mode,
            slow_mo=1000 if args.debug else None,
            args=[
                "--disable-logging",
                "--log-level=3",
            ],
        )

        # Incognito = new isolated browser context (no shared cookies/cache)
        if use_incognito:
            context = browser.new_context()
        else:
            context = browser.new_context()

        page = context.new_page()

        print("Navigating to Google Maps...")
        page.goto("https://www.google.com/maps", timeout=60000)
        # wait is added for dev phase. can remove it in production
        page.wait_for_timeout(5000)
        
        # Handle cookie consent and other popups
        try:
            # Try to accept cookies if button appears - supports multiple languages
            cookie_selectors = [
                '//button[contains(., "Accept all")]',  # English
                '//button[contains(., "Alle akzeptieren")]',  # German
                '//button[contains(., "Aceptar todo")]',  # Spanish
                '//button[contains(., "Tout accepter")]',  # French
                '//button[contains(., "Accetta tutto")]',  # Italian
                '//button[contains(., "Accept")]',
                '//button[contains(@aria-label, "Accept")]',
                '//button[contains(@class, "consent")]',
                'button[role="button"]:has-text("Accept")',
                'button[role="button"]:has-text("Alle akzeptieren")',
            ]
            for selector in cookie_selectors:
                try:
                    if page.locator(selector).count() > 0:
                        page.locator(selector).first.click()
                        if args.debug:
                            print(f"DEBUG: Clicked cookie consent button with: {selector}")
                        # Wait longer for consent to process and page to redirect
                        page.wait_for_timeout(3000)
                        # Check if we're now on maps page
                        if "google.com/maps" in page.url:
                            break
                        # If still on consent page, wait a bit more
                        page.wait_for_timeout(2000)
                        break
                except Exception:
                    continue
        except Exception as e:
            if args.debug:
                print(f"DEBUG: No cookie consent handling needed: {e}")
        
        if args.debug:
            print("DEBUG: Page loaded, waiting for manual inspection if needed")
            page.wait_for_timeout(3000)
        
        for search_for_index, search_for in enumerate(search_list):
            print(f"TAG: {search_for.strip()}")
            print(f"-----\n{search_for_index} - {search_for}".strip())
            
            if args.debug:
                print(f"DEBUG: Starting search for: {search_for.strip()}")

            try:
                # Google Maps frequently changes IDs; prefer stable attributes.
                # The user-provided markup suggests: <input role="combobox" name="q" id="ucc-1" ...>
                search_box_selectors = [
                    'css=input[name="q"][role="combobox"]',
                    'css=input[name="q"]',
                    'css=input[role="combobox"]',
                    'css=input#searchboxinput',
                    'xpath=//input[starts-with(@id, "ucc-") and @role="combobox"]',
                    'xpath=//input[starts-with(@id, "ucc-")]',
                    'xpath=//input[contains(@aria-label, "Search")]',
                    'xpath=//input[@type="text" and @role="combobox"]',
                ]

                search_box = None
                last_error = None
                for selector in search_box_selectors:
                    try:
                        candidate = page.locator(selector).first
                        candidate.wait_for(state="visible", timeout=8000)
                        # Some inputs exist but are offscreen/hidden; check visibility explicitly.
                        if candidate.is_visible():
                            search_box = candidate
                            if args.debug:
                                print(f"DEBUG: Found visible search box with selector: {selector}")
                            break
                    except Exception as e:
                        last_error = e
                        continue

                if not search_box:
                    raise Exception(f"Could not find a visible search box. Last error: {last_error}")

                if args.debug:
                    print("DEBUG: Focusing search box...")
                search_box.click()
                page.wait_for_timeout(200)

                # Clear any existing query (Maps sometimes keeps the last location/query)
                try:
                    search_box.fill("")
                except Exception:
                    pass

                if args.debug:
                    print(f"DEBUG: Typing query: {search_for.strip()}")
                search_box.fill(search_for.strip())
                page.wait_for_timeout(250)

                page.keyboard.press("Enter")
                if args.debug:
                    print("DEBUG: Pressed Enter, waiting for results...")
                    page.wait_for_timeout(3000)

                # Wait for results sidepanel/listings to start updating
                page.wait_for_timeout(5000)

                # scrolling
                if args.debug:
                    print("DEBUG: Starting to scroll for results...")
                page.hover('//a[contains(@href, "https://www.google.com/maps/place")]')

                # this variable is used to detect if the bot
                # scraped the same number of listings in the previous iteration
                previously_counted = 0
                scroll_count = 0
                while True:
                    scroll_count += 1
                    if args.debug:
                        print(f"DEBUG: Scroll iteration {scroll_count}")
                    
                    page.mouse.wheel(0, 10000)
                    page.wait_for_timeout(3000)

                    current_count = page.locator('//a[contains(@href, "https://www.google.com/maps/place")]').count()
                    if args.debug:
                        print(f"DEBUG: Found {current_count} listings")

                    if current_count >= total:
                        listings = page.locator('//a[contains(@href, "https://www.google.com/maps/place")]').all()[:total]
                        listings = [listing.locator("xpath=..") for listing in listings]
                        print(f"Total Scraped: {len(listings)}")
                        break
                    else:
                        # logic to break from loop to not run infinitely
                        # in case arrived at all available listings
                        if current_count == previously_counted:
                            listings = page.locator('//a[contains(@href, "https://www.google.com/maps/place")]').all()
                            print(f"Arrived at all available\nTotal Scraped: {len(listings)}")
                            break
                        else:
                            previously_counted = current_count
                            print(f"Currently Scraped: {current_count}")

            except Exception as e:
                print(f"ERROR during search/scrolling: {e}")
                if args.debug:
                    print("DEBUG: Taking screenshot for debugging...")
                    page.screenshot(path=f"debug_error_search_{search_for_index}.png")
                continue

            # scraping
            for listing_index, listing in enumerate(listings):
                try:
                    if args.debug:
                        print(f"DEBUG: Processing listing {listing_index + 1}/{len(listings)}")
                    
                    listing.click()
                    if args.debug:
                        print("DEBUG: Clicked on listing, waiting for details...")
                        page.wait_for_timeout(3000)
                    
                    page.wait_for_timeout(5000)

                    # Wait for details panel to appear
                    try:
                        page.wait_for_selector('//h1', timeout=8000)
                        if args.debug:
                            print("DEBUG: Details panel appeared")
                    except Exception as e:
                        print(f"[WARN] Details panel did not appear: {e}")
                        if args.debug:
                            print("DEBUG: Taking screenshot of missing details panel...")
                            page.screenshot(path=f"debug_no_details_{listing_index}.png")
                        page.wait_for_timeout(2000)

                    # Robust selectors for review count
                    review_count_selectors = [
                        '//button[@jsaction="pane.reviewChart.moreReviews"]//span',
                        '//span[contains(text(), "reviews")]',
                        '//span[contains(@aria-label, "reviews")]',
                        '//button[contains(@aria-label, "reviews")]//span'
                    ]
                    address_xpath = '//button[@data-item-id="address"]//div[contains(@class, "fontBodyMedium")]'
                    website_xpath = '//a[@data-item-id="authority"]//div[contains(@class, "fontBodyMedium")]'
                    phone_number_xpath = '//button[contains(@data-item-id, "phone:tel:")]//div[contains(@class, "fontBodyMedium")]'
                    reviews_average_xpath = '//div[@jsaction="pane.reviewChart.moreReviews"]//div[@role="img"]'
                    
                    business = Business()
                    business.tag = search_for.strip()

                    # Get business name with improved logic
                    business.name = ""
                    
                    # Try multiple selectors for business name
                    name_selectors = [
                        '//h1[contains(@class, "DUwDvf")]',  
                        '//h1[contains(@class, "fontHeadlineLarge")]',  
                        '//div[@data-attrid="title"]//span',  
                        '//h1[@data-attrid="title"]',  
                        '//h1//span[not(contains(@class, "google-symbols"))]'
                    ]
                    
                    for selector in name_selectors:
                        try:
                            if page.locator(selector).count() > 0:
                                name_text = page.locator(selector).first.inner_text().strip()
                                # Filter out unwanted text like "Sponsored", "Results", etc.
                                if (name_text and 
                                    len(name_text) > 2 and 
                                    not name_text.lower().startswith('sponsored') and
                                    name_text.lower() != 'results' and
                                    not name_text.lower().startswith('search')):
                                    business.name = name_text
                                    break
                        except Exception as e:
                            continue
                    
                    # Fallback to aria-label if all selectors fail
                    if not business.name:
                        try:
                            name_attr = listing.get_attribute('aria-label')
                            if name_attr and len(name_attr.strip()) > 2:
                                business.name = name_attr.strip()
                        except:
                            pass
                    
                    # Debug print if still missing
                    if not business.name:
                        try:
                            print("[DEBUG] All <h1> text:", h1_texts)
                        except Exception as e:
                            print(f"[DEBUG] Could not get <h1> text: {e}")
                        try:
                            print("[DEBUG] All <span> in <h1>:", [el.text_content() for el in page.locator('//h1//span').all()])
                        except Exception as e:
                            print(f"[DEBUG] Could not get <span> in <h1>: {e}")

                    # Get review count with debug
                    business.reviews_count = ""
                    for sel in review_count_selectors:
                        try:
                            if page.locator(sel).count() > 0:
                                raw_text = page.locator(sel).first.text_content()
                                if raw_text:
                                    digits = ''.join(ch for ch in raw_text if ch.isdigit())
                                    if digits:
                                        business.reviews_count = int(digits)
                                        break
                        except Exception as e:
                            print(f"[DEBUG] Review count selector {sel} failed: {e}")
                    if page.locator(address_xpath).count() > 0:
                        business.address = page.locator(address_xpath).all()[0].inner_text()
                    else:
                        business.address = ""
                    if page.locator(website_xpath).count() > 0:
                        business.website = page.locator(website_xpath).all()[0].inner_text()
                    else:
                        business.website = ""
                    if page.locator(phone_number_xpath).count() > 0:
                        business.phone_number = page.locator(phone_number_xpath).all()[0].inner_text()
                    else:
                        business.phone_number = ""

                    if page.locator(reviews_average_xpath).count() > 0:
                        try:
                            rating_attr = page.locator(reviews_average_xpath).get_attribute('aria-label')
                            if rating_attr:
                                business.reviews_average = float(
                                    rating_attr.split()[0].replace(',', '.').strip()
                                )
                            else:
                                business.reviews_average = ""
                        except Exception as e:
                            print(f"Error getting rating: {e}")
                            business.reviews_average = ""
                    else:
                        business.reviews_average = ""

                    business.latitude, business.longitude = extract_coordinates_from_url(page.url)

                    # Insert to DB directly
                    business_dict = asdict(business)
                    inserted = insert_business(search_for.strip(), args.batch_name, business_dict)
                    mobile = bool(_phone_key(business.phone_number))
                    print(f"STATS: scraped=1, inserted={1 if inserted else 0}, skipped={0 if inserted else 1}, mobile={1 if mobile else 0}")
                except Exception as e:
                    print(f'Error occured: {e}')

    try:
        context.close()
    except Exception:
        pass
    try:
        browser.close()
    except Exception:
        pass

if __name__ == "__main__":
    main()