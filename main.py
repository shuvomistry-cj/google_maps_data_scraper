"""This script serves as an example on how to use Python 
   & Playwright to scrape/extract data from Google Maps"""

from playwright.sync_api import sync_playwright
from dataclasses import dataclass, asdict, field
import pandas as pd
import argparse
import os
import sys

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

def main():
    
    ########
    # input 
    ########
    
    # read search from arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--search", type=str)
    parser.add_argument("-t", "--total", type=int)
    args = parser.parse_args()
    
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
        input_file_name = 'input.txt'
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
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        page.goto("https://www.google.com/maps", timeout=60000)
        # wait is added for dev phase. can remove it in production
        page.wait_for_timeout(5000)
        
        for search_for_index, search_for in enumerate(search_list):
            print(f"-----\n{search_for_index} - {search_for}".strip())

            page.locator('//input[@id="searchboxinput"]').fill(search_for)
            page.wait_for_timeout(3000)

            page.keyboard.press("Enter")
            page.wait_for_timeout(5000)

            # scrolling
            page.hover('//a[contains(@href, "https://www.google.com/maps/place")]')

            # this variable is used to detect if the bot
            # scraped the same number of listings in the previous iteration
            previously_counted = 0
            while True:
                page.mouse.wheel(0, 10000)
                page.wait_for_timeout(3000)

                if (
                    page.locator(
                        '//a[contains(@href, "https://www.google.com/maps/place")]'
                    ).count()
                    >= total
                ):
                    listings = page.locator(
                        '//a[contains(@href, "https://www.google.com/maps/place")]'
                    ).all()[:total]
                    listings = [listing.locator("xpath=..") for listing in listings]
                    print(f"Total Scraped: {len(listings)}")
                    break
                else:
                    # logic to break from loop to not run infinitely
                    # in case arrived at all available listings
                    if (
                        page.locator(
                            '//a[contains(@href, "https://www.google.com/maps/place")]'
                        ).count()
                        == previously_counted
                    ):
                        listings = page.locator(
                            '//a[contains(@href, "https://www.google.com/maps/place")]'
                        ).all()
                        print(f"Arrived at all available\nTotal Scraped: {len(listings)}")
                        break
                    else:
                        previously_counted = page.locator(
                            '//a[contains(@href, "https://www.google.com/maps/place")]'
                        ).count()
                        print(
                            f"Currently Scraped: ",
                            page.locator(
                                '//a[contains(@href, "https://www.google.com/maps/place")]'
                            ).count(),
                        )

            business_list = BusinessList()

            # scraping
            for listing in listings:
                try:
                    listing.click()
                    page.wait_for_timeout(5000)

                    # Wait for details panel to appear
                    try:
                        page.wait_for_selector('//h1', timeout=8000)
                    except Exception as e:
                        print(f"[WARN] Details panel did not appear: {e}")
                        page.wait_for_timeout(2000)

                    # Robust selectors for name and review count
                    name_selectors = [
                        '//h1[contains(@class, "fontHeadlineLarge")]//span',
                        '//h1[contains(@class, "DUwDvf")]//span',
                        '//h1//span[@data-testid="place-header-title"]'
                    ]
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
                    if not business.reviews_count:
                        print("[DEBUG] Could not extract review count from any selector.")
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

                    # Print the scraped data for this listing to the console for verification
                    print("Scraped Business:")
                    print(asdict(business))

                    business_list.business_list.append(business)
                except Exception as e:
                    print(f'Error occured: {e}')
            
            #########
            # output
            #########
            # Create a clean filename without the output directory
            clean_search = search_for.strip().replace(' ', '_')
            filename = f"google_maps_data_{clean_search}"
            
            # Save to Excel only
            excel_path = business_list.save_to_excel(filename)
            print(f"Data saved to:\n- {excel_path}")

        browser.close()


if __name__ == "__main__":
    main()
