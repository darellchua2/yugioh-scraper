import concurrent
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
import requests
import bs4 as bs
from bs4 import BeautifulSoup, Tag
import pandas as pd
import datetime
from dotenv import load_dotenv
import re
import logging
import time

from ..utilities.aws_utilities import upload_data

try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.chrome.options import Options
    from selenium.common.exceptions import TimeoutException, NoSuchElementException
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False
    logging.warning(
        "Selenium not available. Install with: pip install selenium")


def get_rarity_mapping_dict() -> dict:
    """
    Returns a dictionary mapping Yuyutei rarity codes to standardized rarity names.

    Returns:
        dict: Dictionary with rarity codes as keys and full rarity names as values
    """
    rarity_dict = {
        '10000SE': '10000 Secret Rare',
        '20thSE': '20th Secret Rare',
        'CR': "Collector's Rare",
        'N': 'Common',
        'P-EXSE': 'Extra Secret Parallel Rare',
        'EXSE': 'Extra Secret Rare',
        'GR': 'Gold Rare',
        'GSE': 'Gold Secret Rare',
        'P-HR': 'Holographic Parallel Rare',
        'HR': 'Holographic Rare',
        'KC-N': 'Kaiba Corporation Common',
        'KC-R': 'Kaiba Corporation Rare',
        'KC-UR': 'Kaiba Corporation Ultra Rare',
        'M-GR': 'Millennium Gold Rare',
        'M': 'Millennium Rare',
        'M-SE': 'Millennium Secret Rare',
        'M-SR': 'Millennium Super Rare',
        'M-UR': 'Millennium Ultra Rare',
        'P-N': 'Normal Parallel Rare',
        'NR': 'Normal Rare',
        'PG': 'Premium Gold Rare',
        'PSE': 'Prismatic Secret Rare',
        'R': 'Rare',
        'P-SE': 'Secret Parallel Rare',
        'SE': 'Secret Rare',
        'P-SR': 'Super Parallel Rare',
        'SR': 'Super Rare',
        'UL': 'Ultimate Rare',
        'P-UR': 'Ultra Parallel Rare',
        'UR': 'Ultra Rare',
        'QCSE': 'Quarter Century Secret Rare',
        'SP': 'Super Rare',
        'ｼｰｸﾚｯﾄ': 'Secret Rare',
        'NP': 'Normal Parallel Rare',
        "SPECIAL RED": "Secret Rare Special Red Version"
    }
    return rarity_dict


def update_rarity_for_308():
    """
    Returns additional rarity mapping for specific card sets (308).

    Returns:
        dict: Dictionary with additional rarity code mappings
    """
    rarity_dict = {
        'PR': 'Ultra Parallel Rare'
    }

    return rarity_dict


def get_card_price2(x) -> str | float | None:
    """
    Extracts and converts price information from Japanese text.

    Args:
        x (str): Price text containing Japanese yen symbol and amount

    Returns:
        str | float | None: Extracted price as float, or original string if conversion fails, or None if no match
    """
    pattern = re.compile(r'(.+) 円')  # \u5186 refers to 円
    match = pattern.search(x)
    if match:
        try:
            return float(match.group(1).replace(",", ""))
        except:
            return match.group(1)

    return None


def get_set_code(text_string: str):
    """
    Extracts set code from text enclosed in square brackets.

    Args:
        text_string (str): Text containing set code in format [CODE]

    Returns:
        str | None: Extracted set code or None if no match found
    """
    pattern = re.compile(r'\[(.+)\]')
    pattern_search = pattern.search(text_string)
    if pattern_search:
        return pattern_search.group(1)

    return None


def get_card_set_codes_from_card_set(obj: dict) -> list[dict]:
    """
    Scrapes individual card data from a specific card set page on Yuyutei.

    Args:
        obj (dict): Dictionary containing 'url' and 'set_code' for the card set

    Returns:
        list[dict]: List of dictionaries containing card data with keys:
                   - card_rarity: Rarity code
                   - set_code: Set code
                   - Price: Card price
                   - card_set_card_code: Individual card code
                   - url: Source URL
    """
    list_needed: list = []
    try:
        url: str = obj['url']
        logging.info('url: %s', url)
        set_code = obj['set_code']
        response = requests.get(url, timeout=10)
        response.encoding = 'utf-8'
        source = response.text
        soup = bs.BeautifulSoup(source, 'html.parser')

        rarity_divs = soup.find_all(id=re.compile("^(card-list).?"))

        for rarity_div in rarity_divs:
            rarity_span = rarity_div.find('span')
            rarity_code = None

            # search for rarity code
            if rarity_span:
                rarity_code = rarity_span.text

            card_divs = rarity_div.find_all(
                'div', "col-md")
            for card_div in card_divs:
                card_obj = {'card_rarity': rarity_code, 'set_code': set_code}
                set_card_code = ""
                jap_price = None
                set_card_name_jap = None

                set_card_code_span = card_div.find('span')
                if set_card_code_span:
                    set_card_code = set_card_code_span.text

                jap_price_strong = card_div.find('strong')
                jap_price_strong_text = jap_price_strong.text
                if jap_price_strong:
                    jap_price = get_card_price2(jap_price_strong_text)

                set_card_name_jap_h4 = card_div.find('h4')
                set_card_name_jap_h4_text = set_card_name_jap_h4.text

                if "（イラス" in set_card_name_jap_h4_text or "（イラ" in set_card_name_jap_h4_text or "(新規" in set_card_name_jap_h4_text or "(海外" in set_card_name_jap_h4_text:
                    set_card_code = set_card_code + "b"

                # ADD FOR QCAC Check
                if "(SPEC" in set_card_name_jap_h4_text and set_code == "QCAC":
                    rarity_code = "SPECIAL RED"

                card_obj['Price'] = jap_price
                card_obj['card_set_card_code'] = set_card_code
                card_obj['url'] = obj['url']
                if rarity_code is not None:  # to make sure that rarity code is not going to throw error
                    list_needed.append(card_obj.copy())

    except requests.ConnectionError as e:
        logging.error(f"ConnectionError: {e}")
    except Exception as e:
        logging.error(f"Exception: {e}")
    return list_needed


def get_set_list_selenium(url: str) -> list[dict]:
    """
    Uses Selenium to extract card set information from Yuyutei website with dynamic content.

    This function uses a headless Chrome browser to load the page and wait for JavaScript
    to populate the sidebar with set links, then extracts all /sell/ygo/s/ pattern links.

    Args:
        url (str): URL of the Yuyutei search page

    Returns:
        list[dict]: List of unique dictionaries containing:
                   - url: Full URL to the set page
                   - set_code: Extracted set code
    """
    if not SELENIUM_AVAILABLE:
        logging.error(
            "Selenium not available. Falling back to regular method.")
        return get_set_list_v2(url)

    dict_list = []
    driver = None

    try:
        # Setup Chrome options for headless browsing (Docker-compatible)
        chrome_options = Options()
        chrome_options.add_argument("--headless=new")  # Use new headless mode
        chrome_options.add_argument("--no-sandbox")  # Required for Docker
        chrome_options.add_argument(
            "--disable-dev-shm-usage")  # Required for Docker
        chrome_options.add_argument("--disable-gpu")  # Required for Docker
        chrome_options.add_argument("--disable-software-rasterizer")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-plugins")
        chrome_options.add_argument("--disable-images")  # Speed up loading
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument(
            "--remote-debugging-port=9222")  # For debugging
        chrome_options.add_argument(
            "--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")

        # Additional Docker-specific options
        chrome_options.add_argument("--disable-background-timer-throttling")
        chrome_options.add_argument("--disable-backgrounding-occluded-windows")
        chrome_options.add_argument("--disable-renderer-backgrounding")
        chrome_options.add_argument("--disable-features=TranslateUI")
        chrome_options.add_argument("--disable-ipc-flooding-protection")

        # Initialize the driver
        driver = webdriver.Chrome(options=chrome_options)
        logging.info(f"Loading page with Selenium: {url}")

        # Load the page
        driver.get(url)

        # Wait for page to load
        time.sleep(3)

        # Look for the シングルカード販売 section
        logging.info("Looking for シングルカード販売 section...")

        try:
            # Try to find and click the シングルカード販売 button/link to expand it
            single_card_elements = driver.find_elements(
                By.XPATH, "//*[contains(text(), 'シングルカード販売')]")
            logging.info(
                f"Found {len(single_card_elements)} elements containing 'シングルカード販売'")

            # Try to click the main シングルカード販売 element to expand it
            for element in single_card_elements:
                try:
                    if element.is_displayed() and element.is_enabled():
                        logging.info(
                            f"Clicking on シングルカード販売 element: {element.tag_name}")
                        driver.execute_script("arguments[0].click();", element)
                        time.sleep(2)
                        break
                except Exception as e:
                    logging.debug(f"Could not click element: {e}")
                    continue

            # Look for category elements and try to expand them
            categories = ["最新弾", "基本ブースターパック", "その他ブースターパック",
                          "構築済みデッキ", "デュエルターミナル", "限定パック", "プロモーションカード"]

            for category in categories:
                try:
                    logging.info(f"Looking for category: {category}")
                    category_elements = driver.find_elements(
                        By.XPATH, f"//*[contains(text(), '{category}')]")

                    for cat_element in category_elements:
                        if cat_element.is_displayed():
                            logging.info(f"Clicking on category: {category}")
                            driver.execute_script(
                                "arguments[0].click();", cat_element)
                            time.sleep(1)
                            break

                except Exception as e:
                    logging.debug(
                        f"Could not interact with category {category}: {e}")
                    continue

            # Wait a bit more for any dynamic content to load
            time.sleep(3)

            # Now extract all links with /sell/ygo/s/ pattern
            logging.info("Extracting all /sell/ygo/s/ links...")
            sell_links = driver.find_elements(
                By.XPATH, "//a[contains(@href, '/sell/ygo/s/')]")
            logging.info(f"Found {len(sell_links)} sell links with Selenium")

            for i, link in enumerate(sell_links):
                try:
                    href = link.get_attribute('href')
                    text = link.text.strip()
                    link_id = link.get_attribute('id') or ''

                    if href and '/sell/ygo/s/' in href and '/sell/ygo/s/search' not in href:
                        # Extract set code from URL
                        set_code_match = re.search(
                            r'/sell/ygo/s/([a-zA-Z0-9]+)', href)
                        if set_code_match:
                            set_code = set_code_match.group(1).upper()

                            # Remove hash fragments
                            clean_url = href.split('#')[0]

                            obj = {
                                'url': clean_url,
                                'set_code': set_code
                            }

                            # Check for duplicates
                            if not any(item['set_code'] == set_code for item in dict_list):
                                dict_list.append(obj.copy())
                                logging.info(
                                    f"Selenium found: {set_code} - {clean_url}")

                except Exception as e:
                    logging.debug(f"Error processing link {i}: {e}")
                    continue

            # Also look for any toggle-buy links that we might need to convert
            buy_links = driver.find_elements(
                By.XPATH, "//a[contains(@href, '/buy/ygo/s/') and contains(@id, 'toggle-buy-ygo-s-')]")
            logging.info(
                f"Found {len(buy_links)} buy links (will skip as requested)")

            for link in buy_links:
                try:
                    href = link.get_attribute('href')
                    link_id = link.get_attribute('id') or ''
                    text = link.text.strip()
                    logging.info(
                        f"Skipping buy link: id='{link_id}' href='{href}' text='{text[:50]}...'")
                except:
                    pass

        except TimeoutException:
            logging.warning("Timeout waiting for page elements to load")
        except Exception as e:
            logging.error(f"Error during Selenium interaction: {e}")

    except Exception as e:
        logging.error(f"Error setting up Selenium: {e}")

    finally:
        if driver:
            try:
                driver.quit()
            except:
                pass

    # Add manually specified URLs
    dict_list = add_additional_url(dict_list)

    # Remove duplicates
    seen_keys = set()
    unique_data = []
    for item in dict_list:
        if item['url'] not in seen_keys:
            seen_keys.add(item['url'])
            unique_data.append(item)

    logging.info(f"Selenium method found {len(unique_data)} unique sets")
    return unique_data


def get_set_list_v2(url: str) -> list[dict]:
    """
    Extracts card set information from Yuyutei website by finding the シングルカード販売 section.

    This function searches for the "シングルカード販売" (Single Card Sales) section on the page
    and extracts all nested links that match the '/ygo/s/' pattern. Falls back to alternative
    methods if the primary section is not found.

    Args:
        url (str): URL of the Yuyutei search page

    Returns:
        list[dict]: List of unique dictionaries containing:
                   - url: Full URL to the set page
                   - set_code: Extracted set code
    """
    dict_list = []
    try:
        response = requests.get(url, timeout=10)
        response.encoding = 'utf-8'
        source = response.text
    except requests.RequestException as e:
        logging.info(f"Error fetching URL: {e}")
        return []

    soup = BeautifulSoup(source, 'html.parser')

    # Look for the section that starts with "シングルカード販売"
    single_card_section = None

    # Find all text nodes that contain "シングルカード販売"
    text_elements = soup.find_all(string=re.compile(r'シングルカード販売'))
    logging.info(
        f"Found {len(text_elements)} text elements containing 'シングルカード販売'")

    for i, element in enumerate(text_elements):
        logging.info(f"Text element {i+1}: '{element.strip()}'")
        # Get the parent element that contains this text
        parent = element.parent
        if parent:
            logging.info(
                f"  Parent element: <{parent.name}> with class: {parent.get('class', 'None')}")
            # Find the container element (could be a div, section, etc.)
            container = parent
            while container and container.name not in ['div', 'section', 'aside', 'nav']:
                container = container.parent

            if container:
                logging.info(
                    f"  Found container: <{container.name}> with class: {container.get('class', 'None')} and id: {container.get('id', 'None')}")
                single_card_section = container
                break

    if single_card_section:
        logging.info("Found シングルカード販売 section")

        # Categories to look for under シングルカード販売
        categories = [
            "最新弾",
            "基本ブースターパック",
            "その他ブースターパック",
            "構築済みデッキ",
            "デュエルターミナル",
            "限定パック",
            "プロモーションカード"
        ]

        logging.info(f"Looking for categories: {categories}")

        # Search for each category and its associated links
        for category in categories:
            logging.info(f"\n--- Searching for category: {category} ---")

            # Find text elements that contain this category name
            category_elements = single_card_section.find_all(
                string=re.compile(category))
            logging.info(
                f"Found {len(category_elements)} text elements containing '{category}'")

            for cat_element in category_elements:
                logging.info(f"Category element text: '{cat_element.strip()}'")

                # Get the parent container of this category
                category_parent = cat_element.parent
                if category_parent:
                    logging.info(
                        f"Category parent: <{category_parent.name}> with class: {category_parent.get('class', 'None')}")

                    # Look for container that might hold the category and its links
                    category_container = category_parent
                    for _ in range(5):  # Search up to 5 levels up
                        if category_container and category_container.parent:
                            category_container = category_container.parent

                            # Look for links within this container level
                            container_links = category_container.find_all(
                                ['a', '*'], attrs={'onclick': True})
                            if container_links:
                                logging.info(
                                    f"Found {len(container_links)} clickable elements at container level <{category_container.name}>")

                                for link in container_links:
                                    onclick_value = link.get('onclick', '')
                                    link_text = link.text.strip()

                                    # Look for location.href patterns in onclick
                                    location_href_match = re.search(
                                        r"location\.href\s*=\s*['\"]([^'\"]+)['\"]", onclick_value)
                                    if location_href_match:
                                        url = location_href_match.group(1)
                                        logging.info(
                                            f"  ✓ Found location.href URL in {category}: {url}")

                                        # Check if URL matches the /ygo/s/ pattern
                                        if '/ygo/s/' in url and '/ygo/s/search' not in url:
                                            set_code_match = re.search(
                                                r'/ygo/s/([a-zA-Z0-9]+)', url)
                                            if set_code_match:
                                                set_code = set_code_match.group(
                                                    1).upper()
                                                logging.info(
                                                    f"  ✓ Extracted set code from {category}: {set_code}")

                                                # Create full URL
                                                if url.startswith('/'):
                                                    full_url = 'https://yuyu-tei.jp' + url
                                                elif not url.startswith('http'):
                                                    full_url = 'https://yuyu-tei.jp' + url
                                                else:
                                                    full_url = url

                                                # Remove hash fragments for consistency
                                                full_url = full_url.split('#')[
                                                    0]

                                                obj = {
                                                    'url': full_url,
                                                    'set_code': set_code
                                                }

                                                # Check for duplicates
                                                if not any(item['set_code'] == set_code for item in dict_list):
                                                    dict_list.append(
                                                        obj.copy())
                                                    logging.info(
                                                        f"  ✓ Added to dict_list from {category}: {obj}")
                                                else:
                                                    logging.info(
                                                        f"  ◦ Duplicate set_code {set_code}, skipping")
                                break  # Found links at this level, no need to go deeper
                        else:
                            break

        # Search for sell links only (skip buy links)
        logging.info("\n--- Searching for sell links (skipping buy links) ---")

        # Look for direct sell links first
        sell_links = single_card_section.find_all(
            'a', href=re.compile(r'/sell/ygo/s/'))
        logging.info(f"Found {len(sell_links)} direct sell links")

        for i, link in enumerate(sell_links):
            link_id = link.get('id', '')
            href = link.get('href', '')
            text = link.text.strip()
            logging.info(
                f"Sell link {i+1}: id='{link_id}', href='{href}', text='{text}'")

            # Extract set code from href
            set_code_match = re.search(r'/sell/ygo/s/([a-zA-Z0-9]+)', href)
            if set_code_match:
                set_code = set_code_match.group(1).upper()
                logging.info(
                    f"  ✓ Extracted set code from sell URL: {set_code}")

                # Create full URL if needed
                if href.startswith('/'):
                    full_url = 'https://yuyu-tei.jp' + href
                elif not href.startswith('http'):
                    full_url = 'https://yuyu-tei.jp' + href
                else:
                    full_url = href

                # Remove hash fragments for consistency
                full_url = full_url.split('#')[0]

                obj = {
                    'url': full_url,
                    'set_code': set_code
                }

                # Check for duplicates
                if not any(item['set_code'] == set_code for item in dict_list):
                    dict_list.append(obj.copy())
                    logging.info(
                        f"  ✓ Added to dict_list from sell link: {obj}")
                else:
                    logging.info(
                        f"  ◦ Duplicate set_code {set_code}, skipping")

        # Search for toggle-buy-ygo-s- pattern links but skip them (just log for info)
        logging.info(
            "\n--- Found toggle-buy-ygo-s- pattern links (skipping as requested) ---")
        toggle_links = single_card_section.find_all(
            'a', id=re.compile(r'^toggle-buy-ygo-s-'))
        logging.info(
            f"Found {len(toggle_links)} toggle-buy-ygo-s- pattern links (will skip these)")

        for i, link in enumerate(toggle_links):
            link_id = link.get('id', '')
            href = link.get('href', '')
            text = link.text.strip()
            logging.info(
                f"Skipping buy link {i+1}: id='{link_id}', href='{href}', text='{text}'")
            logging.info(f"  ✗ Skipping because it's a buy link")

        # Additional fallback: Search for any /sell/ygo/s/ patterns only (skip buy links)
        logging.info(
            "\n--- Final fallback: Searching for /sell/ygo/s/ patterns only ---")
        all_sell_elements = single_card_section.find_all(['a', '*'],
                                                         href=lambda x: x and '/sell/ygo/s/' in x
                                                         )
        logging.info(
            f"Found {len(all_sell_elements)} elements with /sell/ygo/s/ patterns")

        for element in all_sell_elements:
            href = element.get('href', '')
            onclick = element.get('onclick', '')
            element_id = element.get('id', '')
            text = element.text.strip()

            # Process href (only sell links)
            if href and '/sell/ygo/s/' in href and '/ygo/s/search' not in href:
                set_code_match = re.search(r'/sell/ygo/s/([a-zA-Z0-9]+)', href)
                if set_code_match:
                    set_code = set_code_match.group(1).upper()
                    if not any(item['set_code'] == set_code for item in dict_list):
                        if href.startswith('/'):
                            full_url = 'https://yuyu-tei.jp' + href
                        else:
                            full_url = href

                        full_url = full_url.split('#')[0]
                        obj = {'url': full_url, 'set_code': set_code}
                        dict_list.append(obj.copy())
                        logging.info(
                            f"  ✓ Added from final fallback (href): {obj}")

            # Process onclick (only sell links)
            if onclick and '/sell/ygo/s/' in onclick:
                location_href_match = re.search(
                    r"location\.href\s*=\s*['\"]([^'\"]+)['\"]", onclick)
                if location_href_match:
                    url = location_href_match.group(1)
                    if '/sell/ygo/s/' in url and '/ygo/s/search' not in url:
                        set_code_match = re.search(
                            r'/sell/ygo/s/([a-zA-Z0-9]+)', url)
                        if set_code_match:
                            set_code = set_code_match.group(1).upper()
                            if not any(item['set_code'] == set_code for item in dict_list):
                                if url.startswith('/'):
                                    full_url = 'https://yuyu-tei.jp' + url
                                else:
                                    full_url = url
                                full_url = full_url.split('#')[0]
                                obj = {'url': full_url, 'set_code': set_code}
                                dict_list.append(obj.copy())
                                logging.info(
                                    f"  ✓ Added from final fallback (onclick): {obj}")

        # Log any buy links found for information (but don't process them)
        buy_elements = single_card_section.find_all('a',
                                                    href=lambda x: x and '/buy/ygo/s/' in x
                                                    )
        if buy_elements:
            logging.info(
                f"\n--- Found {len(buy_elements)} buy links (skipped as requested) ---")
            for i, element in enumerate(buy_elements[:5]):  # Show first 5 only
                href = element.get('href', '')
                onclick = element.get('onclick', '')
                logging.info(
                    f"Skipped buy element {i+1}: href='{href}', onclick='{onclick[:50]}...'")
        else:
            logging.info("\n--- No buy links found ---")

        logging.info(
            f"Total items added from シングルカード販売 section: {len(dict_list)}")
    else:
        # Fallback: Original method - check for the old div structure
        logging.warning(
            "シングルカード販売 section not found, falling back to original method")
        div: Tag | None = soup.find('div', id='side-sell-single')
        if div and isinstance(div, Tag):
            logging.info("Found original sidebar structure")
            inputs = div.find_all(
                "a", attrs={'id': re.compile(r'^(side-sell-ygo-s-).?')})

            for a in inputs:
                obj = {}
                a_text = a.text.strip() if a.text else None
                a_href = a.get('href', None)
                if a_href:
                    obj['url'] = a_href
                if a_text:
                    obj['set_code'] = get_set_code(a_text)
                dict_list.append(obj.copy())
        else:
            # Alternative method - scan all links
            logging.warning(
                "Original sidebar not found, using alternative method")
            all_links = soup.find_all('a', href=True)

            for link in all_links:
                href = link.get('href', '')
                text = link.text.strip()

                # Check if link matches the pattern for set pages
                if '/ygo/s/' in href and href != '/ygo/s/search':
                    # Extract set code from URL (e.g., '/ygo/s/wpp6' -> 'WPP6')
                    set_code_match = re.search(r'/ygo/s/([a-zA-Z0-9]+)', href)
                    if set_code_match:
                        set_code = set_code_match.group(1).upper()

                        # Create full URL if it's relative
                        if href.startswith('/'):
                            full_url = 'https://yuyu-tei.jp' + href
                        else:
                            full_url = href

                        obj = {
                            'url': full_url,
                            'set_code': set_code
                        }
                        dict_list.append(obj.copy())

                    # Look for set codes in brackets like [WPP6], [SD48], etc.
                    set_code_match = re.search(r'\[([A-Z0-9]+)\]', text)
                    if set_code_match:
                        set_code = set_code_match.group(1)

                        # Create full URL if it's relative
                        if href.startswith('/'):
                            full_url = 'https://yuyu-tei.jp' + href
                        else:
                            full_url = href

                        obj = {
                            'url': full_url,
                            'set_code': set_code
                        }
                        dict_list.append(obj.copy())

    dict_list = add_additional_url(dict_list)

    # Remove duplicates while preserving the first occurrence
    seen_keys = set()
    unique_data = []
    for item in dict_list:
        if item['url'] not in seen_keys:
            seen_keys.add(item['url'])
            unique_data.append(item)

    return unique_data


def add_additional_url(dict_list: list[dict]):
    """
    Adds manually specified URLs for card sets that may not be found through scraping.

    Args:
        dict_list (list[dict]): Existing list of set dictionaries

    Returns:
        list[dict]: Updated list with additional QCCU and QCCP set entries
    """
    dict_list.append({'url': 'https://yuyu-tei.jp/sell/ygo/s/search?search_word=qccu&page=2',
                      'set_code': 'QCCU'})
    dict_list.append({'url': 'https://yuyu-tei.jp/sell/ygo/s/search?search_word=qccp&page=2',
                      'set_code': 'QCCP'})
    return dict_list


def replace_dt_rarity_name(input_df: pd.DataFrame):
    """
    Updates rarity names for Duel Terminal (DT) cards to include proper parallel rare designations.

    Args:
        input_df (pd.DataFrame): DataFrame containing card data with 'card_set_card_code' and 'mapped_rarity' columns

    Returns:
        pd.DataFrame: Updated DataFrame with corrected DT rarity names
    """
    # Define your regex condition
    regex_condition = "^DT.*"

    # Use the .str.match method to filter rows based on the regex condition
    filtered_rows = input_df[input_df['card_set_card_code'].str.match(
        regex_condition)]

    # Define your dictionary for replacing values in 'mapped_rarity' column
    mapping_dict = {
        "Rare": "Duel Terminal Rare Parallel Rare",
        "Common": "Duel Terminal Normal Parallel Rare",
        "Ultra Rare": 'Duel Terminal Ultra Parallel Rare',
        "Super Rare": 'Duel Terminal Super Parallel Rare',
        "Secret Rare": 'Duel Terminal Secret Parallel Rare',
        "Normal Rare": 'Duel Terminal Normal Rare Parallel Rare'
    }

    # Replace values in 'mapped_rarity' based on the dictionary
    filtered_rows['mapped_rarity'] = filtered_rows['mapped_rarity'].map(
        mapping_dict)

    # Update the original DataFrame with the modified values
    input_df.loc[filtered_rows.index,
                 'mapped_rarity'] = filtered_rows['mapped_rarity']

    return input_df


def yuyutei_scrape(dev_type=None):
    """
    Main function to scrape card pricing data from Yuyutei website and upload to database.

    This function orchestrates the entire scraping process:
    1. Fetches all card sets from the main page
    2. Scrapes individual card data from each set using concurrent threads
    3. Processes and normalizes the data
    4. Uploads results to AWS S3 and database

    Args:
        dev_type (optional): Development mode parameter (currently unused)

    Returns:
        None: Function performs side effects (data upload) and logs execution time
    """
    load_dotenv()
    start = datetime.datetime.now()

    logging.info(start.strftime("%Y-%m-%d %H:%M:%S"))

    url2 = "https://yuyu-tei.jp/sell/ygo/s/search"

    # Try Selenium method first for dynamic content, fallback to regular method
    if SELENIUM_AVAILABLE:
        logging.info(
            "Using Selenium method to extract set links from dynamic content")
        card_set_obj_list: list[dict] = get_set_list_selenium(url2)
    else:
        logging.warning(
            "Selenium not available, using regular method (may find fewer sets)")
        card_set_obj_list: list[dict] = get_set_list_v2(url2)
    final_list: list[dict] = []

    with ThreadPoolExecutor(4) as executor:
        futures: list = []

        for obj in card_set_obj_list:
            futures.append(executor.submit(
                get_card_set_codes_from_card_set, obj))

        for future in concurrent.futures.as_completed(futures):
            try:
                future_list = future.result()
                final_list.extend(future_list)
            except Exception as e:
                logging.error(e)
                continue

    # Check if final_list is empty
    if not final_list:
        logging.warning("No data found. Exiting the function.")
        return  # Exit the function early

    df = pd.DataFrame(final_list)
    logging.info(f"DataFrame shape: {df.shape}")
    logging.info(f"DataFrame columns: {df.columns}")

    rarity_dict = get_rarity_mapping_dict()
    df["mapped_rarity"] = df["card_rarity"].map(rarity_dict)
    df['date'] = datetime.datetime.now()

    # Drop card_carity column
    df = df.drop(columns=['card_rarity'])

    # Drop rows with missing values in key columns
    df = df.dropna(subset=['mapped_rarity', 'card_set_card_code'])

    df = replace_dt_rarity_name(df)

    buffer = BytesIO()
    df.to_csv(buffer, index=False)

    df = df[['Price', 'card_set_card_code', 'mapped_rarity', 'date']]
    upload_data(df, 'yuyutei', 'append', 'yugioh_data')
    upload_data(df, 'yuyutei_latest', 'replace', 'yugioh_data')

    end = datetime.datetime.now()
    logging.info(f"Start time: {start.strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info(f"End time: {end.strftime('%Y-%m-%d %H:%M:%S')}")
    difference = end - start
    logging.info(f"The time difference between the 2 times is: {difference}")


if __name__ == "__main__":
    yuyutei_scrape()
