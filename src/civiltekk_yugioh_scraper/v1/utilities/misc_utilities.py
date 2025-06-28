import re
import requests
import logging
import time
import os
import platform
from typing import Dict, Generator, Mapping, Tuple, Optional
from ..config import JAPANESE_CHARS_REGEX, WINDOWS_EXPORT_PATH, LINUX_EXPORT_PATH, READ_TIMEOUT_ERROR, JSON_ERROR, BASE_TEKKX_PRODUCT_URL, BIGWEB_DEFAULT_HEADER, HEADERS


def check_for_jap_chars(x: str) -> bool:
    """
    Check if a string contains Japanese characters.

    Args:
        x (str): The string to check.

    Returns:
        bool: True if the string contains Japanese characters, False otherwise.
    """
    try:
        pattern = re.compile(JAPANESE_CHARS_REGEX, re.U)
        return bool(pattern.match(x))
    except Exception as e:
        logging.error(f"Error in checking for Japanese characters: {e}")
        return False


def list_files(directory: str) -> None:
    """
    Lists files in the given directory.

    Args:
        directory (str): The directory path.
    """
    try:
        for filename in os.listdir(directory):
            if os.path.isfile(os.path.join(directory, filename)):
                print(filename)
    except Exception as e:
        logging.error(f"Error listing files in directory {directory}: {e}")


def get_file_path(filename: str) -> str:
    """
    Get the full file path based on the operating system.

    Args:
        filename (str): The name of the file.

    Returns:
        str: The full file path.

    Raises:
        TypeError: If the filename is not a string.
        OSError: If the operating system is not supported.
    """
    if not isinstance(filename, str):
        raise TypeError("Filename must be a string")

    system_platform: str = platform.system()

    if system_platform == "Windows":
        export_path = os.path.join(WINDOWS_EXPORT_PATH, filename)
    elif system_platform == "Linux":
        export_path = os.path.join(LINUX_EXPORT_PATH, filename)
    else:
        raise OSError(f"Unsupported operating system: {system_platform}")

    return export_path


def run_request_until_response(url: str, params: dict, max_counter: int = 5) -> requests.Response | None:
    response = None
    counter = 0
    while not response and counter < max_counter:
        time.sleep(1)
        try:
            response = requests.get(url, timeout=10)
            print(url)
        except requests.exceptions.ReadTimeout as e:
            error_string = ""
            for key, value in params.items():
                error_string = error_string + \
                    "{key}:{value}".format(key=key, value=value) + "\n"
            # logging.exception("{error_string}".format(
            #     error_string=error_string))
            print(
                "ReadTimeoutError: {url} - Counter {counter}".format(url=url, counter=counter))
        finally:
            counter += 1

    if counter == max_counter:
        print("Exceeded trying {max_counter} times for {url}".format(
            url=url, max_counter=max_counter))

    return response


def run_yugipedia_request_until_response(url: str, params: Mapping[str, str | int] | Dict[str, str | str], headers=HEADERS, max_counter: int = 5) -> requests.Response:
    headers = headers or {}
    response = None
    counter = 0

    while not response and counter < max_counter:
        time.sleep(0.5)
        try:
            response = requests.get(
                url, params=params, timeout=50, headers=headers)
            return response
        except requests.exceptions.ReadTimeout:
            print(f"ReadTimeoutError: {url} - Counter {counter}")
        finally:
            counter += 1

    print(f"Exceeded trying {max_counter} times for {url}")

    # Create a dummy response with an error status
    fake_response = requests.Response()
    fake_response.status_code = 504  # Gateway Timeout
    fake_response._content = b"Request failed after retries"
    fake_response.url = url
    return fake_response


def run_wiki_request_until_response(url: str, header: dict, params: dict, max_counter: int = 5) -> Optional[dict]:
    """
    Runs a request to the MediaWiki API with retries if it times out or fails to decode JSON.

    Args:
        url (str): The URL to request.
        header (dict): The HTTP headers.
        params (dict): The request parameters.
        max_counter (int): Maximum number of retry attempts.

    Returns:
        Optional[dict]: The JSON response, or None if the request fails.
    """
    response_json: dict | None = None
    counter = 0
    while not response_json and counter < max_counter:
        time.sleep(1.0)
        try:
            logging.info(f"Running URL: {url}")
            print(url, params)
            response = requests.get(
                url, headers=header, params=params, timeout=50)
            if response.status_code == 200:
                response_json = response.json()
                break
        except requests.exceptions.ReadTimeout:
            logging.exception(
                f"{READ_TIMEOUT_ERROR} for {url} - Counter {counter}")
        except requests.exceptions.JSONDecodeError:
            logging.exception(f"{JSON_ERROR} for {url} - Counter {counter}")
        except Exception as e:
            logging.exception(f"Error in request: {e}")
        finally:
            counter += 1

    if counter == max_counter:
        logging.error(f"Exceeded trying {max_counter} times for {url}")

    return response_json


def process_redirections(inventory_list: list[dict], set_card_list: list[dict], set_list: list[dict], rarity_list: list[str], url: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """
    Process URL redirections based on rarity and card sets.

    Args:
        inventory_list (list[dict]): The inventory list.
        set_card_list (list[dict]): The set card list.
        set_list (list[dict]): The set list.
        rarity_list (list[str]): The rarity list.
        url (str): The URL to process.

    Returns:
        tuple: A tuple of (combined_url, set_card_name_found, set_card_code_found, yugipedia_rarity).
    """
    set_card_code_found, yugipedia_rarity, combined_url, set_card_found = None, None, None, None

    try:
        rarity_list_2_word_string = r"|".join([rarity_item.replace(" ", "-").replace("'", "").lower()
                                               for rarity_item in rarity_list if rarity_item not in ["Rare", "Parallel Rare"]])
        pattern2_string_before_compile = fr"(https:\/\/tekkx\.com\/product\/)(.{2, 4}-.{2, 5})-.+-({rarity_list_2_word_string})-?\d{{0,1}}?\/$"
        pattern2 = re.compile(pattern2_string_before_compile)
        pattern3 = re.compile(
            r"(https:\/\/tekkx\.com\/product\/)(.{2,4}-.{2,5})-(.+)-(rare)-?\d{0,1}?\/$")

        pattern2_matcher = pattern2.match(url)
        if pattern2_matcher:
            set_card_code, rarity = pattern2_matcher.group(
                2), pattern2_matcher.group(3)

        pattern3_matcher = pattern3.match(url)
        if not set_card_code and pattern3_matcher:
            set_card_code, rarity = pattern3_matcher.group(
                2), pattern3_matcher.group(3)

        if rarity:
            yugipedia_rarity = next(filter(lambda x: x.lower().replace(
                "'", "").replace(" ", "-") == rarity, rarity_list), None)

        if set_card_code and yugipedia_rarity:
            set_card = next(iter(filter(lambda set_card: set_card['set_card_code_updated'] and set_card_code ==
                                        set_card['set_card_code_updated'].lower() and set_card['rarity_name'] == yugipedia_rarity, set_card_list)), None)
            set_card_code_found = set_card['set_card_code_updated'] if set_card else ""

        if set_card_code_found and yugipedia_rarity:
            set_card_found = next(iter(filter(
                lambda set_card: set_card['rarity_name'] == yugipedia_rarity and set_card['set_card_code_updated'] == set_card_code_found, set_card_list)), None)

        set_card_name_found = set_card_found['set_card_name_combined'] if set_card_found else ""

        if set_card_found and yugipedia_rarity and set_card_code_found:
            inventory = next(iter(filter(lambda inventory: inventory['set_card_code_updated'] ==
                                         set_card_code_found and inventory['rarity_name'] == yugipedia_rarity, inventory_list)), None)
            if inventory:
                combined_url = BASE_TEKKX_PRODUCT_URL.format(
                    slug=inventory['post_name'])

    except Exception as e:
        logging.error(f"Error processing redirections for URL {url}: {e}")

    return combined_url, set_card_name_found, set_card_code_found, yugipedia_rarity


def split(list_a: list, chunk_size: int) -> Generator[list, None, None]:
    """
    Splits a list into chunks of the specified size.

    Args:
        list_a (List): The list to be split.
        chunk_size (int): The size of each chunk.

    Yields:
        List: Chunks of the original list.
    """
    try:
        for i in range(0, len(list_a), chunk_size):
            yield list_a[i:i + chunk_size]
    except Exception as e:
        logging.error(f"Error splitting list: {e}")


def log_error(params: dict, error_string: str, url: str, counter: int) -> None:
    """
    Logs an error message with request details.

    Args:
        params (dict): The request parameters.
        error_string (str): The error message.
        url (str): The URL of the request.
        counter (int): The retry counter.
    """
    try:
        error_details = "\n".join(
            [f"{key}: {value}" for key, value in params.items()])
        logging.error(
            f"{error_string}: {url} - Counter {counter}\n{error_details}")
    except Exception as e:
        logging.error(f"Error logging request details: {e}")
