import os
from dotenv import load_dotenv

load_dotenv()


MEDIAWIKI_URL = "https://yugipedia.com/api.php"
SEMANTIC_URL = "https://yugipedia.com/index.php"
BASE_TEKKX_PRODUCT_URL = "https://tekkx.com/product/{slug}/"

DEFAULT_CARD_QUANTITY_INTERVAL = 250
BUCKET_NAME = 'yugioh-storage'


JAPANESE_CHARS_REGEX = r'[\u3000-\u303f\u3040-\u309f\u30a0-\u30ff\uff00-\uff9f\u4e00-\u9faf\u3400-\u4dbf]+ (?=[A-Za-z ]+–)'
WINDOWS_EXPORT_PATH: str = r'\\192.168.50.227\personal'
LINUX_EXPORT_PATH: str = r'/home/silentx'
READ_TIMEOUT_ERROR = "ReadTimeoutError"
JSON_ERROR = "JSONDecodeError"

BIGWEB_DEFAULT_HEADER = {
    "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    "content-type": "application/json",
    "sec-ch-ua": '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"'
}

HEADERS: dict[str, str] = {
    'authority': 'yugipedia.com',
    'User-Agent': 'CardBot/1.0 - (https://civiltekk.com;darellchua2@gmail.com)',
    'From': 'darellchua3@gmail.com'  # This is another valid field
}

TABLE_YUGIOH_CARDS = 'yugioh_cards2'
TABLE_YUGIOH_SETS = 'yugioh_sets3'
TABLE_YUGIOH_RARITIES = 'yugioh_rarities3'
TABLE_YUGIOH_OVERALL_CARD_CODE_LISTS = 'overall_card_code_list2'

RDS_HOST = os.getenv('RDS_HOST')
NAME = os.getenv('USER')
PASSWORD = os.getenv('PASSWORD')
TEKKX_SCALABLE_DB_NAME = os.getenv('DB_NAME', "tekkx_scalable")
YUGIOH_DB = os.getenv("YUGIOH_DB", "yugioh_data")
DB_PORT = os.getenv("DB_PORT", "3307")

RARITY_CATEGORIES_TO_SKIP = ["Variant card",
                             "Unlimited Edition",
                             "Rarity (grade)",
                             "Rarity",
                             "BAM Legend"]
